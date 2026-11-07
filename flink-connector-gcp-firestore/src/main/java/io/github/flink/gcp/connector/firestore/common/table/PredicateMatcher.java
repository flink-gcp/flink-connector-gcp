/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.firestore.common.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Finds the comparisons in the predicates of a Native-mode or Datastore-mode scan, which each mode
 * then translates into filters of its own query ({@code docs/adr/0197}, {@code docs/adr/0184}).
 *
 * <p>A comparison is {@code reference op literal} either way round, for {@code =}, {@code <>},
 * {@code <}, {@code <=}, {@code >} and {@code >=}; a bare {@code BOOLEAN} reference (equal to
 * {@code TRUE}) or its {@code NOT} (equal to {@code FALSE}); or {@code reference IS NOT NULL}. The
 * reference is a physical column or a field nested in {@code ROW} columns. Under {@code AND}, every
 * child is searched, so a predicate yields the comparisons of all its conjuncts. An {@code OR}
 * whose every disjunct is an equality on one and the same reference, which is how the planner
 * presents {@code IN}, yields one disjunction of those equalities. A comparison with a {@code NULL}
 * literal yields nothing.
 *
 * <p>Two limits of both modes' queries apply across the predicates: only the first {@code <>} or
 * {@code IS NOT NULL} translates, so a query never carries two not-equal filters, and a comparison
 * or disjunction translates only while the product of the filters' widths ({@link
 * Translator#width(Object)}: the disjuncts each expands to) stays within {@value #MAX_DISJUNCTS},
 * the most disjuncts a query's filter may expand to.
 *
 * <p>A nested path is resolved only where no {@code NOT NULL} field lies below a nullable {@code
 * ROW}: Flink takes such a field to be never {@code NULL}, although it is {@code NULL} under a
 * {@code NULL} parent, so its comparisons can evaluate where the stored document has no value.
 *
 * <p>The modes share this search, {@link Comparison#timestampBounds(int)}, {@link
 * Comparison#hasScalarType()} and {@link OrderedStrings}: what a reference may name, and the form a
 * literal takes, belong to each mode's {@link Translator}. Every predicate stays a residual Flink
 * evaluates, so a translation must only never exclude a row Flink keeps.
 */
@Internal
public final class PredicateMatcher {

    /** The most disjuncts the filters of one query may expand to, in either mode. */
    static final int MAX_DISJUNCTS = 30;

    /** The planner's comparison functions, by the operator of {@code reference op literal}. */
    private static final Map<FunctionDefinition, Operator> OPERATORS =
            Map.of(
                    BuiltInFunctionDefinitions.EQUALS,
                    Operator.EQUAL,
                    BuiltInFunctionDefinitions.NOT_EQUALS,
                    Operator.NOT_EQUAL,
                    BuiltInFunctionDefinitions.LESS_THAN,
                    Operator.LESS_THAN,
                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                    Operator.LESS_THAN_OR_EQUAL,
                    BuiltInFunctionDefinitions.GREATER_THAN,
                    Operator.GREATER_THAN,
                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL);

    private PredicateMatcher() {}

    /**
     * Translates the planner's predicates.
     *
     * @param rowType the physical row type
     * @param filters the predicates, each one conjunct of the scan's condition
     * @param translator the mode's translation of one comparison
     * @param <T> the mode's filter type
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    public static <T> PushedFilters<T> translate(
            RowType rowType, List<ResolvedExpression> filters, Translator<T> translator) {
        Search<T> search = new Search<>(rowType, translator);
        List<ResolvedExpression> accepted = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            int before = search.out.size();
            search.collect(filter);
            if (search.out.size() > before) {
                accepted.add(filter);
            }
        }
        return new PushedFilters<>(accepted, filters, search.out);
    }

    /** The search's state across the predicates: the filters so far and the query's limits. */
    private static final class Search<T> {
        private final RowType rowType;
        private final Translator<T> translator;
        private final List<T> out = new ArrayList<>();
        private boolean notEqualTaken;
        private int disjuncts = 1;

        private Search(RowType rowType, Translator<T> translator) {
            this.rowType = rowType;
            this.translator = translator;
        }

        private void collect(ResolvedExpression expression) {
            if (isCall(expression, BuiltInFunctionDefinitions.AND)) {
                for (ResolvedExpression child : expression.getResolvedChildren()) {
                    collect(child);
                }
                return;
            }
            if (isCall(expression, BuiltInFunctionDefinitions.OR)) {
                disjunction(expression);
                return;
            }
            Optional<Comparison> comparison = comparison(rowType, expression);
            if (!comparison.isPresent()) {
                return;
            }
            Operator operator = comparison.get().operator();
            boolean notEqual = operator == Operator.NOT_EQUAL || operator == Operator.IS_NOT_NULL;
            if (notEqual && notEqualTaken) {
                return;
            }
            List<T> translated = translator.translate(comparison.get());
            long width = 1;
            for (T filter : translated) {
                width *= translator.width(filter);
            }
            if (!translated.isEmpty() && width * disjuncts <= MAX_DISJUNCTS) {
                notEqualTaken |= notEqual;
                disjuncts *= (int) width;
                out.addAll(translated);
            }
        }

        private void disjunction(ResolvedExpression expression) {
            List<ResolvedExpression> leaves = new ArrayList<>();
            flattenOr(expression, leaves);
            if ((long) leaves.size() * disjuncts > MAX_DISJUNCTS) {
                return;
            }
            List<String> path = null;
            List<T> branches = new ArrayList<>();
            long width = 0;
            for (ResolvedExpression leaf : leaves) {
                Optional<Comparison> comparison = comparison(rowType, leaf);
                if (!comparison.isPresent()
                        || comparison.get().operator() != Operator.EQUAL
                        || (path != null && !path.equals(comparison.get().path()))) {
                    return;
                }
                path = comparison.get().path();
                List<T> branch = translator.translate(comparison.get());
                if (branch.size() != 1) {
                    // An equality that translates to no filter, or to several, is no one branch.
                    return;
                }
                branches.addAll(branch);
                width += translator.width(branch.get(0));
            }
            if (width * disjuncts <= MAX_DISJUNCTS) {
                disjuncts *= (int) width;
                out.add(translator.anyOf(branches));
            }
        }
    }

    private static void flattenOr(ResolvedExpression expression, List<ResolvedExpression> out) {
        if (isCall(expression, BuiltInFunctionDefinitions.OR)) {
            for (ResolvedExpression child : expression.getResolvedChildren()) {
                flattenOr(child, out);
            }
        } else {
            out.add(expression);
        }
    }

    private static boolean isCall(ResolvedExpression expression, FunctionDefinition function) {
        return expression instanceof CallExpression
                && ((CallExpression) expression).getFunctionDefinition().equals(function);
    }

    private static Optional<Comparison> comparison(RowType rowType, ResolvedExpression expression) {
        if (isReference(expression)) {
            return reference(rowType, expression)
                    .filter(reference -> reference.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(reference -> new Comparison(reference, Operator.EQUAL, null, true));
        }
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.NOT)) {
            if (children.size() != 1 || !isReference(children.get(0))) {
                return Optional.empty();
            }
            return reference(rowType, children.get(0))
                    .filter(reference -> reference.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(reference -> new Comparison(reference, Operator.EQUAL, null, false));
        }
        if (function.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            if (children.size() != 1 || !isReference(children.get(0))) {
                return Optional.empty();
            }
            return reference(rowType, children.get(0))
                    .map(reference -> new Comparison(reference, Operator.IS_NOT_NULL, null, null));
        }
        Operator operator = OPERATORS.get(function);
        if (operator == null || children.size() != 2) {
            return Optional.empty();
        }
        ResolvedExpression field;
        ValueLiteralExpression literal;
        if (isReference(children.get(0)) && children.get(1) instanceof ValueLiteralExpression) {
            field = children.get(0);
            literal = (ValueLiteralExpression) children.get(1);
        } else if (isReference(children.get(1))
                && children.get(0) instanceof ValueLiteralExpression) {
            // 5 < n is n > 5.
            field = children.get(1);
            literal = (ValueLiteralExpression) children.get(0);
            operator = operator.reversed();
        } else {
            return Optional.empty();
        }
        if (literal.isNull()) {
            return Optional.empty();
        }
        Operator op = operator;
        return reference(rowType, field)
                .map(reference -> new Comparison(reference, op, literal, null));
    }

    private static boolean isReference(ResolvedExpression expression) {
        return expression instanceof FieldReferenceExpression
                || expression instanceof NestedFieldReferenceExpression;
    }

    private static Optional<Reference> reference(RowType rowType, ResolvedExpression expression) {
        String[] names =
                expression instanceof NestedFieldReferenceExpression
                        ? ((NestedFieldReferenceExpression) expression).getFieldNames()
                        : new String[] {((FieldReferenceExpression) expression).getName()};
        LogicalType type = rowType;
        int column = -1;
        boolean nullableAbove = false;
        for (int i = 0; i < names.length; i++) {
            if (!(type instanceof RowType)) {
                return Optional.empty();
            }
            RowType row = (RowType) type;
            int index = row.getFieldNames().indexOf(names[i]);
            if (index < 0) {
                return Optional.empty();
            }
            if (i == 0) {
                column = index;
            } else {
                nullableAbove |= type.isNullable();
            }
            type = row.getTypeAt(index);
            if (nullableAbove && !type.isNullable()) {
                return Optional.empty();
            }
        }
        return Optional.of(new Reference(column, Arrays.asList(names), type));
    }

    /**
     * One mode's translation of a comparison into filters of its query, all of which a row must
     * satisfy.
     *
     * @param <T> the mode's filter type
     */
    public interface Translator<T> {

        /**
         * Translates one comparison.
         *
         * @param comparison the comparison
         * @return the filters, each a necessary condition of the comparison; empty when the
         *     comparison has none that never excludes a row Flink keeps
         */
        List<T> translate(Comparison comparison);

        /**
         * Combines the translations of a disjunction's equalities into one filter that admits a row
         * any of them admits.
         *
         * @param branches one filter per equality, in the predicate's order
         * @return the filter
         */
        T anyOf(List<T> branches);

        /**
         * Returns how many disjuncts a filter this translator made expands to in the service's
         * normal form, which a query limits: one for a comparison, the value count for an {@code
         * IN}.
         *
         * @param filter the filter
         * @return the width, at least one
         */
        default int width(T filter) {
            return 1;
        }
    }

    /** The operator of {@code reference op literal}, or a test of the reference alone. */
    public enum Operator {
        /** {@code =}. */
        EQUAL,
        /** {@code <>}. */
        NOT_EQUAL,
        /** {@code <}. */
        LESS_THAN,
        /** {@code <=}. */
        LESS_THAN_OR_EQUAL,
        /** {@code >}. */
        GREATER_THAN,
        /** {@code >=}. */
        GREATER_THAN_OR_EQUAL,
        /** {@code IS NOT NULL}, which carries no literal. */
        IS_NOT_NULL;

        /**
         * Returns whether the operator is one of {@code <}, {@code <=}, {@code >} and {@code >=}.
         *
         * @return whether it bounds a range
         */
        public boolean isRange() {
            return this == LESS_THAN
                    || this == LESS_THAN_OR_EQUAL
                    || this == GREATER_THAN
                    || this == GREATER_THAN_OR_EQUAL;
        }

        /** Returns the operator of the same comparison with its operands swapped. */
        Operator reversed() {
            switch (this) {
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                default:
                    return this;
            }
        }
    }

    /** A physical column, or a field nested in ROW columns, with its type. */
    private static final class Reference {
        private final int column;
        private final List<String> path;
        private final LogicalType type;

        private Reference(int column, List<String> path, LogicalType type) {
            this.column = column;
            this.path = Collections.unmodifiableList(path);
            this.type = type;
        }
    }

    /** One comparison between a reference and a non-null value, or an {@code IS NOT NULL}. */
    public static final class Comparison {
        private final Reference reference;
        private final Operator operator;
        @Nullable private final ValueLiteralExpression literal;
        @Nullable private final Boolean bareBoolean;

        private Comparison(
                Reference reference,
                Operator operator,
                @Nullable ValueLiteralExpression literal,
                @Nullable Boolean bareBoolean) {
            this.reference = reference;
            this.operator = operator;
            this.literal = literal;
            this.bareBoolean = bareBoolean;
        }

        /**
         * Returns the operator, with the reference on its left.
         *
         * @return the operator
         */
        public Operator operator() {
            return operator;
        }

        /**
         * Returns whether the reference names the physical column at the index itself, not a field
         * nested in it.
         *
         * @param index the column's index in the physical row type
         * @return whether the reference is that column
         */
        public boolean isColumn(int index) {
            return reference.path.size() == 1 && reference.column == index;
        }

        /**
         * Returns the field names from the column down, the column's own first.
         *
         * @return the path, one element for a column
         */
        public List<String> path() {
            return reference.path;
        }

        /**
         * Returns the path's first {@code length} names joined by dots: the form the table's marker
         * options name a field by, and a Datastore filter's property path.
         *
         * @param length how many names, from one to the path's length
         * @return the joined names
         */
        public String joinedPath(int length) {
            return String.join(".", reference.path.subList(0, length));
        }

        /**
         * Returns the type of the referenced column or field.
         *
         * @return the type
         */
        public LogicalType type() {
            return reference.type;
        }

        /**
         * Returns the value as the given class, as {@link ValueLiteralExpression#getValueAs(Class)}
         * does; a bare {@code BOOLEAN} reference and its {@code NOT} carry a {@link Boolean}, and
         * an {@code IS NOT NULL} carries none.
         *
         * @param clazz the class
         * @param <V> the value type
         * @return the value, or empty when the literal has no such form
         */
        public <V> Optional<V> valueAs(Class<V> clazz) {
            if (literal != null) {
                return literal.getValueAs(clazz);
            }
            return bareBoolean != null && clazz == Boolean.class
                    ? Optional.of(clazz.cast(bareBoolean))
                    : Optional.empty();
        }

        /**
         * Returns the value as a {@code long}, from an integer literal or a decimal one without a
         * fractional part.
         *
         * @return the value, or empty when no {@code long} equals it
         */
        public Optional<Long> longValue() {
            Optional<Long> integer = valueAs(Long.class);
            if (integer.isPresent()) {
                return integer;
            }
            Optional<Integer> narrow = valueAs(Integer.class);
            if (narrow.isPresent()) {
                return narrow.map(Integer::longValue);
            }
            try {
                return valueAs(BigDecimal.class).map(BigDecimal::longValueExact);
            } catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        /**
         * Returns the value as a {@code double}, from a floating-point literal, an integer one, or
         * the nearest {@code double} to a decimal one, which is what Flink compares a {@code
         * DOUBLE} column with.
         *
         * @return the value, or empty when the literal is not numeric
         */
        public Optional<Double> doubleValue() {
            Optional<Double> number = valueAs(Double.class);
            if (number.isPresent()) {
                return number;
            }
            Optional<Long> integer = valueAs(Long.class);
            if (integer.isPresent()) {
                return integer.map(Long::doubleValue);
            }
            Optional<Integer> narrow = valueAs(Integer.class);
            if (narrow.isPresent()) {
                return narrow.map(Integer::doubleValue);
            }
            return valueAs(BigDecimal.class).map(BigDecimal::doubleValue);
        }

        /**
         * Returns whether the referenced type is one both modes compare as a single value: {@code
         * CHAR}, {@code VARCHAR}, {@code BIGINT}, {@code DOUBLE}, {@code BOOLEAN}, {@code BINARY},
         * {@code VARBINARY} or {@code TIMESTAMP_LTZ}. Only such a type takes {@code IS NOT NULL}:
         * whether an empty array, map or embedded entity matches {@code != null} is unmeasured.
         *
         * @return whether the type is scalar
         */
        public boolean hasScalarType() {
            switch (reference.type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                case BIGINT:
                case DOUBLE:
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Returns the bounds a stored timestamp must satisfy for this comparison to hold of the
         * value the converter reads from it, truncated to the column's precision.
         *
         * @param precision the {@code TIMESTAMP_LTZ} column's precision, from 0 to 9
         * @return the bounds, all of which the stored value must satisfy; empty when the comparison
         *     has none that never excludes a value Flink keeps
         */
        public List<TimestampBound> timestampBounds(int precision) {
            return valueAs(Instant.class)
                    .map(
                            value ->
                                    TimestampBound.of(
                                            operator, value, TimestampBound.unitNanos(precision)))
                    .orElse(List.of());
        }
    }
}
