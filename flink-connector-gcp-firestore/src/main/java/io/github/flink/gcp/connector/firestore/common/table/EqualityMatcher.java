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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Finds the equalities in the predicates of a Native-mode or Datastore-mode scan, which each mode
 * then translates into a filter of its own query ({@code docs/adr/0197}, {@code docs/adr/0184}).
 *
 * <p>An equality is {@code reference = literal} either way round, a bare {@code BOOLEAN} reference
 * (equal to {@code TRUE}) or its {@code NOT} (equal to {@code FALSE}), where the reference is a
 * physical column or a field nested in {@code ROW} columns. Under {@code AND}, every child is
 * searched, so a predicate yields the equalities of all its conjuncts; an {@code OR} yields none,
 * and so does a comparison with a {@code NULL} literal.
 *
 * <p>A nested path is resolved only where no {@code NOT NULL} field lies below a nullable {@code
 * ROW}: Flink takes such a field to be never {@code NULL}, although it is {@code NULL} under a
 * {@code NULL} parent, so its comparisons can evaluate where the stored document has no value.
 *
 * <p>The modes share only this search: what a reference may name, and the form a literal takes,
 * belong to each mode's {@link Translator}. Every predicate stays a residual Flink evaluates, so a
 * translation must only never exclude a row Flink keeps.
 */
@Internal
public final class EqualityMatcher {

    private EqualityMatcher() {}

    /**
     * Translates the planner's predicates.
     *
     * @param rowType the physical row type
     * @param filters the predicates, each one conjunct of the scan's condition
     * @param translator the mode's translation of one equality
     * @param <T> the mode's filter type
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    public static <T> PushedFilters<T> translate(
            RowType rowType, List<ResolvedExpression> filters, Translator<T> translator) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<T> translated = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            List<T> conjuncts = new ArrayList<>();
            collect(rowType, filter, translator, conjuncts);
            if (!conjuncts.isEmpty()) {
                accepted.add(filter);
                translated.addAll(conjuncts);
            }
        }
        return new PushedFilters<>(accepted, filters, translated);
    }

    private static <T> void collect(
            RowType rowType, ResolvedExpression expression, Translator<T> translator, List<T> out) {
        if (expression instanceof CallExpression
                && ((CallExpression) expression)
                        .getFunctionDefinition()
                        .equals(BuiltInFunctionDefinitions.AND)) {
            for (ResolvedExpression child : expression.getResolvedChildren()) {
                collect(rowType, child, translator, out);
            }
            return;
        }
        equality(rowType, expression).flatMap(translator::translate).ifPresent(out::add);
    }

    private static Optional<Equality> equality(RowType rowType, ResolvedExpression expression) {
        if (isReference(expression)) {
            return reference(rowType, expression)
                    .filter(reference -> reference.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(reference -> new Equality(reference, null, Boolean.TRUE));
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
                    .map(reference -> new Equality(reference, null, Boolean.FALSE));
        }
        if (!function.equals(BuiltInFunctionDefinitions.EQUALS) || children.size() != 2) {
            return Optional.empty();
        }
        ResolvedExpression field;
        ValueLiteralExpression literal;
        if (isReference(children.get(0)) && children.get(1) instanceof ValueLiteralExpression) {
            field = children.get(0);
            literal = (ValueLiteralExpression) children.get(1);
        } else if (isReference(children.get(1))
                && children.get(0) instanceof ValueLiteralExpression) {
            field = children.get(1);
            literal = (ValueLiteralExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        if (literal.isNull()) {
            return Optional.empty();
        }
        return reference(rowType, field).map(reference -> new Equality(reference, literal, null));
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
     * One mode's translation of an equality into a filter of its query.
     *
     * @param <T> the mode's filter type
     */
    @FunctionalInterface
    public interface Translator<T> {

        /**
         * Translates one equality.
         *
         * @param equality the equality
         * @return the filter, or empty when the equality has none that never excludes a row Flink
         *     keeps
         */
        Optional<T> translate(Equality equality);
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

    /** One equality between a reference and a non-null value. */
    public static final class Equality {
        private final Reference reference;
        private final ValueLiteralExpression literal;
        private final Boolean bareBoolean;

        private Equality(Reference reference, ValueLiteralExpression literal, Boolean bareBoolean) {
            this.reference = reference;
            this.literal = literal;
            this.bareBoolean = bareBoolean;
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
         * does; a bare {@code BOOLEAN} reference and its {@code NOT} carry a {@link Boolean}.
         *
         * @param clazz the class
         * @param <V> the value type
         * @return the value, or empty when the literal has no such form
         */
        public <V> Optional<V> valueAs(Class<V> clazz) {
            if (literal != null) {
                return literal.getValueAs(clazz);
            }
            return clazz == Boolean.class ? Optional.of(clazz.cast(bareBoolean)) : Optional.empty();
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
            try {
                return valueAs(BigDecimal.class).map(BigDecimal::longValueExact);
            } catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        /**
         * Returns the value as a {@code double}, from a floating-point literal or the nearest
         * {@code double} to a decimal one, which is what Flink compares a {@code DOUBLE} column
         * with.
         *
         * @return the value, or empty when the literal is not numeric
         */
        public Optional<Double> doubleValue() {
            Optional<Double> number = valueAs(Double.class);
            return number.isPresent()
                    ? number
                    : valueAs(BigDecimal.class).map(BigDecimal::doubleValue);
        }
    }
}
