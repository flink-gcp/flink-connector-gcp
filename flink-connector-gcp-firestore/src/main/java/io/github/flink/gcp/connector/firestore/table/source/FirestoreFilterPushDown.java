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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Translates the predicates of a {@code firestore} scan into equality filters of its collection
 * query ({@code docs/adr/0197}).
 *
 * <p>Every filter is a necessary condition of the predicate it came from, never an exact one: the
 * service compares numbers across integer and floating-point types, matches {@code NaN} to {@code
 * NaN}, and treats {@code -0.0} as {@code 0}, so it can return a document Flink's predicate
 * rejects. Every predicate therefore stays a Flink residual, and what each filter must never do is
 * drop a document Flink would keep. That is why only these shapes translate:
 *
 * <ul>
 *   <li>{@code column = literal}, either way round, a bare {@code BOOLEAN} column (equal to {@code
 *       TRUE}), and its {@code NOT} (equal to {@code FALSE}), on a top-level physical column other
 *       than the document id;
 *   <li>a column of type {@code STRING}/{@code VARCHAR}, {@code BIGINT}, {@code DOUBLE}, {@code
 *       BOOLEAN}, {@code BYTES}/{@code VARBINARY}, or {@code TIMESTAMP_LTZ} of precision 6 or more,
 *       which the converter reads without changing the stored value;
 *   <li>a string or bytes literal of at most 1,500 bytes, the prefix of a value a Standard-edition
 *       database considers in a query, and a timestamp the service can represent;
 *   <li>under {@code AND}, every child that translates (a conjunction of necessary conditions is
 *       one); an {@code OR} translates to nothing.
 * </ul>
 *
 * <p>A column the {@code reference-field-paths} marker names is declined, because a reference never
 * equals a string. {@code IS NULL} is declined because a missing field, which Flink reads as {@code
 * NULL}, matches no filter.
 */
@Internal
final class FirestoreFilterPushDown {

    /** The bytes of a string or bytes value the service indexes; a longer literal is declined. */
    private static final int MAX_INDEXED_VALUE_BYTES = 1_500;

    /** The coarsest timestamp precision that reads a stored value unchanged. */
    private static final int STORED_TIMESTAMP_PRECISION = 6;

    private FirestoreFilterPushDown() {}

    /**
     * Translates the planner's predicates.
     *
     * @param schema the table's schema
     * @param filters the predicates, each one conjunct of the scan's condition
     * @return the predicates accepted and the equalities they translate to; every predicate remains
     *     a residual
     */
    static State translate(FirestoreTableSchema schema, List<ResolvedExpression> filters) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<FieldEquality> equalities = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            List<FieldEquality> translated = new ArrayList<>();
            collect(schema, filter, translated);
            if (!translated.isEmpty()) {
                accepted.add(filter);
                equalities.addAll(translated);
            }
        }
        return new State(accepted, filters, equalities);
    }

    private static void collect(
            FirestoreTableSchema schema, ResolvedExpression expression, List<FieldEquality> out) {
        if (expression instanceof CallExpression
                && ((CallExpression) expression)
                        .getFunctionDefinition()
                        .equals(BuiltInFunctionDefinitions.AND)) {
            for (ResolvedExpression child : expression.getResolvedChildren()) {
                collect(schema, child, out);
            }
            return;
        }
        equality(schema, expression).ifPresent(out::add);
    }

    private static Optional<FieldEquality> equality(
            FirestoreTableSchema schema, ResolvedExpression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return column(schema, (FieldReferenceExpression) expression)
                    .filter(column -> column.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(column -> new FieldEquality(column.name, Boolean.TRUE));
        }
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.NOT)) {
            if (children.size() != 1 || !(children.get(0) instanceof FieldReferenceExpression)) {
                return Optional.empty();
            }
            return column(schema, (FieldReferenceExpression) children.get(0))
                    .filter(column -> column.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(column -> new FieldEquality(column.name, Boolean.FALSE));
        }
        if (!function.equals(BuiltInFunctionDefinitions.EQUALS) || children.size() != 2) {
            return Optional.empty();
        }
        FieldReferenceExpression field;
        ValueLiteralExpression literal;
        if (children.get(0) instanceof FieldReferenceExpression
                && children.get(1) instanceof ValueLiteralExpression) {
            field = (FieldReferenceExpression) children.get(0);
            literal = (ValueLiteralExpression) children.get(1);
        } else if (children.get(1) instanceof FieldReferenceExpression
                && children.get(0) instanceof ValueLiteralExpression) {
            field = (FieldReferenceExpression) children.get(1);
            literal = (ValueLiteralExpression) children.get(0);
        } else {
            return Optional.empty();
        }
        if (literal.isNull()) {
            return Optional.empty();
        }
        Optional<Column> column = column(schema, field);
        if (!column.isPresent()) {
            return Optional.empty();
        }
        return value(column.get().type, literal)
                .map(value -> new FieldEquality(column.get().name, value));
    }

    /** The top-level physical column a reference names, unless it is the id or a reference. */
    private static Optional<Column> column(
            FirestoreTableSchema schema, FieldReferenceExpression field) {
        RowType rowType = schema.getRowType();
        int index = rowType.getFieldNames().indexOf(field.getName());
        if (index < 0 || index == schema.getKeyIndex() || schema.isReference(field.getName())) {
            return Optional.empty();
        }
        return Optional.of(new Column(field.getName(), rowType.getTypeAt(index)));
    }

    private static Optional<Serializable> value(LogicalType type, ValueLiteralExpression literal) {
        try {
            switch (type.getTypeRoot()) {
                case VARCHAR:
                    return literal.getValueAs(String.class)
                            .filter(
                                    value ->
                                            value.getBytes(StandardCharsets.UTF_8).length
                                                    <= MAX_INDEXED_VALUE_BYTES)
                            .map(Serializable.class::cast);
                case BIGINT:
                    Optional<Long> integer = literal.getValueAs(Long.class);
                    if (integer.isPresent()) {
                        return Optional.of(integer.get());
                    }
                    return literal.getValueAs(BigDecimal.class)
                            .map(BigDecimal::longValueExact)
                            .map(Serializable.class::cast);
                case DOUBLE:
                    Optional<Double> number = literal.getValueAs(Double.class);
                    if (number.isPresent()) {
                        return Optional.of(number.get());
                    }
                    return literal.getValueAs(BigDecimal.class)
                            .map(BigDecimal::doubleValue)
                            .map(Serializable.class::cast);
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class).map(Serializable.class::cast);
                case VARBINARY:
                    return literal.getValueAs(byte[].class)
                            .filter(value -> value.length <= MAX_INDEXED_VALUE_BYTES)
                            .map(Blob::fromBytes);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    if (((LocalZonedTimestampType) type).getPrecision()
                            < STORED_TIMESTAMP_PRECISION) {
                        // The converter truncates a stored timestamp to the column's precision,
                        // so Flink keeps documents a filter on the literal would not match.
                        return Optional.empty();
                    }
                    return literal.getValueAs(Instant.class)
                            .filter(FirestoreFilterPushDown::isRepresentable)
                            .map(
                                    value ->
                                            Timestamp.ofTimeSecondsAndNanos(
                                                    value.getEpochSecond(), value.getNano()));
                default:
                    // CHAR and BINARY: what an equality on a fixed-length value matches is
                    // unmeasured
                    // (the planner presented a CHAR comparison as a cast);
                    // ARRAY, MAP and ROW are not scalar.
                    return Optional.empty();
            }
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
    }

    private static boolean isRepresentable(Instant instant) {
        // Whole seconds suffice: MIN_VALUE carries no nanoseconds and MAX_VALUE all of them.
        return instant.getEpochSecond() >= Timestamp.MIN_VALUE.getSeconds()
                && instant.getEpochSecond() <= Timestamp.MAX_VALUE.getSeconds();
    }

    /** The planner's result and the equalities a collection query adds. */
    static final class State {
        private static final State EMPTY =
                new State(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        private final List<FieldEquality> equalities;

        private State(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                List<FieldEquality> equalities) {
            this.accepted = List.copyOf(accepted);
            this.remaining = List.copyOf(remaining);
            this.equalities = List.copyOf(equalities);
        }

        static State empty() {
            return EMPTY;
        }

        /** Returns a state that pushes nothing and keeps every predicate. */
        static State declined(List<ResolvedExpression> filters) {
            return new State(Collections.emptyList(), filters, Collections.emptyList());
        }

        SupportsFilterPushDown.Result result() {
            return SupportsFilterPushDown.Result.of(accepted, remaining);
        }

        List<FieldEquality> equalities() {
            return equalities;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            State state = (State) o;
            return accepted.equals(state.accepted)
                    && remaining.equals(state.remaining)
                    && equalities.equals(state.equalities);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted, remaining, equalities);
        }
    }

    /**
     * One equality filter on a literal field name. The value is a {@code String}, {@code Long},
     * {@code Double}, {@code Boolean}, {@link Blob} or {@link Timestamp}, each serializable with
     * the query factory that carries it.
     */
    static final class FieldEquality implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String field;
        private final Serializable value;

        FieldEquality(String field, Serializable value) {
            this.field = field;
            this.value = value;
        }

        String field() {
            return field;
        }

        /** Returns the value as the client library takes it. */
        Object value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldEquality that = (FieldEquality) o;
            return field.equals(that.field) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, value);
        }

        @Override
        public String toString() {
            return field + " == " + value;
        }
    }

    private static final class Column {
        private final String name;
        private final LogicalType type;

        private Column(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }
    }
}
