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

package io.github.flink.gcp.connector.datastore.table.source;

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
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Translates the predicates of a {@code datastore} scan into equality filters of its kind query,
 * when the table opts in with {@code scan.filter-push-down.enabled} ({@code docs/adr/0184}).
 *
 * <p>A Datastore filter never matches a value excluded from indexes, so a filter is a necessary
 * condition of its predicate only for the entities whose filtered properties are indexed; enabling
 * the option is the user's assertion that every one is. Beyond that, each filter must never drop an
 * entity Flink would keep, and it may return entities Flink rejects: an equality on an integer also
 * matches an array holding it, and one on a string also matches the same bytes stored as a blob.
 * Every predicate therefore stays a Flink residual. Only these shapes translate:
 *
 * <ul>
 *   <li>{@code column = literal}, either way round, a bare {@code BOOLEAN} column (equal to {@code
 *       TRUE}), and its {@code NOT} (equal to {@code FALSE}), on a top-level physical column other
 *       than the key, whose name holds no {@code .} or backquote and which {@code
 *       sink.unindexed-columns} does not name;
 *   <li>a column of type {@code STRING}/{@code VARCHAR}, {@code BIGINT}, {@code BOOLEAN}, {@code
 *       BYTES}/{@code VARBINARY}, or {@code TIMESTAMP_LTZ} of precision 6 or more, which the
 *       converter reads only from a stored value of that type;
 *   <li>a string or bytes literal of at most 1,500 bytes, the longest value the service indexes,
 *       and a timestamp of whole microseconds the service can represent;
 *   <li>under {@code AND}, every child that translates; an {@code OR} translates to nothing.
 * </ul>
 *
 * <p>{@code DOUBLE} is declined: the converter reads a stored integer into it, which an equality on
 * a floating-point value does not match, and the service tells {@code 0.0} from {@code -0.0}. Only
 * equalities translate, so the query stays one the source cuts into key ranges.
 */
@Internal
final class DatastoreFilterPushDown {

    /** The bytes of a string or bytes value the service indexes; a longer literal is refused. */
    private static final int MAX_INDEXED_VALUE_BYTES = 1_500;

    /** The precision the service stores a timestamp at. */
    private static final int STORED_TIMESTAMP_PRECISION = 6;

    private DatastoreFilterPushDown() {}

    /**
     * Translates the planner's predicates.
     *
     * @param schema the table's schema
     * @param filters the predicates, each one conjunct of the scan's condition
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    static State translate(DatastoreTableSchema schema, List<ResolvedExpression> filters) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<Filter> equalities = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            List<Filter> translated = new ArrayList<>();
            collect(schema, filter, translated);
            if (!translated.isEmpty()) {
                accepted.add(filter);
                equalities.addAll(translated);
            }
        }
        return new State(accepted, filters, equalities);
    }

    private static void collect(
            DatastoreTableSchema schema, ResolvedExpression expression, List<Filter> out) {
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

    private static Optional<Filter> equality(
            DatastoreTableSchema schema, ResolvedExpression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return column(schema, (FieldReferenceExpression) expression)
                    .filter(column -> column.type.is(LogicalTypeRoot.BOOLEAN))
                    .map(column -> equal(column.name, Value.newBuilder().setBooleanValue(true)));
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
                    .map(column -> equal(column.name, Value.newBuilder().setBooleanValue(false)));
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
        return value(column.get().type, literal).map(value -> equal(column.get().name, value));
    }

    private static Filter equal(String property, Value.Builder value) {
        return Filter.newBuilder()
                .setPropertyFilter(
                        PropertyFilter.newBuilder()
                                .setProperty(PropertyReference.newBuilder().setName(property))
                                .setOp(PropertyFilter.Operator.EQUAL)
                                .setValue(value))
                .build();
    }

    /**
     * The top-level physical column a reference names, unless it is the key, a column written
     * excluded from indexes, or one whose name the service would read as a path.
     */
    private static Optional<Column> column(
            DatastoreTableSchema schema, FieldReferenceExpression field) {
        RowType rowType = schema.getRowType();
        String name = field.getName();
        int index = rowType.getFieldNames().indexOf(name);
        if (index < 0
                || index == schema.getKeyIndex()
                || schema.isUnindexed(name)
                || name.indexOf('.') >= 0
                || name.indexOf('`') >= 0) {
            return Optional.empty();
        }
        return Optional.of(new Column(name, rowType.getTypeAt(index)));
    }

    private static Optional<Value.Builder> value(LogicalType type, ValueLiteralExpression literal) {
        try {
            switch (type.getTypeRoot()) {
                case VARCHAR:
                    return literal.getValueAs(String.class)
                            .filter(
                                    value ->
                                            value.getBytes(StandardCharsets.UTF_8).length
                                                    <= MAX_INDEXED_VALUE_BYTES)
                            .map(value -> Value.newBuilder().setStringValue(value));
                case BIGINT:
                    Optional<Long> integer = literal.getValueAs(Long.class);
                    if (!integer.isPresent()) {
                        integer =
                                literal.getValueAs(BigDecimal.class)
                                        .map(BigDecimal::longValueExact);
                    }
                    return integer.map(value -> Value.newBuilder().setIntegerValue(value));
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class)
                            .map(value -> Value.newBuilder().setBooleanValue(value));
                case VARBINARY:
                    return literal.getValueAs(byte[].class)
                            .filter(value -> value.length <= MAX_INDEXED_VALUE_BYTES)
                            .map(
                                    value ->
                                            Value.newBuilder()
                                                    .setBlobValue(ByteString.copyFrom(value)));
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    if (((LocalZonedTimestampType) type).getPrecision()
                            < STORED_TIMESTAMP_PRECISION) {
                        // The converter truncates a stored timestamp to the column's precision,
                        // so Flink keeps entities a filter on the literal would not match.
                        return Optional.empty();
                    }
                    return literal.getValueAs(Instant.class)
                            .filter(DatastoreFilterPushDown::isStorable)
                            .map(
                                    value ->
                                            Value.newBuilder()
                                                    .setTimestampValue(
                                                            com.google.protobuf.Timestamp
                                                                    .newBuilder()
                                                                    .setSeconds(
                                                                            value.getEpochSecond())
                                                                    .setNanos(value.getNano())));
                default:
                    // DOUBLE: the converter reads a stored integer, which a floating-point
                    // equality does not match. CHAR and BINARY: unmeasured, as in Native mode.
                    // ARRAY, ROW (embedded entities, geographical points, key values) and MAP are
                    // not scalar.
                    return Optional.empty();
            }
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
    }

    /**
     * Whether the service stores the instant as it is: whole microseconds, in its range. A literal
     * with a finer part equals no stored value, so declining it loses nothing.
     */
    private static boolean isStorable(Instant instant) {
        // Whole seconds suffice: MIN_VALUE carries no nanoseconds and MAX_VALUE all of them.
        return instant.getNano() % 1_000 == 0
                && instant.getEpochSecond() >= Timestamp.MIN_VALUE.getSeconds()
                && instant.getEpochSecond() <= Timestamp.MAX_VALUE.getSeconds();
    }

    /** The planner's result and the filters a kind query adds. */
    static final class State {
        private static final State EMPTY =
                new State(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        private final List<Filter> equalities;

        private State(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                List<Filter> equalities) {
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

        /**
         * Returns the query reading the kind under the translated filters, or {@code null} when
         * none translated and the scan reads the kind whole.
         */
        @Nullable
        Query query(String kind) {
            if (equalities.isEmpty()) {
                return null;
            }
            Filter filter =
                    equalities.size() == 1
                            ? equalities.get(0)
                            : Filter.newBuilder()
                                    .setCompositeFilter(
                                            CompositeFilter.newBuilder()
                                                    .setOp(CompositeFilter.Operator.AND)
                                                    .addAllFilters(equalities))
                                    .build();
            return Query.newBuilder()
                    .addKind(KindExpression.newBuilder().setName(kind))
                    .setFilter(filter)
                    .build();
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

    private static final class Column {
        private final String name;
        private final LogicalType type;

        private Column(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }
    }
}
