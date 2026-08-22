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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

/** Translates Flink SQL predicates into the safe subset Bigtable can serve. */
final class BigtableFilterPushDown {

    private BigtableFilterPushDown() {}

    /** Translates one conjunctive filter list and records both exact and best-effort pushdown. */
    static State translate(BigtableTableSchema schema, List<ResolvedExpression> filters) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        @Nullable List<ByteStringRange> rowKeyRanges = null;
        @Nullable Filters.Filter cellPredicate = null;

        for (ResolvedExpression filter : filters) {
            Optional<List<ByteStringRange>> exact = rowKeyRanges(schema, filter);
            if (exact.isPresent()) {
                accepted.add(filter);
                rowKeyRanges =
                        rowKeyRanges == null
                                ? exact.get()
                                : RowRanges.intersect(rowKeyRanges, exact.get());
                continue;
            }

            Optional<Filters.Filter> bestEffort = cellPredicate(schema, filter);
            if (bestEffort.isPresent()) {
                accepted.add(filter);
                remaining.add(filter);
                cellPredicate =
                        cellPredicate == null
                                ? bestEffort.get()
                                : and(cellPredicate, bestEffort.get());
            } else {
                remaining.add(filter);
            }
        }
        return new State(accepted, remaining, rowKeyRanges, cellPredicate);
    }

    /** Returns an exact row-key range translation, or empty when any part is not exact. */
    private static Optional<List<ByteStringRange>> rowKeyRanges(
            BigtableTableSchema schema, ResolvedExpression expression) {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.AND)) {
            @Nullable List<ByteStringRange> result = null;
            for (ResolvedExpression child : children) {
                Optional<List<ByteStringRange>> translated = rowKeyRanges(schema, child);
                if (!translated.isPresent()) {
                    return Optional.empty();
                }
                result =
                        result == null
                                ? translated.get()
                                : RowRanges.intersect(result, translated.get());
            }
            return result == null ? Optional.empty() : Optional.of(result);
        }
        if (function.equals(BuiltInFunctionDefinitions.OR)) {
            List<ByteStringRange> result = new ArrayList<>();
            for (ResolvedExpression child : children) {
                Optional<List<ByteStringRange>> translated = rowKeyRanges(schema, child);
                if (!translated.isPresent()) {
                    return Optional.empty();
                }
                result.addAll(translated.get());
            }
            return Optional.of(RowRanges.coalesce(result));
        }
        if (function.equals(BuiltInFunctionDefinitions.IS_NULL)
                || function.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            if (children.size() != 1 || !isRowKey(schema, children.get(0))) {
                return Optional.empty();
            }
            return Optional.of(
                    function.equals(BuiltInFunctionDefinitions.IS_NULL)
                            ? Collections.emptyList()
                            : unbounded());
        }
        if (function.equals(BuiltInFunctionDefinitions.IN)) {
            return inRanges(schema, children);
        }
        Comparison comparison = Comparison.of(function);
        return comparison == null
                ? Optional.empty()
                : comparisonRanges(schema, comparison, children);
    }

    private static Optional<List<ByteStringRange>> inRanges(
            BigtableTableSchema schema, List<ResolvedExpression> children) {
        if (children.size() < 2 || !isRowKey(schema, children.get(0))) {
            return Optional.empty();
        }
        if (!supportsEquality(schema.getRowKeyType())) {
            return Optional.empty();
        }
        List<ByteStringRange> ranges = new ArrayList<>();
        for (int i = 1; i < children.size(); i++) {
            Optional<ByteString> literal = literal(schema.getRowKeyType(), children.get(i));
            if (!literal.isPresent() || literal.get().isEmpty()) {
                // The SDK cannot express an empty-key bound. Real Bigtable rejects an empty row
                // key, but the emulator accepts one, so dropping it from an otherwise pushable IN
                // list would change the result there.
                return Optional.empty();
            }
            ranges.add(equalityRange(schema.getRowKeyType(), literal.get()));
        }
        return Optional.of(RowRanges.coalesce(ranges));
    }

    private static Optional<List<ByteStringRange>> comparisonRanges(
            BigtableTableSchema schema, Comparison comparison, List<ResolvedExpression> children) {
        if (children.size() != 2) {
            return Optional.empty();
        }
        ResolvedExpression value;
        Comparison normalized;
        if (isRowKey(schema, children.get(0))) {
            value = children.get(1);
            normalized = comparison;
        } else if (isRowKey(schema, children.get(1))) {
            value = children.get(0);
            normalized = comparison.reverse();
        } else {
            return Optional.empty();
        }
        if ((normalized.isOrdered() && !supportsOrdering(schema.getRowKeyType()))
                || (!normalized.isOrdered() && !supportsEquality(schema.getRowKeyType()))) {
            return Optional.empty();
        }
        Optional<ByteString> encoded = literal(schema.getRowKeyType(), value);
        // ByteStringRange normalises an empty bound to unbounded. Leave every comparison against
        // that emulator-only row key to Flink instead of claiming a different range is exact.
        return encoded.filter(key -> !key.isEmpty())
                .map(key -> ranges(schema.getRowKeyType(), normalized, key));
    }

    private static List<ByteStringRange> ranges(
            LogicalType type, Comparison comparison, ByteString key) {
        switch (comparison) {
            case EQUALS:
                return Collections.singletonList(equalityRange(type, key));
            case NOT_EQUALS:
                return complementOf(equalityRange(type, key));
            case LESS_THAN:
                return Collections.singletonList(ByteStringRange.unbounded().endOpen(key));
            case LESS_THAN_OR_EQUAL:
                return Collections.singletonList(ByteStringRange.unbounded().endClosed(key));
            case GREATER_THAN:
                return Collections.singletonList(ByteStringRange.unbounded().startOpen(key));
            case GREATER_THAN_OR_EQUAL:
                return Collections.singletonList(ByteStringRange.unbounded().startClosed(key));
            default:
                throw new IllegalStateException("Unknown comparison " + comparison);
        }
    }

    private static ByteStringRange singleton(ByteString key) {
        return ByteStringRange.unbounded().startClosed(key).endClosed(key);
    }

    /** Fixed-width decoders ignore suffix bytes, so their equality set is a key prefix. */
    private static ByteStringRange equalityRange(LogicalType type, ByteString key) {
        return usesPrefixEquality(type) ? ByteStringRange.prefix(key) : singleton(key);
    }

    /** Returns every key outside one non-empty range. */
    private static List<ByteStringRange> complementOf(ByteStringRange range) {
        List<ByteStringRange> complement = new ArrayList<>(2);
        complement.add(ByteStringRange.unbounded().endOpen(range.getStart()));
        if (range.getEndBound() == BoundType.OPEN) {
            complement.add(ByteStringRange.unbounded().startClosed(range.getEnd()));
        } else if (range.getEndBound() == BoundType.CLOSED) {
            complement.add(ByteStringRange.unbounded().startOpen(range.getEnd()));
        }
        return complement;
    }

    private static List<ByteStringRange> unbounded() {
        return Collections.singletonList(ByteStringRange.unbounded());
    }

    private static boolean isRowKey(BigtableTableSchema schema, ResolvedExpression expression) {
        return expression instanceof FieldReferenceExpression
                && ((FieldReferenceExpression) expression).getFieldIndex()
                        == schema.getRowKeyIndex();
    }

    private static boolean supportsEquality(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case BOOLEAN:
            case BINARY:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                // These decoders admit multiple byte encodings for one SQL value.
                return false;
            default:
                return true;
        }
    }

    private static boolean supportsOrdering(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
            case VARBINARY:
                return true;
            default:
                // The other HBase encodings do not sort like their signed SQL values.
                return false;
        }
    }

    private static boolean usesPrefixEquality(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    private static Optional<ByteString> literal(LogicalType type, ResolvedExpression expression) {
        if (!(expression instanceof ValueLiteralExpression)) {
            return Optional.empty();
        }
        ValueLiteralExpression literal = (ValueLiteralExpression) expression;
        if (literal.isNull()) {
            return Optional.empty();
        }
        try {
            Optional<Object> internal = internalValue(type, literal);
            if (!internal.isPresent()) {
                return Optional.empty();
            }
            GenericRowData row = GenericRowData.of(internal.get());
            return Optional.of(ByteString.copyFrom(CellValueCodec.encoder(type).encode(row, 0)));
        } catch (ArithmeticException | IllegalArgumentException e) {
            // A literal that cannot be represented by the declared row-key type is not pushable.
            return Optional.empty();
        }
    }

    private static Optional<Object> internalValue(
            LogicalType type, ValueLiteralExpression literal) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return literal.getValueAs(String.class).map(StringData::fromString);
            case BOOLEAN:
                return value(literal, Boolean.class);
            case BINARY:
            case VARBINARY:
                return value(literal, byte[].class);
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                return literal.getValueAs(BigDecimal.class)
                        .map(
                                value ->
                                        DecimalData.fromBigDecimal(
                                                value, decimal.getPrecision(), decimal.getScale()));
            case TINYINT:
                return literal.getValueAs(BigDecimal.class).map(BigDecimal::byteValueExact);
            case SMALLINT:
                return literal.getValueAs(BigDecimal.class).map(BigDecimal::shortValueExact);
            case INTEGER:
                return literal.getValueAs(BigDecimal.class).map(BigDecimal::intValueExact);
            case BIGINT:
                return literal.getValueAs(BigDecimal.class).map(BigDecimal::longValueExact);
            case DATE:
                return literal.getValueAs(LocalDate.class)
                        .map(value -> Math.toIntExact(value.toEpochDay()));
            case TIME_WITHOUT_TIME_ZONE:
                return literal.getValueAs(LocalTime.class)
                        .map(value -> Math.toIntExact(value.toNanoOfDay() / 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return literal.getValueAs(LocalDateTime.class)
                        .map(TimestampData::fromLocalDateTime);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return literal.getValueAs(Instant.class).map(TimestampData::fromInstant);
            case INTERVAL_YEAR_MONTH:
                return literal.getValueAs(Period.class)
                        .map(Period::toTotalMonths)
                        .map(Math::toIntExact);
            case INTERVAL_DAY_TIME:
                return literal.getValueAs(Duration.class).map(Duration::toMillis);
            case FLOAT:
            case DOUBLE:
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private static <T> Optional<Object> value(ValueLiteralExpression literal, Class<T> type) {
        return literal.getValueAs(type).map(value -> (Object) value);
    }

    /** Returns a necessary positive cell-existence predicate for a residual SQL expression. */
    private static Optional<Filters.Filter> cellPredicate(
            BigtableTableSchema schema, ResolvedExpression expression) {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.AND)) {
            @Nullable Filters.Filter result = null;
            for (ResolvedExpression child : children) {
                Optional<Filters.Filter> translated = cellPredicate(schema, child);
                if (translated.isPresent()) {
                    result = result == null ? translated.get() : and(result, translated.get());
                }
            }
            return Optional.ofNullable(result);
        }
        if (function.equals(BuiltInFunctionDefinitions.OR)) {
            Filters.InterleaveFilter result = FILTERS.interleave();
            for (ResolvedExpression child : children) {
                Optional<Filters.Filter> translated = cellPredicate(schema, child);
                if (!translated.isPresent()) {
                    return Optional.empty();
                }
                result.filter(translated.get());
            }
            return Optional.of(result);
        }
        if (function.equals(BuiltInFunctionDefinitions.IS_NOT_NULL) && children.size() == 1) {
            return cellReference(schema, children.get(0));
        }
        if (function.equals(BuiltInFunctionDefinitions.IN) && !children.isEmpty()) {
            return qualifierReference(schema, children.get(0));
        }
        if (Comparison.of(function) != null) {
            @Nullable Filters.Filter result = null;
            for (ResolvedExpression child : children) {
                Optional<Filters.Filter> translated = qualifierReference(schema, child);
                if (translated.isPresent()) {
                    result = result == null ? translated.get() : and(result, translated.get());
                }
            }
            return Optional.ofNullable(result);
        }
        return Optional.empty();
    }

    private static Optional<Filters.Filter> cellReference(
            BigtableTableSchema schema, ResolvedExpression expression) {
        if (expression instanceof FieldReferenceExpression) {
            int index = ((FieldReferenceExpression) expression).getFieldIndex();
            for (BigtableTableSchema.Family family : schema.getFamilies()) {
                if (family.getIndex() == index) {
                    return Optional.of(FILTERS.family().exactMatch(family.getName()));
                }
            }
        }
        return qualifierReference(schema, expression);
    }

    private static Optional<Filters.Filter> qualifierReference(
            BigtableTableSchema schema, ResolvedExpression expression) {
        if (!(expression instanceof NestedFieldReferenceExpression)) {
            return Optional.empty();
        }
        int[] indexes = ((NestedFieldReferenceExpression) expression).getFieldIndices();
        if (indexes.length != 2) {
            return Optional.empty();
        }
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            if (family.getIndex() == indexes[0]
                    && indexes[1] >= 0
                    && indexes[1] < family.getQualifiers().size()) {
                String qualifier = family.getQualifiers().get(indexes[1]).getName();
                return Optional.of(
                        FILTERS.chain()
                                .filter(FILTERS.family().exactMatch(family.getName()))
                                .filter(FILTERS.qualifier().exactMatch(qualifier)));
            }
        }
        return Optional.empty();
    }

    /** A row-level conjunction: evaluate {@code right} only for rows where {@code left} matches. */
    private static Filters.Filter and(Filters.Filter left, Filters.Filter right) {
        return FILTERS.condition(left).then(right);
    }

    private enum Comparison {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL;

        @Nullable
        private static Comparison of(FunctionDefinition function) {
            if (function.equals(BuiltInFunctionDefinitions.EQUALS)) {
                return EQUALS;
            }
            if (function.equals(BuiltInFunctionDefinitions.NOT_EQUALS)) {
                return NOT_EQUALS;
            }
            if (function.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
                return LESS_THAN;
            }
            if (function.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
                return LESS_THAN_OR_EQUAL;
            }
            if (function.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
                return GREATER_THAN;
            }
            if (function.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
                return GREATER_THAN_OR_EQUAL;
            }
            return null;
        }

        private boolean isOrdered() {
            return this != EQUALS && this != NOT_EQUALS;
        }

        private Comparison reverse() {
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

    /** Immutable filter state carried through planner copies of the dynamic source. */
    static final class State {

        private static final State EMPTY =
                new State(Collections.emptyList(), Collections.emptyList(), null, null);

        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        @Nullable private final List<ByteStringRange> rowKeyRanges;
        @Nullable private final Filters.Filter cellPredicate;

        private State(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                @Nullable List<ByteStringRange> rowKeyRanges,
                @Nullable Filters.Filter cellPredicate) {
            this.accepted = Collections.unmodifiableList(new ArrayList<>(accepted));
            this.remaining = Collections.unmodifiableList(new ArrayList<>(remaining));
            this.rowKeyRanges =
                    rowKeyRanges == null
                            ? null
                            : Collections.unmodifiableList(RowRanges.copyAll(rowKeyRanges));
            this.cellPredicate = cellPredicate;
        }

        static State empty() {
            return EMPTY;
        }

        SupportsFilterPushDown.Result result() {
            return SupportsFilterPushDown.Result.of(accepted, remaining);
        }

        @Nullable
        List<ByteStringRange> rowKeyRanges() {
            return rowKeyRanges == null ? null : RowRanges.copyAll(rowKeyRanges);
        }

        @Nullable
        Filters.Filter cellPredicate() {
            return cellPredicate;
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
            // The ranges are compared as ranges rather than as RowRanges.format() renderings,
            // because a rendering is for a reader and never an identity (#910). The collision this
            // comment used to cite -- "*" for both an unbounded bound and a bound at the row key
            // "*" (0x2A) -- was closed by #947, which escapes that byte
            // — though not, today, decisive: translate derives the ranges from the accepted filters
            // and the schema, which the enclosing BigtableDynamicSource.equals already compares, so
            // no pair of states can differ here alone. This keeps the comparison exact anyway, at
            // no cost, so that a future range not derived from the filters cannot slip through.
            // List.equals settles null against empty too, so no separate nullness guard is needed.
            return accepted.equals(state.accepted)
                    && remaining.equals(state.remaining)
                    && Objects.equals(rowKeyRanges, state.rowKeyRanges)
                    && Objects.equals(proto(cellPredicate), proto(state.cellPredicate));
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted, remaining, rowKeyRanges, proto(cellPredicate));
        }

        @Nullable
        private static Object proto(@Nullable Filters.Filter filter) {
            return filter == null ? null : filter.toProto();
        }
    }
}
