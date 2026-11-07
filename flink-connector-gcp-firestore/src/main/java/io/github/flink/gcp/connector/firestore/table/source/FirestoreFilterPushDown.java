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
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Filter;
import io.github.flink.gcp.connector.firestore.common.table.OrderedStrings;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Comparison;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Operator;
import io.github.flink.gcp.connector.firestore.common.table.PushedFilters;
import io.github.flink.gcp.connector.firestore.common.table.TimestampBound;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Translates the comparisons {@link PredicateMatcher} finds in the predicates of a {@code
 * firestore} scan into filters of its collection query ({@code docs/adr/0197}).
 *
 * <p>Every filter is a necessary condition of the predicate it came from, never an exact one: the
 * service compares numbers across integer and floating-point types, matches {@code NaN} to {@code
 * NaN}, and treats {@code -0.0} as {@code 0}, so it can return a document Flink's predicate
 * rejects. Every predicate therefore stays a Flink residual, and what each filter must never do is
 * drop a document Flink would keep. A range filter matches only values of the literal's type, and
 * an integer and a floating-point value compare as numbers, so a range never drops a value the
 * converter reads into the column. That is why only these comparisons translate:
 *
 * <ul>
 *   <li>on the document id column, an equality, and with {@code scan.filter-push-down.enabled} a
 *       range, on a value the service can store as an id in the collection, translated to a filter
 *       on the document's name;
 *   <li>with {@code scan.filter-push-down.enabled}, on any other physical column, or a field nested
 *       in {@code ROW} columns, one that is not a geographical point's field and that the {@code
 *       reference-field-paths} marker does not name (a reference never equals a string), each name
 *       one literal path segment;
 *   <li>{@code IS NOT NULL}, as a range from the type's lowest value, and {@code =}, {@code <>} (as
 *       the ranges either side of its value), {@code <}, {@code <=}, {@code >} and {@code >=}, of
 *       type {@code STRING}/{@code VARCHAR}, {@code BIGINT}, {@code DOUBLE}, {@code BOOLEAN},
 *       {@code BYTES}/ {@code VARBINARY} or {@code TIMESTAMP_LTZ}, and {@code =} alone of {@code
 *       CHAR} and {@code BINARY} beside {@code IS NOT NULL}. A {@code TIMESTAMP_LTZ} below
 *       microseconds compares the bounds {@link TimestampBound} rounds to the column's precision,
 *       which the converter truncates to. No {@code !=} is sent: an Enterprise-edition database
 *       returns a document lacking the field for it, from which no resume cursor can be taken,
 *       while a range matches only values of its literal's type;
 *   <li>with a string or bytes literal of at most 1,500 bytes for an equality, and under 1,500 for
 *       any other operator: a Standard-edition database considers only that prefix of a value;
 *   <li>for a range on a string or the document id, a literal {@link OrderedStrings} finds ordered
 *       alike by UTF-8 bytes, as the service orders, and by UTF-16 code units, as Flink orders two
 *       strings it holds as Java strings.
 * </ul>
 *
 * <p>{@code IS NULL} is declined because a missing field, which Flink reads as {@code NULL},
 * matches no filter. An inequality orders the query by its field, so {@link
 * FirestoreCollectionQueryFactory} keeps that field in the projection a resume cursor is taken
 * from.
 */
@Internal
final class FirestoreFilterPushDown {

    /** The bytes of a string or bytes value the service indexes. */
    private static final int MAX_INDEXED_VALUE_BYTES = 1_500;

    private FirestoreFilterPushDown() {}

    /**
     * Translates the planner's predicates.
     *
     * @param schema the table's schema
     * @param enabled whether comparisons other than an equality, or {@code IN}, on the document id
     *     translate, which {@code scan.filter-push-down.enabled} decides
     * @param filters the predicates, each one conjunct of the scan's condition
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    static PushedFilters<FieldFilter> translate(
            FirestoreTableSchema schema, boolean enabled, List<ResolvedExpression> filters) {
        return PredicateMatcher.translate(
                schema.getRowType(),
                filters,
                new PredicateMatcher.Translator<FieldFilter>() {
                    @Override
                    public List<FieldFilter> translate(Comparison comparison) {
                        return FirestoreFilterPushDown.translate(schema, enabled, comparison);
                    }

                    @Override
                    public FieldFilter anyOf(List<FieldFilter> branches) {
                        return FieldFilter.anyOf(branches);
                    }

                    @Override
                    public int width(FieldFilter filter) {
                        return filter.width();
                    }
                });
    }

    private static List<FieldFilter> translate(
            FirestoreTableSchema schema, boolean enabled, Comparison comparison) {
        Operator operator = comparison.operator();
        if (comparison.isColumn(schema.getKeyIndex())) {
            // An id the service cannot store names no document, and the client library refuses
            // some of them (an empty one, a path) before sending the query. A document always has
            // its id, so neither <> nor IS NOT NULL says anything about it worth sending.
            if (operator != Operator.EQUAL && !(enabled && operator.isRange())) {
                return List.of();
            }
            return comparison
                    .valueAs(String.class)
                    .filter(FirestoreDocumentLookups::isStorableId)
                    .filter(id -> OrderedStrings.admits(operator, id))
                    .map(id -> List.of(FieldFilter.documentId(operator, id)))
                    .orElse(List.of());
        }
        if (!enabled) {
            return List.of();
        }
        int length = comparison.path().size();
        for (int prefix = 1; prefix < length; prefix++) {
            // A geographical point is one value, not a map of a latitude and a longitude.
            if (schema.isGeoPoint(comparison.joinedPath(prefix))) {
                return List.of();
            }
        }
        if (schema.isReference(comparison.joinedPath(length))
                || schema.isGeoPoint(comparison.joinedPath(length))) {
            return List.of();
        }
        if (operator == Operator.IS_NOT_NULL) {
            // Only a scalar: whether an empty array or map matches a filter is unmeasured, and
            // Flink reads either as a value that is not NULL.
            return comparison.hasScalarType()
                    ? atLeastLowest(comparison.path(), comparison.type())
                    : List.of();
        }
        List<FieldFilter> filters = new ArrayList<>();
        for (FieldFilter filter : values(comparison)) {
            filters.add(filter.isNotEqual() ? filter.asRanges() : filter);
        }
        return filters;
    }

    /**
     * Returns {@code IS NOT NULL} as a range from the lowest value of the column's type: a missing
     * field and a stored null match no range, and Flink reads both as NULL. An Enterprise edition
     * database returns a document lacking the field for {@code != null}, and a cursor cannot be
     * taken from such a document, so the range replaces it. A {@code DOUBLE} also takes {@code
     * NaN}, which no range matches and Flink keeps.
     */
    private static List<FieldFilter> atLeastLowest(List<String> path, LogicalType type) {
        Serializable lowest;
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                lowest = "";
                break;
            case BINARY:
            case VARBINARY:
                lowest = Blob.fromBytes(new byte[0]);
                break;
            case BIGINT:
                lowest = Long.MIN_VALUE;
                break;
            case BOOLEAN:
                lowest = Boolean.FALSE;
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                lowest = Timestamp.MIN_VALUE;
                break;
            case DOUBLE:
                return List.of(
                        FieldFilter.anyOf(
                                List.of(
                                        FieldFilter.field(
                                                path,
                                                Operator.GREATER_THAN_OR_EQUAL,
                                                Double.NEGATIVE_INFINITY),
                                        FieldFilter.field(path, Operator.EQUAL, Double.NaN))));
            default:
                return List.of();
        }
        return List.of(FieldFilter.field(path, Operator.GREATER_THAN_OR_EQUAL, lowest));
    }

    private static List<FieldFilter> values(Comparison comparison) {
        LogicalType type = comparison.type();
        Operator operator = comparison.operator();
        List<String> path = comparison.path();
        // A Standard-edition database compares only a value's first 1,500 bytes, so a literal of
        // that length could equal the prefix of a longer value Flink orders after it.
        int maxBytes =
                operator == Operator.EQUAL ? MAX_INDEXED_VALUE_BYTES : MAX_INDEXED_VALUE_BYTES - 1;
        Optional<Serializable> value;
        switch (type.getTypeRoot()) {
            case CHAR:
            case BINARY:
                // The planner presents a fixed-length column bare only beside a literal of its
                // length; the converter reads a stored value unpadded, as under VARCHAR. No
                // ordering of fixed-length values was measured.
                if (operator != Operator.EQUAL) {
                    return List.of();
                }
                value =
                        type.is(LogicalTypeRoot.CHAR)
                                ? string(comparison, maxBytes)
                                : bytes(comparison, maxBytes);
                break;
            case VARCHAR:
                value = string(comparison, maxBytes);
                break;
            case VARBINARY:
                value = bytes(comparison, maxBytes);
                break;
            case BIGINT:
                value = comparison.longValue().map(Serializable.class::cast);
                break;
            case DOUBLE:
                // The client library refuses NaN in any comparison but an equality.
                value =
                        comparison
                                .doubleValue()
                                .filter(v -> operator == Operator.EQUAL || !Double.isNaN(v))
                                .map(Serializable.class::cast);
                break;
            case BOOLEAN:
                value = comparison.valueAs(Boolean.class).map(Serializable.class::cast);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                List<FieldFilter> bounds = new ArrayList<>();
                for (TimestampBound bound :
                        comparison.timestampBounds(
                                ((LocalZonedTimestampType) type).getPrecision())) {
                    Instant at = bound.value();
                    bounds.add(
                            FieldFilter.field(
                                    path,
                                    bound.operator(),
                                    Timestamp.ofTimeSecondsAndNanos(
                                            at.getEpochSecond(), at.getNano())));
                }
                return bounds;
            default:
                // ARRAY, MAP and ROW are not scalar.
                return List.of();
        }
        return value.map(v -> List.of(FieldFilter.field(path, operator, v))).orElse(List.of());
    }

    private static Optional<Serializable> string(Comparison comparison, int maxBytes) {
        return comparison
                .valueAs(String.class)
                .filter(v -> v.getBytes(StandardCharsets.UTF_8).length <= maxBytes)
                .filter(v -> OrderedStrings.admits(comparison.operator(), v))
                .map(Serializable.class::cast);
    }

    private static Optional<Serializable> bytes(Comparison comparison, int maxBytes) {
        return comparison
                .valueAs(byte[].class)
                .filter(v -> v.length <= maxBytes)
                .map(Blob::fromBytes);
    }

    /**
     * One filter: a comparison on the document id or on a field path of literal segments, or a
     * disjunction of such comparisons. A value is a {@code String}, {@code Long}, {@code Double},
     * {@code Boolean}, {@link Blob} or {@link Timestamp}, each serializable with the query factory
     * that carries it. No filter the query receives is a not-equal one: {@code <>} is sent as the
     * ranges either side of its value ({@link #asRanges()}).
     */
    static final class FieldFilter implements Serializable {

        private static final long serialVersionUID = 1L;

        /** The segments of the field path, or {@code null} for the document id. */
        @Nullable private final String[] segments;

        @Nullable private final Operator operator;

        private final Serializable value;

        /** The disjuncts, or {@code null} for a comparison. */
        @Nullable private final List<FieldFilter> anyOf;

        private FieldFilter(
                @Nullable String[] segments,
                @Nullable Operator operator,
                Serializable value,
                @Nullable List<FieldFilter> anyOf) {
            this.segments = segments;
            this.operator = operator;
            this.value = value;
            this.anyOf = anyOf;
        }

        /** Returns a comparison of the document id, which the client library takes as a string. */
        static FieldFilter documentId(Operator operator, String id) {
            return new FieldFilter(null, operator, id, null);
        }

        /** Returns a comparison of the field the segments address, outermost first. */
        static FieldFilter field(List<String> segments, Operator operator, Serializable value) {
            return new FieldFilter(segments.toArray(new String[0]), operator, value, null);
        }

        /** Returns a filter any of whose disjuncts admits a document. */
        static FieldFilter anyOf(List<FieldFilter> disjuncts) {
            return new FieldFilter(null, null, null, List.copyOf(disjuncts));
        }

        /**
         * Returns the field path the query is ordered by because of this filter, or {@code null}:
         * an inequality on a field, alone or in a disjunction, orders the query by it, so a resume
         * cursor needs its value.
         */
        @Nullable
        FieldPath orderedPath() {
            if (anyOf != null) {
                for (FieldFilter branch : anyOf) {
                    FieldPath path = branch.orderedPath();
                    if (path != null) {
                        return path;
                    }
                }
                return null;
            }
            return segments != null && operator != Operator.EQUAL ? FieldPath.of(segments) : null;
        }

        /** Returns how many disjuncts the filter expands to: its branches, or one. */
        int width() {
            return anyOf == null ? 1 : anyOf.size();
        }

        /** Returns whether the filter is a not-equal comparison of a field. */
        boolean isNotEqual() {
            return anyOf == null && segments != null && operator == Operator.NOT_EQUAL;
        }

        /**
         * Returns a not-equal comparison as the ranges either side of its value, and, for a double,
         * an equality on {@code NaN}, which Flink's {@code <>} keeps and no range matches. Ranges
         * never match a document lacking the field, which an Enterprise-edition database returns
         * for {@code !=} and from which no resume cursor can be taken.
         */
        FieldFilter asRanges() {
            List<String> path = Arrays.asList(segments);
            List<FieldFilter> branches = new ArrayList<>();
            branches.add(field(path, Operator.LESS_THAN, value));
            branches.add(field(path, Operator.GREATER_THAN, value));
            if (value instanceof Double) {
                branches.add(field(path, Operator.EQUAL, Double.NaN));
            }
            return anyOf(branches);
        }

        /** Returns the filter as the client library takes it. */
        Filter toFilter() {
            if (anyOf != null) {
                Filter[] filters = new Filter[anyOf.size()];
                for (int i = 0; i < filters.length; i++) {
                    filters[i] = anyOf.get(i).toFilter();
                }
                return Filter.or(filters);
            }
            FieldPath path = segments == null ? FieldPath.documentId() : FieldPath.of(segments);
            switch (operator) {
                case EQUAL:
                    return Filter.equalTo(path, value);
                case LESS_THAN:
                    return Filter.lessThan(path, value);
                case LESS_THAN_OR_EQUAL:
                    return Filter.lessThanOrEqualTo(path, value);
                case GREATER_THAN:
                    return Filter.greaterThan(path, value);
                case GREATER_THAN_OR_EQUAL:
                    return Filter.greaterThanOrEqualTo(path, value);
                default:
                    throw new IllegalStateException("No filter for " + operator);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldFilter that = (FieldFilter) o;
            return Arrays.equals(segments, that.segments)
                    && operator == that.operator
                    && Objects.equals(value, that.value)
                    && Objects.equals(anyOf, that.anyOf);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(segments) + Objects.hash(operator, value, anyOf);
        }

        @Override
        public String toString() {
            if (anyOf != null) {
                return "anyOf" + anyOf;
            }
            return (segments == null ? FieldPath.documentId() : FieldPath.of(segments))
                    + " "
                    + operator
                    + " "
                    + value;
        }
    }
}
