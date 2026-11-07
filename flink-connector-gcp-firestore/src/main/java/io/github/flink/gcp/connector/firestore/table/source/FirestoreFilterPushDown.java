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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.firestore.common.table.EqualityMatcher;
import io.github.flink.gcp.connector.firestore.common.table.EqualityMatcher.Equality;
import io.github.flink.gcp.connector.firestore.common.table.PushedFilters;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Translates the equalities {@link EqualityMatcher} finds in the predicates of a {@code firestore}
 * scan into equality filters of its collection query ({@code docs/adr/0197}).
 *
 * <p>Every filter is a necessary condition of the predicate it came from, never an exact one: the
 * service compares numbers across integer and floating-point types, matches {@code NaN} to {@code
 * NaN}, and treats {@code -0.0} as {@code 0}, so it can return a document Flink's predicate
 * rejects. Every predicate therefore stays a Flink residual, and what each filter must never do is
 * drop a document Flink would keep. That is why only these equalities translate:
 *
 * <ul>
 *   <li>on the document id column, a value the service can store as an id in the collection,
 *       translated to a filter on the document's name;
 *   <li>on any other physical column, or a field nested in {@code ROW} columns, one that is not a
 *       geographical point's field and that the {@code reference-field-paths} marker does not name
 *       (a reference never equals a string), each name one literal path segment;
 *   <li>of type {@code STRING}/{@code VARCHAR}/{@code CHAR}, {@code BIGINT}, {@code DOUBLE}, {@code
 *       BOOLEAN}, {@code BYTES}/{@code VARBINARY}/{@code BINARY}, or {@code TIMESTAMP_LTZ} of
 *       precision 6 or more, which the converter reads without changing the stored value;
 *   <li>with a string or bytes literal of at most 1,500 bytes, the prefix of a value a
 *       Standard-edition database considers in a query, and a timestamp the service can represent.
 * </ul>
 *
 * <p>{@code IS NULL} is declined because a missing field, which Flink reads as {@code NULL},
 * matches no filter.
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
    static PushedFilters<FieldEquality> translate(
            FirestoreTableSchema schema, List<ResolvedExpression> filters) {
        return EqualityMatcher.translate(
                schema.getRowType(), filters, equality -> translate(schema, equality));
    }

    private static Optional<FieldEquality> translate(
            FirestoreTableSchema schema, Equality equality) {
        if (equality.isColumn(schema.getKeyIndex())) {
            // An id the service cannot store names no document, and the client library refuses
            // some of them (an empty one, a path) before sending the query.
            return equality.valueAs(String.class)
                    .filter(FirestoreDocumentLookups::isStorableId)
                    .map(FieldEquality::documentId);
        }
        int length = equality.path().size();
        for (int prefix = 1; prefix < length; prefix++) {
            // A geographical point is one value, not a map of a latitude and a longitude.
            if (schema.isGeoPoint(equality.joinedPath(prefix))) {
                return Optional.empty();
            }
        }
        if (schema.isReference(equality.joinedPath(length))) {
            return Optional.empty();
        }
        return value(equality.type(), equality)
                .map(value -> FieldEquality.field(equality.path(), value));
    }

    private static Optional<Serializable> value(LogicalType type, Equality equality) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // The planner presents a CHAR column bare only beside a literal of its length;
                // the converter reads a stored string unpadded, as under VARCHAR.
                return equality.valueAs(String.class)
                        .filter(
                                value ->
                                        value.getBytes(StandardCharsets.UTF_8).length
                                                <= MAX_INDEXED_VALUE_BYTES)
                        .map(Serializable.class::cast);
            case BIGINT:
                return equality.longValue().map(Serializable.class::cast);
            case DOUBLE:
                return equality.doubleValue().map(Serializable.class::cast);
            case BOOLEAN:
                return equality.valueAs(Boolean.class).map(Serializable.class::cast);
            case BINARY:
            case VARBINARY:
                return equality.valueAs(byte[].class)
                        .filter(value -> value.length <= MAX_INDEXED_VALUE_BYTES)
                        .map(Blob::fromBytes);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (((LocalZonedTimestampType) type).getPrecision() < STORED_TIMESTAMP_PRECISION) {
                    // The converter truncates a stored timestamp to the column's precision,
                    // so Flink keeps documents a filter on the literal would not match.
                    return Optional.empty();
                }
                return equality.valueAs(Instant.class)
                        .filter(FirestoreFilterPushDown::isRepresentable)
                        .map(
                                value ->
                                        Timestamp.ofTimeSecondsAndNanos(
                                                value.getEpochSecond(), value.getNano()));
            default:
                // ARRAY, MAP and ROW are not scalar.
                return Optional.empty();
        }
    }

    private static boolean isRepresentable(Instant instant) {
        // Whole seconds suffice: MIN_VALUE carries no nanoseconds and MAX_VALUE all of them.
        return instant.getEpochSecond() >= Timestamp.MIN_VALUE.getSeconds()
                && instant.getEpochSecond() <= Timestamp.MAX_VALUE.getSeconds();
    }

    /**
     * One equality filter, on the document id or on a field path of literal segments. The value is
     * a {@code String}, {@code Long}, {@code Double}, {@code Boolean}, {@link Blob} or {@link
     * Timestamp}, each serializable with the query factory that carries it.
     */
    static final class FieldEquality implements Serializable {

        private static final long serialVersionUID = 1L;

        /** The segments of the field path, or {@code null} for the document id. */
        private final String[] segments;

        private final Serializable value;

        private FieldEquality(String[] segments, Serializable value) {
            this.segments = segments;
            this.value = value;
        }

        /** Returns an equality on the document id, which the client library takes as a string. */
        static FieldEquality documentId(String id) {
            return new FieldEquality(null, id);
        }

        /** Returns an equality on the field the segments address, outermost first. */
        static FieldEquality field(List<String> segments, Serializable value) {
            return new FieldEquality(segments.toArray(new String[0]), value);
        }

        /** Returns the field path the filter compares. */
        FieldPath path() {
            return segments == null ? FieldPath.documentId() : FieldPath.of(segments);
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
            return Arrays.equals(segments, that.segments) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(segments) + Objects.hashCode(value);
        }

        @Override
        public String toString() {
            return path() + " == " + value;
        }
    }
}
