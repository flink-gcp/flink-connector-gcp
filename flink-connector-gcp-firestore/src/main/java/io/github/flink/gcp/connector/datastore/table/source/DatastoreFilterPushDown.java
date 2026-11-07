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
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.firestore.common.table.EqualityMatcher;
import io.github.flink.gcp.connector.firestore.common.table.EqualityMatcher.Equality;
import io.github.flink.gcp.connector.firestore.common.table.PushedFilters;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Translates the equalities {@link EqualityMatcher} finds in the predicates of a {@code datastore}
 * scan into equality filters of its kind query ({@code docs/adr/0184}).
 *
 * <p>An equality on the key column becomes a filter on {@code __key__}, which is always indexed, so
 * it translates whatever the table's options say. A Datastore filter never matches a value excluded
 * from indexes, so an equality on a property is a necessary condition of its predicate only for the
 * entities whose filtered properties are indexed: such equalities translate only when the table
 * opts in with {@code scan.filter-push-down.enabled}, the user's assertion that every one is.
 *
 * <p>Beyond that, each filter must never drop an entity Flink would keep, and it may return
 * entities Flink rejects: an equality on an integer also matches an array holding it, one on a
 * string also matches the same bytes stored as a blob, and one on a property path also matches a
 * top-level property whose name is the whole path, dots included. Every predicate therefore stays a
 * Flink residual. Only these equalities translate:
 *
 * <ul>
 *   <li>on the key column, a name or an id that can name a root entity of the kind, other than a
 *       name that begins and ends with two underscores;
 *   <li>on any other physical column, or a field nested in {@code ROW} columns, one that {@code
 *       sink.unindexed-columns} does not name and that is not inside a geographical point or a key
 *       value, the names joined by dots into a property path. The API has no quoting for a filter's
 *       property, so a name holding a dot reads as a path too, which also matches the property of
 *       that name;
 *   <li>of type {@code STRING}/{@code VARCHAR}/{@code CHAR}, {@code BIGINT}, {@code BOOLEAN},
 *       {@code BYTES}/{@code VARBINARY}/{@code BINARY}, or {@code TIMESTAMP_LTZ} of precision 6 or
 *       more, which the converter reads only from a stored value of that type, and {@code DOUBLE}
 *       under a finite literal with a fractional part, which no integer equals;
 *   <li>with a string or bytes literal of at most 1,500 bytes, the longest value the service
 *       indexes, and a timestamp of whole microseconds the service can represent.
 * </ul>
 *
 * <p>A {@code DOUBLE} equality on an integral literal, zero included, is declined: the converter
 * reads a stored integer into the column, which an equality on a floating-point value does not
 * match, and the service tells {@code 0.0} from {@code -0.0}. Only equalities translate, so the
 * query stays one the source cuts into key ranges.
 */
@Internal
final class DatastoreFilterPushDown {

    /** The property a filter names to compare entity keys. */
    private static final String KEY_PROPERTY = "__key__";

    /** The bytes of a string or bytes value the service indexes; a longer literal is refused. */
    private static final int MAX_INDEXED_VALUE_BYTES = 1_500;

    /** The precision the service stores a timestamp at. */
    private static final int STORED_TIMESTAMP_PRECISION = 6;

    private DatastoreFilterPushDown() {}

    /**
     * Translates the planner's predicates.
     *
     * @param schema the table's schema
     * @param keys the keys of the table's kind, namespace and database
     * @param properties whether equalities on properties translate, which {@code
     *     scan.filter-push-down.enabled} decides
     * @param filters the predicates, each one conjunct of the scan's condition
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    static PushedFilters<Filter> translate(
            DatastoreTableSchema schema,
            DatastoreLookupKeys keys,
            boolean properties,
            List<ResolvedExpression> filters) {
        return EqualityMatcher.translate(
                schema.getRowType(),
                filters,
                equality -> translate(schema, keys, properties, equality));
    }

    /**
     * Returns the query reading the kind under the filters, or {@code null} when there are none and
     * the scan reads the kind whole.
     *
     * @param kind the kind
     * @param filters the translated filters
     * @return the query, or {@code null}
     */
    @Nullable
    static Query query(String kind, List<Filter> filters) {
        if (filters.isEmpty()) {
            return null;
        }
        Filter filter =
                filters.size() == 1
                        ? filters.get(0)
                        : Filter.newBuilder()
                                .setCompositeFilter(
                                        CompositeFilter.newBuilder()
                                                .setOp(CompositeFilter.Operator.AND)
                                                .addAllFilters(filters))
                                .build();
        return Query.newBuilder()
                .addKind(KindExpression.newBuilder().setName(kind))
                .setFilter(filter)
                .build();
    }

    private static Optional<Filter> translate(
            DatastoreTableSchema schema,
            DatastoreLookupKeys keys,
            boolean properties,
            Equality equality) {
        if (equality.isColumn(schema.getKeyIndex())) {
            Optional<Key> key =
                    schema.isKeyId()
                            ? equality.longValue().map(keys::key)
                            : equality.valueAs(String.class)
                                    // The emulator stores a name of four underscores, which
                                    // DatastoreTableSchema.isReserved lets through; whether the
                                    // service takes one in a filter is unmeasured.
                                    .filter(name -> !isUnderscoreBracketed(name))
                                    .map(keys::key);
            return key.map(value -> equal(KEY_PROPERTY, Value.newBuilder().setKeyValue(value)));
        }
        if (!properties || schema.isUnindexed(equality.path().get(0))) {
            return Optional.empty();
        }
        int length = equality.path().size();
        for (int prefix = 1; prefix < length; prefix++) {
            String path = equality.joinedPath(prefix);
            // A geographical point and a key value are each one value, not an embedded entity.
            if (schema.isGeoPoint(path) || schema.isReference(path)) {
                return Optional.empty();
            }
        }
        return value(equality.type(), equality)
                .map(value -> equal(equality.joinedPath(length), value));
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

    private static boolean isUnderscoreBracketed(String name) {
        return name.length() >= 4 && name.startsWith("__") && name.endsWith("__");
    }

    private static Optional<Value.Builder> value(LogicalType type, Equality equality) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // The planner presents a CHAR column bare only beside a literal of its length; the
                // converter reads a stored string unpadded, as under VARCHAR.
                return equality.valueAs(String.class)
                        .filter(
                                value ->
                                        value.getBytes(StandardCharsets.UTF_8).length
                                                <= MAX_INDEXED_VALUE_BYTES)
                        .map(value -> Value.newBuilder().setStringValue(value));
            case BIGINT:
                return equality.longValue().map(value -> Value.newBuilder().setIntegerValue(value));
            case DOUBLE:
                Optional<Double> number = equality.doubleValue();
                // The converter reads a stored integer into a DOUBLE column, and the service
                // matches no integer to a floating-point value, so only a value no integer equals
                // translates. NaN and the infinities are left out: what the service does with them
                // in a filter is unmeasured.
                return number.filter(DatastoreFilterPushDown::isFraction)
                        .map(value -> Value.newBuilder().setDoubleValue(value));
            case BOOLEAN:
                return equality.valueAs(Boolean.class)
                        .map(value -> Value.newBuilder().setBooleanValue(value));
            case BINARY:
            case VARBINARY:
                return equality.valueAs(byte[].class)
                        .filter(value -> value.length <= MAX_INDEXED_VALUE_BYTES)
                        .map(value -> Value.newBuilder().setBlobValue(ByteString.copyFrom(value)));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (((LocalZonedTimestampType) type).getPrecision() < STORED_TIMESTAMP_PRECISION) {
                    // The converter truncates a stored timestamp to the column's precision, so
                    // Flink keeps entities a filter on the literal would not match.
                    return Optional.empty();
                }
                return equality.valueAs(Instant.class)
                        .filter(DatastoreFilterPushDown::isStorable)
                        .map(
                                value ->
                                        Value.newBuilder()
                                                .setTimestampValue(
                                                        com.google.protobuf.Timestamp.newBuilder()
                                                                .setSeconds(value.getEpochSecond())
                                                                .setNanos(value.getNano())));
            default:
                // ARRAY and ROW (embedded entities, geographical points, key values) are not
                // scalar.
                return Optional.empty();
        }
    }

    /** Whether the value is finite and has a fractional part, so that no integer equals it. */
    private static boolean isFraction(double value) {
        return Double.isFinite(value) && value != Math.rint(value);
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
}
