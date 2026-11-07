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
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.firestore.common.table.OrderedStrings;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Comparison;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Operator;
import io.github.flink.gcp.connector.firestore.common.table.PushedFilters;
import io.github.flink.gcp.connector.firestore.common.table.TimestampBound;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Translates the comparisons {@link PredicateMatcher} finds in the predicates of a {@code
 * datastore} scan into filters of its kind query ({@code docs/adr/0184}).
 *
 * <p>An equality on the key column becomes a filter on {@code __key__}, which is always indexed, so
 * it translates whatever the table's options say. A Datastore filter never matches a value excluded
 * from indexes, so a comparison on a property is a necessary condition of its predicate only for
 * the entities whose filtered properties are indexed: such comparisons, and a range on the key,
 * translate only when the table opts in with {@code scan.filter-push-down.enabled}, the user's
 * assertion that every one is. Any filter but an equality makes the source read the query as one
 * split.
 *
 * <p>Beyond that, each filter must never drop an entity Flink would keep, and it may return
 * entities Flink rejects: an equality on an integer also matches an array holding it, one on a
 * string also matches the same bytes stored as a blob, and one on a property path also matches a
 * top-level property whose name is the whole path, dots included. A range crosses value types in
 * the service's order (null, then integers and timestamps by their microseconds, booleans, strings
 * and blobs by their bytes, floating-point values), so it matches every value of the column's own
 * type it should and values of other types the converter reads as mismatches. Every predicate
 * therefore stays a Flink residual. Only these comparisons translate:
 *
 * <ul>
 *   <li>on the key column, a name or an id that can name a root entity of the kind, other than a
 *       name that begins and ends with two underscores: {@code =}, and with the option {@code <},
 *       {@code <=}, {@code >} and {@code >=};
 *   <li>on any other physical column, or a field nested in {@code ROW} columns, one that {@code
 *       sink.unindexed-columns} does not name and that is not inside a geographical point or a key
 *       value, the names joined by dots into a property path. The API has no quoting for a filter's
 *       property, so a name holding a dot reads as a path too, which also matches the property of
 *       that name;
 *   <li>{@code =}, {@code <>}, {@code <}, {@code <=}, {@code >}, {@code >=} and {@code IS NOT NULL}
 *       of type {@code STRING}/{@code VARCHAR}, {@code BIGINT}, {@code BOOLEAN}, {@code BYTES}/
 *       {@code VARBINARY}, {@code TIMESTAMP_LTZ} or {@code DOUBLE}, and {@code =} and {@code IS NOT
 *       NULL} of {@code CHAR} and {@code BINARY}, with a string or bytes literal of at most 1,500
 *       bytes, the longest value the service indexes. A {@code TIMESTAMP_LTZ} compares the
 *       whole-microsecond bounds {@link TimestampBound} rounds to the column's precision, and a
 *       range on a string or a key name takes only a literal {@link OrderedStrings} finds ordered
 *       alike by UTF-8 bytes, as the service orders, and by UTF-16 code units, as Flink may.
 * </ul>
 *
 * <p>The converter reads a stored integer into a {@code DOUBLE} column, and the service orders
 * every integer below every floating-point value and matches no integer to one. So an integral
 * {@code DOUBLE} equality becomes {@code IN} of the integer and the floating-point value, {@code
 * -0.0} included for zero; {@code >} and {@code >=} compare an integer bound, which every
 * floating-point value passes; {@code <}, {@code <=} and {@code <>} compare the value itself, which
 * every integer below it passes. A literal that is not finite translates to nothing.
 */
@Internal
final class DatastoreFilterPushDown {

    /** The property a filter names to compare entity keys. */
    private static final String KEY_PROPERTY = "__key__";

    /** The bytes of a string or bytes value the service indexes; a longer literal is refused. */
    private static final int MAX_INDEXED_VALUE_BYTES = 1_500;

    /** The largest magnitude of an integer the converter reads into a DOUBLE column. */
    private static final long MAX_EXACT_INTEGER = 1L << 53;

    private DatastoreFilterPushDown() {}

    /**
     * Translates the planner's predicates.
     *
     * @param schema the table's schema
     * @param keys the keys of the table's kind, namespace and database
     * @param properties whether comparisons on properties, and ranges on the key, translate, which
     *     {@code scan.filter-push-down.enabled} decides
     * @param filters the predicates, each one conjunct of the scan's condition
     * @return the predicates accepted and the filters they translate to; every predicate remains a
     *     residual
     */
    static PushedFilters<Filter> translate(
            DatastoreTableSchema schema,
            DatastoreLookupKeys keys,
            boolean properties,
            List<ResolvedExpression> filters) {
        return PredicateMatcher.translate(
                schema.getRowType(),
                filters,
                new PredicateMatcher.Translator<Filter>() {
                    @Override
                    public List<Filter> translate(Comparison comparison) {
                        return DatastoreFilterPushDown.translate(
                                schema, keys, properties, comparison);
                    }

                    @Override
                    public Filter anyOf(List<Filter> branches) {
                        return composite(CompositeFilter.Operator.OR, branches);
                    }

                    @Override
                    public int width(Filter filter) {
                        // An integral DOUBLE equality is an IN, one disjunct per value.
                        return filter.hasPropertyFilter()
                                        && filter.getPropertyFilter().getOp()
                                                == PropertyFilter.Operator.IN
                                ? filter.getPropertyFilter()
                                        .getValue()
                                        .getArrayValue()
                                        .getValuesCount()
                                : 1;
                    }
                });
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
                        : composite(CompositeFilter.Operator.AND, filters);
        return Query.newBuilder()
                .addKind(KindExpression.newBuilder().setName(kind))
                .setFilter(filter)
                .build();
    }

    private static Filter composite(CompositeFilter.Operator operator, List<Filter> filters) {
        return Filter.newBuilder()
                .setCompositeFilter(
                        CompositeFilter.newBuilder().setOp(operator).addAllFilters(filters))
                .build();
    }

    private static List<Filter> translate(
            DatastoreTableSchema schema,
            DatastoreLookupKeys keys,
            boolean properties,
            Comparison comparison) {
        Operator operator = comparison.operator();
        if (comparison.isColumn(schema.getKeyIndex())) {
            // Every entity has its key, so neither <> nor IS NOT NULL says anything worth sending.
            if (operator != Operator.EQUAL && !(properties && operator.isRange())) {
                return List.of();
            }
            Optional<Key> key =
                    schema.isKeyId()
                            ? comparison.longValue().map(keys::key)
                            : comparison
                                    .valueAs(String.class)
                                    // The emulator stores a name of four underscores, which
                                    // DatastoreTableSchema.isReserved lets through; whether the
                                    // service takes one in a filter is unmeasured.
                                    .filter(name -> !isUnderscoreBracketed(name))
                                    .filter(name -> OrderedStrings.admits(operator, name))
                                    .map(keys::key);
            return key.map(
                            value ->
                                    List.of(
                                            filter(
                                                    KEY_PROPERTY,
                                                    operator,
                                                    Value.newBuilder().setKeyValue(value))))
                    .orElse(List.of());
        }
        if (!properties || schema.isUnindexed(comparison.path().get(0))) {
            return List.of();
        }
        int length = comparison.path().size();
        for (int prefix = 1; prefix < length; prefix++) {
            String path = comparison.joinedPath(prefix);
            // A geographical point and a key value are each one value, not an embedded entity.
            if (schema.isGeoPoint(path) || schema.isReference(path)) {
                return List.of();
            }
        }
        String property = comparison.joinedPath(length);
        if (schema.isGeoPoint(property) || schema.isReference(property)) {
            // A ROW column read from one value, which no scalar filter below covers.
            return List.of();
        }
        if (operator == Operator.IS_NOT_NULL) {
            // Only a scalar: whether an empty array or an embedded entity matches != null is
            // unmeasured, and Flink reads either as a value that is not NULL.
            return comparison.hasScalarType()
                    ? List.of(
                            filter(
                                    property,
                                    Operator.NOT_EQUAL,
                                    Value.newBuilder().setNullValue(NullValue.NULL_VALUE)))
                    : List.of();
        }
        return values(property, comparison);
    }

    private static List<Filter> values(String property, Comparison comparison) {
        LogicalType type = comparison.type();
        Operator operator = comparison.operator();
        switch (type.getTypeRoot()) {
            case CHAR:
            case BINARY:
                // The planner presents a fixed-length column bare only beside a literal of its
                // length; the converter reads a stored value unpadded, as under VARCHAR. No
                // ordering of fixed-length values was measured.
                if (operator != Operator.EQUAL) {
                    return List.of();
                }
                return type.is(LogicalTypeRoot.CHAR)
                        ? string(property, comparison)
                        : bytes(property, comparison);
            case VARCHAR:
                return string(property, comparison);
            case VARBINARY:
                return bytes(property, comparison);
            case BIGINT:
                return one(
                        property,
                        operator,
                        comparison.longValue().map(v -> Value.newBuilder().setIntegerValue(v)));
            case BOOLEAN:
                return one(
                        property,
                        operator,
                        comparison
                                .valueAs(Boolean.class)
                                .map(v -> Value.newBuilder().setBooleanValue(v)));
            case DOUBLE:
                return comparison
                        .doubleValue()
                        .filter(Double::isFinite)
                        .map(v -> doubles(property, operator, v))
                        .orElse(List.of());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                List<Filter> bounds = new ArrayList<>();
                for (TimestampBound bound :
                        comparison.timestampBounds(
                                ((LocalZonedTimestampType) type).getPrecision())) {
                    bounds.add(filter(property, bound.operator(), timestamp(bound.value())));
                }
                return bounds;
            default:
                // ARRAY and ROW (embedded entities, geographical points, key values) are not
                // scalar.
                return List.of();
        }
    }

    private static List<Filter> string(String property, Comparison comparison) {
        return one(
                property,
                comparison.operator(),
                comparison
                        .valueAs(String.class)
                        .filter(
                                v ->
                                        v.getBytes(StandardCharsets.UTF_8).length
                                                <= MAX_INDEXED_VALUE_BYTES)
                        .filter(v -> OrderedStrings.admits(comparison.operator(), v))
                        .map(v -> Value.newBuilder().setStringValue(v)));
    }

    private static List<Filter> bytes(String property, Comparison comparison) {
        return one(
                property,
                comparison.operator(),
                comparison
                        .valueAs(byte[].class)
                        .filter(v -> v.length <= MAX_INDEXED_VALUE_BYTES)
                        .map(v -> Value.newBuilder().setBlobValue(ByteString.copyFrom(v))));
    }

    private static List<Filter> one(
            String property, Operator operator, Optional<Value.Builder> value) {
        return value.map(v -> List.of(filter(property, operator, v))).orElse(List.of());
    }

    /** The filters of a finite DOUBLE comparison; see the class comment. */
    private static List<Filter> doubles(String property, Operator operator, double value) {
        Value.Builder floating = Value.newBuilder().setDoubleValue(value);
        switch (operator) {
            case EQUAL:
                if (value != Math.rint(value) || Math.abs(value) > MAX_EXACT_INTEGER) {
                    // No integer the converter reads equals it.
                    return List.of(filter(property, operator, floating));
                }
                ArrayValue.Builder values =
                        ArrayValue.newBuilder()
                                .addValues(Value.newBuilder().setIntegerValue((long) value))
                                .addValues(floating);
                if (value == 0) {
                    // The service tells -0.0 from 0.0, and Flink does not.
                    values.addValues(Value.newBuilder().setDoubleValue(-value));
                }
                return List.of(
                        filter(
                                property,
                                PropertyFilter.Operator.IN,
                                Value.newBuilder().setArrayValue(values)));
            case GREATER_THAN:
                // An integer n read as a double passes n > v exactly when n > floor(v).
                return integerBound(property, Operator.GREATER_THAN, Math.floor(value));
            case GREATER_THAN_OR_EQUAL:
                return integerBound(property, Operator.GREATER_THAN_OR_EQUAL, Math.ceil(value));
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                // The service tells -0.0 from 0.0, and Flink's d <= -0.0 keeps a stored 0.0.
                return List.of(
                        filter(
                                property,
                                operator,
                                Value.newBuilder().setDoubleValue(value == 0 ? 0.0d : value)));
            default:
                return List.of(filter(property, operator, floating));
        }
    }

    private static List<Filter> integerBound(String property, Operator operator, double bound) {
        if (bound < Long.MIN_VALUE || bound >= 0x1p63) {
            return List.of();
        }
        return List.of(
                filter(property, operator, Value.newBuilder().setIntegerValue((long) bound)));
    }

    private static Value.Builder timestamp(Instant value) {
        return Value.newBuilder()
                .setTimestampValue(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(value.getEpochSecond())
                                .setNanos(value.getNano()));
    }

    private static Filter filter(String property, Operator operator, Value.Builder value) {
        return filter(property, operator(operator), value);
    }

    private static Filter filter(
            String property, PropertyFilter.Operator operator, Value.Builder value) {
        return Filter.newBuilder()
                .setPropertyFilter(
                        PropertyFilter.newBuilder()
                                .setProperty(PropertyReference.newBuilder().setName(property))
                                .setOp(operator)
                                .setValue(value))
                .build();
    }

    private static PropertyFilter.Operator operator(Operator operator) {
        switch (operator) {
            case EQUAL:
                return PropertyFilter.Operator.EQUAL;
            case NOT_EQUAL:
                return PropertyFilter.Operator.NOT_EQUAL;
            case LESS_THAN:
                return PropertyFilter.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return PropertyFilter.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return PropertyFilter.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return PropertyFilter.Operator.GREATER_THAN_OR_EQUAL;
            default:
                throw new IllegalArgumentException("No filter operator for " + operator);
        }
    }

    private static boolean isUnderscoreBracketed(String name) {
        return name.length() >= 4 && name.startsWith("__") && name.endsWith("__");
    }
}
