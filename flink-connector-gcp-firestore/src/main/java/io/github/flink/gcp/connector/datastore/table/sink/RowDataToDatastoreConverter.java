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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.LatLngValue;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Turns one Flink value into the Datastore value an entity property holds, following the type
 * mapping {@link DatastoreTableSchema} checked: STRING to a string, BIGINT to an integer, DOUBLE to
 * a floating-point number, BOOLEAN to a boolean, BYTES to a blob, TIMESTAMP_LTZ to a timestamp,
 * ARRAY to an array and ROW to an embedded entity without a key, except for the ROWs the schema
 * markers name, which are geographical points and key values. A SQL {@code NULL} is a Datastore
 * null.
 *
 * <p>A key value is built in the table's own project, database and namespace, from its last path
 * element and its ancestors, root first. A path element holds a kind and exactly one of a name and
 * an id; a value that is not a complete key fails the record, as a geographical point with a NULL
 * or out-of-range coordinate does.
 *
 * <p>A converter built as excluded marks its value, and every value nested in it, excluded from
 * indexes. An array value itself never carries the mark, which Datastore refuses on one: its
 * elements do.
 */
@Internal
abstract class RowDataToDatastoreConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Whether the value is excluded from indexes. */
    final boolean excluded;

    private RowDataToDatastoreConverter(boolean excluded) {
        this.excluded = excluded;
    }

    /**
     * Converts a non-null Flink internal value.
     *
     * @throws IllegalArgumentException naming the field if the value has no Datastore form
     */
    abstract Value<?> convertNonNull(Object value);

    /** Converts a Flink internal value, {@code null} included. */
    final Value<?> convert(Object value) {
        return value == null
                ? NullValue.newBuilder().setExcludeFromIndexes(excluded).build()
                : convertNonNull(value);
    }

    /**
     * Creates the converter of the field at {@code path}.
     *
     * @param context the checked schema and the partition key values are written in
     * @param path the field path
     * @param type the field's type
     * @param excluded whether the value is excluded from indexes
     * @return the converter
     */
    private static RowDataToDatastoreConverter create(
            Context context, String path, LogicalType type, boolean excluded) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new ToString(excluded);
            case BOOLEAN:
                return new ToBoolean(excluded);
            case BIGINT:
                return new ToLong(excluded);
            case DOUBLE:
                return new ToDouble(excluded);
            case BINARY:
            case VARBINARY:
                return new ToBlob(excluded);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ToTimestamp(excluded);
            case ARRAY:
                LogicalType element = ((ArrayType) type).getElementType();
                return new ToList(excluded, element, create(context, path, element, excluded));
            case ROW:
                if (context.schema.isGeoPoint(path)) {
                    return new ToLatLng(excluded, path);
                }
                if (context.schema.isReference(path)) {
                    return new ToKey(
                            excluded,
                            path,
                            context.database,
                            context.namespace,
                            ((RowType) type).getFieldCount() > ToKey.ANCESTORS);
                }
                return new ToEntity(
                        excluded, Properties.of(context, path + ".", (RowType) type, excluded));
            default:
                // DatastoreTableSchema refused every other type when the table was planned.
                throw new IllegalStateException("No Datastore form for type " + type);
        }
    }

    /**
     * Creates the properties of a table row: every column but the key column, each excluded from
     * indexes when {@code sink.unindexed-columns} names it.
     *
     * @param schema the checked schema
     * @param database the database the table's key values address
     * @param namespace the namespace the table's key values address, empty for the default one
     * @return the properties
     */
    static Properties forEntity(
            DatastoreTableSchema schema, DatabaseDestination database, String namespace) {
        Context context = new Context(schema, database, namespace);
        RowType row = schema.getRowType();
        List<String> names = new ArrayList<>();
        List<RowData.FieldGetter> getters = new ArrayList<>();
        List<RowDataToDatastoreConverter> values = new ArrayList<>();
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (i == schema.getKeyIndex()) {
                continue;
            }
            String name = row.getFieldNames().get(i);
            names.add(name);
            getters.add(RowData.createFieldGetter(row.getTypeAt(i), i));
            values.add(create(context, name, row.getTypeAt(i), schema.isUnindexed(name)));
        }
        return new Properties(names, getters, values);
    }

    /** What a converter is created against: the schema's markers and the table's partition. */
    private static final class Context {
        private final DatastoreTableSchema schema;
        private final DatabaseDestination database;
        private final String namespace;

        private Context(
                DatastoreTableSchema schema, DatabaseDestination database, String namespace) {
            this.schema = schema;
            this.database = database;
            this.namespace = namespace;
        }
    }

    /** The named values of a row, set as the properties of an entity. */
    static final class Properties implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String[] names;
        private final RowData.FieldGetter[] getters;
        private final RowDataToDatastoreConverter[] values;

        private Properties(
                List<String> names,
                List<RowData.FieldGetter> getters,
                List<RowDataToDatastoreConverter> values) {
            this.names = names.toArray(new String[0]);
            this.getters = getters.toArray(new RowData.FieldGetter[0]);
            this.values = values.toArray(new RowDataToDatastoreConverter[0]);
        }

        /** The properties of a nested ROW, each excluded from indexes when it is. */
        private static Properties of(
                Context context, String prefix, RowType row, boolean excluded) {
            List<String> names = new ArrayList<>();
            List<RowData.FieldGetter> getters = new ArrayList<>();
            List<RowDataToDatastoreConverter> values = new ArrayList<>();
            for (int i = 0; i < row.getFieldCount(); i++) {
                String name = row.getFieldNames().get(i);
                names.add(name);
                getters.add(RowData.createFieldGetter(row.getTypeAt(i), i));
                values.add(create(context, prefix + name, row.getTypeAt(i), excluded));
            }
            return new Properties(names, getters, values);
        }

        /**
         * Sets every property of {@code row} on {@code entity}.
         *
         * @param row the row
         * @param entity the entity builder
         * @param <B> the entity builder type
         * @return the entity builder
         */
        <B extends BaseEntity.Builder<?, B>> B setAll(RowData row, B entity) {
            for (int i = 0; i < names.length; i++) {
                entity.set(names[i], values[i].convert(getters[i].getFieldOrNull(row)));
            }
            return entity;
        }
    }

    private static final class ToString extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToString(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            return StringValue.newBuilder(value.toString()).setExcludeFromIndexes(excluded).build();
        }
    }

    private static final class ToBoolean extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToBoolean(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            return BooleanValue.newBuilder((Boolean) value).setExcludeFromIndexes(excluded).build();
        }
    }

    private static final class ToLong extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToLong(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            return LongValue.newBuilder((Long) value).setExcludeFromIndexes(excluded).build();
        }
    }

    private static final class ToDouble extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToDouble(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            return DoubleValue.newBuilder((Double) value).setExcludeFromIndexes(excluded).build();
        }
    }

    private static final class ToBlob extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToBlob(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            return BlobValue.newBuilder(Blob.copyFrom((byte[]) value))
                    .setExcludeFromIndexes(excluded)
                    .build();
        }
    }

    private static final class ToTimestamp extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        ToTimestamp(boolean excluded) {
            super(excluded);
        }

        @Override
        Value<?> convertNonNull(Object value) {
            TimestampData timestamp = (TimestampData) value;
            long millis = timestamp.getMillisecond();
            return TimestampValue.newBuilder(
                            Timestamp.ofTimeSecondsAndNanos(
                                    Math.floorDiv(millis, 1000L),
                                    (int) Math.floorMod(millis, 1000L) * 1_000_000
                                            + timestamp.getNanoOfMillisecond()))
                    .setExcludeFromIndexes(excluded)
                    .build();
        }
    }

    private static final class ToLatLng extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;
        private final String path;

        ToLatLng(boolean excluded, String path) {
            super(excluded);
            this.path = path;
        }

        @Override
        Value<?> convertNonNull(Object value) {
            RowData row = (RowData) value;
            if (row.isNullAt(0) || row.isNullAt(1)) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is a geographical point, so its latitude and longitude must"
                                + " not be NULL; write a NULL point instead.");
            }
            LatLng point;
            try {
                point = LatLng.of(row.getDouble(0), row.getDouble(1));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Field '" + path + "' is not a valid geographical point: " + e.getMessage(),
                        e);
            }
            return LatLngValue.newBuilder(point).setExcludeFromIndexes(excluded).build();
        }
    }

    private static final class ToKey extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;

        /** The index of the {@code ancestors} field, after a path element's three. */
        private static final int ANCESTORS = 3;

        /** The fields of a path element: kind, name and id. */
        private static final int PATH_ELEMENT_FIELDS = 3;

        private final String path;
        private final DatabaseDestination database;
        private final String namespace;
        private final boolean withAncestors;

        ToKey(
                boolean excluded,
                String path,
                DatabaseDestination database,
                String namespace,
                boolean withAncestors) {
            super(excluded);
            this.path = path;
            this.database = database;
            this.namespace = namespace;
            this.withAncestors = withAncestors;
        }

        @Override
        Value<?> convertNonNull(Object value) {
            RowData row = (RowData) value;
            List<PathElement> ancestors = Collections.emptyList();
            if (withAncestors && !row.isNullAt(ANCESTORS)) {
                ArrayData array = row.getArray(ANCESTORS);
                ancestors = new ArrayList<>(array.size());
                for (int i = 0; i < array.size(); i++) {
                    if (array.isNullAt(i)) {
                        throw new IllegalArgumentException(
                                "Field '"
                                        + path
                                        + "' is a key value, but its ancestor "
                                        + (i + 1)
                                        + " is NULL; every ancestor is a path element.");
                    }
                    ancestors.add(
                            element(
                                    array.getRow(i, PATH_ELEMENT_FIELDS),
                                    "its ancestor " + (i + 1)));
                }
            }
            PathElement last = element(row, "its last path element");
            Key.Builder key =
                    last.hasId()
                            ? Key.newBuilder(
                                    database.getProject(),
                                    last.getKind(),
                                    last.getId(),
                                    database.getDatabaseId())
                            : Key.newBuilder(
                                    database.getProject(),
                                    last.getKind(),
                                    last.getName(),
                                    database.getDatabaseId());
            return KeyValue.newBuilder(key.setNamespace(namespace).addAncestors(ancestors).build())
                    .setExcludeFromIndexes(excluded)
                    .build();
        }

        /** Reads a path element: a kind and exactly one of a name and an id. */
        private PathElement element(RowData element, String which) {
            if (element.isNullAt(0) || element.getString(0).toString().isEmpty()) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is a key value, but "
                                + which
                                + " has no kind; every path element has one.");
            }
            String kind = element.getString(0).toString();
            boolean hasName = !element.isNullAt(1);
            if (hasName == !element.isNullAt(2)) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is a key value, but "
                                + which
                                + (hasName
                                        ? " has both a name and an id"
                                        : " has neither a name nor an id")
                                + "; a complete key's path element has exactly one.");
            }
            try {
                return hasName
                        ? PathElement.of(kind, element.getString(1).toString())
                        : PathElement.of(kind, element.getLong(2));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is not a valid key value: "
                                + which
                                + " is refused: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    private static final class ToList extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;
        private final ArrayData.ElementGetter getter;
        private final RowDataToDatastoreConverter element;

        ToList(boolean excluded, LogicalType elementType, RowDataToDatastoreConverter element) {
            super(excluded);
            this.getter = ArrayData.createElementGetter(elementType);
            this.element = element;
        }

        @Override
        Value<?> convertNonNull(Object value) {
            ArrayData array = (ArrayData) value;
            ListValue.Builder list = ListValue.newBuilder();
            for (int i = 0; i < array.size(); i++) {
                list.addValue(element.convert(getter.getElementOrNull(array, i)));
            }
            return list.build();
        }
    }

    private static final class ToEntity extends RowDataToDatastoreConverter {
        private static final long serialVersionUID = 1L;
        private final Properties properties;

        ToEntity(boolean excluded, Properties properties) {
            super(excluded);
            this.properties = properties;
        }

        @Override
        Value<?> convertNonNull(Object value) {
            FullEntity<IncompleteKey> entity =
                    properties.setAll((RowData) value, FullEntity.newBuilder()).build();
            return EntityValue.newBuilder(entity).setExcludeFromIndexes(excluded).build();
        }
    }
}
