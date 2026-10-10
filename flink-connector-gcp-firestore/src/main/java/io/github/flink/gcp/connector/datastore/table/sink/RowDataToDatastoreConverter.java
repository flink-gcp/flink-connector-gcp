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
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns one Flink value into the Datastore value an entity property holds, following the type
 * mapping {@link DatastoreTableSchema} checked: STRING to a string, BIGINT to an integer, DOUBLE to
 * a floating-point number, BOOLEAN to a boolean, BYTES to a blob, TIMESTAMP_LTZ to a timestamp,
 * ARRAY to an array and ROW to an embedded entity without a key. A SQL {@code NULL} is a Datastore
 * null.
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

    /** Converts a non-null Flink internal value. */
    abstract Value<?> convertNonNull(Object value);

    /** Converts a Flink internal value, {@code null} included. */
    final Value<?> convert(Object value) {
        return value == null
                ? NullValue.newBuilder().setExcludeFromIndexes(excluded).build()
                : convertNonNull(value);
    }

    /**
     * Creates the converter of a value of {@code type}.
     *
     * @param type the field's type
     * @param excluded whether the value is excluded from indexes
     * @return the converter
     */
    static RowDataToDatastoreConverter create(LogicalType type, boolean excluded) {
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
                return new ToList(excluded, element, create(element, excluded));
            case ROW:
                return new ToEntity(excluded, Properties.of((RowType) type, excluded));
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
     * @return the properties
     */
    static Properties forEntity(DatastoreTableSchema schema) {
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
            values.add(create(row.getTypeAt(i), schema.isUnindexed(name)));
        }
        return new Properties(names, getters, values);
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
        private static Properties of(RowType row, boolean excluded) {
            List<String> names = new ArrayList<>();
            List<RowData.FieldGetter> getters = new ArrayList<>();
            List<RowDataToDatastoreConverter> values = new ArrayList<>();
            for (int i = 0; i < row.getFieldCount(); i++) {
                names.add(row.getFieldNames().get(i));
                getters.add(RowData.createFieldGetter(row.getTypeAt(i), i));
                values.add(create(row.getTypeAt(i), excluded));
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
