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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;
import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one Flink value into the Firestore value {@code FirestoreWrite} accepts for it, following
 * the type mapping {@link FirestoreTableSchema} checked: STRING to {@code String}, BIGINT to {@code
 * Long}, DOUBLE to {@code Double}, BOOLEAN to {@code Boolean}, BYTES to a {@code Blob},
 * TIMESTAMP_LTZ to a {@code Timestamp}, ARRAY to a list, ROW and MAP to a map, and the marked
 * fields to a {@code GeoPoint} or a {@code FirestoreDocumentReference}. A SQL {@code NULL} is a
 * Firestore null.
 */
@Internal
abstract class RowDataToFirestoreConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The field path, for a failure message. */
    final String path;

    private RowDataToFirestoreConverter(String path) {
        this.path = path;
    }

    /**
     * Converts a non-null Flink internal value.
     *
     * @throws IllegalArgumentException naming the field if the value has no Firestore form
     */
    abstract Object convertNonNull(Object value);

    /** Converts a Flink internal value, {@code null} included. */
    final Object convert(Object value) {
        return value == null ? null : convertNonNull(value);
    }

    /**
     * Creates the converter of the field at {@code path}.
     *
     * @param schema the checked schema, which knows the markers
     * @param path the field path
     * @param type the field's type
     * @return the converter
     */
    static RowDataToFirestoreConverter create(
            FirestoreTableSchema schema, String path, LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return schema.isReference(path) ? new ToReference(path) : new ToString(path);
            case BOOLEAN:
            case BIGINT:
            case DOUBLE:
                return new Identity(path);
            case BINARY:
            case VARBINARY:
                return new ToBlob(path);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ToTimestamp(path);
            case ARRAY:
                LogicalType element = ((ArrayType) type).getElementType();
                return new ToList(path, element, create(schema, path, element));
            case MAP:
                MapType map = (MapType) type;
                return new ToMap(
                        path,
                        map.getKeyType(),
                        map.getValueType(),
                        create(schema, path + ".value", map.getValueType()));
            case ROW:
                RowType row = (RowType) type;
                if (schema.isGeoPoint(path)) {
                    return new ToGeoPoint(path);
                }
                return new ToFields(schema, path + ".", row, -1);
            default:
                // FirestoreTableSchema refused every other type when the table was planned.
                throw new IllegalStateException(
                        "No Firestore form for field '" + path + "' of type " + type);
        }
    }

    private static final class Identity extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        Identity(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            return value;
        }
    }

    private static final class ToString extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        ToString(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            return value.toString();
        }
    }

    private static final class ToReference extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        ToReference(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            try {
                return FirestoreDocumentReference.of(value.toString());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is a reference, but its value is not a document path: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    private static final class ToBlob extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        ToBlob(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            return Blob.fromBytes((byte[]) value);
        }
    }

    private static final class ToTimestamp extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        ToTimestamp(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            TimestampData timestamp = (TimestampData) value;
            long millis = timestamp.getMillisecond();
            return Timestamp.ofTimeSecondsAndNanos(
                    Math.floorDiv(millis, 1000L),
                    (int) Math.floorMod(millis, 1000L) * 1_000_000
                            + timestamp.getNanoOfMillisecond());
        }
    }

    private static final class ToGeoPoint extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;

        ToGeoPoint(String path) {
            super(path);
        }

        @Override
        Object convertNonNull(Object value) {
            RowData row = (RowData) value;
            if (row.isNullAt(0) || row.isNullAt(1)) {
                throw new IllegalArgumentException(
                        "Field '"
                                + path
                                + "' is a geographical point, so its latitude and longitude must"
                                + " not be NULL; write a NULL point instead.");
            }
            try {
                return new GeoPoint(row.getDouble(0), row.getDouble(1));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Field '" + path + "' is not a valid geographical point: " + e.getMessage(),
                        e);
            }
        }
    }

    private static final class ToList extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;
        private final ArrayData.ElementGetter getter;
        private final RowDataToFirestoreConverter element;

        ToList(String path, LogicalType elementType, RowDataToFirestoreConverter element) {
            super(path);
            this.getter = ArrayData.createElementGetter(elementType);
            this.element = element;
        }

        @Override
        Object convertNonNull(Object value) {
            ArrayData array = (ArrayData) value;
            List<Object> list = new ArrayList<>(array.size());
            for (int i = 0; i < array.size(); i++) {
                list.add(element.convert(getter.getElementOrNull(array, i)));
            }
            return list;
        }
    }

    private static final class ToMap extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;
        private final ArrayData.ElementGetter keyGetter;
        private final ArrayData.ElementGetter valueGetter;
        private final RowDataToFirestoreConverter value;

        ToMap(
                String path,
                LogicalType keyType,
                LogicalType valueType,
                RowDataToFirestoreConverter value) {
            super(path);
            this.keyGetter = ArrayData.createElementGetter(keyType);
            this.valueGetter = ArrayData.createElementGetter(valueType);
            this.value = value;
        }

        @Override
        Object convertNonNull(Object data) {
            MapData map = (MapData) data;
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            Map<String, Object> fields = new LinkedHashMap<>(map.size() * 2);
            for (int i = 0; i < map.size(); i++) {
                Object key = keyGetter.getElementOrNull(keys, i);
                if (key == null) {
                    throw new IllegalArgumentException(
                            "Field '"
                                    + path
                                    + "' is a map with a NULL key, which is not a field name.");
                }
                fields.put(key.toString(), value.convert(valueGetter.getElementOrNull(values, i)));
            }
            return fields;
        }
    }

    /**
     * Creates the converter of a table row: a map of every column but the document-id column.
     *
     * @param schema the checked schema
     * @return the converter, whose result is a {@code Map<String, Object>}
     */
    static RowDataToFirestoreConverter forDocument(FirestoreTableSchema schema) {
        return new ToFields(schema, "", schema.getRowType(), schema.getKeyIndex());
    }

    private static final class ToFields extends RowDataToFirestoreConverter {
        private static final long serialVersionUID = 1L;
        private final String[] names;
        private final RowData.FieldGetter[] getters;
        private final RowDataToFirestoreConverter[] fields;

        /**
         * A map of the row's fields, each at {@code prefix} followed by its name, leaving out the
         * field at {@code skipIndex} ({@code -1} for none).
         */
        ToFields(FirestoreTableSchema schema, String prefix, RowType row, int skipIndex) {
            super(prefix.isEmpty() ? "" : prefix.substring(0, prefix.length() - 1));
            List<String> names = new ArrayList<>();
            List<RowData.FieldGetter> getters = new ArrayList<>();
            List<RowDataToFirestoreConverter> fields = new ArrayList<>();
            for (int i = 0; i < row.getFieldCount(); i++) {
                if (i == skipIndex) {
                    continue;
                }
                String name = row.getFieldNames().get(i);
                names.add(name);
                getters.add(RowData.createFieldGetter(row.getTypeAt(i), i));
                fields.add(create(schema, prefix + name, row.getTypeAt(i)));
            }
            this.names = names.toArray(new String[0]);
            this.getters = getters.toArray(new RowData.FieldGetter[0]);
            this.fields = fields.toArray(new RowDataToFirestoreConverter[0]);
        }

        @Override
        Object convertNonNull(Object value) {
            RowData row = (RowData) value;
            Map<String, Object> map = new LinkedHashMap<>(names.length * 2);
            for (int i = 0; i < names.length; i++) {
                map.put(names[i], fields[i].convert(getters[i].getFieldOrNull(row)));
            }
            return map;
        }
    }
}
