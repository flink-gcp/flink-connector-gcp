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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.Int32Value;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one value the client library decoded from a document into the Flink value of its column,
 * the read side of {@link FirestoreTableSchema}'s type mapping.
 *
 * <p>A value whose type does not match the column is a mismatch. Under {@link
 * TypeMismatchPolicy#NULL} the innermost nullable field around it reads as NULL; under {@link
 * TypeMismatchPolicy#FAIL}, or when no field around it is nullable, the read fails naming the
 * field. Only these values are read: a string, a reference into a marked STRING (as its path), an
 * integer or a 32-bit BSON integer into BIGINT, a floating-point number or an integer of at most
 * 2^53 in magnitude into DOUBLE, a boolean, bytes of subtype 0, a timestamp (truncated to the
 * column's precision), an array, a map into a MAP or a ROW, and a geographical point into a marked
 * ROW. A string or bytes are read whole: the length a CHAR, VARCHAR, BINARY or VARBINARY column
 * declares is neither checked nor padded to, as the Spanner table reads a STRING whatever its
 * declared length.
 */
@Internal
abstract class FirestoreToRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The largest magnitude a double holds every integer up to. */
    private static final long EXACT_DOUBLE_INTEGER = 1L << 53;

    /** The field path, for a failure message. */
    final String path;

    /** Whether the field may read as NULL. */
    final boolean nullable;

    /** What a mismatch inside this field does. */
    final TypeMismatchPolicy policy;

    private FirestoreToRowDataConverter(String path, LogicalType type, TypeMismatchPolicy policy) {
        this.path = path;
        this.nullable = type.isNullable();
        this.policy = policy;
    }

    /** Converts a non-null decoded value, or throws when its type does not match. */
    abstract Object convertNonNull(Object value) throws Mismatch;

    /**
     * Converts a decoded value: {@code null} reads as NULL where the field is nullable, and a
     * mismatch reads as NULL there too under {@link TypeMismatchPolicy#NULL}.
     */
    final Object convert(Object value) throws Mismatch {
        if (value == null) {
            if (!nullable) {
                throw new Mismatch(
                        path, "is missing or holds null, but the table declares it NOT NULL");
            }
            return null;
        }
        try {
            return convertNonNull(value);
        } catch (Mismatch mismatch) {
            if (nullable) {
                if (policy == TypeMismatchPolicy.NULL) {
                    return null;
                }
                mismatch.readableAsNull = true;
            }
            throw mismatch;
        }
    }

    Mismatch mismatch(Object value, String expected) {
        return new Mismatch(
                path,
                "holds a value of type "
                        + value.getClass().getName()
                        + ", which is not "
                        + expected);
    }

    /**
     * Converts a timestamp, its nanoseconds truncated to a multiple of {@code truncation}: {@code
     * 1} keeps them all. A timestamp's nanoseconds are never negative, also before the epoch, so
     * truncating them rounds down.
     */
    static TimestampData timestampData(Timestamp timestamp, int truncation) {
        int nanos = timestamp.getNanos() - timestamp.getNanos() % truncation;
        return TimestampData.fromEpochMillis(
                Math.addExact(Math.multiplyExact(timestamp.getSeconds(), 1000L), nanos / 1_000_000),
                nanos % 1_000_000);
    }

    /** A stored value the column's type cannot represent. */
    static final class Mismatch extends Exception {
        private static final long serialVersionUID = 1L;

        /** Whether a nullable field lies around the value, which the NULL policy reads as NULL. */
        private boolean readableAsNull;

        Mismatch(String path, String reason) {
            super("Field '" + path + "' " + reason + ".");
        }

        /**
         * Returns whether {@link TypeMismatchPolicy#NULL} would have read a nullable field around
         * the value as NULL instead of failing.
         */
        boolean readableAsNull() {
            return readableAsNull;
        }
    }

    /**
     * Creates the converter of the field at {@code path}.
     *
     * @param schema the checked schema, which knows the markers
     * @param path the field path
     * @param type the field's type
     * @param policy what a mismatch does
     * @return the converter
     */
    static FirestoreToRowDataConverter create(
            FirestoreTableSchema schema, String path, LogicalType type, TypeMismatchPolicy policy) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return schema.isReference(path)
                        ? new FromReference(path, type, policy)
                        : new FromString(path, type, policy);
            case BOOLEAN:
                return new FromBoolean(path, type, policy);
            case BIGINT:
                return new FromInteger(path, type, policy);
            case DOUBLE:
                return new FromDouble(path, type, policy);
            case BINARY:
            case VARBINARY:
                return new FromBlob(path, type, policy);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new FromTimestamp(path, type, policy);
            case ARRAY:
                LogicalType element = ((ArrayType) type).getElementType();
                return new FromList(path, type, policy, create(schema, path, element, policy));
            case MAP:
                MapType map = (MapType) type;
                return new FromMap(
                        path,
                        type,
                        policy,
                        create(schema, path + ".value", map.getValueType(), policy));
            case ROW:
                if (schema.isGeoPoint(path)) {
                    return new FromGeoPoint(path, type, policy);
                }
                RowType row = (RowType) type;
                FirestoreToRowDataConverter[] fields =
                        new FirestoreToRowDataConverter[row.getFieldCount()];
                for (int i = 0; i < fields.length; i++) {
                    fields[i] =
                            create(
                                    schema,
                                    path + "." + row.getFieldNames().get(i),
                                    row.getTypeAt(i),
                                    policy);
                }
                return new FromMapToRow(
                        path, type, policy, row.getFieldNames().toArray(new String[0]), fields);
            default:
                // FirestoreTableSchema refused every other type when the table was planned.
                throw new IllegalStateException(
                        "No Firestore form for field '" + path + "' of type " + type);
        }
    }

    private static final class FromString extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromString(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof String) {
                return StringData.fromString((String) value);
            }
            throw mismatch(value, "a string");
        }
    }

    private static final class FromReference extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromReference(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (!(value instanceof DocumentReference)) {
                throw mismatch(value, "a reference");
            }
            DocumentReference reference = (DocumentReference) value;
            // A decoded reference keeps its own database, but its path is relative to it: one
            // into another database would read as a path in this one.
            if (!reference.equals(reference.getFirestore().document(reference.getPath()))) {
                throw new Mismatch(
                        path, "holds " + reference + ", a reference into another database");
            }
            return StringData.fromString(reference.getPath());
        }
    }

    private static final class FromBoolean extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromBoolean(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof Boolean) {
                return value;
            }
            throw mismatch(value, "a boolean");
        }
    }

    private static final class FromInteger extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromInteger(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof Long) {
                return value;
            }
            if (value instanceof Int32Value) {
                return (long) ((Int32Value) value).value;
            }
            throw mismatch(value, "an integer");
        }
    }

    private static final class FromDouble extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromDouble(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof Double) {
                return value;
            }
            if (value instanceof Long) {
                long integer = (Long) value;
                if (integer >= -EXACT_DOUBLE_INTEGER && integer <= EXACT_DOUBLE_INTEGER) {
                    return (double) integer;
                }
                throw new Mismatch(
                        path,
                        "holds the integer "
                                + integer
                                + ", beyond the 2^53 in magnitude up to which a DOUBLE represents"
                                + " every integer exactly");
            }
            throw mismatch(value, "a floating-point number");
        }
    }

    private static final class FromBlob extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromBlob(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof Blob && ((Blob) value).subtype() == 0) {
                return ((Blob) value).toBytes();
            }
            throw mismatch(value, "bytes of subtype 0");
        }
    }

    private static final class FromTimestamp extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final int truncation;

        FromTimestamp(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
            int precision = ((LocalZonedTimestampType) type).getPrecision();
            int unit = 1;
            for (int digits = precision; digits < 9; digits++) {
                unit *= 10;
            }
            this.truncation = unit;
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (!(value instanceof Timestamp)) {
                throw mismatch(value, "a timestamp");
            }
            return timestampData((Timestamp) value, truncation);
        }
    }

    private static final class FromGeoPoint extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromGeoPoint(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (value instanceof GeoPoint) {
                GeoPoint point = (GeoPoint) value;
                return GenericRowData.of(point.getLatitude(), point.getLongitude());
            }
            throw mismatch(value, "a geographical point");
        }
    }

    private static final class FromList extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final FirestoreToRowDataConverter element;

        FromList(
                String path,
                LogicalType type,
                TypeMismatchPolicy policy,
                FirestoreToRowDataConverter element) {
            super(path, type, policy);
            this.element = element;
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (!(value instanceof List)) {
                throw mismatch(value, "an array");
            }
            List<?> list = (List<?>) value;
            Object[] elements = new Object[list.size()];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = element.convert(list.get(i));
            }
            return new GenericArrayData(elements);
        }
    }

    private static final class FromMap extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final FirestoreToRowDataConverter value;

        FromMap(
                String path,
                LogicalType type,
                TypeMismatchPolicy policy,
                FirestoreToRowDataConverter value) {
            super(path, type, policy);
            this.value = value;
        }

        @Override
        Object convertNonNull(Object data) throws Mismatch {
            if (!(data instanceof Map)) {
                throw mismatch(data, "a map");
            }
            Map<?, ?> map = (Map<?, ?>) data;
            Map<Object, Object> converted = new HashMap<>(map.size() * 2);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(
                        StringData.fromString((String) entry.getKey()),
                        value.convert(entry.getValue()));
            }
            return new GenericMapData(converted);
        }
    }

    private static final class FromMapToRow extends FirestoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final String[] names;
        private final FirestoreToRowDataConverter[] fields;

        FromMapToRow(
                String path,
                LogicalType type,
                TypeMismatchPolicy policy,
                String[] names,
                FirestoreToRowDataConverter[] fields) {
            super(path, type, policy);
            this.names = names;
            this.fields = fields;
        }

        @Override
        Object convertNonNull(Object value) throws Mismatch {
            if (!(value instanceof Map)) {
                throw mismatch(value, "a map");
            }
            Map<?, ?> map = (Map<?, ?>) value;
            GenericRowData row = new GenericRowData(names.length);
            for (int i = 0; i < names.length; i++) {
                row.setField(i, fields[i].convert(map.get(names[i])));
            }
            return row;
        }
    }
}
