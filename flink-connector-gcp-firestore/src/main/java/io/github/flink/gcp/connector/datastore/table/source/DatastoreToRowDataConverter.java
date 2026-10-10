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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Turns one property value into the Flink value of its column, the read side of {@link
 * DatastoreTableSchema}'s type mapping.
 *
 * <p>A value whose type does not match the column is a mismatch. Under {@link
 * TypeMismatchPolicy#NULL} the innermost nullable field around it reads as NULL; under {@link
 * TypeMismatchPolicy#FAIL}, or when no field around it is nullable, the read fails naming the
 * property. Only these values are read: a string, an integer into BIGINT, a floating-point number
 * or an integer of at most 2^53 in magnitude into DOUBLE, a boolean, a blob, a timestamp (truncated
 * to the column's precision), an array, and an embedded entity into a ROW. A key, a geographical
 * point and a value the client library cannot name are mismatches. A string or blob is read whole:
 * the length a CHAR, VARCHAR, BINARY or VARBINARY column declares is neither checked nor padded to,
 * as the Spanner table reads a STRING whatever its declared length.
 */
@Internal
abstract class DatastoreToRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The largest magnitude a double holds every integer up to. */
    private static final long EXACT_DOUBLE_INTEGER = 1L << 53;

    /** The property path, for a failure message. */
    final String path;

    /** Whether the field may read as NULL. */
    final boolean nullable;

    /** What a mismatch inside this field does. */
    final TypeMismatchPolicy policy;

    private DatastoreToRowDataConverter(String path, LogicalType type, TypeMismatchPolicy policy) {
        this.path = path;
        this.nullable = type.isNullable();
        this.policy = policy;
    }

    /** Converts a value that is not a Datastore null, or throws when its type does not match. */
    abstract Object convertNonNull(Value<?> value) throws Mismatch;

    /**
     * Converts a property's value: a missing property and a Datastore null read as NULL where the
     * field is nullable, and a mismatch reads as NULL there too under {@link
     * TypeMismatchPolicy#NULL}.
     */
    final Object convert(@Nullable Value<?> value) throws Mismatch {
        if (value == null || value.getType() == ValueType.NULL) {
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

    Mismatch mismatch(Value<?> value, String expected) {
        return new Mismatch(
                path, "holds a value of type " + value.getType() + ", which is not " + expected);
    }

    /** Returns the value of an entity's property, or {@code null} when it has none. */
    @Nullable
    static Value<?> property(BaseEntity<?> entity, String name) {
        return entity.contains(name) ? entity.getValue(name) : null;
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
            super("Property '" + path + "' " + reason + ".");
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
     * @param path the property path
     * @param type the field's type
     * @param policy what a mismatch does
     * @return the converter
     */
    static DatastoreToRowDataConverter create(
            String path, LogicalType type, TypeMismatchPolicy policy) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new FromString(path, type, policy);
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
                return new FromList(path, type, policy, create(path, element, policy));
            case ROW:
                RowType row = (RowType) type;
                DatastoreToRowDataConverter[] fields =
                        new DatastoreToRowDataConverter[row.getFieldCount()];
                for (int i = 0; i < fields.length; i++) {
                    fields[i] =
                            create(
                                    path + "." + row.getFieldNames().get(i),
                                    row.getTypeAt(i),
                                    policy);
                }
                return new FromEntity(
                        path, type, policy, row.getFieldNames().toArray(new String[0]), fields);
            default:
                // DatastoreTableSchema refused every other type when the table was planned.
                throw new IllegalStateException(
                        "No Datastore form for field '" + path + "' of type " + type);
        }
    }

    private static final class FromString extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromString(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.STRING) {
                return StringData.fromString((String) value.get());
            }
            throw mismatch(value, "a string");
        }
    }

    private static final class FromBoolean extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromBoolean(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.BOOLEAN) {
                return value.get();
            }
            throw mismatch(value, "a boolean");
        }
    }

    private static final class FromInteger extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromInteger(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.LONG) {
                return value.get();
            }
            throw mismatch(value, "an integer");
        }
    }

    private static final class FromDouble extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromDouble(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.DOUBLE) {
                return value.get();
            }
            if (value.getType() == ValueType.LONG) {
                long integer = (Long) value.get();
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

    private static final class FromBlob extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;

        FromBlob(String path, LogicalType type, TypeMismatchPolicy policy) {
            super(path, type, policy);
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.BLOB) {
                return ((Blob) value.get()).toByteArray();
            }
            throw mismatch(value, "a blob");
        }
    }

    private static final class FromTimestamp extends DatastoreToRowDataConverter {
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
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() == ValueType.TIMESTAMP) {
                return timestampData((Timestamp) value.get(), truncation);
            }
            throw mismatch(value, "a timestamp");
        }
    }

    private static final class FromList extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final DatastoreToRowDataConverter element;

        FromList(
                String path,
                LogicalType type,
                TypeMismatchPolicy policy,
                DatastoreToRowDataConverter element) {
            super(path, type, policy);
            this.element = element;
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() != ValueType.LIST) {
                throw mismatch(value, "an array");
            }
            List<?> list = (List<?>) value.get();
            Object[] elements = new Object[list.size()];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = element.convert((Value<?>) list.get(i));
            }
            return new GenericArrayData(elements);
        }
    }

    private static final class FromEntity extends DatastoreToRowDataConverter {
        private static final long serialVersionUID = 1L;
        private final String[] names;
        private final DatastoreToRowDataConverter[] fields;

        FromEntity(
                String path,
                LogicalType type,
                TypeMismatchPolicy policy,
                String[] names,
                DatastoreToRowDataConverter[] fields) {
            super(path, type, policy);
            this.names = names;
            this.fields = fields;
        }

        @Override
        Object convertNonNull(Value<?> value) throws Mismatch {
            if (value.getType() != ValueType.ENTITY) {
                throw mismatch(value, "an embedded entity");
            }
            FullEntity<?> entity = (FullEntity<?>) value.get();
            GenericRowData row = new GenericRowData(names.length);
            for (int i = 0; i < names.length; i++) {
                row.setField(i, fields[i].convert(property(entity, names[i])));
            }
            return row;
        }
    }
}
