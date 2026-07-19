/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts serialized proto rows (the {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer} wire form, whose
 * descriptors follow the {@code BQTableSchemaToProtoDescriptor} conventions) into Avro {@link
 * GenericRecord}s conforming to the schema produced by {@link TableSchemaToAvroConverter}.
 *
 * <p>All descriptor-dependent decisions — proto/Avro field pairing (via {@link
 * BigQuerySchemaUtil#getFieldName}, which tracks the library's field-naming contract), value
 * conversion kinds, decimal scales — are resolved once into a conversion plan at construction time;
 * per-record conversion is a flat loop over the plan.
 *
 * <p>Value conversions decode the Storage API wire forms: {@code TIME}/{@code DATETIME} packed
 * civil-time longs via {@link CivilTimeEncoder} (a string wire form is passed through for {@code
 * DATETIME} and rejected at plan time for {@code TIME}, which Avro represents as {@code
 * time-micros}), {@code NUMERIC}/{@code BIGNUMERIC} bytes via {@link BigDecimalByteStringEncoder}
 * (a string wire form is parsed as a decimal literal) re-encoded as big-endian Avro decimals, and
 * {@code TIMESTAMP} epoch-micros longs kept as-is.
 *
 * <p>{@link #convert} throws {@link IOException} for per-row value failures (for example a decimal
 * exceeding the column's scale), mirroring the serializer contract so callers can route the row to
 * the configured failure policy; descriptor/schema mismatches are configuration errors and throw
 * unchecked at construction time.
 *
 * <p>Instances hold non-serializable descriptors and must be re-created after deserialization.
 */
@Internal
public final class ProtoToAvroConverter {

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

    private final RecordPlan plan;

    /**
     * Creates a converter for one destination's schemas.
     *
     * @param tableSchema the Storage API table schema (the serializer's schema)
     * @param descriptor the serializer's row descriptor
     * @param avroSchema the Avro schema from {@link TableSchemaToAvroConverter#convert}
     */
    public ProtoToAvroConverter(
            TableSchema tableSchema, Descriptors.Descriptor descriptor, Schema avroSchema) {
        this.plan = buildRecordPlan(tableSchema.getFieldsList(), descriptor, avroSchema, "");
    }

    /**
     * Converts one serialized row.
     *
     * @param message the row, parsed against the serializer's descriptor
     * @return the equivalent Avro record
     * @throws IOException if a value cannot be represented in the destination column
     */
    public GenericRecord convert(MessageOrBuilder message) throws IOException {
        return plan.convert(message);
    }

    /** How a proto value is converted to its Avro representation. */
    private enum Kind {
        /** Long, double, boolean, int and string values used as-is. */
        IDENTITY,
        /** {@link ByteString} to {@link ByteBuffer}. */
        BYTES,
        /** Packed civil-time long to micros-of-day. */
        TIME_PACKED,
        /** Packed civil-time long to a canonical datetime string. */
        DATETIME_PACKED,
        /** {@code NUMERIC} bytes or decimal literal string to an Avro decimal. */
        NUMERIC,
        /** {@code BIGNUMERIC} bytes or decimal literal string to an Avro decimal. */
        BIGNUMERIC,
        /** Nested message converted recursively. */
        STRUCT
    }

    private static RecordPlan buildRecordPlan(
            List<TableFieldSchema> fields,
            Descriptors.Descriptor descriptor,
            Schema avroSchema,
            String parentPath) {
        Map<String, Descriptors.FieldDescriptor> protoFieldsByName = new HashMap<>();
        for (Descriptors.FieldDescriptor protoField : descriptor.getFields()) {
            protoFieldsByName.put(BigQuerySchemaUtil.getFieldName(protoField), protoField);
        }
        List<FieldPlan> fieldPlans = new ArrayList<>(fields.size());
        for (TableFieldSchema field : fields) {
            String path =
                    parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
            Descriptors.FieldDescriptor protoField = protoFieldsByName.get(field.getName());
            if (protoField == null) {
                // BQTableSchemaToProtoDescriptor lowercases proto field names.
                protoField =
                        protoFieldsByName.get(field.getName().toLowerCase(java.util.Locale.ROOT));
            }
            Preconditions.checkState(
                    protoField != null,
                    "The serializer's descriptor has no field for schema field %s",
                    path);
            Schema.Field avroField = avroSchema.getField(field.getName());
            Preconditions.checkState(
                    avroField != null, "The Avro schema has no field for schema field %s", path);
            fieldPlans.add(buildFieldPlan(field, protoField, avroField, path));
        }
        return new RecordPlan(avroSchema, fieldPlans.toArray(new FieldPlan[0]));
    }

    private static FieldPlan buildFieldPlan(
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            Schema.Field avroField,
            String path) {
        boolean repeated = field.getMode() == TableFieldSchema.Mode.REPEATED;
        boolean nullable = !repeated && field.getMode() != TableFieldSchema.Mode.REQUIRED;
        Preconditions.checkState(
                protoField.isRepeated() == repeated,
                "Schema field %s and the serializer's descriptor disagree on repeatedness",
                path);
        Schema valueSchema = unwrap(avroField.schema(), repeated, nullable);
        Kind kind = toKind(field, protoField, path);
        RecordPlan nested =
                kind == Kind.STRUCT
                        ? buildRecordPlan(
                                field.getFieldsList(),
                                protoField.getMessageType(),
                                valueSchema,
                                path)
                        : null;
        int scale =
                kind == Kind.NUMERIC || kind == Kind.BIGNUMERIC
                        ? ((LogicalTypes.Decimal) valueSchema.getLogicalType()).getScale()
                        : 0;
        return new FieldPlan(
                protoField, avroField.pos(), kind, repeated, nullable, nested, scale, path);
    }

    private static Schema unwrap(Schema schema, boolean repeated, boolean nullable) {
        if (nullable) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() != Schema.Type.NULL) {
                    return branch;
                }
            }
            throw new IllegalStateException("Union without a non-null branch: " + schema);
        }
        return repeated ? schema.getElementType() : schema;
    }

    private static Kind toKind(
            TableFieldSchema field, Descriptors.FieldDescriptor protoField, String path) {
        Descriptors.FieldDescriptor.JavaType wire = protoField.getJavaType();
        switch (field.getType()) {
            case STRING:
            case JSON:
            case GEOGRAPHY:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.STRING, path)
                        ? Kind.IDENTITY
                        : null;
            case INT64:
            case TIMESTAMP:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path)
                        ? Kind.IDENTITY
                        : null;
            case DATE:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.INT, path)
                        ? Kind.IDENTITY
                        : null;
            case DOUBLE:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.DOUBLE, path)
                        ? Kind.IDENTITY
                        : null;
            case BOOL:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.BOOLEAN, path)
                        ? Kind.IDENTITY
                        : null;
            case BYTES:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.BYTE_STRING, path)
                        ? Kind.BYTES
                        : null;
            case TIME:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path)
                        ? Kind.TIME_PACKED
                        : null;
            case DATETIME:
                if (wire == Descriptors.FieldDescriptor.JavaType.STRING) {
                    return Kind.IDENTITY;
                }
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path)
                        ? Kind.DATETIME_PACKED
                        : null;
            case NUMERIC:
                checkDecimalWire(wire, path);
                return Kind.NUMERIC;
            case BIGNUMERIC:
                checkDecimalWire(wire, path);
                return Kind.BIGNUMERIC;
            case STRUCT:
                return checkWire(wire, Descriptors.FieldDescriptor.JavaType.MESSAGE, path)
                        ? Kind.STRUCT
                        : null;
            default:
                throw new IllegalArgumentException(
                        "Field "
                                + path
                                + " has type "
                                + field.getType()
                                + ", which WriteMethod.FILE_LOADS does not support.");
        }
    }

    private static boolean checkWire(
            Descriptors.FieldDescriptor.JavaType actual,
            Descriptors.FieldDescriptor.JavaType expected,
            String path) {
        Preconditions.checkState(
                actual == expected,
                "Schema field %s expects a %s wire value but the serializer's descriptor declares"
                        + " %s",
                path,
                expected,
                actual);
        return true;
    }

    private static void checkDecimalWire(Descriptors.FieldDescriptor.JavaType wire, String path) {
        Preconditions.checkState(
                wire == Descriptors.FieldDescriptor.JavaType.BYTE_STRING
                        || wire == Descriptors.FieldDescriptor.JavaType.STRING,
                "Schema field %s expects a BYTE_STRING or STRING wire value but the serializer's"
                        + " descriptor declares %s",
                path,
                wire);
    }

    /** Conversion plan for one record level. */
    private static final class RecordPlan {

        private final Schema avroSchema;
        private final FieldPlan[] fields;

        RecordPlan(Schema avroSchema, FieldPlan[] fields) {
            this.avroSchema = avroSchema;
            this.fields = fields;
        }

        GenericRecord convert(MessageOrBuilder message) throws IOException {
            GenericRecord record = new GenericData.Record(avroSchema);
            for (FieldPlan field : fields) {
                record.put(field.avroPos, field.convert(message));
            }
            return record;
        }
    }

    /** Conversion plan for one field. */
    private static final class FieldPlan {

        private final Descriptors.FieldDescriptor protoField;
        private final int avroPos;
        private final Kind kind;
        private final boolean repeated;
        private final boolean nullable;
        private final RecordPlan nested;
        private final int decimalScale;
        private final String path;

        FieldPlan(
                Descriptors.FieldDescriptor protoField,
                int avroPos,
                Kind kind,
                boolean repeated,
                boolean nullable,
                RecordPlan nested,
                int decimalScale,
                String path) {
            this.protoField = protoField;
            this.avroPos = avroPos;
            this.kind = kind;
            this.repeated = repeated;
            this.nullable = nullable;
            this.nested = nested;
            this.decimalScale = decimalScale;
            this.path = path;
        }

        Object convert(MessageOrBuilder message) throws IOException {
            if (repeated) {
                int count = message.getRepeatedFieldCount(protoField);
                List<Object> values = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    values.add(convertValue(message.getRepeatedField(protoField, i)));
                }
                return values;
            }
            if (nullable && !message.hasField(protoField)) {
                return null;
            }
            return convertValue(message.getField(protoField));
        }

        private Object convertValue(Object value) throws IOException {
            try {
                switch (kind) {
                    case IDENTITY:
                        return value;
                    case BYTES:
                        return ByteBuffer.wrap(((ByteString) value).toByteArray());
                    case TIME_PACKED:
                        return CivilTimeEncoder.decodePacked64TimeMicrosLocalTime((Long) value)
                                        .toNanoOfDay()
                                / 1_000L;
                    case DATETIME_PACKED:
                        return DATETIME_FORMAT.format(
                                CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                        (Long) value));
                    case NUMERIC:
                        return toDecimalBytes(
                                value instanceof ByteString
                                        ? BigDecimalByteStringEncoder.decodeNumericByteString(
                                                (ByteString) value)
                                        : new BigDecimal((String) value));
                    case BIGNUMERIC:
                        return toDecimalBytes(
                                value instanceof ByteString
                                        ? BigDecimalByteStringEncoder.decodeBigNumericByteString(
                                                (ByteString) value)
                                        : new BigDecimal((String) value));
                    case STRUCT:
                        return nested.convert((MessageOrBuilder) value);
                    default:
                        throw new IllegalStateException("Unknown conversion kind: " + kind);
                }
            } catch (ArithmeticException
                    | IllegalArgumentException
                    | java.time.DateTimeException e) {
                throw new IOException(
                        "Value of field "
                                + path
                                + " cannot be represented in the destination"
                                + " column: "
                                + e.getMessage(),
                        e);
            }
        }

        private ByteBuffer toDecimalBytes(BigDecimal value) {
            return ByteBuffer.wrap(
                    value.setScale(decimalScale, RoundingMode.UNNECESSARY)
                            .unscaledValue()
                            .toByteArray());
        }
    }
}
