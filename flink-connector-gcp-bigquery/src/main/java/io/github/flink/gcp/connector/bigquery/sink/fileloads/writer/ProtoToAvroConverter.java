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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
 * civil-time longs via {@link CivilTimeEncoder} (a {@code DATETIME} string wire form is parsed as a
 * BigQuery datetime literal; the {@code TIME} one is rejected at plan time), {@code NUMERIC}/{@code
 * BIGNUMERIC} bytes via {@link BigDecimalByteStringEncoder} (a string wire form is parsed as a
 * decimal literal) re-encoded as big-endian Avro decimals, and {@code TIMESTAMP} epoch-micros longs
 * kept as-is.
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

    /**
     * BigQuery's {@code DATETIME} literal grammar, {@code YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]}:
     * either separator, unpadded month/day/hour, an optional time part. A serializer sending the
     * string wire form writes for the Storage Write API, whose server parses that grammar, so
     * accepting less here would make one serializer work under {@code STORAGE_API_*} and fail under
     * {@code FILE_LOADS}. {@code parseLenient} is what allows the unpadded fields — it makes a
     * fixed-width {@code appendValue} accept 1 to 19 characters — and it applies to the appended
     * ISO formatters too, because it is a parse-time setting on the shared context.
     *
     * <p>It accepts a superset of that grammar — omitted seconds, a lowercase {@code t}, either
     * separator written where the other would do, a year of other than four digits, a signed year.
     * Every one of those has exactly one reading, so accepting it writes the value the author
     * meant, and that is the whole licence: {@link ResolverStyle#STRICT} is what keeps "accept
     * more" from becoming "write something else". Appending {@code ISO_LOCAL_DATE} copies its
     * printer-parser but <b>not</b> its resolver style, and under the {@code SMART} default {@code
     * 2026-02-30} resolves to the 28th and {@code 24:00:00} rolls into the next day — silently,
     * where BigQuery answers with an error.
     */
    private static final DateTimeFormatter DATETIME_LITERAL =
            new DateTimeFormatterBuilder()
                    .parseLenient()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .optionalStart()
                    .optionalStart()
                    .appendLiteral(' ')
                    .optionalEnd()
                    .optionalStart()
                    .appendLiteral('T')
                    .optionalEnd()
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .optionalEnd()
                    // A date-only literal is legal and means midnight; without this the parse
                    // resolves to a LocalDate and LocalDateTime.from rejects it. One field is
                    // enough — java.time resolves an hour with no minute or second to LocalTime,
                    // and ISO_LOCAL_TIME never yields a minute without an hour.
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .toFormatter(Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);

    /** BigQuery's documented {@code DATETIME} range, which no column can hold a value outside. */
    private static final int MIN_YEAR = 1;

    private static final int MAX_YEAR = 9999;

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
        /** Packed civil-time long to micros of the civil epoch. */
        DATETIME_PACKED,
        /** BigQuery datetime literal to micros of the civil epoch. */
        DATETIME_STRING,
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
        // BigQuery column names are case-insensitive (and BQTableSchemaToProtoDescriptor
        // lowercases proto field names), so pairing is uniformly case-insensitive.
        Map<String, Descriptors.FieldDescriptor> protoFieldsByName = new HashMap<>();
        for (Descriptors.FieldDescriptor protoField : descriptor.getFields()) {
            protoFieldsByName.put(
                    BigQuerySchemaUtil.getFieldName(protoField).toLowerCase(Locale.ROOT),
                    protoField);
        }
        List<FieldPlan> fieldPlans = new ArrayList<>(fields.size());
        for (TableFieldSchema field : fields) {
            String path =
                    parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
            Descriptors.FieldDescriptor protoField =
                    protoFieldsByName.get(field.getName().toLowerCase(Locale.ROOT));
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
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.STRING, path);
                return Kind.IDENTITY;
            case INT64:
            case TIMESTAMP:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path);
                return Kind.IDENTITY;
            case DATE:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.INT, path);
                return Kind.IDENTITY;
            case DOUBLE:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.DOUBLE, path);
                return Kind.IDENTITY;
            case BOOL:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.BOOLEAN, path);
                return Kind.IDENTITY;
            case BYTES:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.BYTE_STRING, path);
                return Kind.BYTES;
            case TIME:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path);
                return Kind.TIME_PACKED;
            case DATETIME:
                if (wire == Descriptors.FieldDescriptor.JavaType.STRING) {
                    return Kind.DATETIME_STRING;
                }
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.LONG, path);
                return Kind.DATETIME_PACKED;
            case NUMERIC:
                checkDecimalWire(wire, path);
                return Kind.NUMERIC;
            case BIGNUMERIC:
                checkDecimalWire(wire, path);
                return Kind.BIGNUMERIC;
            case STRUCT:
                checkWire(wire, Descriptors.FieldDescriptor.JavaType.MESSAGE, path);
                return Kind.STRUCT;
            default:
                throw new IllegalArgumentException(
                        "Field "
                                + path
                                + " has type "
                                + field.getType()
                                + ", which WriteMethod.FILE_LOADS does not support.");
        }
    }

    private static void checkWire(
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
    }

    /**
     * Encodes a civil datetime as {@code local-timestamp-micros}: microseconds from {@code
     * 1970-01-01T00:00:00}, the civil instant being read at {@link ZoneOffset#UTC} because Avro
     * fixes that epoch rather than any zone. The inverse of {@code
     * AvroRowConverter.toLocalDateTime}, which reads the same logical type back.
     *
     * <p>A year outside BigQuery's range is rejected rather than staged: the load job is
     * all-or-nothing, so a value no column can hold would fail the whole job instead of the one row
     * that carries it. This fires on the literal path only — {@link
     * CivilTimeEncoder#decodePacked64DatetimeMicrosLocalDateTime} applies the same bound itself, so
     * the packed path cannot reach it (measured against SDK 3.30.0; the check stays because that is
     * the SDK's invariant, not ours).
     */
    private static long toCivilMicros(LocalDateTime value) {
        if (value.getYear() < MIN_YEAR || value.getYear() > MAX_YEAR) {
            throw new IllegalArgumentException(
                    "year "
                            + value.getYear()
                            + " is outside BigQuery's DATETIME range "
                            + MIN_YEAR
                            + " to "
                            + MAX_YEAR);
        }
        return Math.addExact(
                Math.multiplyExact(value.toEpochSecond(ZoneOffset.UTC), 1_000_000L),
                value.getNano() / 1_000L);
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
                        // Zero-copy; Avro's encoder handles non-array-backed buffers.
                        return ((ByteString) value).asReadOnlyByteBuffer();
                    case TIME_PACKED:
                        return CivilTimeEncoder.decodePacked64TimeMicrosLocalTime((Long) value)
                                        .toNanoOfDay()
                                / 1_000L;
                    case DATETIME_PACKED:
                        return toCivilMicros(
                                CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                                        (Long) value));
                    case DATETIME_STRING:
                        return toCivilMicros(LocalDateTime.parse((String) value, DATETIME_LITERAL));
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
