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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Converts Avro records into the protobuf rows the BigQuery Storage Write API accepts, against the
 * schema {@link AvroToTableSchemaConverter} derived and the descriptor {@code
 * BQTableSchemaToProtoDescriptor} built from it.
 *
 * <p>All schema-dependent decisions — Avro/proto field pairing, conversion kinds, decimal scales,
 * nested plans — are resolved once into a conversion plan at construction time; per-record
 * conversion is a flat loop over the plan.
 *
 * <p>Values are read defensively, because the same Avro schema yields two different Java
 * representations: a {@code GenericRecord} decoded without conversions carries the raw base value
 * ({@code long}, {@code int}, {@link ByteBuffer}), while a {@code SpecificRecord} generated with
 * Avro's logical-type conversions carries {@link Instant}, {@link LocalDate}, {@link LocalTime},
 * {@link LocalDateTime}, {@link BigDecimal} or {@link UUID}. Both are accepted for every logical
 * type, and strings are accepted as {@link CharSequence} (so Avro's {@code Utf8} needs no special
 * casing at the call site).
 *
 * <p>{@link #convert} throws {@link IOException} for per-row value failures — a missing value for a
 * {@code REQUIRED} column, a decimal too wide for its column, a value of an unexpected Java type —
 * mirroring the serializer contract so callers can route the row to the configured failure policy.
 * Schema/descriptor mismatches are configuration errors and throw unchecked at construction time.
 *
 * <p>Instances hold non-serializable descriptors and must be re-created after deserialization.
 */
@Internal
public final class AvroRowConverter {

    private final RecordPlan plan;

    /**
     * Creates a converter for one destination's schemas.
     *
     * @param avroSchema the Avro record schema of the incoming records
     * @param tableSchema the derived Storage API table schema
     * @param rowDescriptor the row descriptor derived from {@code tableSchema}
     */
    public AvroRowConverter(
            Schema avroSchema, TableSchema tableSchema, Descriptors.Descriptor rowDescriptor) {
        this.plan = buildRecordPlan(tableSchema.getFieldsList(), rowDescriptor, avroSchema, "");
    }

    /**
     * Converts one record.
     *
     * @param record the Avro record
     * @return the equivalent protobuf row
     * @throws IOException if a value cannot be represented in the destination column
     */
    public DynamicMessage convert(IndexedRecord record) throws IOException {
        return plan.convert(record);
    }

    /** How an Avro value is converted to its protobuf representation. */
    private enum Kind {
        /** {@code int}/{@code long} to {@code int64}. */
        LONG,
        /** {@code float}/{@code double} to {@code double}. */
        DOUBLE,
        /** {@code boolean} to {@code bool}. */
        BOOL,
        /** {@code string}/{@code enum}/{@code uuid} to {@code string}; also carries JSON text. */
        STRING,
        /** {@code bytes}/{@code fixed} to {@code bytes}. */
        BYTES,
        /** {@code date} to days since the epoch. */
        DATE,
        /** {@code time-millis} to a packed civil-time {@code int64}. */
        TIME_MILLIS,
        /** {@code time-micros} to a packed civil-time {@code int64}. */
        TIME_MICROS,
        /** {@code timestamp-millis} to epoch microseconds. */
        TIMESTAMP_MILLIS,
        /** {@code timestamp-micros} to epoch microseconds. */
        TIMESTAMP_MICROS,
        /** {@code local-timestamp-millis} to a packed civil-time {@code int64}. */
        DATETIME_MILLIS,
        /** {@code local-timestamp-micros} to a packed civil-time {@code int64}. */
        DATETIME_MICROS,
        /** {@code decimal} to the {@code NUMERIC} byte encoding. */
        NUMERIC,
        /** {@code decimal} to the {@code BIGNUMERIC} byte encoding. */
        BIGNUMERIC,
        /** Nested record converted recursively. */
        STRUCT,
        /** Avro map converted to repeated {@code STRUCT<key, value>} entries. */
        MAP
    }

    private static RecordPlan buildRecordPlan(
            List<TableFieldSchema> fields,
            Descriptors.Descriptor descriptor,
            Schema avroSchema,
            String parentPath) {
        // Paired by position, not by name. BQTableSchemaToProtoDescriptor emits one proto field per
        // table-schema field, in order, and the table schema here is always the one
        // AvroToTableSchemaConverter just derived — so position is exact where a name is not: the
        // library lowercases with the *default* locale, which under tr_TR turns a column named ID
        // into the proto field "ıd" that no Locale.ROOT key can match.
        Preconditions.checkState(
                descriptor.getFields().size() == fields.size(),
                "The row descriptor has %s fields where the schema at %s has %s",
                descriptor.getFields().size(),
                parentPath.isEmpty() ? "<root>" : parentPath,
                fields.size());
        List<FieldPlan> fieldPlans = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            TableFieldSchema field = fields.get(i);
            String path =
                    parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
            Descriptors.FieldDescriptor protoField = descriptor.getFields().get(i);
            Schema.Field avroField = avroSchema.getField(field.getName());
            Preconditions.checkState(
                    avroField != null, "The Avro schema has no field for schema field %s", path);
            fieldPlans.add(
                    buildFieldPlan(field, protoField, avroField.schema(), avroField.pos(), path));
        }
        return new RecordPlan(descriptor, fieldPlans.toArray(new FieldPlan[0]));
    }

    private static FieldPlan buildFieldPlan(
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            Schema avroSchema,
            int avroPos,
            String path) {
        boolean repeated = field.getMode() == TableFieldSchema.Mode.REPEATED;
        Preconditions.checkState(
                protoField.isRepeated() == repeated,
                "Schema field %s and the row descriptor disagree on repeatedness",
                path);

        Schema base =
                avroSchema.getType() == Schema.Type.UNION
                        ? AvroToTableSchemaConverter.nonNullBranch(avroSchema, path)
                        : avroSchema;
        Schema element = base;
        if (base.getType() == Schema.Type.ARRAY) {
            element = base.getElementType();
            if (element.getType() == Schema.Type.UNION) {
                element = AvroToTableSchemaConverter.nonNullBranch(element, path);
            }
        }

        Kind kind = toKind(field, element, path);
        if (kind == Kind.MAP) {
            return FieldPlan.map(
                    protoField, avroPos, buildMapPlan(field, protoField, element, path), path);
        }
        RecordPlan nested =
                kind == Kind.STRUCT
                        ? buildRecordPlan(
                                field.getFieldsList(), protoField.getMessageType(), element, path)
                        : null;
        return FieldPlan.of(
                protoField,
                avroPos,
                kind,
                repeated,
                nested,
                decimalScale(kind, element, path),
                path);
    }

    private static MapPlan buildMapPlan(
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            Schema map,
            String path) {
        Descriptors.Descriptor entry = protoField.getMessageType();
        Descriptors.FieldDescriptor keyField = entry.findFieldByName("key");
        Preconditions.checkState(
                keyField != null, "The map entry descriptor of %s has no key field", path);
        TableFieldSchema valueSchema =
                field.getFieldsList().stream()
                        .filter(f -> "value".equals(f.getName()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The map entry schema of "
                                                        + path
                                                        + " has no value field"));
        Descriptors.FieldDescriptor valueField = entry.findFieldByName("value");
        Preconditions.checkState(
                valueField != null, "The map entry descriptor of %s has no value field", path);
        return new MapPlan(
                entry,
                keyField,
                // The value is handed to the plan directly rather than read out of a record, so
                // the Avro position is unused here.
                buildFieldPlan(valueSchema, valueField, map.getValueType(), -1, path + ".value"),
                path);
    }

    private static Kind toKind(TableFieldSchema field, Schema avroSchema, String path) {
        LogicalType logicalType = avroSchema.getLogicalType();
        switch (field.getType()) {
            case STRING:
            case JSON:
                return Kind.STRING;
            case INT64:
                return Kind.LONG;
            case DOUBLE:
                return Kind.DOUBLE;
            case BOOL:
                return Kind.BOOL;
            case BYTES:
                return Kind.BYTES;
            case DATE:
                return Kind.DATE;
            case TIME:
                return logicalType instanceof LogicalTypes.TimeMillis
                        ? Kind.TIME_MILLIS
                        : Kind.TIME_MICROS;
            case TIMESTAMP:
                return logicalType instanceof LogicalTypes.TimestampMillis
                        ? Kind.TIMESTAMP_MILLIS
                        : Kind.TIMESTAMP_MICROS;
            case DATETIME:
                return logicalType instanceof LogicalTypes.LocalTimestampMillis
                        ? Kind.DATETIME_MILLIS
                        : Kind.DATETIME_MICROS;
            case NUMERIC:
                return Kind.NUMERIC;
            case BIGNUMERIC:
                return Kind.BIGNUMERIC;
            case STRUCT:
                return avroSchema.getType() == Schema.Type.MAP ? Kind.MAP : Kind.STRUCT;
            default:
                throw new IllegalArgumentException(
                        "Field "
                                + path
                                + " has type "
                                + field.getType()
                                + ", which the Avro serializer does not produce");
        }
    }

    private static int decimalScale(Kind kind, Schema avroSchema, String path) {
        if (kind != Kind.NUMERIC && kind != Kind.BIGNUMERIC) {
            return 0;
        }
        LogicalType logicalType = avroSchema.getLogicalType();
        Preconditions.checkState(
                logicalType instanceof LogicalTypes.Decimal,
                "Schema field %s is a decimal column but the Avro field carries no decimal logical"
                        + " type",
                path);
        return ((LogicalTypes.Decimal) logicalType).getScale();
    }

    /** Conversion plan for one record level. */
    private static final class RecordPlan {

        private final Descriptors.Descriptor descriptor;
        private final FieldPlan[] fields;

        RecordPlan(Descriptors.Descriptor descriptor, FieldPlan[] fields) {
            this.descriptor = descriptor;
            this.fields = fields;
        }

        DynamicMessage convert(IndexedRecord record) throws IOException {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            for (FieldPlan field : fields) {
                field.apply(record.get(field.avroPos), builder);
            }
            return builder.build();
        }
    }

    /** Conversion plan for one map field's entries. */
    private static final class MapPlan {

        private final Descriptors.Descriptor entryDescriptor;
        private final Descriptors.FieldDescriptor keyField;
        private final FieldPlan valuePlan;
        private final String path;

        MapPlan(
                Descriptors.Descriptor entryDescriptor,
                Descriptors.FieldDescriptor keyField,
                FieldPlan valuePlan,
                String path) {
            this.entryDescriptor = entryDescriptor;
            this.keyField = keyField;
            this.valuePlan = valuePlan;
            this.path = path;
        }

        DynamicMessage convert(Object key, Object value) throws IOException {
            DynamicMessage.Builder entry = DynamicMessage.newBuilder(entryDescriptor);
            entry.setField(keyField, toStringValue(key, path + ".key"));
            valuePlan.apply(value, entry);
            return entry.build();
        }
    }

    /** Conversion plan for one field. */
    private static final class FieldPlan {

        private final Descriptors.FieldDescriptor protoField;
        private final int avroPos;
        private final Kind kind;
        private final boolean repeated;
        private final RecordPlan nested;
        private final MapPlan map;
        private final int decimalScale;
        private final String path;

        private FieldPlan(
                Descriptors.FieldDescriptor protoField,
                int avroPos,
                Kind kind,
                boolean repeated,
                RecordPlan nested,
                MapPlan map,
                int decimalScale,
                String path) {
            this.protoField = protoField;
            this.avroPos = avroPos;
            this.kind = kind;
            this.repeated = repeated;
            this.nested = nested;
            this.map = map;
            this.decimalScale = decimalScale;
            this.path = path;
        }

        static FieldPlan of(
                Descriptors.FieldDescriptor protoField,
                int avroPos,
                Kind kind,
                boolean repeated,
                RecordPlan nested,
                int decimalScale,
                String path) {
            return new FieldPlan(
                    protoField, avroPos, kind, repeated, nested, null, decimalScale, path);
        }

        static FieldPlan map(
                Descriptors.FieldDescriptor protoField, int avroPos, MapPlan map, String path) {
            return new FieldPlan(protoField, avroPos, Kind.MAP, true, null, map, 0, path);
        }

        void apply(Object value, DynamicMessage.Builder target) throws IOException {
            if (kind == Kind.MAP) {
                if (value != null) {
                    for (Map.Entry<?, ?> entry : asMap(value, path).entrySet()) {
                        target.addRepeatedField(
                                protoField, map.convert(entry.getKey(), entry.getValue()));
                    }
                }
                return;
            }
            if (repeated) {
                if (value != null) {
                    for (Object item : asCollection(value, path)) {
                        if (item == null) {
                            throw new IOException(
                                    "Field "
                                            + path
                                            + " is a BigQuery REPEATED column, which cannot hold a"
                                            + " null element");
                        }
                        target.addRepeatedField(protoField, convertValue(item));
                    }
                }
                return;
            }
            if (value == null) {
                if (protoField.isRequired()) {
                    throw new IOException(
                            "Field "
                                    + path
                                    + " is a BigQuery REQUIRED column but the record carries no"
                                    + " value for it");
                }
                return;
            }
            target.setField(protoField, convertValue(value));
        }

        private Object convertValue(Object value) throws IOException {
            try {
                switch (kind) {
                    case LONG:
                        return toLong(value, path);
                    case DOUBLE:
                        return toDouble(value, path);
                    case BOOL:
                        return toBoolean(value, path);
                    case STRING:
                        return toStringValue(value, path);
                    case BYTES:
                        return ByteString.copyFrom(toByteArray(value, path));
                    case DATE:
                        return toEpochDay(value, path);
                    case TIME_MILLIS:
                        return CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(
                                toLocalTime(value, 1_000_000L, path));
                    case TIME_MICROS:
                        return CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(
                                toLocalTime(value, 1_000L, path));
                    case TIMESTAMP_MILLIS:
                        return toEpochMicros(value, 1_000L, path);
                    case TIMESTAMP_MICROS:
                        return toEpochMicros(value, 1L, path);
                    case DATETIME_MILLIS:
                        return CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(
                                toLocalDateTime(value, 1_000L, path));
                    case DATETIME_MICROS:
                        return CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(
                                toLocalDateTime(value, 1L, path));
                    case NUMERIC:
                        return BigDecimalByteStringEncoder.encodeToNumericByteString(
                                toBigDecimal(value, decimalScale, path));
                    case BIGNUMERIC:
                        return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                                toBigDecimal(value, decimalScale, path));
                    case STRUCT:
                        return nested.convert(asRecord(value, path));
                    default:
                        throw new IllegalStateException("Unknown conversion kind: " + kind);
                }
            } catch (ArithmeticException
                    | IllegalArgumentException
                    | java.time.DateTimeException e) {
                throw new IOException(
                        "Value of field "
                                + path
                                + " cannot be represented in the destination column: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    private static Map<?, ?> asMap(Object value, String path) throws IOException {
        if (value instanceof Map) {
            return (Map<?, ?>) value;
        }
        throw typeError(path, value, "a map");
    }

    private static Collection<?> asCollection(Object value, String path) throws IOException {
        if (value instanceof Collection) {
            return (Collection<?>) value;
        }
        throw typeError(path, value, "an array");
    }

    private static IndexedRecord asRecord(Object value, String path) throws IOException {
        if (value instanceof IndexedRecord) {
            return (IndexedRecord) value;
        }
        throw typeError(path, value, "a record");
    }

    private static long toLong(Object value, String path) throws IOException {
        if (value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        throw typeError(path, value, "an int or long");
    }

    private static double toDouble(Object value, String path) throws IOException {
        if (value instanceof Float || value instanceof Double) {
            return ((Number) value).doubleValue();
        }
        throw typeError(path, value, "a float or double");
    }

    private static boolean toBoolean(Object value, String path) throws IOException {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw typeError(path, value, "a boolean");
    }

    private static String toStringValue(Object value, String path) throws IOException {
        if (value instanceof CharSequence
                || value instanceof GenericEnumSymbol
                || value instanceof Enum
                || value instanceof UUID) {
            // Copy rather than pass the CharSequence through: Avro's readers hand out a Utf8 whose
            // buffer they reuse for the next record, so a retained reference would mutate under a
            // row still held by the sink.
            return value.toString();
        }
        throw typeError(path, value, "a string, enum symbol or uuid");
    }

    private static byte[] toByteArray(Object value, String path) throws IOException {
        if (value instanceof ByteBuffer) {
            // duplicate() so reading does not consume the caller's buffer: the same record may be
            // handed to the sink again after a failure is routed away.
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (value instanceof GenericFixed) {
            return ((GenericFixed) value).bytes();
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        throw typeError(path, value, "bytes");
    }

    private static int toEpochDay(Object value, String path) throws IOException {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof LocalDate) {
            return Math.toIntExact(((LocalDate) value).toEpochDay());
        }
        throw typeError(path, value, "an int or LocalDate");
    }

    private static LocalTime toLocalTime(Object value, long nanosPerUnit, String path)
            throws IOException {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof Integer || value instanceof Long) {
            return LocalTime.ofNanoOfDay(
                    Math.multiplyExact(((Number) value).longValue(), nanosPerUnit));
        }
        throw typeError(path, value, "an int, long or LocalTime");
    }

    private static long toEpochMicros(Object value, long microsPerUnit, String path)
            throws IOException {
        if (value instanceof Instant) {
            Instant instant = (Instant) value;
            return Math.addExact(
                    Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                    instant.getNano() / 1_000L);
        }
        if (value instanceof Integer || value instanceof Long) {
            return Math.multiplyExact(((Number) value).longValue(), microsPerUnit);
        }
        throw typeError(path, value, "a long or Instant");
    }

    private static LocalDateTime toLocalDateTime(Object value, long microsPerUnit, String path)
            throws IOException {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Integer || value instanceof Long) {
            long micros = Math.multiplyExact(((Number) value).longValue(), microsPerUnit);
            return LocalDateTime.ofEpochSecond(
                    Math.floorDiv(micros, 1_000_000L),
                    (int) Math.floorMod(micros, 1_000_000L) * 1_000,
                    ZoneOffset.UTC);
        }
        throw typeError(path, value, "a long or LocalDateTime");
    }

    private static BigDecimal toBigDecimal(Object value, int scale, String path)
            throws IOException {
        if (value instanceof BigDecimal) {
            // The raw byte form can only ever carry the declared scale, so hold the BigDecimal
            // form to it too: rounding here would silently alter a value the user stated, and
            // letting it through writes more fractional digits than the column can keep.
            return ((BigDecimal) value).setScale(scale, RoundingMode.UNNECESSARY);
        }
        return new BigDecimal(new BigInteger(toByteArray(value, path)), scale);
    }

    private static IOException typeError(String path, Object value, String expected) {
        return new IOException(
                "Field "
                        + path
                        + " expects "
                        + expected
                        + " but the record carries "
                        + (value == null ? "null" : "a " + value.getClass().getName()));
    }
}
