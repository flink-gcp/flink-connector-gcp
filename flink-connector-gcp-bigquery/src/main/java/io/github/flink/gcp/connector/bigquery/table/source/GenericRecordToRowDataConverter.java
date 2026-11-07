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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts the Storage Read API's Avro value shapes to Flink internal values. */
@Internal
final class GenericRecordToRowDataConverter implements Serializable {

    static final int MAX_TIME_PRECISION = 3;

    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    private interface ValueConverter extends Serializable {
        @Nullable
        Object convert(@Nullable Object value, Schema writerSchema);
    }

    private final RowType physicalRowType;
    @Nullable private final int[][] projectedFields;
    private transient List<PathPlan> fields;

    /**
     * Creates a converter for the planner's projection.
     *
     * @param physicalRowType the physical row type
     * @param projectedFields the produced fields as paths of field indices, or {@code null} for
     *     every physical field; a path longer than one names a field nested in ROWs
     */
    GenericRecordToRowDataConverter(RowType physicalRowType, @Nullable int[][] projectedFields) {
        this.physicalRowType =
                Preconditions.checkNotNull(physicalRowType, "physicalRowType must not be null");
        this.projectedFields = projectedFields == null ? null : deepCopy(projectedFields);
        initializeFields();
    }

    private static int[][] deepCopy(int[][] paths) {
        int[][] copy = new int[paths.length][];
        for (int i = 0; i < paths.length; i++) {
            copy[i] = paths[i].clone();
        }
        return copy;
    }

    private void initializeFields() {
        this.fields = new ArrayList<>();
        if (projectedFields == null) {
            for (int i = 0; i < physicalRowType.getFieldCount(); i++) {
                fields.add(plan(new int[] {i}));
            }
        } else {
            for (int[] path : projectedFields) {
                fields.add(plan(path));
            }
        }
    }

    private PathPlan plan(int[] path) {
        String[] names = new String[path.length];
        LogicalType type = physicalRowType;
        for (int i = 0; i < path.length; i++) {
            RowType row = (RowType) type;
            names[i] = row.getFieldNames().get(path[i]);
            type = row.getTypeAt(path[i]);
        }
        String dotted = String.join(".", names);
        return new PathPlan(names, converter(type, dotted), dotted);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        initializeFields();
    }

    RowData convert(GenericRecord record) {
        GenericRowData row = new GenericRowData(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            row.setField(i, fields.get(i).read(record));
        }
        return row;
    }

    private static ValueConverter converter(LogicalType type, String path) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return nullable((value, schema) -> StringData.fromString(value.toString()));
            case BOOLEAN:
                return nullable((value, schema) -> (Boolean) value);
            case TINYINT:
                return nullable((value, schema) -> tinyint(value));
            case SMALLINT:
                return nullable((value, schema) -> smallint(value));
            case INTEGER:
                return nullable((value, schema) -> integer(value));
            case BIGINT:
                return nullable((value, schema) -> exactLong(value));
            case FLOAT:
                return nullable((value, schema) -> ((Number) value).floatValue());
            case DOUBLE:
                return nullable((value, schema) -> ((Number) value).doubleValue());
            case BINARY:
            case VARBINARY:
                return nullable((value, schema) -> bytes(value));
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                return nullable((value, schema) -> decimal(value, schema, decimal));
            case DATE:
                return nullable((value, schema) -> date(value));
            case TIME_WITHOUT_TIME_ZONE:
                int timePrecision = ((TimeType) type).getPrecision();
                Preconditions.checkArgument(
                        timePrecision <= MAX_TIME_PRECISION,
                        "Column %s has %s, but RowData stores time only to milliseconds",
                        path,
                        type);
                return nullable((value, schema) -> time(value, timePrecision));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int timestampPrecision = ((TimestampType) type).getPrecision();
                Preconditions.checkArgument(
                        timestampPrecision <= 6,
                        "Column %s has %s, but BigQuery DATETIME stores at most microseconds",
                        path,
                        type);
                return nullable((value, schema) -> datetime(value, timestampPrecision));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int localTimestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                Preconditions.checkArgument(
                        localTimestampPrecision <= 6,
                        "Column %s has %s, but BigQuery TIMESTAMP stores at most microseconds",
                        path,
                        type);
                return nullable((value, schema) -> timestamp(value, localTimestampPrecision));
            case ROW:
                return row((RowType) type, path);
            case ARRAY:
                return array((ArrayType) type, path);
            case MAP:
                return map((MapType) type, path);
            case MULTISET:
                return multiset((MultisetType) type, path);
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                throw new IllegalArgumentException(
                        "Column "
                                + path
                                + " has unsupported Table source type "
                                + type
                                + ": BigQuery INTERVAL has no lossless Flink interval mapping;"
                                + " use a query source that casts it to STRING if its text form is"
                                + " sufficient");
            default:
                throw new IllegalArgumentException(
                        "Column " + path + " has unsupported Table source type " + type);
        }
    }

    private static ValueConverter row(RowType type, String path) {
        List<PathPlan> nested = new ArrayList<>();
        for (RowType.RowField field : type.getFields()) {
            String fieldPath = path + "." + field.getName();
            nested.add(
                    new PathPlan(
                            new String[] {field.getName()},
                            converter(field.getType(), fieldPath),
                            fieldPath));
        }
        return (value, schema) -> {
            checkNotInterval(nonNull(schema), path);
            if (value == null) {
                return null;
            }
            GenericRecord record = (GenericRecord) value;
            GenericRowData row = new GenericRowData(nested.size());
            for (int i = 0; i < nested.size(); i++) {
                row.setField(i, nested.get(i).read(record));
            }
            return row;
        };
    }

    private static void checkNotInterval(Schema schema, String path) {
        if (isBigQueryInterval(schema)) {
            throw new IllegalArgumentException(
                    "Column "
                            + path
                            + " reads BigQuery INTERVAL, which has no lossless Flink Table"
                            + " source mapping; use a query source that casts it to STRING if"
                            + " its text form is sufficient");
        }
    }

    private static boolean isBigQueryInterval(Schema schema) {
        return "google.sqlType.INTERVAL".equals(schema.getFullName())
                || "INTERVAL".equals(schema.getProp("sqlType"));
    }

    private static ValueConverter array(ArrayType type, String path) {
        Preconditions.checkArgument(
                type.getElementType().getTypeRoot()
                        != org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY,
                "Column %s is a nested array, which BigQuery cannot store",
                path);
        ValueConverter element = converter(type.getElementType(), path + "[]");
        return nullable(
                (value, schema) -> {
                    Collection<?> input = (Collection<?>) value;
                    Schema elementSchema = nonNull(schema).getElementType();
                    Object[] output = new Object[input.size()];
                    int index = 0;
                    for (Object item : input) {
                        output[index++] = element.convert(item, elementSchema);
                    }
                    return new GenericArrayData(output);
                });
    }

    private static ValueConverter map(MapType type, String path) {
        return entries(
                converter(type.getKeyType(), path + ".key"),
                converter(type.getValueType(), path + ".value"));
    }

    private static ValueConverter multiset(MultisetType type, String path) {
        return entries(
                converter(type.getElementType(), path + ".key"),
                nullable((value, schema) -> integer(value)));
    }

    private static ValueConverter entries(ValueConverter key, ValueConverter value) {
        return nullable(
                (raw, schema) -> {
                    Map<Object, Object> output = new LinkedHashMap<>();
                    for (Object item : (Collection<?>) raw) {
                        GenericRecord entry = (GenericRecord) item;
                        output.put(
                                key.convert(
                                        entry.get("key"),
                                        entry.getSchema().getField("key").schema()),
                                value.convert(
                                        entry.get("value"),
                                        entry.getSchema().getField("value").schema()));
                    }
                    return new GenericMapData(output);
                });
    }

    private static ValueConverter nullable(ValueConverter converter) {
        return (value, schema) -> value == null ? null : converter.convert(value, nonNull(schema));
    }

    private static Schema nonNull(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema member : schema.getTypes()) {
            if (member.getType() != Schema.Type.NULL) {
                return member;
            }
        }
        throw new IllegalArgumentException("An Avro union has no non-null member");
    }

    private static byte[] bytes(Object value) {
        if (value instanceof GenericFixed) {
            return ((GenericFixed) value).bytes().clone();
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer input = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[input.remaining()];
            input.get(bytes);
            return bytes;
        }
        return ((byte[]) value).clone();
    }

    private static DecimalData decimal(Object value, Schema writerSchema, DecimalType type) {
        BigDecimal decimal;
        if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else if (value instanceof CharSequence) {
            decimal = new BigDecimal(value.toString());
        } else {
            org.apache.avro.LogicalType logicalType = nonNull(writerSchema).getLogicalType();
            if (!(logicalType instanceof LogicalTypes.Decimal)) {
                throw new IllegalArgumentException(
                        "A BigQuery decimal field's Avro schema provides no decimal scale");
            }
            decimal =
                    new BigDecimal(
                            new BigInteger(bytes(value)),
                            ((LogicalTypes.Decimal) logicalType).getScale());
        }
        DecimalData converted =
                DecimalData.fromBigDecimal(decimal, type.getPrecision(), type.getScale());
        if (converted == null) {
            throw new IllegalArgumentException(
                    "A BigQuery decimal value does not fit " + type.asSummaryString());
        }
        return converted;
    }

    private static byte tinyint(Object value) {
        long exact = exactLong(value);
        if (exact < Byte.MIN_VALUE || exact > Byte.MAX_VALUE) {
            throw integerOverflow("TINYINT");
        }
        return (byte) exact;
    }

    private static short smallint(Object value) {
        long exact = exactLong(value);
        if (exact < Short.MIN_VALUE || exact > Short.MAX_VALUE) {
            throw integerOverflow("SMALLINT");
        }
        return (short) exact;
    }

    private static int integer(Object value) {
        try {
            return Math.toIntExact(exactLong(value));
        } catch (ArithmeticException e) {
            throw integerOverflow("INT");
        }
    }

    private static long exactLong(Object value) {
        try {
            return new BigDecimal(value.toString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw integerOverflow("BIGINT");
        }
    }

    private static IllegalArgumentException integerOverflow(String type) {
        return new IllegalArgumentException("A BigQuery integer value does not fit " + type);
    }

    private static int date(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof LocalDate) {
            return (int) ((LocalDate) value).toEpochDay();
        }
        return (int) LocalDate.parse(value.toString()).toEpochDay();
    }

    private static int time(Object value, int precision) {
        int millis;
        if (value instanceof Integer) {
            millis = (Integer) value;
        } else if (value instanceof Number) {
            millis = (int) (((Number) value).longValue() / 1_000L);
        } else {
            LocalTime time =
                    value instanceof LocalTime
                            ? (LocalTime) value
                            : LocalTime.parse(value.toString());
            millis = (int) (time.toNanoOfDay() / 1_000_000L);
        }
        int unit = powerOfTen(3 - precision);
        return millis / unit * unit;
    }

    private static TimestampData datetime(Object value, int precision) {
        LocalDateTime datetime;
        if (value instanceof LocalDateTime) {
            datetime = (LocalDateTime) value;
        } else if (value instanceof Number) {
            datetime =
                    CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime(
                            ((Number) value).longValue());
        } else {
            datetime = LocalDateTime.parse(value.toString().replace(' ', 'T'));
        }
        return TimestampData.fromLocalDateTime(
                datetime.withNano(truncateNanos(datetime.getNano(), precision)));
    }

    private static TimestampData timestamp(Object value, int precision) {
        Instant instant;
        if (value instanceof Instant) {
            instant = (Instant) value;
        } else if (value instanceof Number) {
            long micros = ((Number) value).longValue();
            long millis = Math.floorDiv(micros, 1_000L);
            int nanosOfMilli = (int) Math.floorMod(micros, 1_000L) * 1_000;
            instant = Instant.ofEpochMilli(millis).plusNanos(nanosOfMilli);
        } else {
            instant = Instant.parse(value.toString());
        }
        return TimestampData.fromInstant(
                Instant.ofEpochSecond(
                        instant.getEpochSecond(), truncateNanos(instant.getNano(), precision)));
    }

    private static int truncateNanos(int nanos, int precision) {
        int unit = powerOfTen(9 - precision);
        return nanos / unit * unit;
    }

    private static int powerOfTen(int exponent) {
        int value = 1;
        for (int i = 0; i < exponent; i++) {
            value *= 10;
        }
        return value;
    }

    /**
     * Reads one field: the field at the end of a path through nested records, which is a produced
     * field of the planner's projection or one field of a whole ROW.
     *
     * <p>A NULL record on the way makes the field NULL, as Flink evaluates a nested field access
     * under a NULL ROW. The path is followed in the returned record whether Storage Read was asked
     * for the whole path or for a prefix of it, so the two read alike.
     */
    private static final class PathPlan implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String[] names;
        private final ValueConverter converter;
        private final String dotted;

        private PathPlan(String[] names, ValueConverter converter, String dotted) {
            this.names = names;
            this.converter = converter;
            this.dotted = dotted;
        }

        @Nullable
        private Object read(GenericRecord record) {
            GenericRecord current = record;
            for (int i = 0; i < names.length - 1; i++) {
                Object value = current.get(names[i]);
                checkNotInterval(nonNull(current.getSchema().getField(names[i]).schema()), dotted);
                if (value == null) {
                    return null;
                }
                current = (GenericRecord) value;
            }
            String leaf = names[names.length - 1];
            return converter.convert(
                    current.get(leaf), current.getSchema().getField(leaf).schema());
        }
    }
}
