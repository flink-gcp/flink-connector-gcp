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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Storage Read Avro shapes independently of a live read session. */
class GenericRecordToRowDataConverterTest {

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"root\",\"fields\":["
                    + "{\"name\":\"s\",\"type\":\"string\"},"
                    + "{\"name\":\"i\",\"type\":\"long\"},"
                    + "{\"name\":\"f\",\"type\":\"double\"},"
                    + "{\"name\":\"ok\",\"type\":\"boolean\"},"
                    + "{\"name\":\"missing\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"bytes\",\"type\":\"bytes\"},"
                    + "{\"name\":\"dec\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":9}},"
                    + "{\"name\":\"day\",\"type\":\"int\"},"
                    + "{\"name\":\"clock\",\"type\":\"long\"},"
                    + "{\"name\":\"civil\",\"type\":\"string\"},"
                    + "{\"name\":\"instant\",\"type\":\"long\"},"
                    + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"nested_record\",\"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}},"
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},"
                    + "{\"name\":\"entries\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"entry_record\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"long\"}]}}},"
                    + "{\"name\":\"counts\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"count_entry_record\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"long\"}]}}}]}";

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private static final RowType TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("s", DataTypes.STRING()),
                                    DataTypes.FIELD("i", DataTypes.INT()),
                                    DataTypes.FIELD("f", DataTypes.FLOAT()),
                                    DataTypes.FIELD("ok", DataTypes.BOOLEAN()),
                                    DataTypes.FIELD("missing", DataTypes.STRING()),
                                    DataTypes.FIELD("bytes", DataTypes.BYTES()),
                                    DataTypes.FIELD("dec", DataTypes.DECIMAL(8, 2)),
                                    DataTypes.FIELD("day", DataTypes.DATE()),
                                    DataTypes.FIELD("clock", DataTypes.TIME(3)),
                                    DataTypes.FIELD("civil", DataTypes.TIMESTAMP(6)),
                                    DataTypes.FIELD(
                                            "instant", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                                    DataTypes.FIELD(
                                            "nested",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("v", DataTypes.STRING()))),
                                    DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.BIGINT())),
                                    DataTypes.FIELD(
                                            "entries",
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                                    DataTypes.FIELD(
                                            "counts", DataTypes.MULTISET(DataTypes.STRING())))
                            .getLogicalType();

    @Test
    void convertsEverySupportedStorageReadShape() throws Exception {
        byte[] serialized =
                InstantiationUtil.serializeObject(new GenericRecordToRowDataConverter(TYPE, null));
        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        GenericRecordToRowDataConverter converter =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        ByteBuffer bytes = ByteBuffer.wrap(new byte[] {1, 2, 3});
        GenericRecord record = record(bytes);

        RowData row = converter.convert(record);

        assertThat(row.getString(0).toString()).isEqualTo("hello");
        assertThat(row.getInt(1)).isEqualTo(7);
        assertThat(row.getFloat(2)).isEqualTo(1.5f);
        assertThat(row.getBoolean(3)).isTrue();
        assertThat(row.isNullAt(4)).isTrue();
        assertThat(row.getBinary(5)).containsExactly(1, 2, 3);
        assertThat(bytes.position()).isZero();
        assertThat(row.getDecimal(6, 8, 2).toBigDecimal()).isEqualByComparingTo("123.45");
        assertThat(row.getInt(7)).isEqualTo((int) LocalDate.of(2026, 8, 12).toEpochDay());
        assertThat(row.getInt(8)).isEqualTo(3_723_004);
        assertThat(row.getTimestamp(9, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("2026-08-12T12:34:56.123456"));
        assertThat(row.getTimestamp(10, 6).toInstant())
                .isEqualTo(Instant.parse("2026-08-12T03:34:56.123456Z"));
        assertThat(row.getRow(11, 1).getString(0).toString()).isEqualTo("inside");
        ArrayData items = row.getArray(12);
        assertThat(items.size()).isEqualTo(2);
        assertThat(items.getLong(0)).isEqualTo(11L);
        assertThat(items.getLong(1)).isEqualTo(12L);
        MapData entries = row.getMap(13);
        assertThat(entries.size()).isEqualTo(1);
        assertThat(entries.keyArray().getString(0).toString()).isEqualTo("a");
        assertThat(entries.valueArray().getLong(0)).isEqualTo(9L);
        MapData counts = row.getMap(14);
        assertThat(counts.size()).isEqualTo(1);
        assertThat(counts.keyArray().getString(0).toString()).isEqualTo("seen");
        assertThat(counts.valueArray().getInt(0)).isEqualTo(2);
    }

    @Test
    void projectionUsesPhysicalNamesAndProducesTheProjectedOrder() {
        GenericRecordToRowDataConverter converter =
                new GenericRecordToRowDataConverter(TYPE, new int[] {10, 0});

        RowData row = converter.convert(record(ByteBuffer.wrap(new byte[] {1})));

        assertThat(row.getArity()).isEqualTo(2);
        assertThat(row.getTimestamp(0, 6).toInstant())
                .isEqualTo(Instant.parse("2026-08-12T03:34:56.123456Z"));
        assertThat(row.getString(1).toString()).isEqualTo("hello");
    }

    @Test
    void emptyProjectionProducesAZeroColumnRow() {
        GenericRecordToRowDataConverter converter =
                new GenericRecordToRowDataConverter(TYPE, new int[0]);

        assertThat(converter.convert(record(ByteBuffer.wrap(new byte[] {1}))).getArity()).isZero();
    }

    @Test
    void rejectsDecimalOverflowInsteadOfTurningItIntoNull() {
        RowType narrow =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("dec", DataTypes.DECIMAL(3, 2)))
                                .getLogicalType();
        GenericRecordToRowDataConverter converter =
                new GenericRecordToRowDataConverter(narrow, null);

        assertThatThrownBy(() -> converter.convert(record(ByteBuffer.wrap(new byte[] {1}))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A BigQuery decimal value does not fit DECIMAL(3, 2)");
    }

    @Test
    void rejectsNarrowIntegerOverflowInsteadOfTruncatingIt() {
        assertIntegerOverflow(DataTypes.TINYINT(), 128L, "TINYINT");
        assertIntegerOverflow(DataTypes.SMALLINT(), 32_768L, "SMALLINT");
        assertIntegerOverflow(DataTypes.INT(), 3_000_000_000L, "INT");
    }

    @Test
    void convertsValidNarrowIntegersAndDoubleWithoutLosingPrecision() {
        GenericRecord record = record(ByteBuffer.wrap(new byte[] {1}));
        record.put("f", 1.234_567_890_123_45d);

        RowType tinyint =
                (RowType) DataTypes.ROW(DataTypes.FIELD("i", DataTypes.TINYINT())).getLogicalType();
        RowType smallint =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("i", DataTypes.SMALLINT())).getLogicalType();
        RowType doubleType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("f", DataTypes.DOUBLE())).getLogicalType();

        assertThat(new GenericRecordToRowDataConverter(tinyint, null).convert(record).getByte(0))
                .isEqualTo((byte) 7);
        assertThat(new GenericRecordToRowDataConverter(smallint, null).convert(record).getShort(0))
                .isEqualTo((short) 7);
        assertThat(
                        new GenericRecordToRowDataConverter(doubleType, null)
                                .convert(record)
                                .getDouble(0))
                .isEqualTo(1.234_567_890_123_45d);
    }

    @Test
    void decodesPreEpochTimestampMicrosWithAFloorRemainder() {
        GenericRecord record = record(ByteBuffer.wrap(new byte[] {1}));
        record.put("instant", -1L);

        RowData row = new GenericRecordToRowDataConverter(TYPE, null).convert(record);

        assertThat(row.getTimestamp(10, 6).toInstant())
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
    }

    @Test
    void truncatesTemporalValuesToEveryDeclaredPrecision() {
        GenericRecord record = record(ByteBuffer.wrap(new byte[] {1}));
        record.put("clock", 3_723_456_000L);
        record.put("civil", "2026-08-12 12:34:56.123456");
        record.put("instant", 1_786_506_896_123_456L);

        for (int precision = 0; precision <= 3; precision++) {
            RowType type =
                    (RowType)
                            DataTypes.ROW(DataTypes.FIELD("clock", DataTypes.TIME(precision)))
                                    .getLogicalType();
            int unit = powerOfTen(3 - precision);
            int expected = 3_723_456 / unit * unit;
            assertThat(new GenericRecordToRowDataConverter(type, null).convert(record).getInt(0))
                    .as("TIME(%s)", precision)
                    .isEqualTo(expected);
        }

        for (int precision = 0; precision <= 6; precision++) {
            int nanos = 123_456_000;
            int unit = powerOfTen(9 - precision);
            int truncated = nanos / unit * unit;
            RowType civilType =
                    (RowType)
                            DataTypes.ROW(DataTypes.FIELD("civil", DataTypes.TIMESTAMP(precision)))
                                    .getLogicalType();
            RowType instantType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    "instant",
                                                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                                            precision)))
                                    .getLogicalType();

            assertThat(
                            new GenericRecordToRowDataConverter(civilType, null)
                                    .convert(record)
                                    .getTimestamp(0, precision)
                                    .toLocalDateTime())
                    .as("TIMESTAMP(%s)", precision)
                    .isEqualTo(LocalDateTime.of(2026, 8, 12, 12, 34, 56, truncated));
            assertThat(
                            new GenericRecordToRowDataConverter(instantType, null)
                                    .convert(record)
                                    .getTimestamp(0, precision)
                                    .toInstant())
                    .as("TIMESTAMP_LTZ(%s)", precision)
                    .isEqualTo(Instant.ofEpochSecond(1_786_506_896L, truncated));
        }
    }

    @Test
    void rejectsTypesBigQueryCannotRepresentAtPlanTime() {
        RowType excessiveTime =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("clock", DataTypes.TIME(6))).getLogicalType();
        RowType nestedArray =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "items",
                                                DataTypes.ARRAY(
                                                        DataTypes.ARRAY(DataTypes.STRING()))))
                                .getLogicalType();

        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(excessiveTime, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("milliseconds");
        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(nestedArray, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nested array");
    }

    private static GenericRecord record(ByteBuffer bytes) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("s", "hello");
        record.put("i", 7L);
        record.put("f", 1.5d);
        record.put("ok", true);
        record.put("missing", null);
        record.put("bytes", bytes);
        record.put("dec", ByteBuffer.wrap(new BigInteger("123450000000").toByteArray()));
        record.put("day", (int) LocalDate.of(2026, 8, 12).toEpochDay());
        record.put("clock", 3_723_004_000L);
        record.put("civil", "2026-08-12 12:34:56.123456");
        Instant instant = Instant.parse("2026-08-12T03:34:56.123456Z");
        record.put("instant", instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L);
        GenericRecord nested = new GenericData.Record(SCHEMA.getField("nested").schema());
        nested.put("v", "inside");
        record.put("nested", nested);
        record.put("items", Arrays.asList(11L, 12L));
        Schema entrySchema = SCHEMA.getField("entries").schema().getElementType();
        GenericRecord entry = new GenericData.Record(entrySchema);
        entry.put("key", "a");
        entry.put("value", 9L);
        record.put("entries", Collections.singletonList(entry));
        Schema countSchema = SCHEMA.getField("counts").schema().getElementType();
        GenericRecord count = new GenericData.Record(countSchema);
        count.put("key", "seen");
        count.put("value", 2L);
        record.put("counts", Collections.singletonList(count));
        return record;
    }

    private static void assertIntegerOverflow(
            org.apache.flink.table.types.DataType type, long value, String summary) {
        RowType narrow = (RowType) DataTypes.ROW(DataTypes.FIELD("i", type)).getLogicalType();
        GenericRecordToRowDataConverter converter =
                new GenericRecordToRowDataConverter(narrow, null);
        GenericRecord record = record(ByteBuffer.wrap(new byte[] {1}));
        record.put("i", value);

        assertThatThrownBy(() -> converter.convert(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A BigQuery integer value does not fit " + summary);
    }

    private static int powerOfTen(int exponent) {
        int value = 1;
        for (int i = 0; i < exponent; i++) {
            value *= 10;
        }
        return value;
    }
}
