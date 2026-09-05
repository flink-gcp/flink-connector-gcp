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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
                    + "{\"name\":\"clock\",\"type\":{\"type\":\"long\","
                    + "\"logicalType\":\"time-micros\"}},"
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
    void convertsDocumentedBigQueryRangeRecordsAsRows() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"ranges\",\"fields\":["
                                        + "{\"name\":\"date_range\",\"type\":[\"null\",{\"type\":\"record\","
                                        + "\"namespace\":\"google.sqlType\",\"name\":\"RANGE_DATE\","
                                        + "\"sqlType\":\"RANGE\",\"fields\":[{\"name\":\"start\","
                                        + "\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
                                        + "{\"name\":\"end\",\"type\":[\"null\",{\"type\":\"int\","
                                        + "\"logicalType\":\"date\"}]}]}]},"
                                        + "{\"name\":\"datetime_range\",\"type\":[\"null\",{\"type\":\"record\","
                                        + "\"namespace\":\"google.sqlType\",\"name\":\"RANGE_DATETIME\","
                                        + "\"sqlType\":\"RANGE\",\"fields\":[{\"name\":\"start\","
                                        + "\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"datetime\"}]},"
                                        + "{\"name\":\"end\",\"type\":[\"null\",{\"type\":\"string\","
                                        + "\"logicalType\":\"datetime\"}]}]}]},"
                                        + "{\"name\":\"timestamp_range\",\"type\":[\"null\",{\"type\":\"record\","
                                        + "\"namespace\":\"google.sqlType\",\"name\":\"RANGE_TIMESTAMP\","
                                        + "\"sqlType\":\"RANGE\",\"fields\":[{\"name\":\"start\","
                                        + "\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                                        + "{\"name\":\"end\",\"type\":[\"null\",{\"type\":\"long\","
                                        + "\"logicalType\":\"timestamp-micros\"}]}]}]}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "date_range",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("start", DataTypes.DATE()),
                                                        DataTypes.FIELD("end", DataTypes.DATE()))),
                                        DataTypes.FIELD(
                                                "datetime_range",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "start", DataTypes.TIMESTAMP(6)),
                                                        DataTypes.FIELD(
                                                                "end", DataTypes.TIMESTAMP(6)))),
                                        DataTypes.FIELD(
                                                "timestamp_range",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "start",
                                                                DataTypes
                                                                        .TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                                                                6)),
                                                        DataTypes.FIELD(
                                                                "end",
                                                                DataTypes
                                                                        .TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                                                                6)))))
                                .getLogicalType();
        GenericRecord root = new GenericData.Record(schema);
        GenericRecord date = recordFor(schema.getField("date_range").schema());
        date.put("start", -1);
        date.put("end", null);
        root.put("date_range", date);
        GenericRecord datetime = recordFor(schema.getField("datetime_range").schema());
        datetime.put("start", null);
        datetime.put("end", "1969-12-31T23:59:59.999999");
        root.put("datetime_range", datetime);
        GenericRecord timestamp = recordFor(schema.getField("timestamp_range").schema());
        timestamp.put("start", -1L);
        timestamp.put("end", null);
        root.put("timestamp_range", timestamp);

        RowData converted = new GenericRecordToRowDataConverter(type, null).convert(root);

        RowData dateRange = converted.getRow(0, 2);
        assertThat(dateRange.getInt(0)).isEqualTo(-1);
        assertThat(dateRange.isNullAt(1)).isTrue();
        RowData datetimeRange = converted.getRow(1, 2);
        assertThat(datetimeRange.isNullAt(0)).isTrue();
        assertThat(datetimeRange.getTimestamp(1, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999"));
        RowData timestampRange = converted.getRow(2, 2);
        assertThat(timestampRange.getTimestamp(0, 6).toInstant())
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat(timestampRange.isNullAt(1)).isTrue();
    }

    @Test
    void rejectsBigQueryIntervalEvenWhenDeclaredAsARowAndNull() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"root_interval\",\"fields\":["
                                        + "{\"name\":\"span\",\"type\":[\"null\",{\"type\":\"record\","
                                        + "\"namespace\":\"google.sqlType\",\"name\":\"INTERVAL\","
                                        + "\"fields\":[{\"name\":\"months\",\"type\":\"int\"},"
                                        + "{\"name\":\"days\",\"type\":\"int\"},{\"name\":\"microseconds\","
                                        + "\"type\":\"long\"}]}]}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "span",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("months", DataTypes.INT()),
                                                        DataTypes.FIELD("days", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "microseconds",
                                                                DataTypes.BIGINT()))))
                                .getLogicalType();
        GenericRecord record = new GenericData.Record(schema);
        record.put("span", null);

        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(type, null).convert(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reads BigQuery INTERVAL")
                .hasMessageContaining("no lossless Flink Table source mapping")
                .hasMessageContaining("query source that casts it to STRING");
    }

    @Test
    void rejectsBothFlinkIntervalFamiliesAtPlanTime() {
        RowType yearMonth =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "span",
                                                DataTypes.INTERVAL(
                                                        DataTypes.YEAR(), DataTypes.MONTH())))
                                .getLogicalType();
        RowType dayTime =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "span",
                                                DataTypes.INTERVAL(
                                                        DataTypes.DAY(), DataTypes.SECOND(6))))
                                .getLogicalType();

        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(yearMonth, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no lossless Flink interval mapping")
                .hasMessageContaining("query source that casts it to STRING");
        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(dayTime, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no lossless Flink interval mapping")
                .hasMessageContaining("query source that casts it to STRING");
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

    @Test
    void fixedBinaryDeclarationsPreserveActualLengthsAndNulls() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"fixed_binary\",\"fields\":["
                                        + "{\"name\":\"value\",\"type\":[\"null\",\"bytes\"]}]}");
        for (int length : new int[] {1, 2, 4}) {
            RowType type =
                    (RowType)
                            DataTypes.ROW(DataTypes.FIELD("value", DataTypes.BINARY(length)))
                                    .getLogicalType();
            GenericRecordToRowDataConverter converter =
                    new GenericRecordToRowDataConverter(type, null);
            GenericRecord record = new GenericData.Record(schema);
            record.put("value", null);
            assertThat(converter.convert(record).isNullAt(0)).isTrue();
            for (int actual : new int[] {0, length - 1, length, length + 1}) {
                byte[] backing = new byte[actual + 2];
                java.util.Arrays.fill(backing, (byte) 0x80);
                ByteBuffer buffer = ByteBuffer.wrap(backing, 1, actual);
                record.put("value", buffer);
                byte[] converted = converter.convert(record).getBinary(0);
                assertThat(converted)
                        .containsExactly(java.util.Arrays.copyOfRange(backing, 1, actual + 1));
                assertThat(buffer.position()).isEqualTo(1);
                assertThat(buffer.remaining()).isEqualTo(actual);
                backing[1] = 0;
                if (actual > 0) {
                    assertThat(converted[0]).isEqualTo((byte) 0x80);
                }
            }
        }
    }

    @Test
    void copiesGenericFixedAndRawByteArrayShapesInsteadOfAliasingThem() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"binary_shapes\",\"fields\":["
                                        + "{\"name\":\"padded\",\"type\":{\"type\":\"fixed\",\"name\":\"fx3\",\"size\":3}},"
                                        + "{\"name\":\"raw\",\"type\":\"bytes\"}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("padded", DataTypes.BYTES()),
                                        DataTypes.FIELD("raw", DataTypes.BYTES()))
                                .getLogicalType();
        byte[] fixedBacking = new byte[] {1, 2, 3};
        byte[] rawBacking = new byte[] {4, 5, 6};
        GenericRecord record = new GenericData.Record(schema);
        record.put(
                "padded", new GenericData.Fixed(schema.getField("padded").schema(), fixedBacking));
        record.put("raw", rawBacking);

        RowData row = new GenericRecordToRowDataConverter(type, null).convert(record);
        fixedBacking[0] = 9;
        rawBacking[0] = 9;

        // Copied, not aliased: both shapes hand out their backing array itself
        // (GenericFixed.bytes() is the internal array), so a row that shared it would change
        // whenever the producer mutates or reuses the buffer — which Avro's decoders are allowed
        // to do, even though this module's cursor currently decodes each record fresh.
        assertThat(row.getBinary(0)).containsExactly(1, 2, 3);
        assertThat(row.getBinary(1)).containsExactly(4, 5, 6);
    }

    @Test
    void convertsBigDecimalAndStringDecimalShapesExactly() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"decimal_shapes\",\"fields\":["
                                        + "{\"name\":\"object\",\"type\":\"bytes\"},"
                                        + "{\"name\":\"text\",\"type\":\"string\"}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("object", DataTypes.DECIMAL(8, 2)),
                                        DataTypes.FIELD("text", DataTypes.DECIMAL(8, 2)))
                                .getLogicalType();
        GenericRecord record = new GenericData.Record(schema);
        record.put("object", new BigDecimal("-12.34"));
        record.put("text", "-98.7");

        RowData row = new GenericRecordToRowDataConverter(type, null).convert(record);

        // Negative values, so a decode that loses or flips the sign cannot pass.
        assertThat(row.getDecimal(0, 8, 2).toBigDecimal()).isEqualByComparingTo("-12.34");
        assertThat(row.getDecimal(1, 8, 2).toBigDecimal()).isEqualByComparingTo("-98.70");
    }

    @Test
    void rejectsADecimalBytesValueWhoseSchemaCarriesNoScale() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"scaleless\",\"fields\":["
                                        + "{\"name\":\"dec\",\"type\":\"bytes\"}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("dec", DataTypes.DECIMAL(8, 2)))
                                .getLogicalType();
        GenericRecord record = new GenericData.Record(schema);
        record.put("dec", ByteBuffer.wrap(new BigInteger("1234").toByteArray()));

        // Without the writer schema's scale the bytes are only an unscaled integer; guessing a
        // scale would read every value of the column a power of ten off.
        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(type, null).convert(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A BigQuery decimal field's Avro schema provides no decimal scale");
    }

    @Test
    void convertsAlternateTemporalObjectAndStringShapes() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"temporal_shapes\",\"fields\":["
                                        + "{\"name\":\"day_object\",\"type\":\"string\"},"
                                        + "{\"name\":\"day_text\",\"type\":\"string\"},"
                                        + "{\"name\":\"time_millis\",\"type\":\"int\"},"
                                        + "{\"name\":\"time_object\",\"type\":\"string\"},"
                                        + "{\"name\":\"time_text\",\"type\":\"string\"},"
                                        + "{\"name\":\"civil_object\",\"type\":\"string\"},"
                                        + "{\"name\":\"civil_packed\",\"type\":\"long\"},"
                                        + "{\"name\":\"instant_object\",\"type\":\"string\"},"
                                        + "{\"name\":\"instant_text\",\"type\":\"string\"}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("day_object", DataTypes.DATE()),
                                        DataTypes.FIELD("day_text", DataTypes.DATE()),
                                        DataTypes.FIELD("time_millis", DataTypes.TIME(3)),
                                        DataTypes.FIELD("time_object", DataTypes.TIME(3)),
                                        DataTypes.FIELD("time_text", DataTypes.TIME(3)),
                                        DataTypes.FIELD("civil_object", DataTypes.TIMESTAMP(6)),
                                        DataTypes.FIELD("civil_packed", DataTypes.TIMESTAMP(6)),
                                        DataTypes.FIELD(
                                                "instant_object",
                                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                                        DataTypes.FIELD(
                                                "instant_text",
                                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)))
                                .getLogicalType();
        LocalDateTime civil = LocalDateTime.of(2026, 8, 12, 12, 34, 56, 123_456_000);
        GenericRecord record = new GenericData.Record(schema);
        record.put("day_object", LocalDate.of(1969, 12, 31));
        record.put("day_text", "1969-12-31");
        record.put("time_millis", 3_600_123);
        record.put("time_object", LocalTime.of(1, 0, 0, 123_000_000));
        record.put("time_text", "01:00:00.123");
        record.put("civil_object", civil);
        // The encoder still takes threeten-bp; the decoder under test answers java.time.
        record.put(
                "civil_packed",
                CivilTimeEncoder.encodePacked64DatetimeMicros(
                        org.threeten.bp.LocalDateTime.of(2026, 8, 12, 12, 34, 56, 123_456_000)));
        record.put("instant_object", Instant.parse("1969-12-31T23:59:59.999999Z"));
        record.put("instant_text", "2026-08-12T03:34:56.123456Z");

        RowData row = new GenericRecordToRowDataConverter(type, null).convert(record);

        // Pre-epoch days and fractional clocks, so an off-by-one in any decode shows up.
        assertThat(row.getInt(0)).isEqualTo(-1);
        assertThat(row.getInt(1)).isEqualTo(-1);
        assertThat(row.getInt(2)).isEqualTo(3_600_123);
        assertThat(row.getInt(3)).isEqualTo(3_600_123);
        assertThat(row.getInt(4)).isEqualTo(3_600_123);
        assertThat(row.getTimestamp(5, 6).toLocalDateTime()).isEqualTo(civil);
        assertThat(row.getTimestamp(6, 6).toLocalDateTime()).isEqualTo(civil);
        assertThat(row.getTimestamp(7, 6).toInstant())
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat(row.getTimestamp(8, 6).toInstant())
                .isEqualTo(Instant.parse("2026-08-12T03:34:56.123456Z"));
    }

    @Test
    void rejectsANonNumericBigintValueRatherThanReadingZero() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"bad_long\",\"fields\":["
                                        + "{\"name\":\"i\",\"type\":\"string\"}]}");
        RowType type =
                (RowType) DataTypes.ROW(DataTypes.FIELD("i", DataTypes.BIGINT())).getLogicalType();
        GenericRecord record = new GenericData.Record(schema);
        record.put("i", "not-a-number");

        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(type, null).convert(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A BigQuery integer value does not fit BIGINT");
    }

    @Test
    void rejectsAUnionFieldWithNoNonNullMember() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"null_only\",\"fields\":["
                                        + "{\"name\":\"u\",\"type\":[\"null\"]}]}");
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "u",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("v", DataTypes.STRING()))))
                                .getLogicalType();
        GenericRecord record = new GenericData.Record(schema);
        record.put("u", null);

        // A union of only null can hold no record at all; taking its null member as the record
        // schema would silently turn the whole column into nulls.
        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(type, null).convert(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An Avro union has no non-null member");
    }

    @Test
    void rejectsAnUnmappedTypeRootAtPlanTime() {
        RowType unmapped =
                (RowType) DataTypes.ROW(DataTypes.FIELD("n", DataTypes.NULL())).getLogicalType();

        assertThatThrownBy(() -> new GenericRecordToRowDataConverter(unmapped, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column n has unsupported Table source type");
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

    private static GenericRecord recordFor(Schema schema) {
        for (Schema member : schema.getTypes()) {
            if (member.getType() != Schema.Type.NULL) {
                return new GenericData.Record(member);
            }
        }
        throw new AssertionError("The test schema has no record member");
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
