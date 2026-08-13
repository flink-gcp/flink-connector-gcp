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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceBuilder;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Type fidelity of the bounded Table source against BigQuery's Storage Read Avro schemas. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryTableSourceFidelityITCase {

    private static final String TABLE = "table_source_fidelity_" + TestNames.runId();
    private static final String OVERFLOW = "12345678901234567890123456789012345678.1";

    @BeforeAll
    static void seed() throws Exception {
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(TABLE)
                        + " (control INT64 NOT NULL, flag BOOL, integer_value INT64, "
                        + "float_value FLOAT64, bytes_value BYTES, text_value STRING, "
                        + "day_value DATE, time_value TIME, datetime_0 DATETIME, "
                        + "datetime_6 DATETIME, timestamp_0 TIMESTAMP, timestamp_6 TIMESTAMP, "
                        + "numeric_default NUMERIC, numeric_param NUMERIC(12, 4), "
                        + "bignumeric_default BIGNUMERIC, bignumeric_param BIGNUMERIC(40, 10), "
                        + "overflow BIGNUMERIC(39, 1), document JSON, place GEOGRAPHY, "
                        + "nested STRUCT<label STRING, amount INT64>, numbers ARRAY<INT64>, "
                        + "children ARRAY<STRUCT<label STRING, amount INT64>>, "
                        + "entries ARRAY<STRUCT<key STRING, value INT64>>, "
                        + "counts ARRAY<STRUCT<key STRING, value INT64>>, "
                        + "range_date RANGE<DATE>, range_datetime RANGE<DATETIME>, "
                        + "range_timestamp RANGE<TIMESTAMP>, span INTERVAL)");
        RealBigQuery.queryRows(
                "INSERT INTO "
                        + RealBigQuery.tablePath(TABLE)
                        + " VALUES "
                        + "(1, TRUE, 7, 1.25, FROM_HEX('0001ff'), 'set', DATE '1969-12-31', "
                        + "TIME '12:34:56.123456', "
                        + "DATETIME '1969-12-31 23:59:59.999999', "
                        + "DATETIME '1969-12-31 23:59:59.999999', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999999+00', "
                        + "TIMESTAMP '1969-12-31 23:59:59.999999+00', "
                        + "NUMERIC '123456789.123456789', NUMERIC '12345678.1234', "
                        + "BIGNUMERIC '0.12345678901234567890123456789012345678', "
                        + "BIGNUMERIC '1234567890123456789012345678.1234567890', "
                        + "BIGNUMERIC '"
                        + OVERFLOW
                        + "', JSON '{\"k\":[1,null]}', ST_GEOGFROMTEXT('POINT(1 2)'), "
                        + "STRUCT('inside' AS label, 7 AS amount), [1, 2], "
                        + "[STRUCT('child' AS label, 8 AS amount)], "
                        + "[STRUCT('a' AS key, 9 AS value)], "
                        + "[STRUCT('seen' AS key, 2 AS value)], "
                        + "RANGE<DATE> '[1969-12-31, 2026-08-13)', "
                        + "RANGE<DATETIME> '[1969-12-31 23:59:59.999999, "
                        + "2026-08-13 12:34:56.123456)', "
                        + "RANGE<TIMESTAMP> '[1969-12-31 23:59:59.999999+00, "
                        + "2026-08-13 03:34:56.123456+00)', "
                        + "INTERVAL '1-2 3 4:5:6.789999' YEAR TO SECOND), "
                        + "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                        + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                        + "ARRAY<INT64>[], ARRAY<STRUCT<label STRING, amount INT64>>[], "
                        + "ARRAY<STRUCT<key STRING, value INT64>>[], "
                        + "ARRAY<STRUCT<key STRING, value INT64>>[], NULL, NULL, NULL, NULL), "
                        + "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                        + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                        + "ARRAY<INT64>[], ARRAY<STRUCT<label STRING, amount INT64>>[], "
                        + "ARRAY<STRUCT<key STRING, value INT64>>[], "
                        + "ARRAY<STRUCT<key STRING, value INT64>>[], "
                        + "RANGE<DATE> '[UNBOUNDED, 2027-01-01)', "
                        + "RANGE<DATETIME> '[2027-01-01 00:00:00, UNBOUNDED)', "
                        + "RANGE<TIMESTAMP> '[UNBOUNDED, UNBOUNDED)', NULL)");
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void readsSupportedStorageShapesThroughTheTableConverter() throws Exception {
        assertRawTimeMicros();
        TableEnvironment table = tableEnvironment();
        table.executeSql(
                "CREATE TABLE fidelity (control BIGINT NOT NULL, flag BOOLEAN, "
                        + "integer_value BIGINT, float_value DOUBLE, bytes_value BYTES, "
                        + "text_value STRING, day_value DATE, time_value TIME(3), "
                        + "datetime_0 TIMESTAMP(0), datetime_6 TIMESTAMP(6), "
                        + "timestamp_0 TIMESTAMP_LTZ(0), timestamp_6 TIMESTAMP_LTZ(6), "
                        + "numeric_default DECIMAL(38, 9), numeric_param DECIMAL(12, 4), "
                        + "bignumeric_default DECIMAL(38, 38), "
                        + "bignumeric_param DECIMAL(38, 10), document STRING, place STRING, "
                        + "nested ROW<label STRING, amount BIGINT>, numbers ARRAY<BIGINT>, "
                        + "children ARRAY<ROW<label STRING, amount BIGINT>>, "
                        + "entries MAP<STRING, BIGINT>, counts MULTISET<STRING>) "
                        + options());

        List<Row> rows = rows(table, "SELECT * FROM fidelity WHERE control <= 2 ORDER BY control");

        assertThat(rows).hasSize(2);
        Row set = rows.get(0);
        assertThat(set.getField("control")).isEqualTo(1L);
        assertThat(set.getField("flag")).isEqualTo(true);
        assertThat(set.getField("integer_value")).isEqualTo(7L);
        assertThat(set.getField("float_value")).isEqualTo(1.25d);
        assertThat((byte[]) set.getField("bytes_value")).containsExactly(0, 1, -1);
        assertThat(set.getField("text_value")).isEqualTo("set");
        assertThat(set.getField("day_value")).isEqualTo(LocalDate.of(1969, 12, 31));
        DataType timeType = table.from("fidelity").getResolvedSchema().getColumnDataTypes().get(7);
        DataType expectedTimeType =
                flinkRetainsSqlTimePrecision() ? DataTypes.TIME(3) : DataTypes.TIME(0);
        assertThat(timeType).isEqualTo(expectedTimeType);
        LocalTime expectedTime =
                timeType.equals(DataTypes.TIME(3))
                        ? LocalTime.of(12, 34, 56, 123_000_000)
                        : LocalTime.of(12, 34, 56);
        assertThat(set.getField("time_value")).isEqualTo(expectedTime);
        assertThat(set.getField("datetime_0"))
                .isEqualTo(LocalDateTime.of(1969, 12, 31, 23, 59, 59));
        assertThat(set.getField("datetime_6"))
                .isEqualTo(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000));
        assertThat(set.getField("timestamp_0")).isEqualTo(Instant.parse("1969-12-31T23:59:59Z"));
        assertThat(set.getField("timestamp_6"))
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat((BigDecimal) set.getField("numeric_default"))
                .isEqualByComparingTo("123456789.123456789");
        assertThat((BigDecimal) set.getField("numeric_param"))
                .isEqualByComparingTo("12345678.1234");
        assertThat((BigDecimal) set.getField("bignumeric_default"))
                .isEqualByComparingTo("0.12345678901234567890123456789012345678");
        assertThat((BigDecimal) set.getField("bignumeric_param"))
                .isEqualByComparingTo("1234567890123456789012345678.1234567890");
        assertThat(set.getField("document")).isEqualTo("{\"k\":[1,null]}");
        assertThat(set.getField("place")).isEqualTo("POINT(1 2)");
        assertRow((Row) set.getField("nested"), "inside", 7L);
        assertThat((Object[]) set.getField("numbers")).containsExactly(1L, 2L);
        Object[] children = (Object[]) set.getField("children");
        assertThat(children).hasSize(1);
        assertRow((Row) children[0], "child", 8L);
        assertThat(set.getField("entries")).isEqualTo(Collections.singletonMap("a", 9L));
        assertThat(set.getField("counts")).isEqualTo(Collections.singletonMap("seen", 2));

        Row unset = rows.get(1);
        assertThat(unset.getField("control")).isEqualTo(2L);
        for (int field = 1; field <= 18; field++) {
            assertThat(unset.getField(field)).as("nullable field %s", field).isNull();
        }
        assertThat((Object[]) unset.getField("numbers")).isEmpty();
        assertThat((Object[]) unset.getField("children")).isEmpty();
        assertThat((Map<?, ?>) unset.getField("entries")).isEmpty();
        assertThat((Map<?, ?>) unset.getField("counts")).isEmpty();
    }

    @Test
    void readsRangesAsSourceOnlyRowsIncludingUnboundedEndpoints() throws Exception {
        TableEnvironment table = tableEnvironment();
        table.executeSql(
                "CREATE TABLE ranges (control BIGINT NOT NULL, "
                        + "range_date ROW<`start` DATE, `end` DATE>, "
                        + "range_datetime ROW<`start` TIMESTAMP(6), `end` TIMESTAMP(6)>, "
                        + "range_timestamp ROW<`start` TIMESTAMP_LTZ(6), "
                        + "`end` TIMESTAMP_LTZ(6)>) "
                        + options());

        List<Row> rows = rows(table, "SELECT * FROM ranges ORDER BY control");

        assertThat(rows).hasSize(3);
        assertRow(
                (Row) rows.get(0).getField("range_date"),
                LocalDate.of(1969, 12, 31),
                LocalDate.of(2026, 8, 13));
        assertRow(
                (Row) rows.get(0).getField("range_datetime"),
                LocalDateTime.parse("1969-12-31T23:59:59.999999"),
                LocalDateTime.parse("2026-08-13T12:34:56.123456"));
        assertRow(
                (Row) rows.get(0).getField("range_timestamp"),
                Instant.parse("1969-12-31T23:59:59.999999Z"),
                Instant.parse("2026-08-13T03:34:56.123456Z"));
        assertThat(rows.get(1).getField("range_date")).isNull();
        assertThat(rows.get(1).getField("range_datetime")).isNull();
        assertThat(rows.get(1).getField("range_timestamp")).isNull();
        assertRow((Row) rows.get(2).getField("range_date"), null, LocalDate.of(2027, 1, 1));
        assertRow(
                (Row) rows.get(2).getField("range_datetime"),
                LocalDateTime.of(2027, 1, 1, 0, 0),
                null);
        assertRow((Row) rows.get(2).getField("range_timestamp"), null, null);
    }

    @Test
    void decimalOverflowFailsInsteadOfRoundingOrBecomingNull() throws Exception {
        List<FieldValueList> control =
                RealBigQuery.queryRows(
                        "SELECT CAST(overflow AS STRING) FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " WHERE control = 1");
        assertThat(control).singleElement();
        assertThat(control.get(0).get(0).getStringValue()).isEqualTo(OVERFLOW);
        TableEnvironment table = tableEnvironment();
        table.executeSql(
                "CREATE TABLE overflow_source (control BIGINT NOT NULL, "
                        + "overflow DECIMAL(38, 1)) "
                        + options());

        assertThatThrownBy(
                        () -> rows(table, "SELECT overflow FROM overflow_source WHERE control = 1"))
                .hasStackTraceContaining("A BigQuery decimal value does not fit DECIMAL(38, 1)");
    }

    @Test
    void measuresAndRejectsTheUndocumentedIntervalRecord() throws Exception {
        List<FieldValueList> control =
                RealBigQuery.queryRows(
                        "SELECT control, CAST(span AS STRING) FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " WHERE control <= 2 ORDER BY control");
        assertThat(control).hasSize(2);
        assertThat(control.get(0).get(0).getLongValue()).isEqualTo(1L);
        assertThat(control.get(0).get(1).getStringValue()).isEqualTo("1-2 3 4:5:6.789999");
        assertThat(control.get(1).get(0).getLongValue()).isEqualTo(2L);
        assertThat(control.get(1).get(1).isNull()).isTrue();

        ReadSession session;
        try (ReadSessionCreator creator = new ReadClientSessionCreator(null)) {
            session =
                    creator.create(
                            CreateReadSessionRequest.newBuilder()
                                    .setParent("projects/" + RealBigQuery.project())
                                    .setMaxStreamCount(1)
                                    .setReadSession(
                                            ReadSession.newBuilder()
                                                    .setTable(
                                                            RealBigQuery.destination(TABLE)
                                                                    .toTablePath())
                                                    .setDataFormat(DataFormat.AVRO)
                                                    .setReadOptions(
                                                            ReadSession.TableReadOptions
                                                                    .newBuilder()
                                                                    .addSelectedFields("control")
                                                                    .addSelectedFields("span")
                                                                    .setRowRestriction(
                                                                            "control <= 2")))
                                    .build());
        }
        Schema writerSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        Schema intervalSchema = nonNull(writerSchema.getField("span").schema());
        assertThat(intervalSchema.getFullName()).isEqualTo("google.sqlType.INTERVAL");
        assertThat(intervalSchema.getFields())
                .extracting(Schema.Field::name)
                .containsExactly("months", "days", "microseconds");
        List<GenericRecord> raw = read(session, writerSchema);
        assertThat(raw).hasSize(2);
        GenericRecord populated = recordWithControl(raw, 1L);
        GenericRecord unset = recordWithControl(raw, 2L);
        GenericRecord interval = (GenericRecord) populated.get("span");
        assertThat(interval.get("months")).isEqualTo(14);
        assertThat(interval.get("days")).isEqualTo(3);
        assertThat(interval.get("microseconds")).isEqualTo(14_706_789_999L);
        assertThat(unset.get("span")).isNull();

        TableEnvironment table = tableEnvironment();
        table.executeSql(
                "CREATE TABLE interval_source (control BIGINT NOT NULL, "
                        + "span ROW<months INT, days INT, microseconds BIGINT>) "
                        + options());
        assertThatThrownBy(() -> rows(table, "SELECT span FROM interval_source"))
                .hasStackTraceContaining("reads BigQuery INTERVAL")
                .hasStackTraceContaining("no lossless Flink Table source mapping");
    }

    private static List<GenericRecord> read(ReadSession session, Schema writerSchema)
            throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
        BinaryDecoder decoder = null;
        try (RowStreamOpener opener =
                        new ReadClientRowStreamOpener(
                                null, BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS);
                RowStream stream = opener.open(session.getStreams(0).getName(), 0)) {
            ReadRowsResponse response;
            while ((response = stream.next()) != null) {
                decoder =
                        DecoderFactory.get()
                                .binaryDecoder(
                                        response.getAvroRows().getSerializedBinaryRows().newInput(),
                                        decoder);
                for (long row = 0; row < response.getRowCount(); row++) {
                    records.add(reader.read(null, decoder));
                }
            }
        }
        return records;
    }

    private static void assertRawTimeMicros() throws Exception {
        List<FieldValueList> control =
                RealBigQuery.queryRows(
                        "SELECT CAST(time_value AS STRING) FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " WHERE control = 1");
        assertThat(control).singleElement();
        assertThat(control.get(0).get(0).getStringValue()).isEqualTo("12:34:56.123456");

        ReadSession session;
        try (ReadSessionCreator creator = new ReadClientSessionCreator(null)) {
            session =
                    creator.create(
                            CreateReadSessionRequest.newBuilder()
                                    .setParent("projects/" + RealBigQuery.project())
                                    .setMaxStreamCount(1)
                                    .setReadSession(
                                            ReadSession.newBuilder()
                                                    .setTable(
                                                            RealBigQuery.destination(TABLE)
                                                                    .toTablePath())
                                                    .setDataFormat(DataFormat.AVRO)
                                                    .setReadOptions(
                                                            ReadSession.TableReadOptions
                                                                    .newBuilder()
                                                                    .addSelectedFields("control")
                                                                    .addSelectedFields("time_value")
                                                                    .setRowRestriction(
                                                                            "control = 1")))
                                    .build());
        }
        Schema writerSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        assertThat(nonNull(writerSchema.getField("time_value").schema()).getLogicalType().getName())
                .isEqualTo("time-micros");
        GenericRecord record = recordWithControl(read(session, writerSchema), 1L);
        assertThat(record.get("time_value")).isEqualTo(45_296_123_456L);
    }

    private static Schema nonNull(Schema schema) {
        for (Schema member : schema.getTypes()) {
            if (member.getType() != Schema.Type.NULL) {
                return member;
            }
        }
        throw new AssertionError("The Storage Read schema has no non-null member");
    }

    private static GenericRecord recordWithControl(List<GenericRecord> records, long control) {
        List<GenericRecord> matches = new ArrayList<>();
        for (GenericRecord record : records) {
            if (Long.valueOf(control).equals(record.get("control"))) {
                matches.add(record);
            }
        }
        assertThat(matches).as("record with control %s", control).singleElement();
        return matches.get(0);
    }

    private static TableEnvironment tableEnvironment() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    private static boolean flinkRetainsSqlTimePrecision() {
        String version = DataTypes.class.getPackage().getImplementationVersion();
        return version != null && version.startsWith("2.3.");
    }

    private static String options() {
        return TableDdl.withOptions(RealBigQuery.project(), RealBigQuery.dataset(), TABLE);
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static void assertRow(Row actual, Object... expected) {
        assertThat(actual.getArity()).isEqualTo(expected.length);
        for (int field = 0; field < expected.length; field++) {
            assertThat(actual.getField(field))
                    .as("nested field %s", field)
                    .isEqualTo(expected[field]);
        }
    }
}
