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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.RealGcs;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * End-to-end integration test against <b>real</b> BigQuery and Cloud Storage (the sink's post-load
 * cleanup and BigQuery load jobs have no emulator: goccy/bigquery-emulator supports neither {@code
 * gs://} load jobs nor a storage endpoint).
 *
 * <p>Runs a MiniCluster DataStream job in batch mode with dynamic destinations across two tables —
 * the acceptance scenario of issue #14. Load jobs are free; the test only costs cents of storage
 * for minutes.
 *
 * <p>Also the only place a {@code GEOGRAPHY} column is loaded end to end (#126): a staged Avro
 * {@code string} against an explicit destination schema saying {@code GEOGRAPHY}, a pairing
 * BigQuery's documentation describes for CSV and JSON but not for Avro.
 *
 * <p>{@link #everySupportedColumnTypeSurvivesTheLoad()} is the second job, covering every column
 * type this write method supports with its value asserted after the load. Load jobs are the only
 * thing that can catch a staging encoding the service refuses (#282), so the coverage is here
 * rather than in a converter unit test.
 *
 * <p>Requires application-default credentials plus:
 *
 * <ul>
 *   <li>{@code BQ_IT_PROJECT} — project to write to (and run jobs in)
 *   <li>{@code BQ_IT_DATASET} — existing dataset for the destination tables
 *   <li>{@code BQ_IT_GCS_BUCKET} — existing bucket for staging (a lifecycle rule is recommended)
 * </ul>
 *
 * <p>Skipped automatically when the variables are absent, keeping {@code ./mvnw verify} and CI
 * credential-free.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryFileLoadsITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String TABLE_A = "file_loads_it_a_" + RUN_ID;
    private static final String TABLE_B = "file_loads_it_b_" + RUN_ID;
    private static final String TABLE_TYPES = "file_loads_it_types_" + RUN_ID;
    // One staging directory per test, because multiTableBatchLoad asserts its own is empty after
    // the load and a failing sibling keeps its staged files by design.
    private static final String STAGING_ROOT = "flink-file-loads-it/" + RUN_ID;
    private static final String STAGING_PREFIX = STAGING_ROOT + "/multi";
    private static final String TYPES_STAGING_PREFIX = STAGING_ROOT + "/types";

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("value")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    // Here to prove the claim #126 rests on: that a GEOGRAPHY column survives the
                    // whole FILE_LOADS path. The staging converters have folded GEOGRAPHY in with
                    // STRING and JSON since FILE_LOADS was written, but nothing could derive such a
                    // column until the marker options existed, so no load job had ever carried one
                    // — and BigQuery's own documentation spells out WKT loading for CSV and JSON
                    // only, never for Avro. What is under test is the pairing: an Avro `string`
                    // field against an explicit destination schema that says GEOGRAPHY.
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("boundary")
                                    .setType(TableFieldSchema.Type.GEOGRAPHY)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /**
     * Rows travel as {@code "table|name|value|boundary"} strings (an empty value or boundary means
     * NULL).
     */
    private static final class RowSerializer extends FixedSchemaProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split("\\|", -1);
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(field("name"), parts[1]);
            if (!parts[2].isEmpty()) {
                row.setField(field("value"), Long.parseLong(parts[2]));
            }
            if (!parts[3].isEmpty()) {
                row.setField(field("boundary"), parts[3]);
            }
            return row.build().toByteString();
        }
    }

    /**
     * A NULLABLE column; the three that are not take the mode they need off the returned builder.
     */
    private static TableFieldSchema.Builder column(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE);
    }

    /**
     * Every type {@link
     * io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.TableSchemaToAvroConverter}
     * maps, in both shapes it wraps them in. Only {@code name} is {@code REQUIRED}, so every other
     * column also travels the {@code ["null", T]} union path staged files actually carry; {@code
     * INTERVAL} and {@code RANGE} are absent because this write method rejects them.
     */
    private static final TableSchema TYPES_SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            column("name", TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(column("i", TableFieldSchema.Type.INT64))
                    .addFields(column("f", TableFieldSchema.Type.DOUBLE))
                    .addFields(column("b", TableFieldSchema.Type.BOOL))
                    .addFields(column("blob", TableFieldSchema.Type.BYTES))
                    .addFields(column("ts", TableFieldSchema.Type.TIMESTAMP))
                    .addFields(column("d", TableFieldSchema.Type.DATE))
                    .addFields(column("t", TableFieldSchema.Type.TIME))
                    .addFields(column("dt", TableFieldSchema.Type.DATETIME))
                    .addFields(column("num", TableFieldSchema.Type.NUMERIC))
                    .addFields(column("bignum", TableFieldSchema.Type.BIGNUMERIC))
                    .addFields(column("payload", TableFieldSchema.Type.JSON))
                    .addFields(column("boundary", TableFieldSchema.Type.GEOGRAPHY))
                    .addFields(
                            column("child", TableFieldSchema.Type.STRUCT)
                                    .addFields(column("c_name", TableFieldSchema.Type.STRING))
                                    .addFields(column("c_value", TableFieldSchema.Type.INT64)))
                    .addFields(
                            column("tags", TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REPEATED))
                    .build();

    /**
     * The columns the unset row leaves NULL, <b>derived</b> from the schema rather than listed: a
     * column added above must not be able to drop silently out of the unset-row check, which would
     * read exactly like a passing one. The geography column is read through {@code ST_ASTEXT},
     * which renames it.
     */
    private static final List<String> NULLABLE_COLUMNS =
            TYPES_SCHEMA.getFieldsList().stream()
                    // Not `== NULLABLE`: every mode but REQUIRED and REPEATED is nullable, which
                    // is how the production converters read it and includes an unset mode.
                    .filter(f -> f.getMode() != TableFieldSchema.Mode.REQUIRED)
                    .filter(f -> f.getMode() != TableFieldSchema.Mode.REPEATED)
                    .map(f -> "boundary".equals(f.getName()) ? "boundary_wkt" : f.getName())
                    .collect(Collectors.toList());

    private static final Instant SEEN_AT = Instant.parse("2026-07-26T01:02:03.456789Z");
    private static final long SEEN_AT_MICROS =
            SEEN_AT.getEpochSecond() * 1_000_000L + SEEN_AT.getNano() / 1_000L;
    private static final LocalDate DATE = LocalDate.of(2026, 7, 26);
    private static final LocalTime TIME = LocalTime.of(1, 2, 3, 456_789_000);
    private static final LocalDateTime DATETIME =
            LocalDateTime.of(2026, 7, 26, 1, 2, 3, 456_789_000);
    private static final BigDecimal NUM = new BigDecimal("1234567.89");
    private static final BigDecimal BIGNUM =
            new BigDecimal("123456789012345678901234567890.0123456789");

    /**
     * Writes {@link #TYPES_SCHEMA} in the Storage Write API wire forms every shipped serializer
     * produces: packed civil-time longs for {@code TIME}/{@code DATETIME}, epoch micros for {@code
     * TIMESTAMP}, an epoch day for {@code DATE}, big-endian decimal bytes for the two decimals.
     * {@code "unset"} sets only {@code name}.
     */
    private static final class TypesSerializer extends FixedSchemaProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TYPES_SCHEMA;
        }

        @Override
        public ByteString serialize(String element) {
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(field("name"), element);
            if ("unset".equals(element)) {
                return row.build().toByteString();
            }
            Descriptors.Descriptor child = field("child").getMessageType();
            return row.setField(field("i"), 42L)
                    .setField(field("f"), 1.5d)
                    .setField(field("b"), true)
                    .setField(field("blob"), ByteString.copyFromUtf8("blob"))
                    .setField(field("ts"), SEEN_AT_MICROS)
                    .setField(field("d"), (int) DATE.toEpochDay())
                    .setField(field("t"), CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(TIME))
                    .setField(
                            field("dt"),
                            CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(DATETIME))
                    .setField(
                            field("num"),
                            BigDecimalByteStringEncoder.encodeToNumericByteString(NUM))
                    .setField(
                            field("bignum"),
                            BigDecimalByteStringEncoder.encodeToBigNumericByteString(BIGNUM))
                    .setField(field("payload"), "{\"k\":1}")
                    .setField(field("boundary"), "POINT(1 2)")
                    .setField(
                            field("child"),
                            DynamicMessage.newBuilder(child)
                                    .setField(child.findFieldByName("c_name"), "kid")
                                    .setField(child.findFieldByName("c_value"), 7L)
                                    .build())
                    .addRepeatedField(field("tags"), "a")
                    .addRepeatedField(field("tags"), "b")
                    .build()
                    .toByteString();
        }
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE_A, TABLE_B, TABLE_TYPES);
        RealGcs.deletePrefix(STAGING_ROOT);
    }

    @Test
    void multiTableBatchLoad() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        env.fromData(
                        TABLE_A + "|alpha|1|POINT(1 2)",
                        TABLE_A + "|beta|2|LINESTRING(0 0, 1 1)",
                        // A NULL geography as well as a populated one: the column is NULLABLE, and
                        // an unset one has to load too.
                        TABLE_A + "|gamma||",
                        TABLE_A + "|delta|4|",
                        TABLE_B + "|epsilon|5|POINT(3 4)",
                        TABLE_B + "|zeta|6|")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (element, context) ->
                                                RealBigQuery.destination(
                                                        element.substring(0, element.indexOf('|'))))
                                .serializer(new RowSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(RealGcs.uri(STAGING_PREFIX))
                                                .build())
                                .build());

        env.execute("file-loads-it");

        String tableAPath = RealBigQuery.tablePath(TABLE_A);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + tableAPath))
                .containsExactly(4L);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(TABLE_B)))
                .containsExactly(2L);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT value FROM "
                                        + tableAPath
                                        + " WHERE name = 'beta' AND value IS NOT NULL"))
                .containsExactly(2L);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM "
                                        + tableAPath
                                        + " WHERE name = 'gamma' AND value IS NULL"))
                .containsExactly(1L);

        // The GEOGRAPHY column: the value BigQuery parsed out of the staged Avro string, read back
        // as WKT. A load that had stored the text verbatim in a STRING column would fail ST_AsText.
        assertThat(
                        RealBigQuery.queryRows(
                                "SELECT ST_ASTEXT(boundary) FROM "
                                        + tableAPath
                                        + " WHERE name = 'alpha'"))
                // OrDefault(null), not getStringValue(): a NULL cell means no geometry
                // was stored, and that must fail this assertion rather than throw
                // inside it, as the retired queryStrings did.
                .extracting(row -> row.get(0).getStringValueOrDefault(null))
                .containsExactly("POINT(1 2)");
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM "
                                        + tableAPath
                                        + " WHERE name = 'gamma' AND boundary IS NULL"))
                .containsExactly(1L);

        // Staged objects are deleted after a successful load.
        assertThat(RealGcs.list(STAGING_PREFIX)).isEmpty();
    }

    /**
     * Every column type {@code FILE_LOADS} supports, written through a load job and read back with
     * typed accessors.
     *
     * <p>This exists because #282 was invisible for as long as this class carried {@code
     * STRING}/{@code INT64}/{@code GEOGRAPHY} only: a {@code DATETIME} column was staged as an Avro
     * {@code string}, and BigQuery rejected the whole load job — {@code Field v has incompatible
     * types. Configured schema: datetime; Avro file: string.} — while the converter unit tests
     * stayed green, because they assert the two converters agree with <em>each other</em> and the
     * disagreement was with the service. Only a load job can catch that class of defect, and every
     * type below was equally unverified.
     *
     * <p>The {@code DATETIME} <em>string</em> wire form is deliberately not a column here: once
     * parsed it joins the same {@code local-timestamp-micros} path the packed form takes, so the
     * service-side risk is identical, and the parse itself is covered by {@code
     * ProtoToAvroConverterTest}.
     */
    @Test
    void everySupportedColumnTypeSurvivesTheLoad() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        env.fromData("set", "unset")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destination(RealBigQuery.destination(TABLE_TYPES))
                                .serializer(new TypesSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(RealGcs.uri(TYPES_STAGING_PREFIX))
                                                .build())
                                .build());

        env.execute("file-loads-types-it");

        // The connector auto-created the table, so the column types are its own derivation.
        assertThat(RealBigQuery.tableFields(TABLE_TYPES))
                .extracting(Field::getName, Field::getType)
                .containsExactly(
                        tuple("name", LegacySQLTypeName.STRING),
                        tuple("i", LegacySQLTypeName.INTEGER),
                        tuple("f", LegacySQLTypeName.FLOAT),
                        tuple("b", LegacySQLTypeName.BOOLEAN),
                        tuple("blob", LegacySQLTypeName.BYTES),
                        tuple("ts", LegacySQLTypeName.TIMESTAMP),
                        tuple("d", LegacySQLTypeName.DATE),
                        tuple("t", LegacySQLTypeName.TIME),
                        tuple("dt", LegacySQLTypeName.DATETIME),
                        tuple("num", LegacySQLTypeName.NUMERIC),
                        tuple("bignum", LegacySQLTypeName.BIGNUMERIC),
                        tuple("payload", LegacySQLTypeName.JSON),
                        tuple("boundary", LegacySQLTypeName.GEOGRAPHY),
                        tuple("child", LegacySQLTypeName.RECORD),
                        tuple("tags", LegacySQLTypeName.STRING));

        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT name, i, f, b, blob, ts, d, t, dt, num, bignum, payload,"
                                + " ST_ASTEXT(boundary) AS boundary_wkt, child, tags FROM "
                                + RealBigQuery.tablePath(TABLE_TYPES)
                                + " ORDER BY name");
        assertThat(rows).hasSize(2);

        FieldValueList set = rows.get(0);
        assertThat(set.get("name").getStringValue()).isEqualTo("set");
        assertThat(set.get("i").getLongValue()).isEqualTo(42L);
        assertThat(set.get("f").getDoubleValue()).isEqualTo(1.5d);
        assertThat(set.get("b").getBooleanValue()).isTrue();
        assertThat(set.get("blob").getBytesValue())
                .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
        assertThat(set.get("ts").getTimestampValue()).isEqualTo(SEEN_AT_MICROS);
        assertThat(set.get("d").getStringValue()).isEqualTo("2026-07-26");
        // Canonical civil renderings: a packed-encoding or logical-type misread produces a wildly
        // different value, so exact equality is what makes these diagnostic.
        assertThat(set.get("t").getStringValue()).isEqualTo("01:02:03.456789");
        assertThat(set.get("dt").getStringValue()).isEqualTo("2026-07-26T01:02:03.456789");
        // Comparing, not equals: BigQuery pads the scale to the column's (9 / 38).
        assertThat(set.get("num").getNumericValue()).isEqualByComparingTo(NUM);
        assertThat(set.get("bignum").getNumericValue()).isEqualByComparingTo(BIGNUM);
        assertThat(set.get("payload").getStringValue()).isEqualTo("{\"k\":1}");
        assertThat(set.get("boundary_wkt").getStringValue()).isEqualTo("POINT(1 2)");
        assertThat(set.get("child").getRecordValue().get("c_name").getStringValue())
                .isEqualTo("kid");
        assertThat(set.get("child").getRecordValue().get("c_value").getLongValue()).isEqualTo(7L);
        assertThat(set.get("tags").getRepeatedValue())
                .extracting(FieldValue::getStringValue)
                .containsExactly("a", "b");

        FieldValueList unset = rows.get(1);
        assertThat(unset.get("name").getStringValue()).isEqualTo("unset");
        for (String column : NULLABLE_COLUMNS) {
            assertThat(unset.get(column).isNull())
                    .as("column %s of the unset row", column)
                    .isTrue();
        }
        // A BigQuery REPEATED column cannot be NULL, so an unset one is the empty array.
        assertThat(unset.get("tags").getRepeatedValue()).isEmpty();
    }
}
