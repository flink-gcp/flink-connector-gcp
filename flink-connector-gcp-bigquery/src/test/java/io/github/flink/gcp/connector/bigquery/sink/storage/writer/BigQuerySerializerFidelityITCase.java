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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroRecordSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypes;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypesChild;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Column-type fidelity of all three serializers against <b>real</b> BigQuery — the encodings where
 * an emulator divergence would be invisible until a user hit it (the #16 acceptance criterion added
 * on the issue): {@code NUMERIC}/{@code BIGNUMERIC} (decimal byte encoding), {@code TIME}/{@code
 * DATETIME} (packed civil-time encoding) — both of which goccy 0.8.1 reads back as unrelated
 * values, so the emulator ITs exclude them entirely — plus {@code TIMESTAMP} microsecond precision,
 * {@code BYTES}, {@code JSON} including {@code REPEATED JSON} (which the emulator rejects
 * outright), and {@code GEOGRAPHY}.
 *
 * <p>The protobuf method runs the <b>full</b> {@code WellKnownTypes} fixture, absorbing what used
 * to be two emulator workarounds: {@code BigQueryProtoRepeatedJsonITCase} (deleted) and the {@code
 * SingularWellKnownTypes} fixture message (deleted) that existed only so {@link
 * BigQueryProtoWellKnownTypesITCase}'s write half had a message without repeated JSON columns. The
 * proto serializer derives no {@code NUMERIC}/{@code TIME}/{@code DATETIME} column — no proto
 * scalar maps to them — so the well-known-type matrix <em>is</em> the complete proto fidelity
 * surface; the Avro and JSON methods carry those types.
 *
 * <p>Writer-level (build the sink, drive its writer directly) rather than a MiniCluster pipeline:
 * fidelity is about bytes on the wire and values read back, and the pipeline is exercised by {@code
 * BigQueryDefaultStreamAtLeastOnceITCase}. Typed accessors ({@code getNumericValue}, {@code
 * getTimestampValue}, {@code getBytesValue}) compare exact values rather than strings wherever the
 * client exposes one.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQuerySerializerFidelityITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String PROTO_TABLE = "fidelity_proto_" + RUN_ID;
    private static final String AVRO_TABLE = "fidelity_avro_" + RUN_ID;
    private static final String JSON_TABLE = "fidelity_json_" + RUN_ID;

    private static final Instant SEEN_AT = Instant.parse("2026-07-26T01:02:03.456789Z");
    private static final long SEEN_AT_MICROS =
            SEEN_AT.getEpochSecond() * 1_000_000L + SEEN_AT.getNano() / 1_000L;

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    @AfterAll
    static void dropTables() {
        RealBigQuery.deleteTables(PROTO_TABLE, AVRO_TABLE, JSON_TABLE);
    }

    /** Builds the writer through the production factory and writes the rows in one flush. */
    @SafeVarargs
    private static <T> void writeRows(
            String table, BigQueryProtoSerializationSchema<? super T> serializer, T... rows)
            throws Exception {
        @SuppressWarnings("unchecked")
        BigQueryDefaultStreamSink<T> sink =
                (BigQueryDefaultStreamSink<T>)
                        BigQuerySink.<T>builder()
                                .table(RealBigQuery.destination(table))
                                .serializer(serializer)
                                .build();
        SinkWriter<T> writer =
                sink.createWriter(
                        new StreamWriterRowAppenderFactory(sink.getOptions()),
                        new BigQueryTableAdmin(),
                        TestSinkWriterMetricGroup.create());
        try {
            for (T row : rows) {
                writer.write(row, CONTEXT);
            }
            writer.flush(true);
        } finally {
            writer.close();
        }
    }

    /**
     * The full {@code WellKnownTypes} fixture on the service — the write half the emulator could
     * only run against the singular subset, including the {@code REPEATED JSON} column it rejects
     * outright. Ports the assertions of the retired emulator write half (unset wrapper → NULL,
     * set-to-default → the default, the single claim the wrapper mapping exists to make) and of the
     * retired {@code BigQueryProtoRepeatedJsonITCase} ({@code w_rep_struct} elements and lengths),
     * and adds {@code w_ts} compared as exact epoch micros.
     */
    @Test
    void protoWellKnownTypesRoundTripOnTheService() throws Exception {
        ProtoMessageSerializationSchema<WellKnownTypes> serializer =
                ProtoMessageSerializationSchema.of(
                        WellKnownTypes.class, ProtoSchemaOptions.defaults());
        RealBigQuery.createTable(PROTO_TABLE, serializer.getTableSchema(null));

        writeRows(
                PROTO_TABLE,
                serializer,
                WellKnownTypes.newBuilder()
                        .setWString(StringValue.of("set"))
                        // Explicitly set to the type default: must not read back as NULL.
                        .setWInt64(Int64Value.of(0L))
                        .setWBool(BoolValue.of(false))
                        .setWBytes(BytesValue.of(ByteString.copyFromUtf8("bytes")))
                        .setWDuration(Duration.newBuilder().setSeconds(1L).setNanos(500_000_000))
                        .setWMask(
                                FieldMask.newBuilder()
                                        .addPaths("user.display_name")
                                        .addPaths("photo"))
                        .setWStruct(
                                Struct.newBuilder()
                                        .putFields(
                                                "k", Value.newBuilder().setNumberValue(1).build()))
                        // A JSON null and a JSON array: the two shapes a JSON column is least
                        // likely to accept, and both are what JsonFormat produces here.
                        .setWValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .setWList(
                                ListValue.newBuilder()
                                        .addValues(Value.newBuilder().setNumberValue(1).build()))
                        .setWAny(
                                Any.newBuilder()
                                        .setTypeUrl("type.googleapis.com/x.Y")
                                        .setValue(ByteString.copyFromUtf8("p")))
                        .setWTs(
                                Timestamp.newBuilder()
                                        .setSeconds(SEEN_AT.getEpochSecond())
                                        .setNanos(SEEN_AT.getNano()))
                        .addWRepStruct(
                                Struct.newBuilder()
                                        .putFields(
                                                "a", Value.newBuilder().setBoolValue(true).build()))
                        .addWRepStruct(Struct.getDefaultInstance())
                        .addWRepInt64(Int64Value.of(1L))
                        .addWRepInt64(Int64Value.of(2L))
                        .addWRepDuration(Duration.newBuilder().setSeconds(1L))
                        .putWMapInt64("m", Int64Value.of(5L))
                        .setWChild(
                                WellKnownTypesChild.newBuilder()
                                        .setCString(StringValue.of("child")))
                        .build(),
                // Every wrapper left unset: those columns must be NULL, not 0 / "" / false.
                WellKnownTypes.newBuilder().setWString(StringValue.of("unset")).build());

        assertThat(rows(protoQuery()))
                .containsExactly(
                        "set|0|false|1500000|user.display_name,photo|{\"k\":1.0}|null|[1.0]"
                                + "|2|{\"a\":true},{}|1,2|1000000|m=5|child",
                        "unset|null|null|null|null|null|null|null|0|null|null|null|null|null");

        // Typed exact comparisons where the client exposes an accessor: Timestamp as an exact
        // epoch-micros long, the BytesValue wrapper's payload byte for byte.
        List<FieldValueList> typed =
                RealBigQuery.queryRows(
                        "SELECT w_ts, w_bytes FROM "
                                + RealBigQuery.tablePath(PROTO_TABLE)
                                + " WHERE w_string = 'set'");
        assertThat(typed).hasSize(1);
        assertThat(typed.get(0).get(0).getTimestampValue()).isEqualTo(SEEN_AT_MICROS);
        assertThat(typed.get(0).get(1).getBytesValue())
                .isEqualTo("bytes".getBytes(StandardCharsets.UTF_8));
    }

    private static String protoQuery() {
        return "SELECT w_string, w_int64, w_bool, w_duration, w_mask, w_struct, w_value, w_list,"
                + " ARRAY_LENGTH(w_rep_struct),"
                + " (SELECT STRING_AGG(TO_JSON_STRING(e), ',') FROM UNNEST(w_rep_struct) AS e),"
                + " (SELECT STRING_AGG(CAST(x AS STRING), ',') FROM UNNEST(w_rep_int64) AS x),"
                + " (SELECT STRING_AGG(CAST(d AS STRING), ',') FROM UNNEST(w_rep_duration) AS d),"
                + " (SELECT STRING_AGG(CONCAT(m.key, '=', CAST(m.value AS STRING)), ',')"
                + " FROM UNNEST(w_map_int64) AS m),"
                + " w_child.c_string FROM "
                + RealBigQuery.tablePath(PROTO_TABLE)
                + " ORDER BY w_string";
    }

    private static final String AVRO_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Fidelity\",\"namespace\":\"it\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"num\",\"type\":[\"null\",{\"type\":\"bytes\","
                    + "\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}]},"
                    + "{\"name\":\"bignum\",\"type\":[\"null\",{\"type\":\"bytes\","
                    + "\"logicalType\":\"decimal\",\"precision\":40,\"scale\":10}]},"
                    + "{\"name\":\"t\",\"type\":[\"null\",{\"type\":\"long\","
                    + "\"logicalType\":\"time-micros\"}]},"
                    + "{\"name\":\"dt\",\"type\":[\"null\",{\"type\":\"long\","
                    + "\"logicalType\":\"local-timestamp-micros\"}]},"
                    + "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\","
                    + "\"logicalType\":\"timestamp-micros\"}]},"
                    + "{\"name\":\"blob\",\"type\":[\"null\",\"bytes\"]},"
                    + "{\"name\":\"payload\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"boundary\",\"type\":[\"null\",\"string\"]}]}";

    private static final BigDecimal NUM = new BigDecimal("1234567.89");
    private static final BigDecimal BIGNUM =
            new BigDecimal("123456789012345678901234567890.0123456789");
    private static final LocalTime TIME = LocalTime.of(1, 2, 3, 456_789_000);
    private static final LocalDateTime DATETIME =
            LocalDateTime.of(2026, 7, 26, 1, 2, 3, 456_789_000);

    /**
     * Exactly the types the emulator ITs exclude ({@code NUMERIC}, {@code BIGNUMERIC}, {@code
     * TIME}, {@code DATETIME}) plus the precision-sensitive rest, derived from an Avro schema and
     * read back with typed accessors. The converted logical-type forms ({@link BigDecimal}, {@link
     * LocalTime}, {@link LocalDateTime}) are passed as values — both raw and converted forms are
     * accepted, and the raw encodings are pinned by unit tests.
     */
    @Test
    void avroNumericAndCivilTimeColumnsRoundTripExactly() throws Exception {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        AvroRecordSerializationSchema serializer =
                AvroRecordSerializationSchema.of(
                        schema,
                        AvroSchemaOptions.builder()
                                .jsonFieldPath("payload")
                                .geographyFieldPath("boundary")
                                .build());
        RealBigQuery.createTable(AVRO_TABLE, serializer.getTableSchema(null));

        // The derived column types, from the live table: the half the values below cannot show.
        assertThat(RealBigQuery.tableFields(AVRO_TABLE))
                .extracting(Field::getName, Field::getType)
                .containsExactly(
                        tuple("name", LegacySQLTypeName.STRING),
                        tuple("num", LegacySQLTypeName.NUMERIC),
                        tuple("bignum", LegacySQLTypeName.BIGNUMERIC),
                        tuple("t", LegacySQLTypeName.TIME),
                        tuple("dt", LegacySQLTypeName.DATETIME),
                        tuple("ts", LegacySQLTypeName.TIMESTAMP),
                        tuple("blob", LegacySQLTypeName.BYTES),
                        tuple("payload", LegacySQLTypeName.JSON),
                        tuple("boundary", LegacySQLTypeName.GEOGRAPHY));

        GenericRecord populated = new GenericData.Record(schema);
        populated.put("name", "set");
        populated.put("num", NUM);
        populated.put("bignum", BIGNUM);
        populated.put("t", TIME);
        populated.put("dt", DATETIME);
        populated.put("ts", SEEN_AT);
        populated.put("blob", ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
        populated.put("payload", "{\"k\":1}");
        populated.put("boundary", "POINT(1 2)");
        GenericRecord empty = new GenericData.Record(schema);
        empty.put("name", "unset");
        writeRows(AVRO_TABLE, serializer, populated, empty);

        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT name, num, bignum, t, dt, ts, blob, payload,"
                                + " ST_ASTEXT(boundary) FROM "
                                + RealBigQuery.tablePath(AVRO_TABLE)
                                + " ORDER BY name");
        assertThat(rows).hasSize(2);
        FieldValueList set = rows.get(0);
        assertThat(set.get("name").getStringValue()).isEqualTo("set");
        // Comparing, not equals: BigQuery pads the scale to the column's (9 / 38), and padding
        // must not fail a correct value.
        assertThat(set.get("num").getNumericValue()).isEqualByComparingTo(NUM);
        assertThat(set.get("bignum").getNumericValue()).isEqualByComparingTo(BIGNUM);
        // Canonical civil renderings: a packed-encoding misread produces a wildly different
        // value, so exact equality is diagnostic.
        assertThat(set.get("t").getStringValue()).isEqualTo("01:02:03.456789");
        assertThat(set.get("dt").getStringValue()).isEqualTo("2026-07-26T01:02:03.456789");
        assertThat(set.get("ts").getTimestampValue()).isEqualTo(SEEN_AT_MICROS);
        assertThat(set.get("blob").getBytesValue())
                .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
        assertThat(set.get("payload").getStringValue()).isEqualTo("{\"k\":1}");
        assertThat(set.get(8).getStringValue()).isEqualTo("POINT(1 2)");
        FieldValueList unset = rows.get(1);
        assertThat(unset.get("name").getStringValue()).isEqualTo("unset");
        for (int i = 1; i < unset.size(); i++) {
            assertThat(unset.get(i).isNull()).as("column %d of the unset row", i).isTrue();
        }
    }

    private static TableFieldSchema jsonField(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(
                        "name".equals(name)
                                ? TableFieldSchema.Mode.REQUIRED
                                : TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    /**
     * The same column types reached from JSON documents through a supplied schema, exercising the
     * pinned library quirks against the real service: a {@code BYTES} column takes a JSON array of
     * byte values (never base64), a {@code JSON} column takes the JSON <em>text</em> as a string,
     * and a <b>bare number in a {@code TIMESTAMP} column is epoch microseconds</b> — the exact
     * place where a divergence would be invisible, so {@code ts_epoch} is written as a bare number
     * and must read back as the same instant {@code ts} got from its RFC 3339 string.
     */
    @Test
    void jsonDocumentNumericAndCivilTimeColumnsRoundTripExactly() throws Exception {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(jsonField("name", TableFieldSchema.Type.STRING))
                        .addFields(jsonField("num", TableFieldSchema.Type.NUMERIC))
                        .addFields(jsonField("bignum", TableFieldSchema.Type.BIGNUMERIC))
                        .addFields(jsonField("t", TableFieldSchema.Type.TIME))
                        .addFields(jsonField("dt", TableFieldSchema.Type.DATETIME))
                        .addFields(jsonField("ts", TableFieldSchema.Type.TIMESTAMP))
                        .addFields(jsonField("ts_epoch", TableFieldSchema.Type.TIMESTAMP))
                        .addFields(jsonField("blob", TableFieldSchema.Type.BYTES))
                        .addFields(jsonField("payload", TableFieldSchema.Type.JSON))
                        .build();
        JsonDocumentSerializationSchema serializer = JsonDocumentSerializationSchema.of(schema);
        RealBigQuery.createTable(JSON_TABLE, serializer.getTableSchema(null));

        writeRows(
                JSON_TABLE,
                serializer,
                "{\"name\":\"set\","
                        + "\"num\":\"1234567.89\","
                        + "\"bignum\":\"123456789012345678901234567890.0123456789\","
                        + "\"t\":\"01:02:03.456789\","
                        + "\"dt\":\"2026-07-26T01:02:03.456789\","
                        + "\"ts\":\"2026-07-26T01:02:03.456789Z\","
                        + "\"ts_epoch\":"
                        + SEEN_AT_MICROS
                        + ","
                        + "\"blob\":[98,108,111,98],"
                        + "\"payload\":\"{\\\"k\\\":1}\"}",
                "{\"name\":\"unset\"}");

        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT name, num, bignum, t, dt, ts, ts_epoch, blob, payload FROM "
                                + RealBigQuery.tablePath(JSON_TABLE)
                                + " ORDER BY name");
        assertThat(rows).hasSize(2);
        FieldValueList set = rows.get(0);
        assertThat(set.get("name").getStringValue()).isEqualTo("set");
        assertThat(set.get("num").getNumericValue()).isEqualByComparingTo(NUM);
        assertThat(set.get("bignum").getNumericValue()).isEqualByComparingTo(BIGNUM);
        assertThat(set.get("t").getStringValue()).isEqualTo("01:02:03.456789");
        assertThat(set.get("dt").getStringValue()).isEqualTo("2026-07-26T01:02:03.456789");
        assertThat(set.get("ts").getTimestampValue()).isEqualTo(SEEN_AT_MICROS);
        // The quirk under test: the bare number was interpreted as epoch micros, nothing else.
        assertThat(set.get("ts_epoch").getTimestampValue()).isEqualTo(SEEN_AT_MICROS);
        assertThat(set.get("blob").getBytesValue())
                .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
        assertThat(set.get("payload").getStringValue()).isEqualTo("{\"k\":1}");
        FieldValueList unset = rows.get(1);
        assertThat(unset.get("name").getStringValue()).isEqualTo("unset");
        for (int i = 1; i < unset.size(); i++) {
            assertThat(unset.get(i).isNull()).as("column %d of the unset row", i).isTrue();
        }
    }

    /**
     * Returns one line per row, joining every column so a wrong conversion shows up.
     *
     * <p>{@code w_value} is deliberately ambiguous in this rendering: the JSON literal {@code null}
     * and a NULL column both print as {@code null}. The unset row, where every column is NULL, is
     * what disambiguates the set one — whose {@code w_value} is set, so its {@code null} is the
     * JSON one. (Carried over from the retired emulator write half, whose assertion this ports.)
     */
    private static List<String> rows(String sql) throws InterruptedException {
        List<String> rows = new ArrayList<>();
        for (FieldValueList row : RealBigQuery.queryRows(sql)) {
            List<String> cells = new ArrayList<>(row.size());
            for (FieldValue value : row) {
                cells.add(value.isNull() ? "null" : value.getStringValue());
            }
            rows.add(String.join("|", cells));
        }
        return rows;
    }
}
