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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.testproto.SingularWellKnownTypes;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Integration test for protobuf well-known types against the BigQuery emulator
 * (goccy/bigquery-emulator): messages written through the {@link BigQuerySink} facade into a table
 * created from the serializer's own derived schema.
 *
 * <p>Two things here are out of reach of the unit tests, which see only the derived schema, the
 * derived descriptor and the converted values: that a table can actually be <b>created</b> with a
 * scalar column where a {@code STRUCT} used to be and {@code JSON} for {@code Struct}/{@code
 * Value}/{@code ListValue}; and that an unset wrapper reads back as <b>NULL</b> while one
 * explicitly set to the type default reads back as that default — the single claim the wrapper
 * mapping exists to make, and one no client-side assertion can stand in for.
 *
 * <p>The two halves use different fixtures on purpose. Emulator 0.8.1 rejects <em>every</em> insert
 * into a table carrying an {@code ARRAY<JSON>} column, populated or empty alike ({@code "Value has
 * type JSON which cannot be inserted into column w_rep_struct, which has type ARRAY<JSON>"}), so
 * the write half uses {@code SingularWellKnownTypes}. Creating such a table works, which is why the
 * schema half still uses the full matrix; writing into one is verified against real BigQuery by
 * {@link BigQueryProtoRepeatedJsonITCase}.
 *
 * <p><b>Revisit when the nightly real-GCP workflow lands (#28, #16):</b> the write half should then
 * run against the full {@code WellKnownTypes} fixture on the service, and {@code
 * SingularWellKnownTypes} — which exists for no reason but this emulator limitation — can be
 * deleted along with {@link BigQueryProtoRepeatedJsonITCase}, whose coverage it would subsume.
 *
 * <p>Only one flush happens per table, for the emulator reason recorded on {@link
 * BigQueryDefaultStreamWriterITCase}: on a connection opened after an earlier one has closed, only
 * the first {@code AppendRows} request is durably applied.
 */
class BigQueryProtoWellKnownTypesITCase extends AbstractBigQueryEmulatorITCase {

    private static final String TABLE = "proto_wellknown";
    private static final String SINGULAR_TABLE = "proto_wellknown_singular";

    /** The full matrix, including the repeated and map columns, is a creatable table. */
    @Test
    void derivesACreatableTableForEveryWellKnownType() {
        createTable(
                TABLE,
                ProtoMessageSerializer.of(WellKnownTypes.class, ProtoSchemaOptions.defaults())
                        .getTableSchema(null));

        Schema created = schemaOf(TABLE);
        assertThat(created.getFields())
                .extracting(Field::getName, Field::getType, Field::getMode)
                .contains(
                        tuple("w_int32", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("w_double", LegacySQLTypeName.FLOAT, Field.Mode.NULLABLE),
                        tuple("w_bool", LegacySQLTypeName.BOOLEAN, Field.Mode.NULLABLE),
                        tuple("w_string", LegacySQLTypeName.STRING, Field.Mode.NULLABLE),
                        tuple("w_bytes", LegacySQLTypeName.BYTES, Field.Mode.NULLABLE),
                        tuple("w_duration", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("w_mask", LegacySQLTypeName.STRING, Field.Mode.NULLABLE),
                        tuple("w_struct", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_value", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_list", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_rep_int64", LegacySQLTypeName.INTEGER, Field.Mode.REPEATED),
                        tuple("w_rep_struct", LegacySQLTypeName.JSON, Field.Mode.REPEATED),
                        // Any keeps its two columns: the payload cannot be expanded without the
                        // descriptor its type URL names.
                        tuple("w_any", LegacySQLTypeName.RECORD, Field.Mode.NULLABLE));
        assertThat(created.getFields().get("w_map_struct").getSubFields())
                .extracting(Field::getName, Field::getType)
                .contains(tuple("value", LegacySQLTypeName.JSON));
    }

    @Test
    void writesSingularWellKnownTypesAsScalarAndJsonColumns() throws Exception {
        ProtoMessageSerializer<SingularWellKnownTypes> serializer =
                ProtoMessageSerializer.of(
                        SingularWellKnownTypes.class, ProtoSchemaOptions.defaults());
        createTable(SINGULAR_TABLE, serializer.getTableSchema(null));

        BigQueryDefaultStreamSink<SingularWellKnownTypes> sink =
                (BigQueryDefaultStreamSink<SingularWellKnownTypes>)
                        BigQuerySink.<SingularWellKnownTypes>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, SINGULAR_TABLE))
                                .serializer(serializer)
                                .build();
        SinkWriter<SingularWellKnownTypes> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            writer.write(
                    SingularWellKnownTypes.newBuilder()
                            .setSString(StringValue.of("set"))
                            // Explicitly set to the type default: must not read back as NULL.
                            .setSInt64(Int64Value.of(0L))
                            .setSBool(BoolValue.of(false))
                            .setSDuration(
                                    Duration.newBuilder().setSeconds(1L).setNanos(500_000_000))
                            .setSMask(
                                    FieldMask.newBuilder()
                                            .addPaths("user.display_name")
                                            .addPaths("photo"))
                            .setSStruct(
                                    Struct.newBuilder()
                                            .putFields(
                                                    "k",
                                                    Value.newBuilder().setNumberValue(1).build()))
                            // A JSON null and a JSON array: the two shapes a JSON column is least
                            // likely to accept, and both are what JsonFormat produces here.
                            .setSValue(
                                    Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                            .setSList(
                                    ListValue.newBuilder()
                                            .addValues(
                                                    Value.newBuilder().setNumberValue(1).build()))
                            .setSAny(
                                    Any.newBuilder()
                                            .setTypeUrl("type.googleapis.com/x.Y")
                                            .setValue(ByteString.copyFromUtf8("p")))
                            .build(),
                    CONTEXT);
            // Every wrapper left unset: those columns must be NULL, not 0 / "" / false.
            writer.write(
                    SingularWellKnownTypes.newBuilder().setSString(StringValue.of("unset")).build(),
                    CONTEXT);
            writer.flush(true);
        } finally {
            writer.close();
        }

        assertThat(schemaOf(SINGULAR_TABLE).getFields())
                .extracting(Field::getName, Field::getType, Field::getMode)
                .contains(
                        tuple("s_int64", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("s_duration", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("s_mask", LegacySQLTypeName.STRING, Field.Mode.NULLABLE),
                        tuple("s_struct", LegacySQLTypeName.JSON, Field.Mode.NULLABLE));

        assertThat(rows())
                .containsExactly(
                        "set|0|false|1500000|user.display_name,photo|{\"k\":1.0}|null|[1.0]",
                        "unset|null|null|null|null|null|null|null");
    }

    private static Schema schemaOf(String table) {
        Schema schema =
                restClient
                        .getTable(TableId.of(PROJECT, DATASET, table))
                        .getDefinition()
                        .getSchema();
        assertThat(schema).isNotNull();
        return schema;
    }

    /**
     * Returns one line per row, joining every column so a wrong conversion shows up.
     *
     * <p>{@code s_value} is deliberately ambiguous in this rendering: the JSON literal {@code null}
     * and a NULL column both print as {@code null}. The second row, where every column is NULL, is
     * what disambiguates the first — whose {@code s_value} is set, so its {@code null} is the JSON
     * one.
     */
    private static List<String> rows() throws InterruptedException {
        List<String> rows = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT s_string, s_int64, s_bool, s_duration, s_mask,"
                                                + " s_struct, s_value, s_list FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + "."
                                                + SINGULAR_TABLE
                                                + "` ORDER BY s_string")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> rows.add(join(row)));
        return rows;
    }

    private static String join(FieldValueList row) {
        List<String> cells = new ArrayList<>(row.size());
        for (FieldValue value : row) {
            cells.add(value.isNull() ? "null" : value.getStringValue());
        }
        return String.join("|", cells);
    }
}
