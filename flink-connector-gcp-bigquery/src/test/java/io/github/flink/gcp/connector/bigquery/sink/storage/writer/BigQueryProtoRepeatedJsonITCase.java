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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnown;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test of a <b>{@code REPEATED JSON}</b> column against <b>real</b> BigQuery, which is
 * the one part of the well-known-type mapping the emulator cannot verify: goccy 0.8.1 rejects every
 * insert into an {@code ARRAY<JSON>} column — populated or empty alike, so it is the column type it
 * cannot handle and not the data — with {@code "Value has type JSON which cannot be inserted into
 * column w_rep_struct, which has type ARRAY<JSON>"}. Everything else is covered by {@link
 * BigQueryProtoWellKnownITCase} on the emulator.
 *
 * <p>{@code repeated google.protobuf.Struct} has no other representable shape — a {@code Struct} is
 * mutually recursive, so it cannot be expanded into a {@code STRUCT} — which is why this is worth a
 * test against the service rather than a documented assumption.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set. The destination table
 * is created up front and deleted afterwards.
 */
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(300)
class BigQueryProtoRepeatedJsonITCase {

    private static final String PROJECT = System.getenv("BQ_IT_PROJECT");
    private static final String DATASET = System.getenv("BQ_IT_DATASET");
    private static final String TABLE =
            "proto_repeated_json_it_" + UUID.randomUUID().toString().substring(0, 8);

    private static final BigQuery CLIENT =
            BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    @AfterAll
    static void dropTable() {
        CLIENT.delete(TableId.of(PROJECT, DATASET, TABLE));
    }

    @Test
    void writesRepeatedStructsIntoARepeatedJsonColumn() throws Exception {
        ProtoMessageSerializer<WellKnown> serializer =
                ProtoMessageSerializer.of(WellKnown.class, ProtoSchemaOptions.defaults());
        CLIENT.create(
                TableInfo.newBuilder(
                                TableId.of(PROJECT, DATASET, TABLE),
                                StandardTableDefinition.newBuilder()
                                        .setSchema(
                                                StorageSchemaConverter.toBigQuerySchema(
                                                        serializer.getTableSchema(null)))
                                        .build())
                        .build());

        BigQueryDefaultStreamSink<WellKnown> sink =
                (BigQueryDefaultStreamSink<WellKnown>)
                        BigQuerySink.<WellKnown>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, TABLE))
                                .serializer(serializer)
                                .build();
        SinkWriter<WellKnown> writer =
                sink.createWriter(new StreamWriterRowAppenderFactory(), new BigQueryTableAdmin());
        try {
            writer.write(
                    WellKnown.newBuilder()
                            .setWString(StringValue.of("populated"))
                            .addWRepStruct(
                                    Struct.newBuilder()
                                            .putFields(
                                                    "a",
                                                    Value.newBuilder().setBoolValue(true).build()))
                            .addWRepStruct(Struct.getDefaultInstance())
                            .build(),
                    CONTEXT);
            // The empty case separately: an empty ARRAY<JSON> is what the emulator chokes on even
            // when nothing is written to it.
            writer.write(
                    WellKnown.newBuilder().setWString(StringValue.of("empty")).build(), CONTEXT);
            writer.flush(true);
        } finally {
            writer.close();
        }

        assertThat(rows()).containsExactly("empty|0|", "populated|2|{\"a\":true},{}");
    }

    /** One line per row: the marker string, the array length, and the elements joined. */
    private static List<String> rows() throws InterruptedException {
        List<String> rows = new ArrayList<>();
        CLIENT.query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT w_string, ARRAY_LENGTH(w_rep_struct),"
                                                + " (SELECT STRING_AGG(TO_JSON_STRING(e), ',')"
                                                + " FROM UNNEST(w_rep_struct) AS e) FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + "."
                                                + TABLE
                                                + "` ORDER BY w_string")
                                .build())
                .iterateAll()
                .forEach(
                        (FieldValueList row) ->
                                rows.add(
                                        row.get(0).getStringValue()
                                                + "|"
                                                + row.get(1).getStringValue()
                                                + "|"
                                                + (row.get(2).isNull()
                                                        ? ""
                                                        : row.get(2).getStringValue())));
        return rows;
    }
}
