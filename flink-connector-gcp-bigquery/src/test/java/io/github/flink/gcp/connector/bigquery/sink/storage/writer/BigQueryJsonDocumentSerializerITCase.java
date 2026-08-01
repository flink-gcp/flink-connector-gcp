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

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializerOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link JsonDocumentSerializer} against the BigQuery emulator
 * (goccy/bigquery-emulator): JSON documents written through the {@link BigQuerySink} facade into a
 * table created from the schema handed to the serializer.
 *
 * <p>Covers the column types the emulator round-trips faithfully — {@code REQUIRED}/{@code
 * NULLABLE} scalars, {@code TIMESTAMP}, {@code DATE}, {@code BYTES}, a {@code REPEATED} field, a
 * nested {@code STRUCT} and a {@code JSON} column — and the {@code ignoreUnknownFields} option,
 * which is the reason a JSON stream survives producers that add fields ahead of the table. {@code
 * TIME}, {@code DATETIME} and {@code NUMERIC} are excluded for the reason recorded on {@link
 * BigQueryAvroSerializerITCase}: emulator 0.8.1 implements neither the packed civil-time nor the
 * decimal byte encoding.
 *
 * <p>Only one flush happens here, for the emulator reason recorded on {@link
 * BigQueryDefaultStreamWriterITCase}: on a connection opened after an earlier one has closed, only
 * the first {@code AppendRows} request is durably applied.
 */
class BigQueryJsonDocumentSerializerITCase extends AbstractBigQueryEmulatorITCase {

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static TableSchema schema() {
        return TableSchema.newBuilder()
                .addFields(
                        field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED))
                .addFields(
                        field("count", TableFieldSchema.Type.INT64, TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field(
                                "seen_at",
                                TableFieldSchema.Type.TIMESTAMP,
                                TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field(
                                "seen_on",
                                TableFieldSchema.Type.DATE,
                                TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field("blob", TableFieldSchema.Type.BYTES, TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field(
                                "payload",
                                TableFieldSchema.Type.JSON,
                                TableFieldSchema.Mode.NULLABLE))
                .addFields(
                        field("tags", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REPEATED))
                .addFields(
                        TableFieldSchema.newBuilder()
                                .setName("origin")
                                .setType(TableFieldSchema.Type.STRUCT)
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .addFields(
                                        field(
                                                "host",
                                                TableFieldSchema.Type.STRING,
                                                TableFieldSchema.Mode.NULLABLE))
                                .build())
                .build();
    }

    private static String event(String name, String count, String host) {
        return "{\"name\":\""
                + name
                + "\",\"count\":"
                + count
                + ",\"seen_at\":\"2026-07-26T01:02:03.456789Z\","
                + "\"seen_on\":\"2026-07-26\","
                + "\"blob\":"
                + byteArray(name)
                + ","
                + "\"payload\":\"{\\\"k\\\":1}\","
                + "\"tags\":[\"a\",\"b\"],"
                + "\"origin\":{\"host\":\""
                + host
                + "\"},"
                // Dropped by ignoreUnknownFields; without it the record would fail.
                + "\"produced_by\":\"a newer producer\"}";
    }

    /**
     * A {@code BYTES} column reached from JSON has to be a JSON array of byte values: the client
     * library accepts only that or a {@code ByteString}, never the base64 string that protobuf's
     * own canonical JSON mapping uses. The gap is the library's and is pursued there; #131 tracks
     * it here.
     */
    private static String byteArray(String value) {
        StringBuilder json = new StringBuilder("[");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            json.append(i == 0 ? "" : ",").append(bytes[i]);
        }
        return json.append(']').toString();
    }

    @Test
    void writesJsonDocumentsThroughTheFacade() throws Exception {
        JsonDocumentSerializer serializer =
                JsonDocumentSerializer.of(
                        schema(),
                        JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());
        createTable("json_writes", serializer.getTableSchema(null));

        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, "json_writes"))
                                .serializer(serializer)
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            writer.write(event("alice", "3", "host-a"), CONTEXT);
            writer.write(event("bob", "null", "host-b"), CONTEXT);
            writer.flush(true);
        } finally {
            writer.close();
        }

        assertThat(rows())
                .containsExactly(
                        "alice|3|2026-07-26T01:02:03.456789Z|2026-07-26|alice|{\"k\":1}|a,b|host-a",
                        "bob|null|2026-07-26T01:02:03.456789Z|2026-07-26|bob|{\"k\":1}|a,b|host-b");
    }

    /** Returns one line per row, joining every column so a wrong conversion shows up. */
    private static List<String> rows() throws InterruptedException {
        List<String> rows = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name, count, seen_at, seen_on, blob, payload,"
                                                + " ARRAY_TO_STRING(tags, ','), origin.host FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + ".json_writes` ORDER BY name")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> rows.add(join(row)));
        return rows;
    }

    private static String join(FieldValueList row) {
        StringBuilder line = new StringBuilder();
        line.append(row.get(0).getStringValue())
                .append('|')
                .append(row.get(1).isNull() ? "null" : row.get(1).getLongValue())
                .append('|')
                .append(row.get(2).getTimestampInstant())
                .append('|')
                .append(row.get(3).getStringValue())
                .append('|')
                .append(new String(row.get(4).getBytesValue(), StandardCharsets.UTF_8));
        for (FieldValue value : row.subList(5, row.size())) {
            line.append('|').append(value.getStringValue());
        }
        return line.toString();
    }
}
