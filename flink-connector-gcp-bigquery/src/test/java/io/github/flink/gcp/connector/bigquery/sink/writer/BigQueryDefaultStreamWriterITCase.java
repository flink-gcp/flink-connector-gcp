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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.github.flink.gcp.connector.bigquery.sink.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the plain at-least-once write path against the BigQuery emulator
 * (goccy/bigquery-emulator), driving writers created through the {@link BigQuerySink} facade: rows
 * appended to a pre-existing table across several checkpoint-style flushes, and per-record dynamic
 * destinations fanning out to multiple tables.
 *
 * <p>Emulator deviation (0.8.1, same family as goccy/bigquery-emulator#342): once an earlier
 * Storage Write API connection to the emulator has been closed, a follow-up append on a later
 * connection can be acknowledged yet never become queryable. Real BigQuery makes acknowledged
 * default-stream appends immediately queryable. The multi-flush test is therefore ordered first,
 * before any test closes a connection.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryDefaultStreamWriterITCase extends AbstractBigQueryEmulatorITCase {

    private static void createTable(String table) {
        restClient.create(
                TableInfo.of(
                        TableId.of(DATASET, table),
                        StandardTableDefinition.of(
                                Schema.of(Field.of("name", StandardSQLTypeName.STRING)))));
    }

    private static List<String> queryNames(String table) throws InterruptedException {
        List<String> names = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + "."
                                                + table
                                                + "` ORDER BY name")
                                .build())
                .iterateAll()
                .forEach(row -> names.add(row.get(0).getStringValue()));
        return names;
    }

    @Test
    @Order(1)
    void facadeBuiltWriterAppendsAcrossCheckpointFlushes() throws Exception {
        createTable("plain_writes");
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, "plain_writes"))
                                .serializer(new NameColumnSerializer())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            // First checkpoint interval: flushed rows must be queryable once flush() returns.
            writer.write("alice", CONTEXT);
            writer.write("bob", CONTEXT);
            writer.flush(false);
            assertThat(queryNames("plain_writes")).containsExactly("alice", "bob");
            // Second checkpoint interval on the same writer, then end of input.
            writer.write("carol", CONTEXT);
            writer.flush(true);
            assertThat(queryNames("plain_writes")).containsExactly("alice", "bob", "carol");
        } finally {
            writer.close();
        }
    }

    @Test
    @Order(2)
    void dynamicDestinationsFanOutToMultipleTables() throws Exception {
        createTable("fanout_even");
        createTable("fanout_odd");
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        PROJECT,
                                                        DATASET,
                                                        element.length() % 2 == 0
                                                                ? "fanout_even"
                                                                : "fanout_odd"))
                                .serializer(new NameColumnSerializer())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            writer.write("dave", CONTEXT);
            writer.write("eve", CONTEXT);
            writer.write("mallory", CONTEXT);
            writer.flush(true);
            assertThat(queryNames("fanout_even")).containsExactly("dave");
            assertThat(queryNames("fanout_odd")).containsExactly("eve", "mallory");
        } finally {
            writer.close();
        }
    }
}
