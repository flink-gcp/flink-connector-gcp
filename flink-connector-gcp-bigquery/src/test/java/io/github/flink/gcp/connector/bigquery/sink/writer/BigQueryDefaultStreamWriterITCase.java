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

import io.github.flink.gcp.connector.bigquery.sink.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the plain at-least-once write path against the BigQuery emulator
 * (goccy/bigquery-emulator), driving a writer created through the {@link BigQuerySink} facade: rows
 * appended to a pre-existing table across several checkpoint-style flushes on the same writer,
 * asserting each flush's rows are queryable once {@code flush()} returns.
 *
 * <p>Emulator deviation (0.8.1, same family as goccy/bigquery-emulator#342): on a Storage Write API
 * connection opened <em>after an earlier connection to the emulator has closed</em>, only the first
 * {@code AppendRows} request is durably applied — follow-up requests are acknowledged but their
 * rows never become queryable. The very first connection applies all its requests, and later
 * single-append connections are unaffected. This multi-flush scenario therefore lives in its own
 * test class, so its connection is guaranteed to be the container's first (each {@code *ITCase}
 * class runs in its own forked JVM with a fresh container). Real BigQuery applies every
 * acknowledged default-stream append.
 */
class BigQueryDefaultStreamWriterITCase extends AbstractBigQueryEmulatorITCase {

    @Test
    void facadeBuiltWriterAppendsAcrossCheckpointFlushes() throws Exception {
        NameColumnSerializer serializer = new NameColumnSerializer();
        createTable("plain_writes", serializer.getTableSchema(null));
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, "plain_writes"))
                                .serializer(serializer)
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
}
