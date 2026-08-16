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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code emulatorEndpoint(...)} / {@code emulatorRestEndpoint(...)} wiring, end to end through
 * the sink's <em>production</em> {@code createWriter(WriterInitContext)}.
 *
 * <p>Every other emulator test reaches the emulator through the {@code @VisibleForTesting} {@code
 * createWriter(appenderFactory, tableAdmin, metricGroup)} overload, so it would still pass if the
 * builder's endpoints never reached a client. Here they are the only route: nothing is injected,
 * the destination table does not exist beforehand, and both halves have to work — the gRPC endpoint
 * to open the write stream, the REST one to create the table the first append reports missing.
 */
class BigQueryEmulatorEndpointITCase extends AbstractBigQueryEmulatorITCase {

    @Test
    void aSinkCarryingBothEndpointsCreatesTheTableAndWritesToTheEmulator() throws Exception {
        TableDestination destination =
                TableDestination.of(PROJECT, DATASET, "emulator_endpoint_wiring");
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .destination(destination)
                        .serializer(new NameColumnSerializer())
                        .emulatorEndpoint(grpcEndpoint())
                        .emulatorRestEndpoint(restEndpoint())
                        .build();

        SinkWriter<String> writer = sink.createWriter(new StubWriterInitContext(0));
        try {
            writer.write("alice", CONTEXT);
            writer.write("bob", CONTEXT);
            // One flush only: on this emulator a connection opened after an earlier one closed
            // durably applies its first append alone (see BigQueryDefaultStreamWriterITCase).
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(queryNames("emulator_endpoint_wiring")).containsExactly("alice", "bob");
    }
}
