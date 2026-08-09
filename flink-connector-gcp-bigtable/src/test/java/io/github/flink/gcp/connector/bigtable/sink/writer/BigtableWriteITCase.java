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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the sink's writer against the Bigtable emulator, wired through the public
 * builder and the production {@code DefaultMutationBatcherFactory} in its emulator-endpoint mode —
 * so the client construction that ships is what these exercise.
 */
class BigtableWriteITCase extends AbstractBigtableEmulatorITCase {

    private static final BigtableSerializationSchema<String> SERIALIZER =
            (element, context) ->
                    RowMutationEntry.create(element)
                            .setCell(FAMILY, "payload", 1_000L, "value-" + element);

    @Test
    void appliesEveryMutationByTheTimeAFlushReturns() throws Exception {
        TableDestination table = createTable("writes-on-flush");
        SinkWriter<String> writer = writer(table, BigtableWriterOptions.defaults(), SERIALIZER);

        try {
            for (int i = 0; i < 25; i++) {
                writer.write("row-" + i, TestContexts.NO_OP);
            }
            // Nothing is asserted before the flush: the client batches, so what has reached the
            // service before one is not defined.
            writer.flush(false);

            List<Row> rows = readRows(table);
            assertThat(rows).hasSize(25);
            RowCell cell = rows.get(0).getCells(FAMILY, "payload").get(0);
            assertThat(rows.get(0).getKey().toStringUtf8()).isEqualTo("row-0");
            assertThat(cell.getValue().toStringUtf8()).isEqualTo("value-row-0");
            assertThat(cell.getTimestamp()).isEqualTo(1_000L);
        } finally {
            writer.close();
        }
    }

    @Test
    void writesNothingForRecordsTheSerializerSkips() throws Exception {
        TableDestination table = createTable("skips-nulls");
        SinkWriter<String> writer =
                writer(
                        table,
                        BigtableWriterOptions.defaults(),
                        (element, context) ->
                                element.startsWith("keep")
                                        ? RowMutationEntry.create(element)
                                                .setCell(FAMILY, "payload", 1_000L, "v")
                                        : null);

        try {
            writer.write("keep-1", TestContexts.NO_OP);
            writer.write("drop-1", TestContexts.NO_OP);
            writer.write("keep-2", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly("keep-1", "keep-2");
        } finally {
            writer.close();
        }
    }

    @Test
    void appliesMoreMutationsThanTheInFlightCapAdmitsAtOnce() throws Exception {
        TableDestination table = createTable("backpressures");
        // Below the batch threshold as well, so completions really do have to arrive before the
        // writer may admit more: this is the path where write() yields to the mailbox.
        SinkWriter<String> writer =
                writer(
                        table,
                        BigtableWriterOptions.builder()
                                .maxInFlightMutations(4)
                                .batchElementCount(2)
                                .build(),
                        SERIALIZER);

        try {
            for (int i = 0; i < 40; i++) {
                writer.write("row-" + i, TestContexts.NO_OP);
            }
            writer.flush(false);

            assertThat(readRows(table)).hasSize(40);
        } finally {
            writer.close();
        }
    }

    /**
     * Builds a writer over a batcher the production factory created against the emulator. The
     * writer is created through the sink's injecting overload rather than from a {@code
     * WriterInitContext}, as the Cloud Tasks emulator tests do: the context-driven path — the
     * serializer's {@code open}, the handler's {@code open}, Flink's own metric group — is what
     * {@code BigtableSinkJobITCase} covers by running a job.
     */
    @SuppressWarnings("unchecked")
    private static SinkWriter<String> writer(
            TableDestination table,
            BigtableWriterOptions options,
            BigtableSerializationSchema<String> serializer)
            throws Exception {
        Sink<String> sink =
                BigtableSink.<String>builder()
                        .table(table)
                        .serializer(serializer)
                        .writerOptions(options)
                        .emulatorEndpoint(emulatorEndpoint())
                        .build();
        MutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        null, options, EmulatorEndpoint.parse(emulatorEndpoint()));
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        factory,
                        new BigtableTableAdmin(EmulatorEndpoint.parse(emulatorEndpoint())),
                        new FakeMailboxExecutor(),
                        TestSinkWriterMetricGroup.create());
    }
}
