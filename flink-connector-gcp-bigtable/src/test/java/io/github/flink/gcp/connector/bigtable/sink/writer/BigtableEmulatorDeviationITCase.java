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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The measured differences between the Bigtable emulator and the service, for the three rejections
 * {@code BigtableRejectionRealGcpITCase} pins against real Bigtable. This class asserts what the
 * <b>emulator</b> does, which is the opposite of treating it as an authority: each assertion here
 * is a trap recorded, and pinning them means an emulator image bump has to state what it changed
 * instead of quietly making the deviation table wrong. The table itself is on the connector
 * documentation page.
 *
 * <p>The deviation that matters most is the status. The emulator answers {@code INTERNAL} where the
 * service answers {@code INVALID_ARGUMENT} or {@code NOT_FOUND}, and this sink classifies {@code
 * INTERNAL} as fatal — so an emulator test would conclude "fails the job" for a condition the
 * service makes droppable, which is exactly the wrong lesson to learn cheaply.
 *
 * <p>Measured 2026-08-02 against the pinned {@code google-cloud-cli:441.0.0-emulators} image with
 * {@code google-cloud-bigtable} 2.80.0.
 *
 * <p>Each {@code finally} closes the writer plainly, which is an assertion rather than cleanup: the
 * two cases whose mutation the emulator rejects leave a failure in the batcher's accumulated stats,
 * so a close re-reporting those fails them. That is what #238 was, and this class is where it is
 * pinned without credentials — the report is raised for a rejection whatever status it carried, so
 * the emulator's deviating {@code INTERNAL} pins it as well as the service's own statuses would.
 * (The empty-row-key case is the one the emulator accepts; nothing accumulates, and its close is
 * cleanup like any other.)
 */
class BigtableEmulatorDeviationITCase extends AbstractBigtableEmulatorITCase {

    private static final String GOOD = "good";

    @Test
    void answersInternalRatherThanInvalidArgumentToAnUnalignedTimestamp() throws Exception {
        // Real Bigtable: INVALID_ARGUMENT for the whole request, both entries routed to the
        // handler, nothing written. Here: INTERNAL for the offending entry alone, which this sink
        // treats as fatal, so the handler sees nothing and the good row lands.
        TableDestination table = createTable("deviation-granularity");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        handler,
                        (element, context) ->
                                RowMutationEntry.create(element)
                                        .setCell(
                                                FAMILY,
                                                "payload",
                                                GOOD.equals(element) ? 1_000L : 1_234L,
                                                "v"));

        try {
            writer.write(GOOD, TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);

            assertThatThrownBy(() -> writer.flush(false))
                    .hasStackTraceContaining("INTERNAL")
                    .hasStackTraceContaining("invalid timestamp 1234");
            assertThat(handler.handled).isEmpty();
            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly(GOOD);
        } finally {
            writer.close();
        }
    }

    @Test
    void acceptsAnEmptyRowKeyAndThenCannotReadItBack() throws Exception {
        // Real Bigtable: INVALID_ARGUMENT, "Row keys must be non-empty". Here the write is
        // accepted, and the row it stores breaks the client's own read state machine — so the
        // emulator is not merely more permissive, it reaches a state the service cannot produce.
        TableDestination table = createTable("deviation-empty-row-key");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        handler,
                        (element, context) ->
                                RowMutationEntry.create("")
                                        .setCell(FAMILY, "payload", 1_000L, "v"));

        try {
            writer.write("bad", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(handler.handled).isEmpty();
            assertThatThrownBy(() -> readRows(table)).hasMessageContaining("rowKey missing");
        } finally {
            writer.close();
        }
    }

    @Test
    void answersNotFoundToAMissingTable() throws Exception {
        // Not a deviation but pinned like one, because the auto-creation repair is built on this
        // status: the emulator answers a missing table with NOT_FOUND fanned request-level over
        // every entry (measured 2026-08-08), which is why BigtableAutoCreationITCase can drive
        // the repair end-to-end while the missing-family case above cannot be. An image bump
        // that changes this answer must declare it here. What this test can pin is the status:
        // under CREATE_NEVER the first NOT_FOUND mail becomes the job failure and the drain stops
        // at it, so the per-entry fan-out is pinned by the repair tests instead — an entry
        // answered anything else would fail BigtableAutoCreationITCase's flush. Real Bigtable's
        // answer is pinned by BigtableAutoCreationRealGcpITCase.
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "deviation-missing-table");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        handler,
                        (element, context) ->
                                RowMutationEntry.create(element)
                                        .setCell(FAMILY, "payload", 1_000L, "v"));

        try {
            writer.write(GOOD, TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);

            // Fatal under the default CREATE_NEVER, with the disposition named; nothing routed.
            assertThatThrownBy(() -> writer.flush(false))
                    .hasMessageContaining("createDisposition is CREATE_NEVER")
                    .hasStackTraceContaining("NOT_FOUND")
                    .hasStackTraceContaining("not found");
            assertThat(handler.handled).isEmpty();
        } finally {
            writer.close();
        }
    }

    @Test
    void answersInternalRatherThanNotFoundToAnUnknownColumnFamily() throws Exception {
        // Real Bigtable: NOT_FOUND, and it fails the good entry of the batch too, so nothing is
        // written. Here: INTERNAL, the offending entry only, and the good row lands.
        TableDestination table = createTable("deviation-unknown-family");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        handler,
                        (element, context) ->
                                GOOD.equals(element)
                                        ? RowMutationEntry.create(element)
                                                .setCell(FAMILY, "payload", 1_000L, "v")
                                        : RowMutationEntry.create(element)
                                                .setCell("no-such-family", "payload", 1_000L, "v"));

        try {
            writer.write(GOOD, TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);

            assertThatThrownBy(() -> writer.flush(false))
                    .hasStackTraceContaining("INTERNAL")
                    .hasStackTraceContaining("unknown family");
            assertThat(handler.handled).isEmpty();
            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly(GOOD);
        } finally {
            writer.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static SinkWriter<String> writer(
            TableDestination table,
            FailureHandler<FailedElement> handler,
            BigtableSerializationSchema<String> serializer)
            throws Exception {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();
        Sink<String> sink =
                BigtableSink.<String>builder()
                        .table(table)
                        .serializer(serializer)
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .emulatorEndpoint(emulatorEndpoint())
                        .build();
        MutationBatcher batcher =
                new DefaultMutationBatcherFactory(
                                table, null, options, EmulatorEndpoint.parse(emulatorEndpoint()))
                        .create();
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        batcher,
                        new BigtableTableAdmin(EmulatorEndpoint.parse(emulatorEndpoint())),
                        new FakeMailboxExecutor(),
                        TestSinkWriterMetricGroup.create());
    }

    /** A handler that drops every mutation, recording what it saw. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedMutation> handled = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedMutation) element);
        }
    }
}
