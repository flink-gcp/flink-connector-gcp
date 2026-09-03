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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

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
 * <p>Measured 2026-08-02 against {@code google-cloud-cli:441.0.0-emulators}, re-measured 2026-09-03
 * against the pinned {@code google-cloud-cli:583.0.0-emulators}, both with {@code
 * google-cloud-bigtable} 2.80.0, and re-measured 2026-09-03 under 2.82.0 for the row below that the
 * client change reaches. The image bump moved one row: the emulator now refuses an empty row key
 * instead of storing it. It still answers {@code INTERNAL}, so what the sink sees is unchanged.
 *
 * <p>Each {@code finally} closes the writer plainly, which is an assertion rather than cleanup:
 * every case here leaves a failure in the batcher's accumulated stats, so a close re-reporting
 * those fails them. That is what #238 was, and this class is where it is pinned without credentials
 * — the report is raised for a rejection whatever status it carried, so the emulator's deviating
 * {@code INTERNAL} pins it as well as the service's own statuses would.
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
    void answersInternalRatherThanInvalidArgumentToAnEmptyRowKey() throws Exception {
        // Real Bigtable: INVALID_ARGUMENT for the whole request, "Row keys must be non-empty".
        // Here: the same refusal wrapped in INTERNAL, for the offending entry alone, so the good
        // row lands and the handler sees nothing — the shape the other two rejections already had.
        //
        // This row is the one the image bump changed. Under 441.0.0-emulators the emulator
        // *accepted* an empty row key, and the row it stored broke the client's own read state
        // machine ("rowKey missing") — a state the service cannot produce. Since
        // 583.0.0-emulators it refuses the write, so the emulator now agrees with the service on
        // whether the mutation is legal and deviates only on the status, which is the deviation
        // that matters here.
        TableDestination table = createTable("deviation-empty-row-key");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        table,
                        handler,
                        (element, context) ->
                                GOOD.equals(element)
                                        ? RowMutationEntry.create(element)
                                                .setCell(FAMILY, "payload", 1_000L, "v")
                                        : RowMutationEntry.create("")
                                                .setCell(FAMILY, "payload", 1_000L, "v"));

        try {
            writer.write(GOOD, TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);

            assertThatThrownBy(() -> writer.flush(false))
                    .hasStackTraceContaining("INTERNAL")
                    .hasStackTraceContaining("Row keys must be non-empty");
            assertThat(handler.handled).isEmpty();
            assertThat(readRows(table))
                    .extracting(row -> row.getKey().toStringUtf8())
                    .containsExactly(GOOD);
        } finally {
            writer.close();
        }
    }

    @Test
    void rejectsAClientGeneratedTimestampTheServiceTruncates() throws Exception {
        // The deviation the harness works around, pinned so it cannot close unnoticed.
        //
        // google-cloud-bigtable 2.82.0 stamps the timestamp-less setCell overload with a
        // microsecond Instant.now() and marks the mutation CLIENT_AUTO_GENERATED on the wire.
        // data.proto says the server truncates such a timestamp to the table's granularity and
        // rejects only a USER_SPECIFIED one whose precision does not match, and that is what real
        // Bigtable does — measured 2026-09-03 through the Table sink against a live instance: the
        // write is accepted and the cell stored at a millisecond boundary.
        //
        // The emulator does not implement the field, so it treats the value as user-specified and
        // refuses it. Reported upstream as googleapis/google-cloud-go#20468 with a fix in #20469;
        // the harness workaround and its removal are tracked by #1205. When a pinned image honours
        // the field this test fails, which is the signal to drop the workaround.
        TableDestination table = createTable("deviation-client-generated-timestamp");

        // Retried, because the client reads a microsecond clock and roughly one reading in a
        // thousand lands on a millisecond boundary, which the emulator accepts. A single attempt
        // would be a 0.1% flake. Twenty consecutive aligned readings is not a coincidence worth
        // planning for; it means either the deviation closed or the clock lost its microsecond
        // resolution, and the failure message says so rather than blaming the emulator.
        for (int attempt = 0; attempt < 20; attempt++) {
            // A key per attempt, and asserted by name rather than by the table being empty. An
            // earlier attempt that happened to be aligned was accepted and left a row behind, so
            // "empty" holds only on the run where the first attempt is the rejected one — it would
            // have moved the flake rather than removed it.
            String rowKey = "r" + attempt;
            Throwable failure =
                    catchThrowable(
                            () -> writeCellWithTheClientsWriterClock(table, rowKey, "q", "v"));
            if (failure != null) {
                assertThat(failure).hasStackTraceContaining("invalid timestamp");
                assertThat(readRows(table))
                        .extracting(row -> row.getKey().toStringUtf8())
                        .doesNotContain(rowKey);
                return;
            }
        }
        fail(
                "The emulator accepted twenty client-auto-generated timestamps, so it no longer"
                        + " rejects what this row records. Check which before acting: it may honour"
                        + " Mutation.timestamp_origin (drop the harnessTimestampMicros workaround and"
                        + " close #1205), or accept every unaligned timestamp regardless of origin"
                        + " (a different deviation, and #1205 stays open), or the client may have"
                        + " stopped reading a microsecond clock (this test observes nothing and needs"
                        + " rewriting).");
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
        MutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        null,
                        options,
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"));
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        factory,
                        new BigtableTableAdmin(
                                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint")),
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
