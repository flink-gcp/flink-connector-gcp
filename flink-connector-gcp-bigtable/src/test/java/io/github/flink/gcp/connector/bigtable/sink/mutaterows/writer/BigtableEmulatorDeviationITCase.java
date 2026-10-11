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

package io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
 * Re-measured 2026-10-11 against {@code 588.0.0-emulators} under 2.85.0, which moved both
 * cell-timestamp rows: the emulator now honours {@code Mutation.timestamp_origin}, truncating a
 * client-generated timestamp as the service does (issue #1205), and words its refusal of an
 * explicit unaligned timestamp differently while still answering {@code INTERNAL} for that entry
 * alone.
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
                    .hasStackTraceContaining("timestamp 1234 is not a multiple of 1000");
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
    void truncatesAClientGeneratedTimestampAsTheServiceDoes() throws Exception {
        // Not a deviation but pinned like one, because the harness's timestamp-less writes rest on
        // it. The timestamp-less setCell overload sends a microsecond Instant.now() marked
        // CLIENT_AUTO_GENERATED, which the service truncates to the table's granularity; since
        // 588.0.0-emulators the emulator does too (#1205), where earlier images refused it with
        // "invalid timestamp". newBuilderForEmulator rounds such a timestamp on the client in
        // 2.85.0, so the harness would not notice an image that lost the parity; only this test
        // would, and the harness needs an explicit timestamp again only once the client rounding
        // is gone as well.
        TableDestination table = createTable("deviation-client-generated-timestamp");
        Map<String, Long> sent = new ConcurrentHashMap<>();
        try (BigtableDataClient client = dataClientWithoutTheEmulatorRounding(sent)) {
            for (int attempt = 0; attempt < 20; attempt++) {
                String rowKey = "r" + attempt;
                Throwable failure =
                        catchThrowable(
                                () ->
                                        client.mutateRow(
                                                RowMutation.create(
                                                                TableId.of(table.getTable()),
                                                                rowKey)
                                                        .setCell(FAMILY, "q", "v")));
                assertThat(failure)
                        .as(
                                "a client-auto-generated timestamp; a refusal means the emulator no"
                                        + " longer honours Mutation.timestamp_origin (#1205)")
                        .isNull();
            }
        }
        // Without an unaligned value on the wire the comparison below observes nothing: a client
        // that stopped reading a microsecond clock, or regained its rounding by another path, would
        // pass it on any emulator. A microsecond reading is aligned one time in a thousand.
        assertThat(sent.values())
                .as("timestamps this client sent, at least one of them not a whole millisecond")
                .hasSize(20)
                .anyMatch(micros -> micros % 1_000L != 0);
        Map<String, Long> stored = new HashMap<>();
        for (Row row : readRows(table)) {
            stored.put(row.getKey().toStringUtf8(), row.getCells().get(0).getTimestamp());
        }
        assertThat(stored)
                .as("each stored cell timestamp, against the one sent for its row")
                .hasSize(20)
                .allSatisfy(
                        (rowKey, micros) ->
                                assertThat(micros)
                                        .as("row %s, sent %s", rowKey, sent.get(rowKey))
                                        .isEqualTo(sent.get(rowKey) / 1_000L * 1_000L));
    }

    /**
     * A data client on the emulator without the timestamp rounding {@code newBuilderForEmulator}
     * installs in google-cloud-bigtable 2.85.0. It keeps the rest of the emulator settings and
     * replaces only the channel configurator, which is where the rounding interceptor is added,
     * with one that records the cell timestamp each {@code MutateRow} sends, by row key, so a
     * retried attempt overwrites its own entry rather than adding one.
     */
    private static BigtableDataClient dataClientWithoutTheEmulatorRounding(Map<String, Long> sent)
            throws IOException {
        BigtableDataSettings.Builder settings =
                BigtableDataSettings.newBuilderForEmulator(
                                EMULATOR.getHost(), EMULATOR.getEmulatorPort())
                        .setProjectId(PROJECT)
                        .setInstanceId(INSTANCE);
        InstantiatingGrpcChannelProvider provider =
                (InstantiatingGrpcChannelProvider)
                        settings.stubSettings().getTransportChannelProvider();
        ClientInterceptor recorder =
                new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method,
                            CallOptions callOptions,
                            Channel next) {
                        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                                next.newCall(method, callOptions)) {
                            @Override
                            public void sendMessage(ReqT message) {
                                if (message instanceof MutateRowRequest) {
                                    MutateRowRequest request = (MutateRowRequest) message;
                                    for (Mutation mutation : request.getMutationsList()) {
                                        if (mutation.hasSetCell()) {
                                            sent.put(
                                                    request.getRowKey().toStringUtf8(),
                                                    mutation.getSetCell().getTimestampMicros());
                                        }
                                    }
                                }
                                super.sendMessage(message);
                            }
                        };
                    }
                };
        settings.stubSettings()
                .setTransportChannelProvider(
                        provider.toBuilder()
                                .setChannelConfigurator(
                                        builder -> builder.usePlaintext().intercept(recorder))
                                .build());
        return BigtableDataClient.create(settings.build());
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
        // Real Bigtable: NOT_FOUND for the offending entry (whether the good entry is written has
        // varied, #1534). Here: INTERNAL, the offending entry only, and the good row lands. The
        // status is the deviation this pins.
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
