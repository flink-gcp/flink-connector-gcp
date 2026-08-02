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

import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What Cloud Bigtable rejects a mutation with, and therefore which side of the sink's
 * row-level/fatal boundary each rejection falls on. {@code INVALID_ARGUMENT} is the <i>only</i>
 * routed status, so the whole droppable class rests on which failures carry it, and the emulator
 * validates far too little to answer that — these assertions were written from a run against the
 * service, not from the documented limits.
 *
 * <p>Every case writes through the production write path with a recording handler, so what is
 * asserted is the behaviour a user sees — routed and dropped, or fatal and failing the flush —
 * rather than a status code in isolation.
 *
 * <p>Two conditions that look like they belong here do not, both because the client answers them
 * before the service ever does. {@code Mutation} enforces its own limits inside {@code setCell} —
 * 100,000 mutations and 200 MiB per entry — so "more mutations than a row accepts" and "an entry
 * over the size limit" are thrown by the serializer, never rejected by Bigtable, and the sink
 * routes them as serialization failures with no entry and no row key. Measured 2026-08-02 with
 * {@code google-cloud-bigtable} 2.80.0; a run at 110,000 mutations never reached the wire.
 */
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableRejectionRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String GOOD = "good";

    /** Not a multiple of 1000, so it violates a table's millisecond granularity. */
    private static final long UNALIGNED_TIMESTAMP = 1_234L;

    @Test
    void routesACellTimestampThatIsNotAMultipleOfAThousand() throws Exception {
        // A cell timestamp is in microseconds while a table's granularity is milliseconds. That
        // this is droppable rather than fatal is the point: the connector documentation deferred
        // the claim to this suite because the two differ for anyone running a dropping handler.
        TableDestination table = createTable("granularity");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(table, handler, unalignedTimestamp());

        try {
            writer.write("bad", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(handler.handled).hasSize(1);
            FailedMutation failed = handler.handled.get(0);
            assertThat(StatusCodes.codeOf(failed.getCause()))
                    .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
            assertThat(failed.getCause()).hasMessageContaining("Timestamp granularity mismatch");
            assertThat(readRows(table)).isEmpty();
        } finally {
            closeIgnoringTheBatcherReport(writer);
        }
    }

    @Test
    void routesAnEmptyRowKey() throws Exception {
        TableDestination table = createTable("empty-row-key");
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

            assertThat(handler.handled).hasSize(1);
            FailedMutation failed = handler.handled.get(0);
            assertThat(StatusCodes.codeOf(failed.getCause()))
                    .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
            assertThat(failed.getCause()).hasMessageContaining("Row keys must be non-empty");
        } finally {
            closeIgnoringTheBatcherReport(writer);
        }
    }

    @Test
    void routesEveryEntryOfTheBatchWhenOneOfThemIsRejected() throws Exception {
        // The blast radius of a routed rejection, and the reason the connector documentation no
        // longer claims the entries around a bad one are unaffected: Bigtable rejects the whole
        // MutateRows request, so every entry's future fails with the same status and a dropping
        // handler discards the good records with the bad one. Whether the sink should tell a
        // request-level rejection from a per-entry one is #239; until it does, this pins what
        // happens, so a fix has to come through here.
        TableDestination table = createTable("blast-radius");
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(table, handler, unalignedTimestamp());

        try {
            writer.write(GOOD, TestContexts.NO_OP);
            writer.write("bad", TestContexts.NO_OP);
            writer.flush(false);

            assertThat(handler.handled)
                    .extracting(failed -> failed.getRowKey().toStringUtf8())
                    .containsExactlyInAnyOrder(GOOD, "bad");
            assertThat(readRows(table)).isEmpty();
        } finally {
            closeIgnoringTheBatcherReport(writer);
        }
    }

    @Test
    void failsTheFlushOnAColumnFamilyTheTableDoesNotHave() throws Exception {
        TableDestination table = createTable("unknown-family");
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

            // NOT_FOUND, and fatal: a missing column family fails every record shaped like this
            // one, so it must never reach a handler that may drop it. Bigtable reports it per
            // entry rather than rejecting the request, and reports it for the good entry too.
            assertThatThrownBy(() -> writer.flush(false))
                    .hasMessageContaining(table.getTable())
                    .hasStackTraceContaining("Requested column family not found");
            assertThat(handler.handled).isEmpty();
            assertThat(readRows(table)).isEmpty();
        } finally {
            closeIgnoringTheBatcherReport(writer);
        }
    }

    /** A serializer whose every mutation carries a timestamp the table's granularity forbids. */
    private static BigtableSerializationSchema<String> unalignedTimestamp() {
        return (element, context) ->
                RowMutationEntry.create(element)
                        .setCell(
                                FAMILY,
                                "payload",
                                GOOD.equals(element) ? 1_000L : UNALIGNED_TIMESTAMP,
                                "v");
    }

    /**
     * Closes the writer, tolerating the one exception every case here provokes: gax's batcher
     * re-reports at close every entry failure of its lifetime, whatever the sink's policy already
     * did with them, so a routed-and-dropped mutation still throws from {@code close()}. That is a
     * connector defect rather than a property to assert — #238 — and swallowing it here keeps these
     * tests about the service. Anything else propagates.
     */
    private static void closeIgnoringTheBatcherReport(SinkWriter<String> writer) throws Exception {
        try {
            writer.close();
        } catch (BatchingException e) {
            // Expected until #238 is fixed; the failures it names have already been asserted.
        }
    }

    /** Builds a writer over a batcher the production factory created against the real service. */
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
                        .build();
        MutationBatcher batcher =
                new DefaultMutationBatcherFactory(table, null, options, null).create();
        return ((BigtableMutateRowsSink<String>) sink)
                .createWriter(
                        batcher, new FakeMailboxExecutor(), new RecordingSinkWriterMetricGroup());
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
