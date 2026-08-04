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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableWriter}.
 *
 * <p>Timed out as a class: the fake mailbox blocks on an empty queue exactly as the real one does,
 * so an admission predicate that can hold with nothing in flight hangs the test rather than failing
 * it.
 */
@Timeout(30)
class BigtableWriterTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private final FakeMutationBatcher batcher = new FakeMutationBatcher();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();

    @Test
    void appliesOneMutationPerRecordAndCountsItSent() throws Exception {
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults());

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(batcher.entries).hasSize(2);
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(2);
        assertThat(metricGroup.counterValue("numBytesSend"))
                .isEqualTo(serializedSize("row-1") + serializedSize("row-2"));
        assertThat(inFlight(writer)).isEqualTo(2);
    }

    @Test
    void skipsRecordsTheSerializerReturnsNullFor() throws Exception {
        // Under failJob(), the policy that turns anything reaching the handler into a thrown
        // IOException — so this pins "a skip is not a failure" more strongly than an assertion
        // about a recording handler could.
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.defaults(),
                        (element, context) -> element.equals("skip-me") ? null : entry(element),
                        failJob());

        writer.write("skip-me", TestContexts.NO_OP);
        writer.write("row-1", TestContexts.NO_OP);

        // Written nowhere, and holding nothing: only the record that became a mutation is in
        // flight, and only its bytes are.
        assertThat(batcher.entries).hasSize(1);
        assertThat(inFlight(writer)).isEqualTo(1);
        assertThat(inFlightBytes(writer)).isEqualTo(serializedSize("row-1"));
    }

    @Test
    void handsARecordItCannotSerializeToTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.defaults(),
                        (element, context) -> {
                            throw new IOException("no mutation for " + element);
                        },
                        handler);

        writer.write("row-1", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        FailedMutation failed = handler.handled.get(0);
        assertThat(failed.getEntry()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.getErrorMessage()).isEqualTo("The record could not be serialized.");
        assertThat(failed.getCause()).hasMessage("no mutation for row-1");
        assertThat(batcher.entries).isEmpty();
    }

    @Test
    void failsTheWriteWhenTheDefaultHandlerRejectsAnUnserializableRecord() {
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.defaults(),
                        (element, context) -> {
                            throw new IOException("boom");
                        },
                        failJob());

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class);
    }

    @Test
    void handsARejectedMutationToTheHandlerAndKeepsGoing() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThat(handler.handled).hasSize(1);
        assertThat(handler.handled.get(0).getRowKey().toStringUtf8()).isEqualTo("row-1");
        assertThat(handler.handled.get(0).getErrorMessage()).contains("INVALID_ARGUMENT");
        assertThat(inFlight(writer)).isZero();
        // Dropped by the handler, so the writer carries on.
        writer.flush(false);
    }

    @Test
    void failsTheJobWhenTheServiceRejectsTheRequestItself() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.PERMISSION_DENIED);
        mailbox.drain();

        // Never routed: a permission failure fails every record alike, so a dropping handler must
        // not see it.
        assertThat(handler.handled).isEmpty();
        assertThatThrownBy(() -> writer.write("row-2", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.i.orders");
    }

    @Test
    void capturesAHandlerFailureRaisedInsideACompletionCallback() throws Exception {
        FailureHandler<FailedMutation> throwing =
                mutation -> {
                    throw new IOException("handler said no");
                };
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.defaults(), serializer(), throwing);
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        // A mail cannot throw at its caller, so the failure has to survive until the next write.
        mailbox.drain();

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("handler said no");
    }

    @Test
    void yieldsToTheMailboxAtTheMutationCap() throws Exception {
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.builder().maxInFlightMutations(2).build());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        // The third write must not proceed until a completion mail has run, so the mail is
        // enqueued before it: yield() would otherwise block forever, failing the test by timeout.
        batcher.succeed(0);
        writer.write("row-3", TestContexts.NO_OP);

        assertThat(batcher.entries).hasSize(3);
        assertThat(inFlight(writer)).isEqualTo(2);
    }

    @Test
    void yieldsToTheMailboxAtTheByteCap() throws Exception {
        long oneRecord = serializedSize("row-1");
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.builder().maxInFlightBytes(oneRecord).build());
        writer.write("row-1", TestContexts.NO_OP);

        batcher.succeed(0);
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(batcher.entries).hasSize(2);
        assertThat(inFlightBytes(writer)).isEqualTo(oneRecord);
    }

    @Test
    void admitsAMutationLargerThanTheByteCapOnAnEmptyWriter() throws Exception {
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.builder().maxInFlightBytes(1).build());

        // "Below the cap", never "does it fit": with nothing in flight no completion mail can
        // arrive, so a fits-predicate would park the task thread for good.
        writer.write("row-1", TestContexts.NO_OP);

        assertThat(batcher.entries).hasSize(1);
        assertThat(inFlightBytes(writer)).isGreaterThan(1);
    }

    @Test
    void flushSendsWhatIsBufferedAndDrainsBeforeFlushingTheHandler() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        batcher.completeOnSend = true;

        writer.flush(false);

        assertThat(batcher.sendOutstandingCalls).isEqualTo(1);
        assertThat(inFlight(writer)).isZero();
        assertThat(handler.flushes).isEqualTo(1);
    }

    @Test
    void flushRoutesTheFailuresItDiscoversBeforeFlushingTheHandler() throws Exception {
        OrderRecordingHandler handler = new OrderRecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);

        // Discovered by the drain inside flush, which is the case the ordering rule exists for:
        // flushing the handler first would checkpoint past this dead letter.
        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        writer.flush(false);

        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void closesTheBatcherAndTheHandlerTogether() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);

        writer.close();

        assertThat(batcher.closeCalls).isEqualTo(1);
        assertThat(handler.closes).isEqualTo(1);
    }

    @Test
    void closesTheHandlerEvenWhenTheBatcherShutdownThrowsAnError() {
        // #276: the handler is last, and Flink's IOUtils.closeAll rethrew an Error from inside its
        // loop, leaving it open — since #211 it can own an SDK publisher and a gRPC channel. That
        // the Error reaches the caller as an Error is the other half: Flink halts the JVM on a
        // fatal one, and only if it arrives unwrapped.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        batcher.closeFailure = new NoClassDefFoundError("batcher shutdown blew up");

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("batcher shutdown blew up");
        assertThat(handler.closes).isEqualTo(1);
    }

    @Test
    void wrapsASynchronousBatcherFailure() {
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults());
        batcher.addFailure = new IllegalStateException("batcher is closed");

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.i.orders");
        // Nothing registered a callback, so nothing may be counted in flight.
        assertThat(inFlight(writer)).isZero();
        assertThat(metricGroup.counterValue("numRecordsSend")).isZero();
    }

    private SinkWriter<String> writer(BigtableWriterOptions options) {
        return writer(options, serializer(), failJob());
    }

    private SinkWriter<String> writer(
            BigtableWriterOptions options,
            BigtableSerializationSchema<String> serializer,
            FailureHandler<? super FailedMutation> handler) {
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(serializer)
                                .writerOptions(options)
                                .failedMutationHandler(handler)
                                .build();
        BigtableSinkConfig<String> config = sink.getConfig();
        assertThat(config.getDestination()).isEqualTo(TABLE);
        return sink.createWriter(batcher, mailbox, metricGroup);
    }

    private static BigtableSerializationSchema<String> serializer() {
        return (element, context) -> entry(element);
    }

    private static RowMutationEntry entry(String rowKey) {
        return RowMutationEntry.create(rowKey).setCell("cf", "q", 1_000L, "value");
    }

    private static long serializedSize(String rowKey) {
        return entry(rowKey).toProto().getSerializedSize();
    }

    private static FailureHandler<FailedMutation> failJob() {
        return FailureHandler.failJob();
    }

    @SuppressWarnings("unchecked")
    private static int inFlight(SinkWriter<String> writer) {
        return ((BigtableWriter<String>) writer).getInFlightMutations();
    }

    @SuppressWarnings("unchecked")
    private static long inFlightBytes(SinkWriter<String> writer) {
        return ((BigtableWriter<String>) writer).getInFlightBytes();
    }

    /** A handler that drops every mutation, recording what it saw and its lifecycle calls. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedMutation> handled = new ArrayList<>();
        private int flushes;
        private int closes;

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedMutation) element);
        }

        @Override
        public void flush() {
            flushes++;
        }

        @Override
        public void close() {
            closes++;
        }
    }

    /** A handler recording the order of its callbacks, for the drain-then-flush rule. */
    private static final class OrderRecordingHandler implements FailureHandler<FailedElement> {

        private final List<String> events = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            events.add("handle");
        }

        @Override
        public void flush() {
            events.add("flush");
        }
    }
}
