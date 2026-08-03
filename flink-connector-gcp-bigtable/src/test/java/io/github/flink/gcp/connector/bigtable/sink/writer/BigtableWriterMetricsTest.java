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
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.bigtable.sink.FailedMutation;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the metrics {@link BigtableWriter} registers, asserted through {@link
 * TestSinkWriterMetricGroup} by the names a reporter sees, so a renamed or unregistered metric
 * fails here rather than silently.
 *
 * <p>Separate from {@code BigtableWriterTest}, which asserts the writer's behaviour and carries the
 * two send counters only because they were all this sink had.
 */
@Timeout(30)
class BigtableWriterMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private final FakeMutationBatcher batcher = new FakeMutationBatcher();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void countsARecordTheSerializerRejectedAsASendErrorAndUnderNoStatus() throws Exception {
        SinkWriter<String> writer =
                writer(
                        (element, context) -> {
                            throw new IOException("no mutation for " + element);
                        },
                        dropping());

        writer.write("row-1", TestContexts.NO_OP);

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        // Nothing reached the client, so nothing was sent.
        assertThat(counter("numRecordsSend")).isZero();
        // A serialization failure carries no status, and counting it would put every one of them
        // under UNCLASSIFIED alongside the RPC failures that genuinely carry none.
        assertThat(metrics.hasMetric("errorClass", "UNCLASSIFIED", "errors")).isFalse();
    }

    @Test
    void countsARoutedMutationAsASendErrorAndNamesTheStatusItFailedWith() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        // Counted as sent when the client accepted it; the rejection does not undo that.
        assertThat(counter("numRecordsSend")).isEqualTo(1);
    }

    @Test
    void countsARecordTheHandlerFailedTheJobOverAsARoutedOne() throws Exception {
        FailureHandler<FailedMutation> throwing =
                mutation -> {
                    throw new IOException("handler said no");
                };
        SinkWriter<String> writer = writer(serializer(), throwing);
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        // "Routed", not "dropped": the counter is what a reader watches to tell a policy is firing
        // at all, so a handler that then fails the job must not make the record invisible.
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
    }

    @Test
    void namesTheStatusOfAFatalFailureThatIsNeverRouted() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.PERMISSION_DENIED);
        mailbox.drain();

        // Every failure the client gave up on is counted, not only the routable ones: a job dying
        // of PERMISSION_DENIED should say so under its own name.
        assertThat(errors("PERMISSION_DENIED")).isEqualTo(1);
        // Not routed, so not a send error — the two counters answer different questions.
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void countsEveryFatalFailureAndNotOnlyTheOneThatBecameTheJobFailure() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.PERMISSION_DENIED);
        batcher.fail(1, StatusCode.Code.UNAVAILABLE);
        mailbox.drain();

        // Only the first becomes asyncError, but both are mutations the client gave up on.
        assertThat(errors("PERMISSION_DENIED")).isEqualTo(1);
        assertThat(errors("UNAVAILABLE")).isEqualTo(1);
    }

    @Test
    void countsAFailureCarryingNoStatusAsUnclassified() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        batcher.futures.get(0).setException(new IllegalStateException("no status here"));
        mailbox.drain();

        assertThat(errors("UNCLASSIFIED")).isEqualTo(1);
    }

    @Test
    void namesTheOutermostStatusOfAChainItRoutesOnAnInnerOne() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        // INVALID_ARGUMENT wrapped in an INTERNAL: the writer treats it as fatal (the outermost
        // classifiable status is not INVALID_ARGUMENT), and the counter names what it failed with.
        batcher.futures
                .get(0)
                .setException(
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.INTERNAL,
                                FakeMutationBatcher.apiException(
                                        StatusCode.Code.INVALID_ARGUMENT)));
        mailbox.drain();

        assertThat(errors("INTERNAL")).isEqualTo(1);
        assertThat(metrics.hasMetric("errorClass", "INVALID_ARGUMENT", "errors")).isFalse();
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void namesTheOutermostStatusOfAChainItTreatsAsFatalForABuriedTransientOne() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        // The case the counter and the routing genuinely disagree on: an INVALID_ARGUMENT over an
        // UNAVAILABLE. Routing scans the whole chain and finds the transient one, so the mutation
        // is *not* routed — an unstable service must never produce a dead letter — while the
        // counter names the status the failure was reported with. Counting the routing decision
        // instead would answer a different question, and this is what says which one is meant.
        batcher.futures
                .get(0)
                .setException(
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.INVALID_ARGUMENT,
                                FakeMutationBatcher.apiException(StatusCode.Code.UNAVAILABLE)));
        mailbox.drain();

        assertThat(errors("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(metrics.hasMetric("errorClass", "UNAVAILABLE", "errors")).isFalse();
        assertThat(counter("numRecordsSendErrors")).isZero();
    }

    @Test
    void gaugesReportTheMutationsAndBytesInFlight() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        long bytes = serializedSize("row-1") + serializedSize("row-2");
        assertThat(metrics.<Integer>gaugeValue("inFlightMutations")).isEqualTo(2);
        assertThat(metrics.<Long>gaugeValue("inFlightBytes")).isEqualTo(bytes);
        // The two must not be the same number, or a gauge bound to the wrong field would pass.
        assertThat(bytes).isNotEqualTo(2L);

        batcher.succeed(0);
        batcher.succeed(1);
        mailbox.drain();

        assertThat(metrics.<Integer>gaugeValue("inFlightMutations")).isZero();
        assertThat(metrics.<Long>gaugeValue("inFlightBytes")).isZero();
    }

    @Test
    void stopsReportingInFlightMutationsOnceTheWriterIsClosedMidFlight() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        // Closed without a flush, which is the failure path: nothing completes these two, and the
        // mailbox mails that would release them never run again once the task is torn down. A
        // reporter can still sample between here and the metric group's own close, so a writer
        // that is finished must not report work it will never wait for.
        writer.close();

        assertThat(metrics.<Integer>gaugeValue("inFlightMutations")).isZero();
        assertThat(metrics.<Long>gaugeValue("inFlightBytes")).isZero();
    }

    @Test
    void registersNoPerDestinationCountersForItsOneFixedTable() throws Exception {
        SinkWriter<String> writer = writer(serializer(), dropping());
        writer.write("row-1", TestContexts.NO_OP);

        // A sink writes one table, so destination.TABLE.* could only restate the writer's totals.
        // The name is the one DestinationMetrics would use, which for this sink is the only table
        // it has: TableDestination.toString() is "p.i.orders".
        assertThat(metrics.hasMetric("destination", TABLE.toString(), "recordsSend")).isFalse();
        assertThat(metrics.hasMetric("destination", TABLE.toString(), "sendErrors")).isFalse();
    }

    @Test
    void leavesCurrentSendTimeUnset() {
        writer(serializer(), dropping());

        assertThat(metrics.getCurrentSendTimeGauge()).isNull();
    }

    private long counter(String... identifier) {
        return metrics.counterValue(identifier);
    }

    private long errors(String errorClass) {
        return counter("errorClass", errorClass, "errors");
    }

    private SinkWriter<String> writer(
            BigtableSerializationSchema<String> serializer,
            FailureHandler<? super FailedMutation> handler) {
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(serializer)
                                .writerOptions(BigtableWriterOptions.defaults())
                                .failedMutationHandler(handler)
                                .build();
        return sink.createWriter(batcher, mailbox, metrics);
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

    /** A handler that drops everything, so a routed failure never becomes a job failure. */
    private static FailureHandler<FailedElement> dropping() {
        return element -> {};
    }
}
