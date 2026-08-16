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
import org.junit.jupiter.api.Timeout.ThreadMode;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

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
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");

    /**
     * A stall-warning threshold no test here reaches: these classes drive the writer's own clock
     * only through eviction, so a production-sized one would put a warning in no log and a tiny one
     * would put a warning in every.
     */
    private static final long STALL_WARN_AFTER_NANOS = java.time.Duration.ofHours(1).toNanos();

    private final FakeMutationBatcherFactory factory = new FakeMutationBatcherFactory();
    private final FakeMutationBatcher batcher = factory.batcherFor(TABLE);
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
    void routesOnlyTheMutationTheServiceRefusedAndAppliesTheRestOfItsBatch() throws Exception {
        // #239, and the reason the isolation pass exists: Bigtable rejects the whole MutateRows
        // request, so the report against a good entry says nothing about that entry. Routing it
        // would hand a dropping handler the good records batched with the bad one.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        batcher.rejectedRowKeys.add("row-2");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);
        writer.flush(false);

        // One request carrying all three, then one request per mutation: solo is the whole point,
        // so it is asserted as the shape of the requests rather than as a count of re-submissions.
        assertThat(batcher.sentRowKeys())
                .containsExactly(
                        List.of("row-1", "row-2", "row-3"),
                        List.of("row-1"),
                        List.of("row-2"),
                        List.of("row-3"));
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-2");
        assertThat(handler.handled.get(0).getErrorMessage()).contains("INVALID_ARGUMENT");
        assertThat(inFlight(writer)).isZero();
        assertThat(parked(writer)).isZero();
    }

    @Test
    void failsTheJobOnceConsecutiveConfirmedRejectionsReachTheBound() throws Exception {
        // #361: a dropping policy keeps the job green through anomalous records, but a stream
        // refused wholesale is broken data degraded to one solo request per record — so the bound
        // fails the job, with every rejected mutation routed before it does.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-2");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)")
                .hasMessageContaining("refused 2 mutations in a row")
                .hasMessageContaining("INVALID_ARGUMENT")
                // The pass's own loop budget reports a connector bug; the bound reports the
                // stream. An operator must be able to tell the two failures apart.
                .hasMessageNotContaining("isolation contract");
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-2");
    }

    @Test
    void theBoundTrippingMidPassAbandonsTheRestOfThePark() throws Exception {
        // The throw escapes the pass with mutations still parked behind it: neither applied nor
        // routed, which the failed checkpoint covers — the restart replays them. Pins that the
        // abandoned mutation is not silently routed after the bound has spoken.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-2");
        batcher.rejectedRowKeys.add("row-3");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-2");
        assertThat(parked(writer)).isEqualTo(1);
    }

    @Test
    void aCollateralSuccessInsideTheIsolationPassResetsTheCount() throws Exception {
        // The interleaving the pass was designed around: a good record batched between two bad
        // ones is applied by its solo re-submission, and that success resets the count mid-pass —
        // two bad records in one batch are two runs of one, not one run of two.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-3");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-3");
    }

    @Test
    void aSuccessZeroesTheCountRatherThanCancellingOneRejection() throws Exception {
        // Two confirmed rejections, one success, two more: a counter that merely decremented on
        // success would reach the bound of 3 at the fourth rejection; zeroing keeps every run at
        // two. "Any applied mutation resets the count" means reset, not repayment.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(3).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-2");
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.flush(false);

        writer.write("row-3", TestContexts.NO_OP);
        writer.flush(false);

        batcher.rejectedRowKeys.add("row-4");
        batcher.rejectedRowKeys.add("row-5");
        writer.write("row-4", TestContexts.NO_OP);
        writer.write("row-5", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-2", "row-4", "row-5");
    }

    @Test
    void aRunAccumulatesAcrossFlushesWithNoSuccessBetween() throws Exception {
        // "Consecutive" is about successes, not checkpoint intervals: two rejections in one flush
        // and a third in the next, with nothing applied between them, are one run of three.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(3).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-2");
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.flush(false);

        batcher.rejectedRowKeys.add("row-3");
        writer.write("row-3", TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(3)");
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-2", "row-3");
    }

    @Test
    void anAppliedMutationResetsTheConsecutiveRejectionCount() throws Exception {
        // One bad record an hour must never accumulate into a failure: with a success between
        // them, two rejections stay below a bound of 2 for the whole run.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        writer.write("row-1", TestContexts.NO_OP);
        writer.flush(false);

        writer.write("row-2", TestContexts.NO_OP);
        writer.flush(false);

        batcher.rejectedRowKeys.add("row-3");
        writer.write("row-3", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-3");
    }

    @Test
    void theUnboundedSentinelKeepsIsolatingThroughConsecutiveRejections() throws Exception {
        // -1 restores the unbounded pass for a pipeline that really does want to trickle through
        // arbitrarily bad data. Discriminating: a sentinel misread as a bound of -1 or 0 would
        // fail the job on the first confirmed rejection here.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.builder()
                                .maxConsecutiveRejections(BigtableWriterOptions.UNBOUNDED)
                                .build(),
                        serializer(),
                        handler);
        batcher.rejectedRowKeys.add("row-1");
        batcher.rejectedRowKeys.add("row-2");
        batcher.rejectedRowKeys.add("row-3");

        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("row-1", "row-2", "row-3");
    }

    @Test
    void parksABatchedRejectionInsteadOfRoutingIt() throws Exception {
        // The half of the pass a completed flush hides: between the report and the verdict the
        // mutation is neither in flight nor routed, and only the park says it still exists.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        batcher.fail(1, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        assertThat(handler.handled).isEmpty();
        assertThat(inFlight(writer)).isZero();
        assertThat(parked(writer)).isEqualTo(2);
    }

    @Test
    void isolatesBeforeTheNextRecordSoTheParkCannotGrowAcrossACheckpoint() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        // No flush: the next write is what drains the park, which is what bounds it to one batch
        // rather than to a checkpoint interval's worth of rejections.
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(batcher.sentRowKeys()).containsExactly(List.of("row-1"));
        assertThat(parked(writer)).isZero();
        assertThat(handler.handled).isEmpty();
    }

    @Test
    void sendsWhatTheBatcherHasAccumulatedBeforeItIsolates() throws Exception {
        // What makes a re-submission solo is that the accumulator is empty when it is added: a
        // mutation still waiting in the batcher would otherwise be sent with it, and the verdict
        // would name a batch again — the defect, one request smaller.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        // row-1's request has been answered; row-2 is still waiting to be sent.
        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();
        writer.write("row-3", TestContexts.NO_OP);

        assertThat(batcher.sentRowKeys()).containsExactly(List.of("row-2"), List.of("row-1"));
    }

    @Test
    void discardsTheParkWhenItCloses() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();

        // The park backs a gauge a reporter can still sample, and nothing empties it after close.
        // The mutation itself is covered by at-least-once: no checkpoint completed with it parked.
        writer.close();

        assertThat(parked(writer)).isZero();
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
        batcher.rejectedRowKeys.add("row-1");
        writer.write("row-1", TestContexts.NO_OP);

        // A mail cannot throw at its caller, so the failure raised by the isolation pass's own
        // drain has to survive to the flush that started the pass.
        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessage("handler said no");
    }

    @Test
    void yieldsToTheMailboxAtTheMutationCap() throws Exception {
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.builder().maxInFlightEntries(2).build());
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

        writer.flush(false);

        // One send and no more: with nothing parked, the flush must not run an isolation pass.
        assertThat(batcher.sendOutstandingCalls).isEqualTo(1);
        assertThat(inFlight(writer)).isZero();
        assertThat(handler.flushes).isEqualTo(1);
    }

    @Test
    void flushRoutesTheFailuresItDiscoversBeforeFlushingTheHandler() throws Exception {
        OrderRecordingHandler handler = new OrderRecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        batcher.rejectedRowKeys.add("row-1");
        writer.write("row-1", TestContexts.NO_OP);

        // Discovered by the drain inside flush and confirmed by the isolation pass that drain
        // starts, which is the case the ordering rule exists for: flushing the handler before
        // either would checkpoint past this dead letter.
        writer.flush(false);

        assertThat(handler.events).containsExactly("handle", "flush");
    }

    @Test
    void routesEachRecordToItsOwnTablesBatcher() throws Exception {
        SinkWriter<String> writer = multiTableWriter(failJob());

        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);
        writer.write("orders/row-3", TestContexts.NO_OP);

        assertThat(factory.created).containsExactly(TABLE, OTHER_TABLE);
        assertThat(factory.batcherFor(TABLE).entries)
                .extracting(BigtableWriterTest::rowKey)
                .containsExactly("orders/row-1", "orders/row-3");
        assertThat(factory.batcherFor(OTHER_TABLE).entries)
                .extracting(BigtableWriterTest::rowKey)
                .containsExactly("events/row-2");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
    void sendsEveryLiveBatcherBeforeIsolatingAPark() throws Exception {
        // The pass's opening sendOutstanding covers every batcher, not only the ones holding parked
        // work — and this has to be driven from write(), because flush() sends every batcher itself
        // just before calling the pass and would mask a pass that sent only one.
        //
        // Missing a batcher fails two ways, neither naming the cause: the entry still sitting in
        // its accumulator is counted in flight with no request carrying it, so the drain never
        // reaches zero and the task thread parks inside yield() forever — hence a timeout that can
        // interrupt a blocked take(), since the class-level SAME_THREAD one cannot — or gax's delay
        // timer sends it as a batch of its own, whose rejection parks after the pass took its
        // budget and trips the tripwire on a healthy stream.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                multiTableWriter(
                        BigtableWriterOptions.builder().maxInFlightEntries(1).build(),
                        handler,
                        System::nanoTime);
        factory.batcherFor(TABLE).rejectedRowKeys.add("orders/row-1");

        writer.write("orders/row-1", TestContexts.NO_OP);
        // The service refuses it; the failure mail is queued but has not run.
        factory.batcherFor(TABLE).sendOutstanding();
        // A cap of one makes this write yield, which runs that mail and parks row-1 — and leaves
        // this record accumulated, unsent, on the *other* table's batcher.
        writer.write("events/row-2", TestContexts.NO_OP);
        // Now the park is non-empty, so this write opens with the isolation pass while another
        // table holds an entry no request has carried.
        writer.write("orders/row-3", TestContexts.NO_OP);

        assertThat(factory.batcherFor(OTHER_TABLE).sentRowKeys())
                .containsExactly(List.of("events/row-2"));
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactly("orders/row-1");
        assertThat(parked(writer)).isZero();
    }

    @Test
    void keepsATableIdleForExactlyTheTimeout() throws Exception {
        // "Idle beyond the timeout", not "at" it — the boundary the sweep's comparison decides, and
        // the same direction BigQuery's sweep takes.
        MutableClock clock = new MutableClock();
        SinkWriter<String> writer =
                multiTableWriter(
                        BigtableWriterOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(10))
                                .build(),
                        failJob(),
                        clock);
        writer.write("orders/row-1", TestContexts.NO_OP);

        clock.advance(Duration.ofMinutes(10));
        writer.flush(false);
        assertThat(factory.batcherFor(TABLE).closeCalls).isZero();

        clock.advance(Duration.ofNanos(1));
        writer.flush(false);
        assertThat(factory.batcherFor(TABLE).closeCalls).isEqualTo(1);
    }

    @Test
    void isolatesEachParkedMutationAgainstItsOwnTable() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = multiTableWriter(handler);
        factory.batcherFor(TABLE).rejectedRowKeys.add("orders/row-1");
        factory.batcherFor(OTHER_TABLE).rejectedRowKeys.add("events/row-3");

        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("orders/row-2", TestContexts.NO_OP);
        writer.write("events/row-3", TestContexts.NO_OP);
        writer.flush(false);

        // Each solo re-submission goes to the batcher of its own table: the batched request, then
        // the two of that table's mutations alone.
        assertThat(factory.batcherFor(TABLE).sentRowKeys())
                .containsExactly(
                        List.of("orders/row-1", "orders/row-2"),
                        List.of("orders/row-1"),
                        List.of("orders/row-2"));
        assertThat(factory.batcherFor(OTHER_TABLE).sentRowKeys())
                .containsExactly(List.of("events/row-3"), List.of("events/row-3"));
        assertThat(handler.handled)
                .extracting(failed -> failed.getRowKey().toStringUtf8())
                .containsExactlyInAnyOrder("orders/row-1", "events/row-3");
        // Each failed mutation names the table it was headed for, not the sink's first one.
        assertThat(handler.handled)
                .extracting(FailedMutation::getDestination)
                .containsExactlyInAnyOrder(TABLE, OTHER_TABLE);
    }

    @Test
    void countsInFlightEntriesAcrossEveryTableAtOnce() throws Exception {
        // The in-flight caps are the writer's, summed over every destination rather than shared out
        // among them. Two consequences rest on that and nothing else states them: drainInFlight()
        // means "the writer is empty" — a per-destination split would leave it no single number to
        // wait on — and the park bound in write() stays one number rather than a sum of caps.
        SinkWriter<String> writer = multiTableWriter(failJob());

        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);

        assertThat(inFlight(writer)).isEqualTo(2);
        assertThat(inFlightBytes(writer))
                .isEqualTo(serializedSize("orders/row-1") + serializedSize("events/row-2"));
    }

    @Test
    void opensNoBatcherForARecordTheSerializerSkipsOrRejects() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer =
                writer(
                        BigtableWriterOptions.defaults(),
                        (element, context) -> {
                            if (element.equals("skip-me")) {
                                return null;
                            }
                            throw new IOException("no mutation for " + element);
                        },
                        handler);

        writer.write("skip-me", TestContexts.NO_OP);
        writer.write("row-1", TestContexts.NO_OP);

        // Neither record reaches a table, and a batcher is a channel's worth of resources: a
        // serializer skipping every record must not open one per phantom destination.
        assertThat(factory.created).isEmpty();
        // The rejected one still names where it was headed, which is why the resolve runs before
        // the serializer rather than after it.
        assertThat(handler.handled)
                .extracting(FailedMutation::getDestination)
                .containsExactly(TABLE);
    }

    @Test
    void failsTheWriteWhenTheResolverReturnsNull() {
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .destinationResolver((element, context) -> null)
                                .serializer(serializer())
                                // A dropping handler is the discriminator: a resolver returning
                                // null is a configuration failure, not a bad record, so it must
                                // fail the job rather than let a dropping policy write nothing at
                                // all under a green job.
                                .failedMutationHandler(new RecordingHandler())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(factory, new FakeTableAdmin(), mailbox, metricGroup);

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("resolver returned null");
    }

    @Test
    void failsTheWriteWhenABatcherCannotBeCreatedAndRetriesOnTheNextRecord() throws Exception {
        // A client that cannot be built is a configuration or credentials failure, not a bad
        // record, so it reaches the caller rather than the failure handler. And it leaves no
        // half-populated state: the next record routed to that table tries again.
        SinkWriter<String> writer =
                writer(BigtableWriterOptions.defaults(), serializer(), failJob());
        factory.createFailures.add(new IOException("no credentials"));

        assertThatThrownBy(() -> writer.write("row-1", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.i.orders");

        writer.write("row-2", TestContexts.NO_OP);
        assertThat(factory.created).containsExactly(TABLE);
        assertThat(batcher.entries).extracting(BigtableWriterTest::rowKey).containsExactly("row-2");
    }

    @Test
    void evictsATableIdleBeyondTheTimeoutAndRebuildsItOnTheNextRecord() throws Exception {
        MutableClock clock = new MutableClock();
        SinkWriter<String> writer =
                multiTableWriter(
                        BigtableWriterOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(10))
                                .build(),
                        failJob(),
                        clock);
        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);
        writer.flush(false);

        // Past the timeout for the first table only: the second is written again just before the
        // sweep, so the sweep has to discriminate rather than drop everything it walks.
        clock.advance(Duration.ofMinutes(11));
        writer.write("events/row-3", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(factory.batcherFor(TABLE).closeCalls).isEqualTo(1);
        assertThat(factory.batcherFor(OTHER_TABLE).closeCalls).isZero();
        // The client behind it is not closed with it: it belongs to the factory and to the tables
        // of the same instance that are still live.
        assertThat(factory.closeCalls).isZero();

        writer.write("orders/row-4", TestContexts.NO_OP);
        assertThat(factory.created).containsExactly(TABLE, OTHER_TABLE, TABLE);
    }

    @Test
    void evictsNothingAtEndOfInput() throws Exception {
        // The final flush is followed by close(), which releases everything anyway; evicting there
        // would be a batcher close inside a flush for no benefit.
        MutableClock clock = new MutableClock();
        SinkWriter<String> writer =
                multiTableWriter(
                        BigtableWriterOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(10))
                                .build(),
                        failJob(),
                        clock);
        writer.write("orders/row-1", TestContexts.NO_OP);
        clock.advance(Duration.ofMinutes(11));

        writer.flush(true);

        assertThat(factory.batcherFor(TABLE).closeCalls).isZero();
    }

    @Test
    void closesTheBatcherAndTheHandlerTogether() throws Exception {
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        // A batcher exists only for a table a record was routed to; a writer that wrote nothing
        // holds none, and closes none.
        writer.write("row-1", TestContexts.NO_OP);

        writer.close();

        assertThat(batcher.closeCalls).isEqualTo(1);
        assertThat(handler.closes).isEqualTo(1);
        assertThat(factory.closeCalls).isEqualTo(1);
    }

    @Test
    void closesEveryBatcherOfEveryTableAndTheFactoryAfterThem() throws Exception {
        // Two properties in one teardown, neither reachable with one table. Every shutdown runs
        // before any close, so the unbounded waits overlap rather than costing one per table —
        // sequential ones exceed Flink's task.cancellation.timeout and turn a cancelling task into
        // a fatal TaskManager error. And the factory, which holds the client the batchers were
        // built over, is released after every one of them: released first, a batcher's shutdown
        // would be sending through a dead channel.
        SinkWriter<String> writer = multiTableWriter(failJob());
        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);

        writer.close();

        assertThat(factory.events)
                .containsExactly(
                        "shutdown orders",
                        "shutdown events",
                        "close orders",
                        "close events",
                        "factory");
    }

    @Test
    void closesEveryOtherBatcherWhenOneCloseThrows() throws Exception {
        // One list through Closers, never a loop and then a call (#297): a batcher failing to
        // close must not strand the ones after it, nor the factory holding their client.
        SinkWriter<String> writer = multiTableWriter(failJob());
        writer.write("orders/row-1", TestContexts.NO_OP);
        writer.write("events/row-2", TestContexts.NO_OP);
        IllegalStateException failure = new IllegalStateException("orders close blew up");
        factory.batcherFor(TABLE).closeFailure = failure;

        assertThatThrownBy(writer::close).isSameAs(failure);

        assertThat(factory.batcherFor(OTHER_TABLE).closeCalls).isEqualTo(1);
        assertThat(factory.closeCalls).isEqualTo(1);
    }

    @Test
    void closesTheHandlerEvenWhenTheBatcherShutdownThrowsAnError() throws Exception {
        // #276: the handler is last, and Flink's IOUtils.closeAll rethrew an Error from inside its
        // loop, leaving it open — since #211 it can own an SDK publisher and a gRPC channel. That
        // the Error reaches the caller as an Error is the other half: Flink halts the JVM on a
        // fatal one, and only if it arrives unwrapped.
        RecordingHandler handler = new RecordingHandler();
        SinkWriter<String> writer = writer(BigtableWriterOptions.defaults(), serializer(), handler);
        writer.write("row-1", TestContexts.NO_OP);
        batcher.closeFailure = new NoClassDefFoundError("batcher shutdown blew up");

        assertThatThrownBy(writer::close)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("batcher shutdown blew up");
        assertThat(handler.closes).isEqualTo(1);
        assertThat(factory.closeCalls).isEqualTo(1);
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

    private SinkWriter<String> multiTableWriter(FailureHandler<? super FailedMutation> handler) {
        return multiTableWriter(BigtableWriterOptions.defaults(), handler, System::nanoTime);
    }

    /**
     * A writer routing a row key prefixed {@code events/} to {@link #OTHER_TABLE}, everything else
     * to {@link #TABLE}.
     */
    private SinkWriter<String> multiTableWriter(
            BigtableWriterOptions options,
            FailureHandler<? super FailedMutation> handler,
            LongSupplier nanoClock) {
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                element.startsWith("events/") ? OTHER_TABLE : TABLE)
                                .serializer(serializer())
                                .writerOptions(options)
                                .failedMutationHandler(handler)
                                .build();
        return new BigtableWriter<>(
                sink.getConfig(),
                factory,
                new FakeTableAdmin(),
                mailbox,
                metricGroup,
                sink.getConfig().getWriterOptions().toRecoverySchedule(),
                nanoClock,
                STALL_WARN_AFTER_NANOS);
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
        return sink.createWriter(factory, new FakeTableAdmin(), mailbox, metricGroup);
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
        return ((BigtableWriter<String>) writer).getInFlightEntries();
    }

    @SuppressWarnings("unchecked")
    private static long inFlightBytes(SinkWriter<String> writer) {
        return ((BigtableWriter<String>) writer).getInFlightBytes();
    }

    @SuppressWarnings("unchecked")
    private static int parked(SinkWriter<String> writer) {
        return ((BigtableWriter<String>) writer).getParkedEntries();
    }

    private static String rowKey(RowMutationEntry entry) {
        return entry.toProto().getRowKey().toStringUtf8();
    }

    /** A nanosecond clock a test advances by hand, for the idle-eviction sweep. */
    private static final class MutableClock implements LongSupplier {

        private long nanos;

        @Override
        public long getAsLong() {
            return nanos;
        }

        void advance(Duration by) {
            nanos += by.toNanos();
        }
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
