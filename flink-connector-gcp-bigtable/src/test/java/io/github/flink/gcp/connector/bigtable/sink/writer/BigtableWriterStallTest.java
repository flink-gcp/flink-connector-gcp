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

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for what {@link BigtableWriter}'s two mailbox waits do while the client is not answering
 * ({@code docs/adr/0078}).
 *
 * <p>Driven off the writer's injected clock rather than the wall clock, which is what the Pub/Sub
 * sink's equivalent class does not have: a stall of a minute costs no wall-clock time here, because
 * a scheduled task advances the clock while the wait runs. The clock is therefore read from two
 * threads and its field is {@code volatile}, exactly as the writer's own completion stamp is.
 *
 * <p>{@code SEPARATE_THREAD}, unlike this module's other writer tests: several of the cases here
 * assert that a wait <em>ends</em>, and the way they fail when it does not is a blocked {@code
 * take()} that the default {@code SAME_THREAD} timeout cannot interrupt.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BigtableWriterStallTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    /** Short enough to keep the scheduled offsets small, long enough to be unmistakable. */
    private static final Duration WARN_AFTER = Duration.ofSeconds(30);

    /** What a stall warning says and nothing else in this writer does. */
    private static final String STALL_MARKER = "has been answered for";

    /** A gap short enough not to be a stall, long enough that two of them exceed the threshold. */
    private static final Duration TWO_THIRDS = WARN_AFTER.dividedBy(3).multipliedBy(2);

    private final FakeMutationBatcherFactory factory = new FakeMutationBatcherFactory();
    private final FakeMutationBatcher batcher = factory.batcherFor(TABLE);
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final TestClock clock = new TestClock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void stopTheScheduler() {
        scheduler.shutdownNow();
        // One test interrupts this thread on purpose, and a flag left set would make every later
        // test in this surefire fork see a cancellation that never happened (#316's shared-state
        // hazard in another shape). Read-and-clear unconditionally, since an earlier assertion
        // failing would skip a clear placed inside the test.
        Thread.interrupted();
    }

    @Test
    void aStalledWaitSaysSoWhileItIsStillWaiting() throws Exception {
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());
        // The fake answers a request as it is sent unless told otherwise, and a sink that is
        // answered has no stall to report: switching that off is what makes the wait a wait.
        batcher.autoComplete = false;
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            // The client answers nothing until well past the threshold, and then does — so the
            // warning has to arrive from inside a wait that goes on to end normally, which is the
            // whole point of it: the writer never fails this state itself.
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        pause();
                        batcher.succeed(0);
                    });

            writer.flush(false);

            List<String> stalls = stallWarnings(capture);
            assertThat(stalls).hasSize(1);
            assertThat(stalls.get(0))
                    .contains("draining the outstanding mutations")
                    .contains("1 entries in flight")
                    .contains("over 1 table(s)")
                    // The rendered idle time is what pins the threshold against the clock rather
                    // than against wall time.
                    .contains("PT31S")
                    .contains("numRecordsSend")
                    .contains("10-minute total timeout");
        }
    }

    @Test
    void aWaitThatKeepsBeingAnsweredSaysNothing() throws Exception {
        // The paired negative the log-assertion rule requires: an empty capture is also what a
        // broken capture looks like, so the positive above is only evidence beside this.
        //
        // Each advance is a step of its own, separated from the answer that follows it, so the
        // "clock has moved but the client answered recently" state lasts long enough for the wait
        // to poll it a hundred times. Advancing and answering in one step leaves that state alive
        // for microseconds, which is a test that passes whether or not the writer measures from
        // the last answer at all.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());
        batcher.autoComplete = false;
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            // Two thirds of the threshold per gap, so the wait outlasts the threshold twice over
            // while no single gap does: what is reported is a stall, not a slow service.
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(TWO_THIRDS);
                        pause();
                        succeed(0);
                        pause();
                        clock.advance(TWO_THIRDS);
                        pause();
                        succeed(1);
                        pause();
                        clock.advance(TWO_THIRDS);
                        pause();
                        answerTheRest(2);
                    });

            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void aMutationThatFailsIsStillTheClientAnswering() throws Exception {
        // A failure is an answer, so it restamps the clock like a success. Stamping only successes
        // would report a stall on a client that is refusing everything promptly — and that mutant
        // survived the whole of the Pub/Sub sink's equivalent class, so it is pinned here with a
        // failure as the *only* thing that answers between two advances.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults(), dropping());
        batcher.autoComplete = false;
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(TWO_THIRDS);
                        pause();
                        // INVALID_ARGUMENT rather than a fatal code: it parks for the isolation
                        // pass instead of ending the drain early, so the second gap below is
                        // actually waited out.
                        batcher.fail(0, StatusCode.Code.INVALID_ARGUMENT);
                        pause();
                        // Past the threshold in total now, but only two thirds of it since that
                        // failure — which is an answer only if a failure stamps the clock.
                        clock.advance(TWO_THIRDS);
                        pause();
                        answerTheRest(1);
                    });

            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void anIdleWriterDoesNotReportItsNextWaitAsAStall() throws Exception {
        // Measured from the later of "this wait began" and "the client last answered". A writer
        // whose stream went quiet for ten minutes has an ancient completion stamp, and measuring
        // from that alone would report its very first wait as a ten-minute stall.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());
        batcher.autoComplete = false;
        clock.advance(Duration.ofMinutes(10));
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            onceTheWaitHasBegun(() -> answerTheRest(0));

            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void theWarningIsRateLimitedRatherThanSaidOncePerPass() throws Exception {
        // The wait polls at a millisecond, so a stall of any length is thousands of passes. A
        // per-wait or per-pass line would bury the log; the writer keeps one field for this.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());
        batcher.autoComplete = false;
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        pause();
                        // A second threshold's worth of silence earns a second line, and no more,
                        // however many hundreds of passes the wait makes around them.
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        pause();
                        batcher.succeed(0);
                    });

            writer.flush(false);

            assertThat(stallWarnings(capture)).hasSize(2);
        }
    }

    @Test
    void workTheMailboxStillHasToDoIsNotCountedAsIdle() throws Exception {
        // The idle time is read only once tryYield has come back empty. A completion mail queued
        // behind other work is a mutation the client already answered, so counting the time it
        // waits its turn would report a stall on a healthy writer.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());
        writer.write("row-1", TestContexts.NO_OP);

        // Both mails are already queued and the clock is already past the threshold: every pass of
        // the drain finds work, so it never reads the idle time at all.
        mailbox.execute(() -> {}, "unrelated work");
        batcher.succeed(0);
        clock.advance(WARN_AFTER.multipliedBy(10));

        try (LogCapture capture = LogCapture.of(BigtableWriter.class)) {
            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void anInterruptEndsTheWaitRatherThanBeingSpunOn() {
        // The property this whole rewrite could quietly take away: the blocking yield() ended a
        // cancelled wait of its own accord, tryYield() does not look at the flag, and parkNanos
        // returns on interrupt without clearing it. Nothing else here would notice.
        BigtableWriter<String> writer = writer(BigtableWriterOptions.defaults());

        assertThatThrownBy(
                        () -> {
                            writer.write("row-1", TestContexts.NO_OP);
                            Thread.currentThread().interrupt();
                            writer.flush(false);
                        })
                .isInstanceOf(InterruptedException.class)
                .hasMessageContaining("Bigtable");
    }

    @Test
    void aWaitAtTheInFlightCapSendsWhatTheBatchersAreStillAccumulating() throws Exception {
        // At the cap nothing more can be admitted, so anything still in an accumulator is exactly
        // what the wait is waiting for — and the writer, holding the task thread, cannot add the
        // mutation that would trip the batcher's own threshold instead. Without this send the fake
        // never sends anything and the wait does not end, which is how that mutant fails.
        //
        // The fake does not answer what it sends here, so the wait goes on for hundreds of passes
        // after the send: that is what tells "once per wait" from "once per pass", which a wait
        // ending on its first pass cannot.
        BigtableWriter<String> writer =
                writer(BigtableWriterOptions.builder().maxInFlightEntries(1).build());
        batcher.autoComplete = false;
        writer.write("row-1", TestContexts.NO_OP);

        onceTheWaitHasBegun(
                () -> {
                    pause();
                    batcher.succeed(0);
                });
        writer.write("row-2", TestContexts.NO_OP);

        assertThat(batcher.sendOutstandingCalls).isEqualTo(1);
        assertThat(batcher.sentRowKeys()).containsExactly(List.of("row-1"));
    }

    private List<String> stallWarnings(LogCapture capture) {
        return capture.getMessages().stream()
                .filter(line -> line.contains(STALL_MARKER))
                .collect(Collectors.toList());
    }

    /** Completes the outstanding mutation at the given index, as the client answering it. */
    private void succeed(int index) {
        batcher.succeed(index);
    }

    /**
     * Answers the given mutation and hands the isolation pass back an auto-answering fake, so a
     * solo re-submission completes rather than leaving the pass waiting for a client this test has
     * stopped playing.
     */
    private void answerTheRest(int index) {
        batcher.autoComplete = true;
        batcher.succeed(index);
    }

    /**
     * Runs the given steps once the writer is actually inside a wait, which its first {@code
     * sendOutstanding} marks — both waits send before they wait, and the fake answers nothing while
     * {@code autoComplete} is off.
     *
     * <p>Anchored on that rather than on a delay, because a delay races the test thread reaching
     * the wait at all: a pause longer than it between scheduling and {@code flush()} would let
     * every step run first, and the assertions would then hold for the wrong reason.
     */
    private void onceTheWaitHasBegun(Runnable steps) {
        scheduler.execute(
                () -> {
                    long deadlineNanos = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (batcher.sendOutstandingCalls == 0) {
                        if (System.nanoTime() - deadlineNanos > 0) {
                            throw new AssertionError("the writer never reached a wait");
                        }
                        Thread.onSpinWait();
                    }
                    steps.run();
                });
    }

    /**
     * Long enough for the waiting task thread to poll hundreds of times, which is what tells a
     * once-per-wait action from a once-per-pass one, and what makes a state the wait must observe
     * observable at all.
     */
    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while pacing a stalled wait", e);
        }
    }

    private BigtableWriter<String> writer(BigtableWriterOptions options) {
        return writer(options, FailureHandler.failJob());
    }

    private BigtableWriter<String> writer(
            BigtableWriterOptions options, FailureHandler<? super FailedElement> handler) {
        BigtableMutateRowsSink<String> sink =
                (BigtableMutateRowsSink<String>)
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(
                                        (element, context) ->
                                                RowMutationEntry.create(element)
                                                        .setCell("cf", "q", 1_000L, element))
                                .writerOptions(options)
                                .failedMutationHandler(handler)
                                .build();
        return new BigtableWriter<>(
                sink.getConfig(),
                factory,
                new FakeTableAdmin(),
                mailbox,
                metricGroup,
                options.toRecoverySchedule(),
                clock,
                WARN_AFTER.toNanos());
    }

    private static FailureHandler<FailedElement> dropping() {
        return element -> {};
    }

    /**
     * A nanosecond clock a test advances by hand.
     *
     * <p>{@code volatile} for the same reason the writer's completion stamp is: it is advanced from
     * the scheduler thread while the task thread reads it inside a wait, and read from the gax-side
     * callback that stamps that completion.
     */
    private static final class TestClock implements LongSupplier {

        private volatile long nanos;

        @Override
        public long getAsLong() {
            return nanos;
        }

        void advance(Duration by) {
            nanos += by.toNanos();
        }
    }
}
