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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for what {@link SingleRowRequestWriter}'s waits do while the client is not answering — the
 * single-row shape of {@code BigtableWriterStallTest}, whose reasoning about clocks, pacing and
 * anchoring this class inherits and does not repeat ({@code docs/adr/0078}).
 *
 * <p>{@code SEPARATE_THREAD}: several cases assert that a wait <em>ends</em>, and the way they fail
 * when it does not is a park the default {@code SAME_THREAD} timeout cannot interrupt.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class SingleRowRequestWriterStallTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    /** Short enough to keep the scheduled offsets small, long enough to be unmistakable. */
    private static final Duration WARN_AFTER = Duration.ofSeconds(30);

    /** What a stall warning says and nothing else in this writer does. */
    private static final String STALL_MARKER = "has been answered for";

    /** A gap short enough not to be a stall, long enough that two of them exceed the threshold. */
    private static final Duration TWO_THIRDS = WARN_AFTER.dividedBy(3).multipliedBy(2);

    private final FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final TestClock clock = new TestClock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, ScriptedRowRequest> requests = new LinkedHashMap<>();

    /** What a step staged behind the wait threw, since a throw over there reaches no assertion. */
    private volatile Throwable stagingFailure;

    @AfterEach
    void stopTheSchedulerAndReportWhatItSaw() throws InterruptedException {
        scheduler.shutdownNow();
        // One test interrupts this thread on purpose; read-and-clear unconditionally so no later
        // test in this fork sees a cancellation that never happened.
        Thread.interrupted();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
        Throwable failure = stagingFailure;
        stagingFailure = null;
        if (failure != null) {
            throw new AssertionError("the step staged behind the wait failed", failure);
        }
    }

    @Test
    void aStalledWaitSaysSoWhileItIsStillWaiting() throws Exception {
        SingleRowRequestWriter<String> writer = writer(FailureHandler.failJob());
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        awaitStallWarnings(capture, 1);
                        request("row-1").succeed();
                    });

            writer.flush(false);

            List<String> stalls = stallWarnings(capture);
            assertThat(stalls).hasSize(1);
            assertThat(stalls.get(0))
                    .contains("draining the outstanding requests")
                    .contains("1 requests in flight")
                    .contains("over 1 table(s)")
                    // The rendered idle time pins the threshold against the clock, not wall time.
                    .contains("PT31S")
                    .contains("BigtableRequestOptions.requestTimeout")
                    .contains("requestsCompleted");
        }
    }

    @Test
    void aWaitThatKeepsBeingAnsweredSaysNothing() throws Exception {
        // The paired negative: a failure is an answer too, so it restamps the clock like a
        // success, and each gap here is under the threshold even though the wait outlasts it.
        SingleRowRequestWriter<String> writer = writer(element -> {});
        writer.write("row-1", TestContexts.NO_OP);
        writer.write("row-2", TestContexts.NO_OP);
        writer.write("row-3", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(TWO_THIRDS);
                        pause();
                        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);
                        pause();
                        clock.advance(TWO_THIRDS);
                        pause();
                        request("row-2").succeed();
                        pause();
                        clock.advance(TWO_THIRDS);
                        pause();
                        request("row-3").succeed();
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
        SingleRowRequestWriter<String> writer = writer(FailureHandler.failJob());
        clock.advance(Duration.ofMinutes(10));
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            // Paused before the answer: answering on the same millisecond can end the wait through
            // tryYield before a pass ever measures, which would pass for the mutant that measures
            // from the completion stamp alone.
            onceTheWaitHasBegun(
                    () -> {
                        pause();
                        request("row-1").succeed();
                    });

            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void workTheMailboxStillHasToDoIsNotCountedAsIdle() throws Exception {
        // The idle time is read only once tryYield has come back empty. Work queued behind the
        // wait that is not an answer restamps nothing, so a writer that measured before yielding
        // would report a stall on the pass that runs it, however healthy the client.
        SingleRowRequestWriter<String> writer = writer(FailureHandler.failJob());
        writer.write("row-1", TestContexts.NO_OP);

        // The first mail moves the clock past the threshold and queues the answer from inside the
        // mailbox, so no pass finds the box empty with the clock already advanced: a warning here
        // can only come from a pass that measured with work still queued.
        mailbox.execute(
                () -> {
                    clock.advance(WARN_AFTER.multipliedBy(10));
                    mailbox.execute(() -> request("row-1").succeed(), "the answer");
                },
                "unrelated work");

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            writer.flush(false);

            assertThat(stallWarnings(capture)).isEmpty();
        }
    }

    @Test
    void theWarningIsRateLimitedRatherThanSaidOncePerPass() throws Exception {
        SingleRowRequestWriter<String> writer = writer(FailureHandler.failJob());
        writer.write("row-1", TestContexts.NO_OP);

        try (LogCapture capture = LogCapture.of(SingleRowRequestWriter.class)) {
            onceTheWaitHasBegun(
                    () -> {
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        awaitStallWarnings(capture, 1);
                        clock.advance(WARN_AFTER.plusSeconds(1));
                        awaitStallWarnings(capture, 2);
                        pause();
                        request("row-1").succeed();
                    });

            writer.flush(false);

            assertThat(stallWarnings(capture)).hasSize(2);
        }
    }

    @Test
    void anInterruptEndsTheWaitRatherThanBeingSpunOn() {
        // tryYield() does not look at the flag and parkNanos returns on interrupt without
        // clearing it; nothing else here would notice a cancelled task.
        SingleRowRequestWriter<String> writer = writer(FailureHandler.failJob());

        assertThatThrownBy(
                        () -> {
                            writer.write("row-1", TestContexts.NO_OP);
                            Thread.currentThread().interrupt();
                            writer.flush(false);
                        })
                .isInstanceOf(InterruptedException.class)
                .hasMessageContaining("Bigtable");
    }

    private List<String> stallWarnings(LogCapture capture) {
        return capture.getMessages().stream()
                .filter(line -> line.contains(STALL_MARKER))
                .collect(Collectors.toList());
    }

    private void awaitStallWarnings(LogCapture capture, int count) {
        awaitOrFail(
                () -> stallWarnings(capture).size() >= count,
                () ->
                        "the wait logged "
                                + stallWarnings(capture).size()
                                + " stall warnings, expected "
                                + count);
    }

    /**
     * Runs the given steps once the writer's wait has read the baseline it measures idleness from
     * and then read the clock once more inside the wait — two reads past the count taken on the
     * test thread before the flush. No completion reads the clock in between, since the staged
     * steps are the only thing that answers.
     */
    private void onceTheWaitHasBegun(Runnable steps) {
        int readsBeforeTheWait = clock.reads();
        scheduler.execute(
                () -> {
                    try {
                        awaitOrFail(
                                () -> clock.reads() >= readsBeforeTheWait + 2,
                                () ->
                                        "the wait never read its baseline ("
                                                + clock.reads()
                                                + " clock reads)");
                        steps.run();
                    } catch (Throwable failure) {
                        if (!Thread.currentThread().isInterrupted()) {
                            stagingFailure = failure;
                        }
                        for (ScriptedRowRequest request : requests.values()) {
                            request.future.cancel(true);
                        }
                    }
                });
    }

    private static void awaitOrFail(BooleanSupplier condition, Supplier<String> whatNeverHappened) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadlineNanos > 0) {
                throw new AssertionError(whatNeverHappened.get());
            }
            pause(Duration.ofMillis(1));
        }
    }

    private static void pause() {
        pause(Duration.ofMillis(100));
    }

    private static void pause(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while pacing a stalled wait", e);
        }
    }

    private ScriptedRowRequest request(String rowKey) {
        return requests.computeIfAbsent(rowKey, ScriptedRowRequest::new);
    }

    private SingleRowRequestWriter<String> writer(FailureHandler<? super FailedElement> handler) {
        return new SingleRowRequestWriter<>(
                new SingleRowRequestConfig<>(
                        (element, context) -> TABLE,
                        (element, context) -> request(element),
                        null,
                        BigtableRequestOptions.builder().build(),
                        handler,
                        null,
                        null),
                factory,
                mailbox,
                metricGroup,
                clock,
                WARN_AFTER.toNanos());
    }

    /** A nanosecond clock a test advances by hand, counting its reads for the anchor. */
    private static final class TestClock implements LongSupplier {

        private volatile long nanos;
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public long getAsLong() {
            reads.incrementAndGet();
            return nanos;
        }

        int reads() {
            return reads.get();
        }

        void advance(Duration by) {
            nanos += by.toNanos();
        }
    }
}
