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

package io.github.flink.gcp.connector.base.lifecycle;

import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BoundedShutdown}.
 *
 * <p>{@code @Timeout} because most of these drive a shutdown that never returns: a teardown that
 * stopped bounding it would hang the build rather than fail it.
 */
@Timeout(30)
class BoundedShutdownTest {

    private static final String DESCRIPTION = "topic projects/test-project/topics/t";

    /**
     * Each test owns its counter, so assertions are absolute rather than deltas around whatever
     * sibling tests left behind. That is the point of {@code BoundedShutdown} taking the counter
     * instead of holding one: nothing here is process-wide.
     */
    private final LongAdder abandoned = new LongAdder();

    @Test
    void closeGivesUpOnAShutdownThatNeverReturnsAndStillReleasesTheResource() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger terminationWaits = new AtomicInteger();
        AtomicReference<Thread> shutdownThread = new AtomicReference<>();
        AtomicBoolean released = new AtomicBoolean();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {
                            shutdownThread.set(Thread.currentThread());
                            awaitUninterruptibly(blocked);
                        },
                        (timeout, unit) -> {
                            terminationWaits.incrementAndGet();
                            return true;
                        },
                        DESCRIPTION,
                        () -> released.set(true),
                        Duration.ofMillis(50),
                        abandoned);

        try (LogCapture capture = LogCapture.of(BoundedShutdown.class)) {
            teardown.close();

            // abandonedCount says one teardown was abandoned; only this line says *which* client,
            // and an operator holding a thread dump has nothing else to match it against (#323).
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(DESCRIPTION)
                    .contains("did not finish shutting down");

            assertThat(released).isTrue();
            // Not reached, because it runs behind the shutdown on the same thread.
            assertThat(terminationWaits).hasValue(0);
            // The thread outlives the close by design, so a non-daemon one would keep the JVM from
            // exiting; its name is what identifies it in an operator's thread dump. Captured from
            // inside the shutdown rather than scanned for by name, since sibling tests here run in
            // the same JVM and name their thread after the same resource.
            assertThat(shutdownThread.get())
                    .isNotNull()
                    .matches(Thread::isDaemon, "a daemon thread")
                    .extracting(Thread::getName)
                    .asString()
                    // The caller's name is carried so a thread dump can say which subtask leaked;
                    // on a task thread that reads "... for Sink: Writer (2/4)#1".
                    .startsWith("bounded-shutdown-" + DESCRIPTION + " for ")
                    .endsWith(Thread.currentThread().getName());
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void aTerminationWaitThatRunsOutIsReported() throws Exception {
        // The shutdown returns, so close() neither abandons nor counts anything, and the wait
        // returning false is not an error either - the line is this outcome's only report (#323).
        // Emitted from the background thread, but close() joins it, so it has landed by now.
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {},
                        (t, unit) -> false,
                        DESCRIPTION,
                        () -> {},
                        Duration.ofSeconds(2),
                        abandoned);

        try (LogCapture capture = LogCapture.of(BoundedShutdown.class)) {
            teardown.close();

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(DESCRIPTION)
                    .contains("did not terminate");
        }

        assertThat(abandoned.sum()).isZero();
    }

    @Test
    void theTerminationWaitGetsWhatTheShutdownLeftOfTheBudget() throws Exception {
        Duration timeout = Duration.ofSeconds(2);
        Duration spentByShutdown = Duration.ofMillis(300);
        AtomicLong awaitedNanos = new AtomicLong(-1);
        AtomicReference<Thread> awaitedOn = new AtomicReference<>();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> sleepUninterruptibly(spentByShutdown),
                        (t, unit) -> {
                            awaitedNanos.set(unit.toNanos(t));
                            awaitedOn.set(Thread.currentThread());
                            return true;
                        },
                        DESCRIPTION,
                        null,
                        timeout,
                        abandoned);

        teardown.close();

        // One budget spans both steps: the wait gets what the shutdown left, never a fresh one.
        // Asserted strictly below timeout - spent, so handing over the full budget fails here.
        assertThat(awaitedNanos).hasValueBetween(1L, timeout.minus(spentByShutdown).toNanos() - 1);
        // And it runs on the shutdown thread, not this one. gax hands its full timeout to each
        // background resource in turn rather than sharing one deadline across them, so awaiting
        // here would cost a multiple of the budget instead of the budget.
        assertThat(awaitedOn.get())
                .isNotNull()
                .isNotSameAs(Thread.currentThread())
                .extracting(Thread::getName)
                .asString()
                .startsWith("bounded-shutdown-" + DESCRIPTION);
    }

    @Test
    void aShutdownThatThrowsIsRethrownByCloseAsItself() {
        // On a separate thread its exception would otherwise reach only Flink's JVM-wide uncaught
        // handler, so the caller would report a clean close — and under FAIL mode the whole
        // TaskManager would exit instead of the task. An Error must arrive as an Error, which is
        // the same reason Closers.closeAll does not wrap.
        Error failure = new NoClassDefFoundError("shutdown blew up");
        AtomicBoolean released = new AtomicBoolean();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {
                            throw failure;
                        },
                        (t, unit) -> true,
                        DESCRIPTION,
                        () -> released.set(true),
                        Duration.ofSeconds(30),
                        abandoned);

        assertThatThrownBy(teardown::close).isSameAs(failure);
        assertThat(released).isTrue();
    }

    @Test
    void closeReleasesTheResourceWhenTheTerminationWaitThrows() {
        AtomicBoolean released = new AtomicBoolean();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {},
                        (timeout, unit) -> {
                            throw new IllegalStateException("termination wait blew up");
                        },
                        DESCRIPTION,
                        () -> released.set(true),
                        Duration.ofSeconds(30),
                        abandoned);

        assertThatThrownBy(teardown::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("termination wait blew up");
        assertThat(released).isTrue();
    }

    @Test
    void theBudgetRunsFromTheShutdownCallRatherThanFromTheClose() throws Exception {
        // The property an overlapped teardown rests on: a caller asks every client to shut down
        // and only then closes them, so a budget restarted here would be one timeout per client
        // again rather than one for the whole close.
        Duration timeout = Duration.ofSeconds(1);
        CountDownLatch blocked = new CountDownLatch(1);
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        timeout,
                        abandoned);

        try {
            teardown.start();
            Thread.sleep(timeout.toMillis() + 200);

            long startedAt = System.nanoTime();
            teardown.close();
            Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

            // Comfortably below the timeout a restarted budget would have waited out again.
            assertThat(waited).isLessThan(timeout.dividedBy(2));
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void anInterruptedCloseLeavesTheFlagSetForTheRestOfTheTeardown() throws Exception {
        // Closers.closeAll collects a failure and carries on, and Thread.join clears the flag when
        // it throws — so without restoring it the caller's remaining clients, its admin and its
        // failure handler would all stop honouring the cancellation that interrupted us.
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicBoolean released = new AtomicBoolean();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        DESCRIPTION,
                        () -> released.set(true),
                        Duration.ofMinutes(1),
                        abandoned);

        try {
            teardown.start();
            Thread.currentThread().interrupt();

            assertThatThrownBy(teardown::close).isInstanceOf(InterruptedException.class);

            // Consumes the flag as it reads it, so the assertion cannot pass on a stale one and the
            // rest of this test class is not left interrupted.
            assertThat(Thread.interrupted()).isTrue();
            assertThat(released).isTrue();
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void startingTheShutdownTwiceDoesNotStartASecondThread() throws Exception {
        AtomicInteger shutdowns = new AtomicInteger();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        shutdowns::incrementAndGet,
                        (timeout, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofSeconds(30),
                        abandoned);

        teardown.start();
        teardown.start();
        // close() implies start(), so a client closed without a preceding shutdown still runs
        // one — and one already started is not run again.
        teardown.close();

        assertThat(shutdowns).hasValue(1);
    }

    /**
     * {@code AutoCloseable} strongly encourages idempotence, and without it a second close reruns
     * the release and rethrows the same captured failure — which a caller that closes defensively
     * would meet as a second, spurious teardown error.
     */
    @Test
    void closingTwiceRunsTheReleaseOnceAndRethrowsOnce() {
        AtomicInteger releases = new AtomicInteger();
        Error failure = new NoClassDefFoundError("shutdown blew up");
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {
                            throw failure;
                        },
                        (t, unit) -> true,
                        DESCRIPTION,
                        releases::incrementAndGet,
                        Duration.ofSeconds(30),
                        abandoned);

        assertThatThrownBy(teardown::close).isSameAs(failure);
        assertThatCode(teardown::close).doesNotThrowAnyException();
        assertThat(releases).hasValue(1);
    }

    /**
     * The give-up path is idempotent too — and now that an abandonment is counted, a second close
     * must not count it twice either.
     */
    @Test
    void closingTwiceAfterGivingUpRepeatsNeitherTheReleaseNorTheCount() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger releases = new AtomicInteger();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        DESCRIPTION,
                        releases::incrementAndGet,
                        Duration.ofMillis(50),
                        abandoned);

        try {
            teardown.close();
            teardown.close();

            assertThat(releases).hasValue(1);
            assertThat(abandoned.sum()).isEqualTo(1);
        } finally {
            blocked.countDown();
        }
    }

    /**
     * The counter is this test's own, so these are absolute — no baseline, and no dependence on
     * which sibling ran first.
     */
    @Test
    void anAbandonedTeardownIsCountedAndACompletedOneIsNot() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        BoundedShutdown abandons =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofMillis(50),
                        abandoned);

        try {
            abandons.close();

            assertThat(abandoned.sum()).isEqualTo(1);
        } finally {
            blocked.countDown();
        }

        new BoundedShutdown(
                        () -> {},
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofSeconds(30),
                        abandoned)
                .close();

        // A teardown that finished is not residue, so it must not inflate the count — the half that
        // makes a non-zero reading mean something.
        assertThat(abandoned.sum()).isEqualTo(1);
    }

    /**
     * Past the give-up there is no caller left to throw to — {@code close()} has returned — so this
     * warning is the whole of what a failing teardown gets, cause included. The give-up line before
     * it says the resources leak; without this one, the operator never learns the shutdown then
     * failed outright, which is a different fault with a different fix.
     */
    @Test
    void aShutdownFailingAfterTheGiveUpIsStillReportedWithItsCause() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("shutdown blew up too late");
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {
                            awaitUninterruptibly(blocked);
                            throw failure;
                        },
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofMillis(50),
                        abandoned);

        try (LogCapture capture = LogCapture.of(BoundedShutdown.class)) {
            teardown.close();

            assertThat(abandoned.sum()).isEqualTo(1);
            assertThat(capture.getEvents()).hasSize(1);

            // Only now does the shutdown fail, on the thread close() stopped waiting for. The
            // report is that thread's, so it is awaited rather than assumed to have landed - well
            // inside the class timeout, and carrying what did land, because a lost report is
            // otherwise indistinguishable from a capture that never attached.
            blocked.countDown();
            Awaits.await(
                    "the abandoned teardown to report its failure",
                    Duration.ofSeconds(10),
                    () -> capture.getEvents().size() >= 2,
                    () -> "captured: " + capture.getEvents());

            assertThat(capture.getEvents())
                    .element(1)
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage())
                                        .contains(DESCRIPTION)
                                        .contains("already given up");
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        } finally {
            // Every other blocking case here releases the latch in a finally: an assertion failing
            // above would otherwise leave the shutdown thread parked for a minute and then log into
            // whichever sibling's capture is open by then.
            blocked.countDown();
        }
    }

    /**
     * The counter is required, and rejected at construction rather than where it is used: the one
     * increment sits on the give-up path, ahead of the warning that explains it, so a null would
     * turn a bounded and logged give-up into an NPE with the diagnostic swallowed — during exactly
     * the outage this class exists to survive, and only then.
     */
    @Test
    void aMissingCounterIsRejectedAtConstructionRatherThanOnTheGiveUpPath() {
        assertThatThrownBy(
                        () ->
                                new BoundedShutdown(
                                        () -> {},
                                        (t, unit) -> true,
                                        DESCRIPTION,
                                        null,
                                        Duration.ofSeconds(30),
                                        null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("abandonedCount");
    }

    @Test
    void noReleaseIsFineOnEveryPath() throws Exception {
        // The dead-letter queue passes null: its channel is the next entry in its own
        // Closers.closeAll list, so a null must not be a NullPointerException in the finally.
        BoundedShutdown failing =
                new BoundedShutdown(
                        () -> {
                            throw new IllegalStateException("shutdown blew up");
                        },
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofSeconds(30),
                        abandoned);

        assertThatThrownBy(failing::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("shutdown blew up");
    }

    /**
     * The budget is rejected here as well as at every setter that feeds one: a longer {@code
     * Duration} throws {@code ArithmeticException} from {@code toNanos()} in {@link
     * BoundedShutdown#start()} instead — on a TaskManager, out of a teardown, where it reaches
     * Flink's teardown path and not a caller's {@code try}. This is the backstop for a consumer
     * whose budget is built in code and passes no setter (#334; ADR-0068).
     */
    @Test
    void aBudgetTooLargeForNanosecondsIsRejectedAtConstructionRatherThanAtStart() {
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThatThrownBy(
                        () ->
                                new BoundedShutdown(
                                        () -> {},
                                        (t, unit) -> true,
                                        DESCRIPTION,
                                        null,
                                        expressible.plusNanos(1),
                                        abandoned))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout must be at most")
                .hasMessageContaining("292 years");
    }

    /**
     * And the boundary it accepts really is a budget, which is worth pinning because the arithmetic
     * looks broken there and is not: {@code System.nanoTime() + timeout.toNanos()} overflows, and
     * {@code deadlineNanos - System.nanoTime()} then wraps a second time, the two cancelling to the
     * true remainder (measured 2026-08-08: 106751 days out of a 106751-day budget). Nothing here is
     * defended against that overflow, deliberately — this test is what says a later {@code
     * Math.addExact} or clamp, added to "harden" the stamp, would turn the documented way of saying
     * "effectively unbounded" into an exception or a zero wait.
     *
     * <p>The termination wait is the seam: it is handed what is left, so a fake records it.
     */
    @Test
    void theLargestExpressibleBudgetIsNotSpentTheInstantItStarts() throws Exception {
        AtomicLong remaining = new AtomicLong();
        CountDownLatch awaited = new CountDownLatch(1);
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {},
                        (t, unit) -> {
                            remaining.set(unit.toNanos(t));
                            awaited.countDown();
                            return true;
                        },
                        DESCRIPTION,
                        null,
                        Duration.ofNanos(Long.MAX_VALUE),
                        abandoned);

        teardown.start();
        awaitUninterruptibly(awaited);
        teardown.close();

        // Within a hair of the whole budget, since the shutdown step does nothing: what this
        // rejects is the 0 an overflowed deadline produced.
        assertThat(remaining.get()).isGreaterThan(Duration.ofDays(365L * 100).toNanos());
    }

    @Test
    void theBudgetIsReadableForTheCallerThatHandedItOver() {
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {},
                        (t, unit) -> true,
                        DESCRIPTION,
                        null,
                        Duration.ofSeconds(7),
                        abandoned);

        assertThat(teardown.timeout()).isEqualTo(Duration.ofSeconds(7));
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepUninterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
