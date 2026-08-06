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

package io.github.flink.gcp.connector.base.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
                        Duration.ofMillis(50));

        try {
            teardown.close();

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
                        timeout);

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
                        Duration.ofSeconds(30));

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
                        Duration.ofSeconds(30));

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
                        timeout);

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
                        Duration.ofMinutes(1));

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
                        Duration.ofSeconds(30));

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
                        Duration.ofSeconds(30));

        assertThatThrownBy(teardown::close).isSameAs(failure);
        assertThatCode(teardown::close).doesNotThrowAnyException();
        assertThat(releases).hasValue(1);
    }

    /** The give-up path is idempotent too, and must not count a second abandonment. */
    @Test
    void closingTwiceAfterGivingUpDoesNotRepeatTheGiveUp() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger releases = new AtomicInteger();
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        DESCRIPTION,
                        releases::incrementAndGet,
                        Duration.ofMillis(50));

        try {
            teardown.close();
            teardown.close();

            assertThat(releases).hasValue(1);
        } finally {
            blocked.countDown();
        }
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
                        Duration.ofSeconds(30));

        assertThatThrownBy(failing::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("shutdown blew up");
    }

    @Test
    void theBudgetIsReadableForTheCallerThatHandedItOver() {
        BoundedShutdown teardown =
                new BoundedShutdown(
                        () -> {}, (t, unit) -> true, DESCRIPTION, null, Duration.ofSeconds(7));

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
