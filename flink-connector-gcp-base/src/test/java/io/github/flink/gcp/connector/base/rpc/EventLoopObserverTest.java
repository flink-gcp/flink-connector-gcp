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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.util.function.ThrowingRunnable;

import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the test utility {@link EventLoopObserver} against the group it observes, which lives
 * here: test-utils cannot depend on base.
 */
class EventLoopObserverTest {

    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(
                        EventLoopObserver.awaitNoGroup(
                                GrpcEventLoops::isRunning, Duration.ofSeconds(30)))
                .isTrue();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void anOperationThatRunsATaskOnTheGroupIsSeen() throws Exception {
        assertThat(
                        observe(
                                () -> {
                                    try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
                                        GrpcEventLoops.executeOnGroup(() -> {});
                                    }
                                }))
                .isTrue();
    }

    @Test
    void anOperationWhoseLeaseRunsNothingIsNotSeen() throws Exception {
        // The release still starts every loop of the group, to stop it.
        assertThat(observe(() -> GrpcEventLoops.acquire().close())).isFalse();
    }

    /**
     * An earlier group still shutting down shows live group threads while the operation's own lease
     * is open: one of its loops is busy with a task, so its release, detached already, waits.
     */
    @Test
    void anEarlierGroupStillShuttingDownIsNotCreditedToALaterLease() throws Exception {
        whileAnEarlierGroupShutsDown(
                () -> assertThat(observe(() -> GrpcEventLoops.acquire().close())).isFalse());
    }

    @Test
    void aClientThatRunsATaskOnTheGroupIsSeenWhileItIsOpen() throws Exception {
        EventLoopObserver.LoopStarts starts = loopStartsFromNow();
        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            assertThat(starts.anyStarted()).as("before the task").isFalse();
            GrpcEventLoops.executeOnGroup(() -> {});
            assertThat(starts.anyStarted()).isTrue();
        }
    }

    /**
     * A count that grows only after the first reading: a wait that read it once would miss it. The
     * suppliers stand in for base's, so the retry is exercised without depending on a thread's
     * timing.
     */
    @Test
    void aLoopThatStartsAfterTheFirstReadingIsAwaited() throws Exception {
        AtomicInteger readings = new AtomicInteger();
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        () -> false, () -> readings.getAndIncrement() < 2 ? 0 : 1);

        assertThat(starts.awaitAnyStarted()).isTrue();
        assertThat(readings.get())
                .as("the baseline, a reading that found nothing, and at least one more")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void aClientWhoseLeaseRunsNothingIsNotSeenWhileItIsOpen() throws Exception {
        EventLoopObserver.LoopStarts starts = loopStartsFromNow();
        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            assertThat(starts.anyStarted()).isFalse();
        }
    }

    /**
     * The case a live group thread cannot tell apart: while the later client's lease is open, an
     * earlier group's thread is alive, though nothing ran on the later group.
     */
    @Test
    void anEarlierGroupStillShuttingDownIsNotCreditedToAClientStillOpen() throws Exception {
        whileAnEarlierGroupShutsDown(
                () -> {
                    EventLoopObserver.LoopStarts starts = loopStartsFromNow();
                    try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
                        assertThat(GrpcEventLoopThreads.alive())
                                .as("the earlier group's thread is alive")
                                .isTrue();
                        assertThat(starts.anyStarted()).isFalse();
                    }
                });
    }

    @Test
    void aCountReadWhileAGroupRunsIsRefused() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            assertThatThrownBy(EventLoopObserverTest::loopStartsFromNow)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already running");
        } finally {
            lease.close();
        }
    }

    /**
     * Runs the check while a first group is detached but still shutting down, with one of its loops
     * busy with a task until the check ends, so the release waits for it.
     */
    private static void whileAnEarlierGroupShutsDown(ThrowingRunnable<? extends Exception> check)
            throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
        GrpcEventLoops.Lease first = GrpcEventLoops.acquire();
        Thread releasing =
                new Thread(
                        () -> {
                            try {
                                first.close();
                            } catch (Throwable t) {
                                releaseFailure.set(t);
                            }
                        },
                        "release-first");
        try {
            GrpcEventLoops.executeOnGroup(
                    () -> {
                        worker.set(Thread.currentThread());
                        running.countDown();
                        try {
                            finish.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            releasing.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (GrpcEventLoops.isRunning() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertThat(GrpcEventLoops.isRunning()).as("the first group was detached").isFalse();

            check.run();
            // The release may give up waiting after its bound; the held loop is what keeps the
            // first group's thread alive, and it stays held until the finally below.
            assertThat(worker.get().isAlive())
                    .as("the first group's held loop was alive when the check ended")
                    .isTrue();
        } finally {
            finish.countDown();
            if (releasing.getState() == Thread.State.NEW) {
                first.close();
            }
            releasing.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(releasing.isAlive()).as("the first release is still running").isFalse();
        assertThat(releaseFailure.get()).as("the first release failed").isNull();
    }

    @Test
    void anOperationThatWouldStartWhileAGroupRunsIsRefused() {
        AtomicBoolean ran = new AtomicBoolean();
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            assertThatThrownBy(() -> observe(() -> ran.set(true)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already running");
        } finally {
            lease.close();
        }
        assertThat(ran).as("the operation ran").isFalse();
    }

    private static EventLoopObserver.LoopStarts loopStartsFromNow() {
        return EventLoopObserver.loopStartsFromNow(
                GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
    }

    private static boolean observe(ThrowingRunnable<? extends Exception> operation)
            throws Exception {
        return EventLoopObserver.sawGroupDuring(
                GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning, operation);
    }
}
