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

import com.google.api.gax.core.BackgroundResource;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link GrpcEventLoops.LeasedClient} around a client that is not a gax {@code
 * BackgroundResource}, opened with {@link GrpcEventLoops#openAutoCloseable}, and for the close
 * {@link PerCallClientRunner} uses. The bounded close of a gax client is held by {@code
 * GrpcEventLoopsTest}.
 */
@Timeout(60)
class LeasedClientTest {

    @BeforeEach
    void noLeaseIsOpenBeforehand() {
        assertThat(GrpcEventLoops.isRunning())
                .as("a lease is open before the test: an earlier class in this fork leaked one")
                .isFalse();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void theClientClosesOnceAndBeforeItsLeaseIsReleased() throws Exception {
        FakeClient client = new FakeClient();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        assertThat(leased.client()).isSameAs(client);

        leased.close();
        leased.close();

        assertThat(client.closes).hasValue(1);
        assertThat(client.leaseHeldAtClose).as("the lease outlived the client's close").isTrue();
    }

    /** A client given a closer is closed by it, once, before its lease is released. */
    @Test
    void aClientGivenACloserIsClosedByItInsteadOfItsOwnClose() throws Exception {
        FakeClient client = new FakeClient();
        List<String> steps = new ArrayList<>();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(
                        lease -> client,
                        closed -> {
                            steps.add("closer");
                            steps.add(GrpcEventLoops.isRunning() ? "lease held" : "lease released");
                        });

        leased.close();
        leased.close();

        assertThat(steps).containsExactly("closer", "lease held");
        assertThat(client.closes).as("the client's own close").hasValue(0);
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    /** The closer's interrupt is handled as a client's own close: consumed, then restored. */
    @Test
    void anInterruptTheCloserConsumesIsRestored() throws Exception {
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(
                        lease -> new FakeClient(),
                        client -> {
                            if (Thread.interrupted()) {
                                throw new IOException(
                                        "close interrupted", new InterruptedException());
                            }
                        });

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(leased::close).hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt the closer consumed")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void aFailingCloseStillReleasesTheLeaseAndIsThrown() throws Exception {
        GrpcEventLoops.LeasedClient<AutoCloseable> leased =
                GrpcEventLoops.openAutoCloseable(
                        lease ->
                                () -> {
                                    throw new IllegalStateException("close failed");
                                });

        assertThatThrownBy(leased::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("close failed");
    }

    @Test
    void aCheckedCloseFailureIsThrownWrapped() throws Exception {
        GrpcEventLoops.LeasedClient<AutoCloseable> leased =
                GrpcEventLoops.openAutoCloseable(
                        lease ->
                                () -> {
                                    throw new IOException("close failed");
                                });

        assertThatThrownBy(leased::close)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("close failed");
    }

    @Test
    void aFailedCreationReleasesTheLease() {
        assertThatThrownBy(
                        () ->
                                GrpcEventLoops.openAutoCloseable(
                                        lease -> {
                                            assertThat(GrpcEventLoops.isRunning()).isTrue();
                                            throw new IOException("cannot create");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("cannot create");
        // noLeaseIsLeftOpen() is the assertion that matters.
    }

    @Test
    void aFactoryReturningNoClientIsRefusedAndReleasesTheLease() {
        assertThatThrownBy(() -> GrpcEventLoops.openAutoCloseable(lease -> null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned null");
    }

    /**
     * A client such as Bigtable's data client consumes an interrupt it finds set; inside {@code
     * interruptibleReleases()} that interrupt is what ends the release's wait, so it comes back
     * before the lease is released.
     */
    @Test
    void anInterruptTheClientsCloseConsumesIsRestored() throws Exception {
        AtomicBoolean consumed = new AtomicBoolean();
        GrpcEventLoops.LeasedClient<AutoCloseable> leased =
                GrpcEventLoops.openAutoCloseable(lease -> () -> consumed.set(Thread.interrupted()));
        Thread.currentThread().interrupt();
        try {
            leased.close();

            assertThat(consumed).as("the fake's close saw and consumed the interrupt").isTrue();
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * An interrupt delivered while a client's close waits ends that wait, which Bigtable's and
     * Spanner's clients report only as a close failure caused by the {@link InterruptedException};
     * that interrupt comes back too.
     */
    @Test
    void anInterruptDeliveredDuringTheClientsCloseIsRestored() throws Exception {
        GrpcEventLoops.LeasedClient<AutoCloseable> leased =
                GrpcEventLoops.openAutoCloseable(
                        lease ->
                                () -> {
                                    throw new IllegalStateException(
                                            "failed to close client context",
                                            new InterruptedException());
                                });
        try {
            assertThatThrownBy(leased::close).hasMessage("failed to close client context");
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt the close consumed was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void aConcurrentCloseWaitsForTheFirst() throws Exception {
        FakeClient client = new FakeClient().blocking();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        Thread first = new Thread(leased::close, "first-close");
        AtomicBoolean secondReturned = new AtomicBoolean();
        Thread second =
                new Thread(
                        () -> {
                            leased.close();
                            secondReturned.set(true);
                        },
                        "second-close");
        try {
            first.start();
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            second.start();
            awaitParked(second);
            assertThat(secondReturned)
                    .as("the second close returned while the first ran")
                    .isFalse();
        } finally {
            client.release.countDown();
            first.join(TimeUnit.SECONDS.toMillis(15));
            second.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(secondReturned).isTrue();
        assertThat(client.closes).hasValue(1);
    }

    /**
     * The held-back close {@link PerCallClientRunner} uses: the client closes on a thread whose
     * interrupt is not set, so a library that gives its close up on an interrupted thread does not,
     * and the interrupt comes back afterwards.
     */
    @Test
    void aHeldBackCloseClosesTheClientUninterruptedAndRestoresTheInterrupt() throws Exception {
        FakeClient client = new FakeClient();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        Thread.currentThread().interrupt();
        try {
            leased.closeWithInterruptHeldBack();

            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(client.interruptedAtClose)
                .as("the client closed with the interrupt set")
                .isFalse();
        assertThat(client.closes).hasValue(1);
    }

    /**
     * The held-back interrupt is restored before the lease is released: inside {@code
     * interruptibleReleases()}, the release of the last lease then stops waiting for a busy group
     * after the first second rather than waiting for the whole bound.
     */
    @Test
    void aHeldBackCloseRestoresTheInterruptBeforeTheLeaseIsReleased() throws Exception {
        FakeClient client = new FakeClient();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicLong elapsedNanos = new AtomicLong();
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try (GrpcEventLoops.InterruptibleReleases scope =
                                    GrpcEventLoops.interruptibleReleases()) {
                                Thread.currentThread().interrupt();
                                long start = System.nanoTime();
                                leased.closeWithInterruptHeldBack();
                                elapsedNanos.set(System.nanoTime() - start);
                                interruptedAfter.set(Thread.interrupted());
                            } catch (Throwable t) {
                                thrown.set(t);
                            }
                        },
                        "held-back-close");
        try {
            GrpcEventLoops.executeOnGroup(() -> holdUntil(running, finish));
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            closer.start();
            // Well inside the group's 11 s bound, which a release on an uninterrupted thread
            // would wait out while the task holds a group thread.
            closer.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(thrown.get()).as("the close threw").isNull();
            assertThat(closer.isAlive())
                    .as("the release waited for the busy group: the interrupt was not restored")
                    .isFalse();
        } finally {
            finish.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(client.interruptedAtClose).isFalse();
        assertThat(interruptedAfter).as("the interrupt was restored").isTrue();
        assertThat(elapsedNanos.get()).isGreaterThanOrEqualTo(TimeUnit.SECONDS.toNanos(1));
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    /**
     * Without the held-back close too, an interrupt the client's close consumed is back before the
     * lease is released: inside {@code interruptibleReleases()}, the release then stops waiting for
     * a busy group after the first second.
     */
    @Test
    void anInterruptTheClientsCloseConsumesIsBackBeforeTheLeaseIsReleased() throws Exception {
        AtomicBoolean consumed = new AtomicBoolean();
        GrpcEventLoops.LeasedClient<AutoCloseable> leased =
                GrpcEventLoops.openAutoCloseable(lease -> () -> consumed.set(Thread.interrupted()));
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try (GrpcEventLoops.InterruptibleReleases scope =
                                    GrpcEventLoops.interruptibleReleases()) {
                                Thread.currentThread().interrupt();
                                leased.close();
                            } catch (Throwable t) {
                                thrown.set(t);
                            } finally {
                                Thread.interrupted();
                            }
                        },
                        "consuming-close");
        try {
            GrpcEventLoops.executeOnGroup(() -> holdUntil(running, finish));
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            closer.start();
            closer.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(thrown.get()).as("the close threw").isNull();
            assertThat(closer.isAlive())
                    .as("the release waited for the busy group: the interrupt was not restored")
                    .isFalse();
        } finally {
            finish.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(consumed).as("the fake's close consumed the interrupt").isTrue();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    /**
     * A gax client opened this way would be closed by its own {@code close()}, which does not wait
     * for its channels, so it is refused, shut down first, and its lease released.
     */
    @Test
    void aGaxClientIsRefusedAndShutDown() {
        GaxFake gax = new GaxFake(false);

        assertThatThrownBy(() -> GrpcEventLoops.openAutoCloseable(lease -> gax))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GrpcEventLoops.open");
        assertThat(gax.isShutdown()).as("the refused client was shut down").isTrue();
    }

    /**
     * A holder that owns an emulator channel releases the client's lease even when the channel
     * fails to close, and throws that failure.
     */
    @Test
    void aChannelThatFailsToCloseStillLeavesNoLeaseOpen() throws Exception {
        GaxFake failingChannel = new GaxFake(true);
        GrpcEventLoops.LeasedClient<GaxFake> channel = GrpcEventLoops.open(lease -> failingChannel);
        GaxFake client = new GaxFake(false);
        GrpcEventLoops.LeasedClient<GaxFake> leased;
        try {
            leased = GrpcEventLoops.open(lease -> client, channel);
        } catch (Throwable t) {
            channel.close();
            throw t;
        }

        assertThatThrownBy(leased::close).hasMessage("channel shutdown failed");
        assertThat(client.isShutdown()).as("the client closed first").isTrue();
        // noLeaseIsLeftOpen() is the assertion that matters.
    }

    /**
     * A caller that finds another thread closing the client waits for it, but an interrupt it
     * already carries ends that wait at once, even in the held-back close: only the closing thread
     * holds its interrupt back.
     */
    @Test
    void aWaiterInterruptedBeforeItWaitsReturnsAtOnceWithTheInterrupt() throws Exception {
        FakeClient client = new FakeClient().blocking();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        Thread first = new Thread(leased::close, "first-close");
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread waiter =
                new Thread(
                        () -> {
                            Thread.currentThread().interrupt();
                            leased.closeWithInterruptHeldBack();
                            interruptedAfter.set(Thread.interrupted());
                        },
                        "interrupted-waiter");
        try {
            first.start();
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            waiter.start();
            waiter.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(waiter.isAlive())
                    .as("the interrupted waiter kept waiting for the first close")
                    .isFalse();
            assertThat(first.isAlive()).as("the first close is still under way").isTrue();
        } finally {
            client.release.countDown();
            first.join(TimeUnit.SECONDS.toMillis(15));
            waiter.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(interruptedAfter).as("the waiter's interrupt was restored").isTrue();
        assertThat(client.closes).hasValue(1);
    }

    @Test
    void aWaiterInterruptedWhileItWaitsReturnsWithTheInterrupt() throws Exception {
        FakeClient client = new FakeClient().blocking();
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.openAutoCloseable(lease -> client);
        Thread first = new Thread(leased::close, "first-close");
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread waiter =
                new Thread(
                        () -> {
                            leased.closeWithInterruptHeldBack();
                            interruptedAfter.set(Thread.interrupted());
                        },
                        "waiter");
        try {
            first.start();
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            waiter.start();
            awaitParked(waiter);
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(waiter.isAlive()).as("the interrupt did not end the wait").isFalse();
        } finally {
            client.release.countDown();
            first.join(TimeUnit.SECONDS.toMillis(15));
            waiter.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(interruptedAfter).as("the waiter's interrupt was restored").isTrue();
        assertThat(client.closes).hasValue(1);
    }

    /** Waits, boundedly, until the thread parks; a thread that returned instead fails here. */
    private static void awaitParked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.getState() != Thread.State.WAITING) {
            if (!thread.isAlive()) {
                throw new AssertionError(thread.getName() + " returned instead of waiting");
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError(thread.getName() + " never parked: " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    /** A group task: signals that it runs, then holds its thread until {@code finish} opens. */
    private static void holdUntil(CountDownLatch running, CountDownLatch finish) {
        running.countDown();
        try {
            finish.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A gax client that terminates at once, or whose shutdown fails if {@code failShutdown}. */
    private static final class GaxFake implements BackgroundResource {

        private final boolean failShutdown;
        private final AtomicBoolean shutDown = new AtomicBoolean();

        GaxFake(boolean failShutdown) {
            this.failShutdown = failShutdown;
        }

        @Override
        public void shutdown() {
            shutDown.set(true);
            if (failShutdown) {
                throw new IllegalStateException("channel shutdown failed");
            }
        }

        @Override
        public boolean isShutdown() {
            return shutDown.get();
        }

        @Override
        public boolean isTerminated() {
            return shutDown.get();
        }

        @Override
        public void shutdownNow() {}

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) {
            return true;
        }

        @Override
        public void close() {
            shutdown();
        }
    }

    /** A client that records its closes and, if blocking, holds its close until released. */
    private static final class FakeClient implements AutoCloseable {

        private final AtomicInteger closes = new AtomicInteger();
        private final CountDownLatch closing = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean blocking;
        private volatile boolean interruptedAtClose;
        private volatile boolean leaseHeldAtClose;

        FakeClient blocking() {
            blocking = true;
            return this;
        }

        @Override
        public void close() throws InterruptedException {
            closes.incrementAndGet();
            interruptedAtClose = Thread.currentThread().isInterrupted();
            leaseHeldAtClose = GrpcEventLoops.isRunning();
            closing.countDown();
            if (blocking) {
                release.await();
            }
        }
    }
}
