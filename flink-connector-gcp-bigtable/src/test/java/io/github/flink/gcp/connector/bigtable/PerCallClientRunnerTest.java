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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PerCallClientRunner}. */
@Timeout(60)
class PerCallClientRunnerTest {

    private final FakeClient client = new FakeClient();

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
    void closesTheClientWhenTheCallReturns() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");

        String result =
                runner.call(() -> "settings", this::open, c -> ApiFutures.immediateFuture("done"));

        assertThat(result).isEqualTo("done");
        assertThat(client.closes).hasValue(1);
        assertThat(GrpcEventLoops.isRunning()).as("the call's lease was kept").isFalse();
    }

    @Test
    void aFailedCallClosesTheClientAndThrowsTheFailureItself() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        IllegalStateException failure = new IllegalStateException("refused");

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFailedFuture(failure)))
                .isSameAs(failure);
        assertThat(failure.getSuppressed())
                .anySatisfy(
                        s -> assertThat(s).hasMessageContaining("The Bigtable call was awaited"));
        assertThat(client.closes).hasValue(1);
    }

    @Test
    void aCallThatCannotStartClosesTheClient() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> {
                                            throw new IllegalArgumentException("bad request");
                                        }))
                .hasMessage("bad request");
        assertThat(client.closes).hasValue(1);
    }

    @Test
    void refusesACallAfterCloseWithoutOpeningAClient() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        runner.close();

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> {
                                            throw new AssertionError(
                                                    "a closed runner prepared settings");
                                        },
                                        settings -> {
                                            throw new AssertionError("a closed runner opened");
                                        },
                                        c -> ApiFutures.immediateFuture("x")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Bigtable test owner")
                .hasMessageContaining("was closed; the job is shutting down");
    }

    /**
     * The JobManager-side rule's second half: a teardown that lands while a call is in flight
     * cancels it through its future and closes the client, and the waiting caller returns at once.
     */
    @Test
    void closeCancelsTheCallInFlightAndClosesItsClient() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> {
                                            started.countDown();
                                            return pending;
                                        });
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        runner.close();

        assertThat(pending.isCancelled()).as("the call in flight was cancelled").isTrue();
        assertThat(client.closes).hasValue(1);
        assertThat(caller.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed while a call was in flight");
        assertThat(client.closes).as("the client was closed once").hasValue(1);
    }

    @Test
    void anInterruptedWaitCancelsTheCallAndRestoresTheInterrupt() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CompletableFuture<Throwable> outcome = new CompletableFuture<>();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                runner.call(() -> "settings", this::open, c -> pending);
                                outcome.complete(null);
                            } catch (Throwable t) {
                                interruptRestored.set(Thread.currentThread().isInterrupted());
                                outcome.complete(t);
                            }
                        });
        caller.start();
        awaitParked(caller);

        caller.interrupt();

        assertThat(outcome.get(10, TimeUnit.SECONDS)).isInstanceOf(InterruptedIOException.class);
        assertThat(interruptRestored).isTrue();
        assertThat(pending.isCancelled()).isTrue();
        assertThat(client.closes).hasValue(1);
        // The client library's close gives its wait up at once on an interrupted thread and then
        // skips the rest of its cleanup, so the interrupt must come back only after the close.
        assertThat(client.interruptedWhileClosing)
                .as("the client was closed with the interrupt still set")
                .isFalse();
        caller.join(10_000);
    }

    @Test
    void aCheckedFailureIsWrappedInAnIOException() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        Exception checked = new Exception("checked");

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFailedFuture(checked)))
                .isInstanceOf(IOException.class)
                .hasCause(checked);
        assertThat(client.closes).hasValue(1);
    }

    /**
     * Building a production data client makes calls of its own, so a teardown must not wait for
     * one: close() returns while the client is still being built, and the client built after it is
     * closed unused.
     */
    @Test
    void closeDoesNotWaitForAClientBeingBuiltAndThatClientIsClosedUnused() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch finishBuilding = new CountDownLatch(1);
        AtomicBoolean started = new AtomicBoolean();
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.call(
                                        () -> "settings",
                                        settings -> {
                                            building.countDown();
                                            await(finishBuilding);
                                            return open(settings);
                                        },
                                        c -> {
                                            started.set(true);
                                            return ApiFutures.immediateFuture("x");
                                        });
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        try {
            assertThat(building.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> closing = CompletableFuture.runAsync(runner::close);
            closing.get(10, TimeUnit.SECONDS);
        } finally {
            finishBuilding.countDown();
        }

        assertThat(caller.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed; the job is shutting down");
        assertThat(started).as("a call started on a client built after close()").isFalse();
        assertThat(client.closes).hasValue(1);
    }

    /**
     * Flink shuts the coordinator's executor down with {@code shutdownNow()} after the enumerator's
     * close, which interrupts a worker still building a client. The client it then closes unused is
     * closed with the interrupt held back, or its library would skip its cleanup, and the interrupt
     * comes back afterwards.
     */
    @Test
    void aLateBuiltClientIsClosedWithTheInterruptHeldBackAndTheInterruptRestored()
            throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.call(
                                        () -> "settings",
                                        settings -> {
                                            runner.close();
                                            // What shutdownNow() delivers to the building worker.
                                            Thread.currentThread().interrupt();
                                            return open(settings);
                                        },
                                        c -> ApiFutures.immediateFuture("x"));
                                return null;
                            } catch (Throwable t) {
                                return t;
                            } finally {
                                interruptedAfter.set(Thread.interrupted());
                            }
                        });

        assertThat(caller.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed; the job is shutting down");
        assertThat(client.closes).hasValue(1);
        assertThat(client.interruptedWhileClosing)
                .as("the late-built client was closed with the interrupt set")
                .isFalse();
        assertThat(interruptedAfter).as("the interrupt was restored").isTrue();
    }

    /**
     * An interrupted owner's close holds the interrupt back while it closes the call's client. The
     * call's future keeps the worker parked until the owner's close has finished, so the owner is
     * the one that closes the client.
     */
    @Test
    void anInterruptedOwnersCloseClosesTheClientWithTheInterruptHeldBack() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        GatedFuture pending = new GatedFuture();
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> {
                                            started.countDown();
                                            return pending;
                                        });
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        Thread.currentThread().interrupt();
        try {
            runner.close();
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the owner's interrupt was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
            pending.release.countDown();
        }

        assertThat(pending.isCancelled()).isTrue();
        assertThat(client.closedBy)
                .as("the owner closed the client")
                .isSameAs(Thread.currentThread());
        assertThat(client.interruptedWhileClosing)
                .as("the owner's close closed the client with the interrupt set")
                .isFalse();
        assertThat(caller.get(10, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
        assertThat(client.closes).hasValue(1);
    }

    /**
     * A call whose wait returns only when the test releases it, whether or not it was cancelled.
     */
    private static final class GatedFuture implements ApiFuture<String> {

        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean cancelled;

        @Override
        public void addListener(Runnable listener, Executor executor) {}

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled;
        }

        @Override
        public String get() throws InterruptedException {
            release.await();
            if (cancelled) {
                throw new CancellationException();
            }
            return "done";
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws InterruptedException {
            return get();
        }
    }

    private LeasedBigtableClient<FakeClient> open(String settings) throws IOException {
        assertThat(settings).isEqualTo("settings");
        return LeasedBigtableClient.open(lease -> client);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /** Waits, boundedly, until the thread parks in the call's wait. */
    private static void awaitParked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.getState() != Thread.State.WAITING && thread.isAlive()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the caller never parked: " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    /** A client that counts its closes and records whether its thread was interrupted. */
    private static final class FakeClient implements AutoCloseable {

        private final AtomicInteger closes = new AtomicInteger();
        private volatile boolean interruptedWhileClosing;
        @Nullable private volatile Thread closedBy;

        @Override
        public void close() {
            closedBy = Thread.currentThread();
            if (Thread.currentThread().isInterrupted()) {
                interruptedWhileClosing = true;
            }
            closes.incrementAndGet();
        }
    }
}
