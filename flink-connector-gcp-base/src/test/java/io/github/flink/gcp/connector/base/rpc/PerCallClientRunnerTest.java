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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
                        s -> assertThat(s).hasMessageContaining("The test owner call was awaited"));
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
                .hasMessageContaining("The test owner")
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
     * A close failure says nothing about a call that succeeded, so the call's result is returned
     * and the failure only logged; the lease is released either way.
     */
    @Test
    void aCloseFailureAfterASuccessfulCallIsLoggedAndTheResultReturned() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        client.closeFailure = new IllegalStateException("close failed");

        String result =
                runner.call(() -> "settings", this::open, c -> ApiFutures.immediateFuture("done"));

        assertThat(result).isEqualTo("done");
        assertThat(client.closes).hasValue(1);
    }

    @Test
    void aCloseFailureAfterAFailedCallIsSuppressedOntoTheCallsFailure() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        IllegalStateException closeFailure = new IllegalStateException("close failed");
        client.closeFailure = closeFailure;
        IllegalArgumentException failure = new IllegalArgumentException("refused");

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFailedFuture(failure)))
                .isSameAs(failure);
        assertThat(failure.getSuppressed()).contains(closeFailure);
    }

    /**
     * An owner's close that finds the calling thread closing the call's client waits for that
     * close, so the owner never returns while the client is still closing.
     */
    @Test
    void anOwnersCloseWaitsForTheCallersCloseOfTheClient() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        client.blocking = true;
        CompletableFuture<String> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFuture("done"));
                            } catch (IOException e) {
                                throw new AssertionError(e);
                            }
                        });
        Thread owner = new Thread(runner::close, "owner-close");
        try {
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            owner.start();
            awaitParked(owner);
        } finally {
            client.release.countDown();
            owner.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(caller.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(client.closes).hasValue(1);
    }

    /**
     * Only the thread that closes a client holds its interrupt back. An owner whose close finds the
     * calling thread closing the client, on a thread already interrupted, stops waiting at once and
     * keeps its interrupt, as Flink's abort of a coordinator close that outran its timeout expects.
     */
    @Test
    void anInterruptedOwnersCloseThatFindsTheClientClosingReturnsAtOnce() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        client.blocking = true;
        CompletableFuture<String> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFuture("done"));
                            } catch (IOException e) {
                                throw new AssertionError(e);
                            }
                        });
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread owner =
                new Thread(
                        () -> {
                            Thread.currentThread().interrupt();
                            runner.close();
                            interruptedAfter.set(Thread.interrupted());
                        },
                        "interrupted-owner-close");
        try {
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            owner.start();
            owner.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(owner.isAlive())
                    .as("the interrupted owner kept waiting for the caller's close")
                    .isFalse();
            assertThat(caller).as("the caller's close is still under way").isNotDone();
        } finally {
            client.release.countDown();
            owner.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(interruptedAfter).as("the owner's interrupt was restored").isTrue();
        assertThat(caller.get(10, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(client.closes).hasValue(1);
    }

    /**
     * The interrupt that ends a call's wait is consumed by the wait, and comes back before the
     * client's lease is released: inside {@code interruptibleReleases()}, as a full lookup cache
     * load runs, the release of the last lease then stops waiting for a busy group after its first
     * second instead of waiting for the whole bound.
     */
    @Test
    void anInterruptedWaitGetsItsInterruptBackBeforeTheLeaseIsReleased() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread caller =
                new Thread(
                        () -> {
                            try (GrpcEventLoops.InterruptibleReleases scope =
                                    GrpcEventLoops.interruptibleReleases()) {
                                runner.call(() -> "settings", this::open, c -> pending);
                            } catch (Throwable t) {
                                outcome.set(t);
                            } finally {
                                interruptedAfter.set(Thread.interrupted());
                            }
                        },
                        "interrupted-load");
        try {
            caller.start();
            awaitParked(caller);
            GrpcEventLoops.executeOnGroup(() -> holdUntil(running, finish));
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();

            caller.interrupt();
            // Well inside the group's 11 s bound, which a release on an uninterrupted thread
            // waits out while the task holds a group thread.
            caller.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(caller.isAlive())
                    .as("the release waited for the busy group: the interrupt came back after it")
                    .isFalse();
        } finally {
            finish.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(outcome.get()).isInstanceOf(InterruptedIOException.class);
        assertThat(interruptedAfter).as("the interrupt was restored").isTrue();
        assertThat(client.interruptedWhileClosing)
                .as("the client was closed with the interrupt set")
                .isFalse();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    /**
     * An interrupt that arrives after a call's result, before its client closes, is held back while
     * the client closes, and the result is still returned.
     */
    @Test
    void anInterruptAfterASuccessfulCallIsHeldBackWhileTheClientCloses() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        String result;
        boolean interruptedAfter;
        try {
            result = runner.call(() -> "settings", this::open, c -> new InterruptingFuture("done"));
        } finally {
            interruptedAfter = Thread.interrupted();
        }

        assertThat(result).isEqualTo("done");
        assertThat(interruptedAfter).as("the interrupt was restored").isTrue();
        assertThat(client.interruptedWhileClosing)
                .as("the client was closed with the interrupt set")
                .isFalse();
    }

    @Test
    void aCloseFailureAfterASuccessfulCallIsLoggedWithTheOwnersName() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        IllegalStateException closeFailure = new IllegalStateException("close failed");
        client.closeFailure = closeFailure;

        try (LogCapture capture = LogCapture.of(PerCallClientRunner.class)) {
            runner.call(() -> "settings", this::open, c -> ApiFutures.immediateFuture("done"));

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMessage()).contains("test owner");
                                assertThat(event.getThrowable()).isSameAs(closeFailure);
                            });
        }
    }

    /** Only an exception from the close after a successful call is logged; an Error is thrown. */
    @Test
    void anErrorClosingTheClientAfterASuccessfulCallIsThrown() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        NoClassDefFoundError closeFailure = new NoClassDefFoundError("closed loader");
        client.closeFailure = closeFailure;

        assertThatThrownBy(
                        () ->
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> ApiFutures.immediateFuture("done")))
                .isSameAs(closeFailure);
        assertThat(client.closes).hasValue(1);
    }

    /**
     * A client whose call could not start is in flight while the calling thread closes it, so an
     * owner close that arrives meanwhile waits for that close rather than returning first.
     */
    @Test
    void anOwnersCloseWaitsForTheCloseOfAClientWhoseCallCouldNotStart() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        client.blocking = true;
        IllegalArgumentException startFailure = new IllegalArgumentException("bad request");
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.call(
                                        () -> "settings",
                                        this::open,
                                        c -> {
                                            throw startFailure;
                                        });
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        AtomicBoolean ownerReturned = new AtomicBoolean();
        Thread owner =
                new Thread(
                        () -> {
                            runner.close();
                            ownerReturned.set(true);
                        },
                        "owner-close");
        try {
            assertThat(client.closing.await(10, TimeUnit.SECONDS)).isTrue();
            owner.start();
            awaitParked(owner);
            assertThat(ownerReturned).as("the owner returned while the client closed").isFalse();
        } finally {
            client.release.countDown();
            owner.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(ownerReturned).isTrue();
        assertThat(caller.get(10, TimeUnit.SECONDS)).isSameAs(startFailure);
        assertThat(client.closes).hasValue(1);
    }

    /**
     * A runner tracks one call in flight. A second call while one is in flight is refused, its
     * client closed unused, and the first stays the call {@code close()} cancels.
     */
    @Test
    void aCallWhileAnotherIsInFlightIsRefusedAndTheFirstStaysCancellable() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Throwable> first =
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
        FakeClient second = new FakeClient();
        AtomicBoolean secondStarted = new AtomicBoolean();
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(
                            () ->
                                    runner.call(
                                            () -> GrpcEventLoops.openAutoCloseable(lease -> second),
                                            c -> {
                                                secondStarted.set(true);
                                                return ApiFutures.immediateFuture("x");
                                            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("one call at a time");
            assertThat(secondStarted).as("the refused call started").isFalse();
            assertThat(second.closes).as("the refused call's client was closed").hasValue(1);

            runner.close();

            assertThat(pending.isCancelled()).as("the first call stayed cancellable").isTrue();
        } finally {
            // Whatever failed above, the first call must end, and release its lease, before the
            // test does.
            runner.close();
            pending.cancel(true);
            try {
                first.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // Left to the failure already in flight, or to the wait below, to report.
            }
        }
        assertThat(first.get(10, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
        assertThat(client.closes).hasValue(1);
    }

    /** A session's calls share the one client opened for it, which closes when the body returns. */
    @Test
    void aSessionRunsItsCallsOnOneClientAndClosesItWhenTheBodyReturns() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        AtomicInteger opens = new AtomicInteger();

        String result =
                runner.session(
                        () -> "settings",
                        settings -> {
                            opens.incrementAndGet();
                            return open(settings);
                        },
                        session -> {
                            String first =
                                    session.call(
                                            c -> {
                                                assertThat(c).isSameAs(client);
                                                assertThat(client.closes).hasValue(0);
                                                return ApiFutures.immediateFuture("first");
                                            });
                            String second =
                                    session.call(
                                            c -> ApiFutures.immediateFuture(first + "+second"));
                            assertThat(client.closes)
                                    .as("the client closed between the session's calls")
                                    .hasValue(0);
                            return second;
                        });

        assertThat(result).isEqualTo("first+second");
        assertThat(opens).hasValue(1);
        assertThat(client.closes).hasValue(1);
    }

    /** The body can reach the session's client for work that makes no call. */
    @Test
    void aSessionHandsItsBodyTheClient() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");

        Object seen = runner.session(() -> "settings", this::open, session -> session.client());

        assertThat(seen).isSameAs(client);
        assertThat(client.closes).hasValue(1);
    }

    /** A refusal the body answers, such as a fallback on {@code INVALID_ARGUMENT}, ends nothing. */
    @Test
    void aFailureTheBodyCatchesLeavesTheSessionOpenForItsNextCall() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        IllegalArgumentException refusal = new IllegalArgumentException("refused");

        String result =
                runner.session(
                        () -> "settings",
                        this::open,
                        session -> {
                            try {
                                session.call(c -> ApiFutures.immediateFailedFuture(refusal));
                                throw new AssertionError("the refused call returned");
                            } catch (IllegalArgumentException e) {
                                assertThat(e).isSameAs(refusal);
                            }
                            return session.call(c -> ApiFutures.immediateFuture("fallback"));
                        });

        assertThat(result).isEqualTo("fallback");
        assertThat(client.closes).hasValue(1);
    }

    /**
     * A teardown that lands on a session's second call cancels that call, closes the client once,
     * and the body's failure says the runner was closed.
     */
    @Test
    void closeCancelsTheSessionsCallInFlightAndClosesItsClient() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Throwable> caller =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                runner.session(
                                        () -> "settings",
                                        this::open,
                                        session -> {
                                            session.call(c -> ApiFutures.immediateFuture("first"));
                                            return session.call(
                                                    c -> {
                                                        started.countDown();
                                                        return pending;
                                                    });
                                        });
                                return null;
                            } catch (Throwable t) {
                                return t;
                            }
                        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        runner.close();

        assertThat(pending.isCancelled()).as("the session's call in flight was cancelled").isTrue();
        assertThat(caller.get(10, TimeUnit.SECONDS))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed while a call was in flight");
        assertThat(client.closes).as("the client was closed once").hasValue(1);
    }

    /**
     * A teardown that lands between a session's calls closes the client, and the session's next
     * call is refused without being started.
     */
    @Test
    void aCallAfterCloseIsRefusedWithoutStarting() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        AtomicBoolean secondStarted = new AtomicBoolean();

        assertThatThrownBy(
                        () ->
                                runner.session(
                                        () -> "settings",
                                        this::open,
                                        session -> {
                                            session.call(c -> ApiFutures.immediateFuture("first"));
                                            runner.close();
                                            assertThat(client.closes)
                                                    .as("close() closed the session's client")
                                                    .hasValue(1);
                                            return session.call(
                                                    c -> {
                                                        secondStarted.set(true);
                                                        return ApiFutures.immediateFuture("x");
                                                    });
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed; the job is shutting down");
        assertThat(secondStarted).as("a call started after close()").isFalse();
        assertThat(client.closes).as("the client was closed once").hasValue(1);
    }

    /**
     * A session runs one call at a time: a second call started while its first is in flight is
     * refused unstarted, and the first stays the call {@code close()} cancels.
     */
    @Test
    void aSessionsSecondConcurrentCallIsRefusedAndTheFirstStaysCancellable() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SettableApiFuture<String> pending = SettableApiFuture.create();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> secondCall = new AtomicReference<>();
        AtomicBoolean secondStarted = new AtomicBoolean();

        assertThatThrownBy(
                        () ->
                                runner.session(
                                        () -> "settings",
                                        this::open,
                                        session -> {
                                            Thread other =
                                                    new Thread(
                                                            () -> {
                                                                await(started);
                                                                try {
                                                                    session.call(
                                                                            c -> {
                                                                                secondStarted.set(
                                                                                        true);
                                                                                return ApiFutures
                                                                                        .immediateFuture(
                                                                                                "x");
                                                                            });
                                                                } catch (Throwable t) {
                                                                    secondCall.set(t);
                                                                }
                                                                // Only once the second call was
                                                                // refused: the close must find
                                                                // the first call still in flight.
                                                                runner.close();
                                                            },
                                                            "second-call");
                                            other.start();
                                            return session.call(
                                                    c -> {
                                                        started.countDown();
                                                        return pending;
                                                    });
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed while a call was in flight");

        assertThat(secondCall.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("one call at a time");
        assertThat(secondStarted).as("the refused call started").isFalse();
        assertThat(pending.isCancelled()).as("the first call was cancelled").isTrue();
        assertThat(client.closes).hasValue(1);
    }

    /**
     * A failure of the body's own that happens to be an {@link InterruptedIOException}, such as a
     * socket timeout, is not an interrupt the session consumed, and sets none.
     */
    @Test
    void aBodysOwnInterruptedIOExceptionSetsNoInterrupt() {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        SocketTimeoutException timeout = new SocketTimeoutException("read timed out");

        assertThatThrownBy(
                        () ->
                                runner.session(
                                        () -> "settings",
                                        this::open,
                                        session -> {
                                            session.call(c -> ApiFutures.immediateFuture("first"));
                                            throw timeout;
                                        }))
                .isSameAs(timeout);
        assertThat(Thread.interrupted()).as("an interrupt was set").isFalse();
        assertThat(client.closes).hasValue(1);
    }

    /**
     * An interrupt a call's wait consumed is set again before the client closes even when the body
     * swallows the {@link InterruptedIOException} and returns.
     */
    @Test
    void anInterruptTheBodySwallowsIsStillRestored() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");

        String result;
        try {
            result =
                    runner.session(
                            () -> "settings",
                            this::open,
                            session -> {
                                Thread.currentThread().interrupt();
                                try {
                                    session.call(c -> SettableApiFuture.<String>create());
                                    throw new AssertionError("the interrupted wait returned");
                                } catch (InterruptedIOException e) {
                                    assertThat(Thread.currentThread().isInterrupted())
                                            .as("the wait left the interrupt set")
                                            .isFalse();
                                    return "swallowed";
                                }
                            });
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(result).isEqualTo("swallowed");
        assertThat(client.closes).hasValue(1);
    }

    /** A session that leaks its handle cannot start a call once its body has returned. */
    @Test
    void aSessionCannotCallOnceItsBodyHasReturned() throws Exception {
        PerCallClientRunner runner = new PerCallClientRunner("test owner");
        AtomicReference<PerCallClientRunner.ClientSession<FakeClient>> leaked =
                new AtomicReference<>();

        runner.session(
                () -> "settings",
                this::open,
                session -> {
                    leaked.set(session);
                    return "done";
                });

        assertThatThrownBy(() -> leaked.get().call(c -> ApiFutures.immediateFuture("x")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("session is no longer open");
        assertThat(client.closes).hasValue(1);
    }

    /** A completed call whose {@code get()} also interrupts the waiting thread, as it returns. */
    private static final class InterruptingFuture implements ApiFuture<String> {

        private final String value;

        private InterruptingFuture(String value) {
            this.value = value;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {}

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public String get() {
            Thread.currentThread().interrupt();
            return value;
        }

        @Override
        public String get(long timeout, TimeUnit unit) {
            return get();
        }
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

    private GrpcEventLoops.LeasedClient<FakeClient> open(String settings) throws IOException {
        assertThat(settings).isEqualTo("settings");
        return GrpcEventLoops.openAutoCloseable(lease -> client);
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

    /**
     * A client that counts its closes and records whether its thread was interrupted; it can be
     * made to fail its close, or to hold it until the test releases it.
     */
    private static final class FakeClient implements AutoCloseable {

        private final AtomicInteger closes = new AtomicInteger();
        private final CountDownLatch closing = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean interruptedWhileClosing;
        @Nullable private volatile Thread closedBy;
        @Nullable private volatile Throwable closeFailure;
        private volatile boolean blocking;

        @Override
        public void close() throws InterruptedException {
            closedBy = Thread.currentThread();
            if (Thread.currentThread().isInterrupted()) {
                interruptedWhileClosing = true;
            }
            closes.incrementAndGet();
            closing.countDown();
            if (blocking) {
                release.await();
            }
            Throwable failure = closeFailure;
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure != null) {
                throw (RuntimeException) failure;
            }
        }
    }
}
