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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs an owner's calls on a client opened for them and closed when they return, and cancels the
 * one in flight when the owner closes.
 *
 * <p>A client a JobManager-side component holds open must not outlive the call it was opened for
 * (ADR-0208): at the end of a job Flink closes an enumerator on a thread it does not wait for, and
 * in practice after it has released the job's class loader (Flink 2.2.1, measured in #2237), so a
 * client still open then can tear its event-loop group down on a closed loader, and under {@code
 * cluster.uncaught-exception-handling: FAIL} the JobManager exits. So every {@link #call(Supplier,
 * SourcedClientOpener, Function)} opens its client, starts the call, waits for it and closes the
 * client, and {@link #close()} cancels a call still in flight through its {@link ApiFuture} before
 * closing that call's client.
 *
 * <p>An owner whose one operation takes several calls, such as a planner that probes and then reads
 * statistics, runs them as a {@link #session(Supplier, SourcedClientOpener, SessionBody) session}:
 * one client opened for the operation and closed when it returns, with each of its calls started
 * through {@link ClientSession#call(Function)}, so that the call in flight is always the one {@code
 * close()} cancels. A single call is a session of one call.
 *
 * <p>The cancellation goes through the future, retries included, and never through the client's
 * shutdown: gax schedules a retry on the client's executor, a shutdown drops it without completing
 * the call's future, and the SDKs' synchronous methods wait on that future uninterruptibly (gax
 * 2.91.0), which would strand the waiting thread. For the same reason the wait here is an
 * interruptible {@code get()} rather than a synchronous SDK method, and an interrupt cancels the
 * call. A cancellation that lands while gax is starting a retry can miss it; the wait then ends
 * when the thread is interrupted, as Flink's executor shutdown does to a coordinator's worker.
 *
 * <p>Every client this object closes, from a call, a session or {@code close()}, is closed with
 * {@link GrpcEventLoops.LeasedClient#closeWithInterruptHeldBack()}. For a client opened with {@link
 * GrpcEventLoops#openAutoCloseable(GrpcEventLoops.ClientFactory)} or its form with a {@link
 * GrpcEventLoops.ClientCloser}, the thread's interrupt is held back while the client closes and
 * restored before its lease is released: such a client library can give up its own close at once on
 * an interrupted thread and skip the rest of its cleanup (Bigtable's data client does), and Flink's
 * {@code shutdownNow()} of a coordinator's executor interrupts a worker still building a client
 * after the owner closed. A gax client's bounded shutdown answers an interrupt with {@code
 * shutdownNow()}, which skips none of its cleanup, so its interrupt is not held back. An interrupt
 * that ended a call's wait is set again before the client closes, and so reaches its close the same
 * way. An interrupt delivered during a close still reaches it. A thread that finds another one
 * closing the same client waits for that close, and an interrupt ends that wait.
 *
 * <p>This object's monitor is held to prepare a session's client, to publish the client and to
 * start each of its calls, never while a client is built or a call runs, unless starting the call
 * itself blocks, as a client library that runs its own callbacks on the starting thread can: a
 * {@code close()} meanwhile waits for that start. Building a client can make calls of its own, so
 * {@link #close()} does not wait for one: it marks this object closed, and a client that finishes
 * building after that is closed by the calling thread without a call being started on it. A call a
 * session starts after {@code close()} is refused. A client published before {@code close()} runs
 * is closed by whichever thread gets to it first, and the other waits for that close. Preparing
 * what the client is built from under the monitor means nothing is prepared after {@link #close()},
 * so an owner may clear its credentials once this object is closed. The flag {@code close()} sets
 * is one-way: an owner is minted per enumerator, or per full lookup cache load, and ends with it
 * ({@code docs/adr/0128}).
 *
 * <p>A session that succeeds returns its result even if its client then fails to close with an
 * exception; the failure is logged, since it says nothing about the calls, and the lease is
 * released either way. An {@link Error} from that close is thrown by whichever thread closed the
 * client: in place of the result when the calling thread did, from {@link #close()} when it did. A
 * session that fails carries a close failure as a suppressed exception, save for a JVM-fatal one,
 * which takes its place.
 *
 * <p>One runner runs one session at a time, and a session one call at a time: it tracks a single
 * client and a single call in flight, which is what {@link #close()} closes and cancels. Its owners
 * make their calls one after another, from a source coordinator's worker thread, which Flink runs
 * as a single thread by default ({@code SourceOperatorFactory}, Flink 1.20.5 and 2.2.1), or from a
 * full lookup cache load. A session that opens while another is in flight is refused with an {@link
 * IllegalStateException}, and its client closed unused, so that it cannot take the place of the one
 * {@code close()} must close; so is a call a session starts while another of its calls is in
 * flight.
 */
@Internal
public final class PerCallClientRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PerCallClientRunner.class);

    /** How the owner is named in this object's failures, such as "Bigtable row key sampler". */
    private final String owner;

    @GuardedBy("this")
    @Nullable
    private GrpcEventLoops.LeasedClient<?> inFlight;

    @GuardedBy("this")
    @Nullable
    private ApiFuture<?> inFlightCall;

    @GuardedBy("this")
    private boolean closed;

    /**
     * Creates the runner.
     *
     * @param owner how the owner is named in this runner's failures and log messages, such as
     *     "Bigtable row key sampler"
     */
    public PerCallClientRunner(String owner) {
        this.owner = Preconditions.checkNotNull(owner, "owner must not be null");
    }

    /**
     * Opens a client, runs one call on it and closes it once the call has returned or failed, for
     * an owner whose client needs nothing prepared under this object's monitor.
     *
     * @param opener builds the client, on the event-loop group
     * @param start starts the call on the client
     * @param <C> the client type
     * @param <T> the call's result type
     * @return the call's result
     * @throws IOException as {@link #call(Supplier, SourcedClientOpener, Function)} does
     * @see #call(Supplier, SourcedClientOpener, Function)
     */
    public <C, T> T call(ClientOpener<C> opener, Function<? super C, ApiFuture<T>> start)
            throws IOException {
        Preconditions.checkNotNull(opener, "opener must not be null");
        return call(() -> opener, ClientOpener::open, start);
    }

    /**
     * Opens a client, runs one call on it and closes it once the call has returned or failed.
     *
     * @param prepare prepares what the client is built from, such as its settings builder; runs
     *     under this object's monitor, so it must not block
     * @param opener builds the client from it, on the event-loop group
     * @param start starts the call on the client
     * @param <S> what the client is built from
     * @param <C> the client type
     * @param <T> the call's result type
     * @return the call's result
     * @throws IOException as {@link #session(Supplier, SourcedClientOpener, SessionBody)} does
     */
    public <S, C, T> T call(
            Supplier<S> prepare,
            SourcedClientOpener<S, C> opener,
            Function<? super C, ApiFuture<T>> start)
            throws IOException {
        Preconditions.checkNotNull(start, "start must not be null");
        return session(prepare, opener, session -> session.call(start));
    }

    /**
     * Opens a client, runs an owner's calls on it one after another and closes it once the body has
     * returned or failed.
     *
     * <p>The body runs on the calling thread and starts each call through {@link
     * ClientSession#call(Function)}, which waits for it; work between the calls, such as building
     * the next request from the last answer, runs there too. A failure the body catches, such as a
     * call the service refused, leaves the session open for its next call; one it throws ends the
     * session. A call whose wait is interrupted throws an {@link InterruptedIOException} with the
     * interrupt consumed; whether the body rethrows, wraps or catches it, the session sets the
     * interrupt again before the client closes and leaves it set when it returns or throws.
     *
     * @param prepare prepares what the client is built from, such as its settings builder; runs
     *     under this object's monitor, so it must not block
     * @param opener builds the client from it, on the event-loop group
     * @param body makes the calls, each through the session it is given
     * @param <S> what the client is built from
     * @param <C> the client type
     * @param <T> the body's result type
     * @return the body's result
     * @throws IOException if this runner is closed before the session opens, or before or during a
     *     call the body then makes, the client cannot be opened, a wait is interrupted ({@link
     *     InterruptedIOException}, with the interrupt restored before the client's lease is
     *     released), or the body throws one, such as for a call that failed with a checked
     *     exception; a call that failed with an unchecked exception, and that the body does not
     *     catch, throws it as itself, so a caller's {@code catch} of a gax {@code ApiException}
     *     subtype still matches
     */
    public <S, C, T> T session(
            Supplier<S> prepare, SourcedClientOpener<S, C> opener, SessionBody<C, T> body)
            throws IOException {
        Preconditions.checkNotNull(prepare, "prepare must not be null");
        Preconditions.checkNotNull(opener, "opener must not be null");
        Preconditions.checkNotNull(body, "body must not be null");
        S source;
        synchronized (this) {
            if (closed) {
                throw closedFailure();
            }
            source = prepare.get();
        }
        GrpcEventLoops.LeasedClient<C> opened = opener.open(source);
        IllegalStateException busy = null;
        boolean refused;
        synchronized (this) {
            refused = closed;
            if (!refused && inFlight != null) {
                // Left alone: it is the client close() must find, with the call it must cancel.
                busy =
                        new IllegalStateException(
                                "The " + owner + " runs one call at a time, and one is in flight.");
            } else if (!refused) {
                // Published before any call starts, so that close() always finds the client, and
                // through it the call in flight once there is one.
                inFlight = opened;
            }
        }
        // Closed before it is forgotten, so a close() racing this finds the client still in
        // flight and waits for the same close rather than returning while it runs.
        try {
            if (refused) {
                // Built after close(), which could not see it: closed here, and never used.
                IOException closedFirst = closedFailure();
                closeAfterFailure(opened, closedFirst);
                throw closedFirst;
            }
            if (busy != null) {
                closeAfterFailure(opened, busy);
                throw busy;
            }
            ClientSession<C> session = new ClientSession<>(opened);
            T result;
            try {
                result = body.run(session);
            } catch (IOException | RuntimeException | Error e) {
                closeRestoringConsumedInterrupt(session, () -> closeAfterFailure(opened, e));
                throw e;
            }
            closeRestoringConsumedInterrupt(session, () -> closeAfterSuccess(opened));
            return result;
        } finally {
            synchronized (this) {
                if (inFlight == opened) {
                    inFlight = null;
                    inFlightCall = null;
                }
            }
        }
    }

    /**
     * Closes a session's client. If one of its waits consumed the thread's interrupt, the interrupt
     * is set again before the client closes, whatever the body did with the {@link
     * InterruptedIOException}, and set again afterwards: it reaches that close as it would have (a
     * gax client's bounded shutdown answers it at once, and a client that closes with its own
     * close() has it held back and restored before the lease is released), a release inside
     * interruptibleReleases() sees it, and if another thread is closing the client, it ends this
     * thread's wait for that close. A failure of the body's own that is an {@link
     * InterruptedIOException}, such as a socket timeout, sets nothing.
     */
    private static void closeRestoringConsumedInterrupt(ClientSession<?> session, Runnable close) {
        if (!session.interruptConsumed) {
            close.run();
            return;
        }
        Thread.currentThread().interrupt();
        try {
            close.run();
        } finally {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts a session's call under this object's monitor and waits for it.
     *
     * @throws IOException if this runner was closed before the call could start, or as {@link
     *     #await(ApiFuture)} does
     */
    private <C, R> R startAndAwait(
            ClientSession<C> session, Function<? super C, ApiFuture<R>> start) throws IOException {
        Preconditions.checkNotNull(start, "start must not be null");
        GrpcEventLoops.LeasedClient<C> client = session.client;
        ApiFuture<R> future;
        synchronized (this) {
            if (closed) {
                // close() has taken the client over, or will close it as it finds it.
                throw closedFailure();
            }
            Preconditions.checkState(
                    inFlight == client, "The %s's session is no longer open.", owner);
            if (inFlightCall != null) {
                throw new IllegalStateException(
                        "The " + owner + " runs one call at a time, and one is in flight.");
            }
            // Started under the monitor, so that close() finds the call as soon as it exists.
            future = start.apply(client.client());
            inFlightCall = future;
        }
        try {
            return await(future);
        } catch (InterruptedIOException e) {
            // await() consumed the interrupt; the session restores it before the client closes.
            session.interruptConsumed = true;
            throw e;
        } finally {
            synchronized (this) {
                if (inFlightCall == future) {
                    inFlightCall = null;
                }
            }
        }
    }

    /**
     * Closes the client of a call that failed, attaching a failure to close it to the call's
     * failure, save for a JVM-fatal one, which {@link Closers} lets through.
     */
    private static void closeAfterFailure(
            GrpcEventLoops.LeasedClient<?> client, Throwable failure) {
        Closers.closeAllSuppressing(failure, client::closeWithInterruptHeldBack);
    }

    /** Closes the client of a call that succeeded, logging a failure to close it. */
    private void closeAfterSuccess(GrpcEventLoops.LeasedClient<?> client) {
        try {
            client.closeWithInterruptHeldBack();
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to close the client of a {} call that succeeded; its lease is released.",
                    owner,
                    e);
        }
    }

    private IOException closedFailure() {
        return new IOException("The " + owner + " was closed; the job is shutting down.");
    }

    /**
     * Waits for the call, interruptibly, cancelling it on an interrupt. A runtime failure is thrown
     * as itself, so a caller's {@code catch} of a gax {@code ApiException} subtype still matches.
     */
    private <T> T await(ApiFuture<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            // The interrupt is restored by the caller, once the client has closed.
            future.cancel(true);
            throw new InterruptedIOException(
                    "Interrupted while waiting for a "
                            + owner
                            + " call; the job is shutting down.");
        } catch (CancellationException e) {
            // Only close() cancels a call, so this is the job shutting down, not a failure of it.
            throw new IOException("The " + owner + " was closed while a call was in flight.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // The failure may have been raised on a gax thread; this records where it was
                // waited for, as the SDK's own synchronous call does.
                cause.addSuppressed(new Exception("The " + owner + " call was awaited here"));
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        }
    }

    /**
     * Refuses later calls, cancels the call in flight if there is one, and closes its client as
     * {@link GrpcEventLoops.LeasedClient#closeWithInterruptHeldBack()} does (holding this thread's
     * interrupt back only for a client opened with {@link
     * GrpcEventLoops#openAutoCloseable(GrpcEventLoops.ClientFactory)} or its form with a {@link
     * GrpcEventLoops.ClientCloser}), or waits for that close if the calling thread got there first;
     * an interrupt ends that wait. A client still being built is not waited for; the thread
     * building it closes it.
     *
     * @throws RuntimeException if this call closed the client and that close failed with an
     *     exception; an {@link Error} from that close is thrown as itself
     */
    public void close() {
        GrpcEventLoops.LeasedClient<?> open;
        ApiFuture<?> future;
        synchronized (this) {
            closed = true;
            open = inFlight;
            future = inFlightCall;
            inFlight = null;
            inFlightCall = null;
        }
        if (future != null) {
            // Cancels the call and any retry gax has scheduled for it, so the client below has
            // nothing in flight and the waiting call() returns at once; one that lands while gax
            // is between two attempts can miss it, and the wait then ends on an interrupt (see the
            // class Javadoc).
            future.cancel(true);
        }
        if (open != null) {
            open.closeWithInterruptHeldBack();
        }
    }

    /**
     * The calls of one session, on the client opened for it. A session's calls go only through
     * this, from the thread running the body, and it is not used once the body has returned.
     *
     * @param <C> the client type
     */
    public final class ClientSession<C> {

        private final GrpcEventLoops.LeasedClient<C> client;

        /** Whether one of this session's waits consumed the thread's interrupt. */
        private volatile boolean interruptConsumed;

        private ClientSession(GrpcEventLoops.LeasedClient<C> client) {
            this.client = client;
        }

        /**
         * Starts a call on the session's client and waits for it, interruptibly; {@link
         * PerCallClientRunner#close()} cancels it while it waits.
         *
         * @param start starts the call on the client
         * @param <R> the call's result type
         * @return the call's result
         * @throws IOException if the runner was closed before the call started or while it ran, the
         *     wait is interrupted ({@link InterruptedIOException}, with the interrupt consumed; the
         *     session restores it), or the call failed with a checked exception; a call that failed
         *     with an unchecked exception throws it as itself
         * @throws IllegalStateException if another of the session's calls is in flight, or the
         *     session's body has already returned
         */
        public <R> R call(Function<? super C, ApiFuture<R>> start) throws IOException {
            return startAndAwait(this, start);
        }

        /**
         * Returns the session's client, for work on it that makes no call, such as building a
         * Firestore query, which is bound to the client it is built from, for a later {@link
         * #call(Function)} to send. Every call still goes through {@code call}: {@link
         * PerCallClientRunner#close()} cancels only a call started there.
         *
         * @return the client, which the session closes when its body returns
         */
        public C client() {
            return client.client();
        }
    }

    /**
     * Makes a session's calls.
     *
     * @param <C> the client type
     * @param <T> the result type
     */
    @FunctionalInterface
    public interface SessionBody<C, T> {

        /**
         * Makes the calls, each through {@code session}.
         *
         * @param session the session's calls
         * @return the result
         * @throws IOException if a call or the work between them fails
         */
        T run(ClientSession<C> session) throws IOException;
    }

    /** Builds the client one session runs on, from what it prepared under the monitor. */
    @FunctionalInterface
    public interface SourcedClientOpener<S, C> {

        /**
         * Builds the client.
         *
         * @param source what {@code prepare} returned, such as the client's settings builder
         * @return the client, on the connector's event-loop group
         * @throws IOException if the client cannot be created
         */
        GrpcEventLoops.LeasedClient<C> open(S source) throws IOException;
    }

    /** Builds the client one session runs on, from nothing prepared under the runner's monitor. */
    @FunctionalInterface
    public interface ClientOpener<C> {

        /**
         * Builds the client.
         *
         * @return the client, on the connector's event-loop group
         * @throws IOException if the client cannot be created
         */
        GrpcEventLoops.LeasedClient<C> open() throws IOException;
    }
}
