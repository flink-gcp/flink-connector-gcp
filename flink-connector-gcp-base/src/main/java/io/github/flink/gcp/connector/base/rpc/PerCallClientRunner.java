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
 * Runs an owner's calls, each on a client opened for it and closed when it returns, and cancels the
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
 * <p>The cancellation goes through the future, retries included, and never through the client's
 * shutdown: gax schedules a retry on the client's executor, a shutdown drops it without completing
 * the call's future, and the SDKs' synchronous methods wait on that future uninterruptibly (gax
 * 2.91.0), which would strand the waiting thread. For the same reason the wait here is an
 * interruptible {@code get()} rather than a synchronous SDK method, and an interrupt cancels the
 * call. A cancellation that lands while gax is starting a retry can miss it; the wait then ends
 * when the thread is interrupted, as Flink's executor shutdown does to a coordinator's worker.
 *
 * <p>Every client this object closes, from {@code call()} or {@code close()}, is closed with {@link
 * GrpcEventLoops.LeasedClient#closeWithInterruptHeldBack()}. For a client opened with {@link
 * GrpcEventLoops#openAutoCloseable}, the thread's interrupt is held back while the client closes
 * and restored before its lease is released: such a client library can give up its own close at
 * once on an interrupted thread and skip the rest of its cleanup (Bigtable's data client does), and
 * Flink's {@code shutdownNow()} of a coordinator's executor interrupts a worker still building a
 * client after the owner closed. A gax client's bounded shutdown answers an interrupt with {@code
 * shutdownNow()}, which skips none of its cleanup, so its interrupt is not held back. An interrupt
 * that ended a call's wait is set again before the client closes, and so reaches its close the same
 * way. An interrupt delivered during a close still reaches it. A thread that finds another one
 * closing the same client waits for that close, and an interrupt ends that wait.
 *
 * <p>This object's monitor is held to prepare a call's client, and to publish the client and start
 * its call, never while a client is built or a call runs. Building a client can make calls of its
 * own, so {@link #close()} does not wait for one: it marks this object closed, and a client that
 * finishes building after that is closed by the calling thread without its call being started. A
 * client published before {@code close()} runs is closed by whichever thread gets to it first, and
 * the other waits for that close. Preparing what the client is built from under the monitor means
 * nothing is prepared after {@link #close()}, so an owner may clear its credentials once this
 * object is closed. The flag {@code close()} sets is one-way: an owner is minted per enumerator, or
 * per full lookup cache load, and ends with it ({@code docs/adr/0128}).
 *
 * <p>A call that succeeds returns its result even if its client then fails to close with an
 * exception; the failure is logged, since it says nothing about the call, and the lease is released
 * either way. An {@link Error} from that close is thrown by whichever thread closed the client: in
 * place of the result when the calling thread did, from {@link #close()} when it did. A call that
 * fails carries a close failure as a suppressed exception, save for a JVM-fatal one, which takes
 * its place.
 *
 * <p>One runner runs one call at a time: it tracks a single call in flight, which is what {@link
 * #close()} cancels. Its owners make their calls one after another, from a source coordinator's
 * worker thread, which Flink runs as a single thread by default ({@code SourceOperatorFactory},
 * Flink 1.20.5 and 2.2.1), or from a full lookup cache load. A call that starts while another is in
 * flight is refused with an {@link IllegalStateException}, and its client closed unused, so that it
 * cannot take the place of the call {@code close()} must cancel.
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
     * @throws IOException if this runner is closed before the call starts or while it runs, the
     *     client cannot be opened, the wait is interrupted ({@link InterruptedIOException}, with
     *     the interrupt restored before the client's lease is released), or the call failed with a
     *     checked exception; a call that failed with an unchecked exception throws it as itself, so
     *     a caller's {@code catch} of a gax {@code ApiException} subtype still matches
     */
    public <S, C, T> T call(
            Supplier<S> prepare,
            SourcedClientOpener<S, C> opener,
            Function<? super C, ApiFuture<T>> start)
            throws IOException {
        Preconditions.checkNotNull(prepare, "prepare must not be null");
        Preconditions.checkNotNull(opener, "opener must not be null");
        Preconditions.checkNotNull(start, "start must not be null");
        S source;
        synchronized (this) {
            if (closed) {
                throw closedFailure();
            }
            source = prepare.get();
        }
        GrpcEventLoops.LeasedClient<C> opened = opener.open(source);
        ApiFuture<T> future = null;
        Throwable startFailure = null;
        boolean refused;
        synchronized (this) {
            refused = closed;
            if (!refused && inFlight != null) {
                // Left alone: it is the call close() must find and cancel.
                startFailure =
                        new IllegalStateException(
                                "The " + owner + " runs one call at a time, and one is in flight.");
            } else if (!refused) {
                // Published before the call starts, and the call started under the monitor, so
                // that close() always finds the client, and the call it cancels once there is one.
                inFlight = opened;
                try {
                    future = start.apply(opened.client());
                    inFlightCall = future;
                } catch (RuntimeException | Error e) {
                    startFailure = e;
                }
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
            if (startFailure != null) {
                // The call could not start: its client is closed and the failure thrown as itself.
                closeAfterFailure(opened, startFailure);
                if (startFailure instanceof Error) {
                    throw (Error) startFailure;
                }
                throw (RuntimeException) startFailure;
            }
            T result;
            try {
                result = await(future);
            } catch (InterruptedIOException e) {
                // The wait consumed the interrupt. Set again before the client closes, it reaches
                // that close as it would have: a gax client's bounded shutdown answers it at once,
                // and a client that closes with its own close() has it held back and restored
                // before the lease is released. Either way a release inside
                // interruptibleReleases() sees it, and if another thread is closing the client,
                // it ends this thread's wait for that close.
                Thread.currentThread().interrupt();
                try {
                    closeAfterFailure(opened, e);
                } finally {
                    Thread.currentThread().interrupt();
                }
                throw e;
            } catch (IOException | RuntimeException | Error e) {
                closeAfterFailure(opened, e);
                throw e;
            }
            closeAfterSuccess(opened);
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
     * interrupt back only for a client opened with {@link GrpcEventLoops#openAutoCloseable}), or
     * waits for that close if the calling thread got there first; an interrupt ends that wait. A
     * client still being built is not waited for; the thread building it closes it.
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
            // nothing in flight and the waiting call() returns at once.
            future.cancel(true);
        }
        if (open != null) {
            open.closeWithInterruptHeldBack();
        }
    }

    /** Builds the client one call runs on, from what the call prepared under the monitor. */
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

    /** Builds the client one call runs on, from nothing prepared under the runner's monitor. */
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
