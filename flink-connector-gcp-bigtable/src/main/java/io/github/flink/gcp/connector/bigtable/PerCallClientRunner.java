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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import io.github.flink.gcp.connector.base.lifecycle.Closers;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Runs a JobManager-side owner's Bigtable calls, each on a client opened for it and closed when it
 * returns, and cancels the one in flight when the owner closes.
 *
 * <p>A client a JobManager-side component holds open must not outlive the call it was opened for
 * (ADR-0208): at the end of a job Flink closes an enumerator on a thread it does not wait for, and
 * in practice after it has released the job's class loader (Flink 2.2.1, measured in #2237), so a
 * client still open then can tear its event-loop group down on a closed loader, and under {@code
 * cluster.uncaught-exception-handling: FAIL} the JobManager exits. So every {@link #call(Supplier,
 * ClientOpener, Function)} opens its client, starts the call, waits for it and closes the client,
 * and {@link #close()} cancels a call still in flight through its {@link ApiFuture} before closing
 * that call's client.
 *
 * <p>The cancellation goes through the future, retries included, and never through the client's
 * shutdown: gax schedules a retry on the client's executor, a shutdown drops it without completing
 * the call's future, and the SDK's synchronous methods wait on that future uninterruptibly (gax
 * 2.90.0), which would strand the coordinator's worker thread. For the same reason the wait here is
 * an interruptible {@code get()} rather than a synchronous SDK method, and an interrupt cancels the
 * call. The interrupt is restored only after the call's client has closed: the client's channels
 * close through gax's {@code GrpcTransportChannel.close()}, whose wait for termination gives up at
 * once on an interrupted thread, and the client library then skips the rest of its cleanup, its
 * background executor included.
 *
 * <p>This object's monitor is held to prepare a call's settings and to start the call, never while
 * a client is built or a call runs. Building a production data client makes calls of its own (each
 * pooled channel's priming and the DirectPath check, each bounded at a minute, and the session
 * configuration, 5 s for each of its attempts; google-cloud-bigtable 2.88.0), so {@link #close()}
 * does not wait for one: it marks this object closed, and a client that finishes building after
 * that is closed by the calling thread without its call being started. Preparing the settings under
 * the monitor means none are built after {@link #close()}, so an owner may clear its credentials
 * once this object is closed. The flag {@code close()} sets is one-way: the object belongs to one
 * row-key sampler or Change Streams coordinator client, each minted per enumerator, and ends with
 * it ({@code docs/adr/0128}).
 */
@Internal
public final class PerCallClientRunner {

    /** How the owner is named in the closed-before-use failure, such as "row key sampler". */
    private final String owner;

    @GuardedBy("this")
    @Nullable
    private LeasedBigtableClient<?> inFlight;

    @GuardedBy("this")
    @Nullable
    private ApiFuture<?> inFlightCall;

    @GuardedBy("this")
    private boolean closed;

    /**
     * Creates the runner.
     *
     * @param owner how the owner is named in the failure a call after {@link #close()} raises
     */
    public PerCallClientRunner(String owner) {
        this.owner = Preconditions.checkNotNull(owner, "owner must not be null");
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
     * @throws IOException if this runner is closed, before the call starts or while it runs, the
     *     client cannot be opened, the wait is interrupted ({@link InterruptedIOException}, with
     *     the interrupt restored once the client has closed), or the call failed with a checked
     *     exception
     */
    public <S, C extends AutoCloseable, T> T call(
            Supplier<S> prepare, ClientOpener<S, C> opener, Function<? super C, ApiFuture<T>> start)
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
        LeasedBigtableClient<C> opened = opener.open(source);
        ApiFuture<T> future = null;
        Throwable startFailure = null;
        synchronized (this) {
            // Started under the monitor, so that close() always finds the call it cancels.
            if (!closed) {
                try {
                    future = start.apply(opened.client());
                    inFlight = opened;
                    inFlightCall = future;
                } catch (RuntimeException | Error e) {
                    startFailure = e;
                }
            }
        }
        if (future == null) {
            // Refused because close() came first, or the call could not start: either way the
            // client built for it is closed here, outside the monitor, and the failure is thrown
            // as itself.
            if (startFailure == null) {
                IOException refused = closedFailure();
                closeUninterrupted(opened, refused);
                throw refused;
            }
            closeUninterrupted(opened, startFailure);
            if (startFailure instanceof Error) {
                throw (Error) startFailure;
            }
            throw (RuntimeException) startFailure;
        }
        // Closed before the call is forgotten, so a close() racing this finds it still in flight
        // and waits for the same close rather than returning while it runs.
        try {
            T result;
            try {
                result = await(future);
            } catch (InterruptedIOException e) {
                // The wait consumed the interrupt; it comes back once the client has closed.
                try {
                    closeUninterrupted(opened, e);
                } finally {
                    Thread.currentThread().interrupt();
                }
                throw e;
            } catch (IOException | RuntimeException | Error e) {
                closeUninterrupted(opened, e);
                throw e;
            }
            closeUninterrupted(opened, null);
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
     * Closes a client with nothing in flight on it, with this thread's interrupt held back for the
     * duration. The client's channels close through gax's {@code GrpcTransportChannel.close()},
     * whose wait for termination gives up at once on an interrupted thread, and the client library
     * then skips the rest of its cleanup, its background executor included; an interrupt that is
     * already set, such as the one Flink's {@code shutdownNow()} of the coordinator's executor
     * delivers to a worker still building a client after the owner closed, is restored afterwards.
     * One that arrives during the close still cuts it short.
     *
     * @param failure the failure being thrown, which a failure closing the client is suppressed
     *     onto, save for a JVM-fatal one; or {@code null} after a call that succeeded
     */
    private static void closeUninterrupted(
            LeasedBigtableClient<?> client, @Nullable Throwable failure) {
        boolean interrupted = Thread.interrupted();
        try {
            if (failure == null) {
                client.close();
            } else {
                Closers.closeAllSuppressing(failure, client);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private IOException closedFailure() {
        return new IOException("The Bigtable " + owner + " was closed; the job is shutting down.");
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
                    "Interrupted while waiting for the Bigtable "
                            + owner
                            + "; the job is shutting down.");
        } catch (CancellationException e) {
            // Only close() cancels a call, so this is the job shutting down, not a failure of it.
            throw new IOException(
                    "The Bigtable " + owner + " was closed while a call was in flight.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // The failure may have been raised on a gax thread; this records where it was
                // waited for, as the SDK's own synchronous call does.
                cause.addSuppressed(new Exception("The Bigtable call was awaited here"));
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        }
    }

    /**
     * Refuses later calls, cancels the call in flight if there is one, and closes its client with
     * this thread's interrupt held back, waiting for that close if the calling thread got there
     * first. A client still being built is not waited for; the thread building it closes it.
     */
    public void close() {
        LeasedBigtableClient<?> open;
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
            closeUninterrupted(open, null);
        }
    }

    /** Builds the client one call runs on. */
    @FunctionalInterface
    public interface ClientOpener<S, C extends AutoCloseable> {

        /**
         * Builds the client.
         *
         * @param source what {@code prepare} returned, such as the client's settings builder
         * @return the client, on the connector's event-loop group
         * @throws IOException if the client cannot be created
         */
        LeasedBigtableClient<C> open(S source) throws IOException;
    }
}
