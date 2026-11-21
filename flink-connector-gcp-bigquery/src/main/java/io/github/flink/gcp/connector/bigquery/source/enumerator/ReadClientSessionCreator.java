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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.bigquery.source.BigQueryReadClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

/**
 * Creates read sessions through a {@link BigQueryReadClient}.
 *
 * <p>Named after the SDK resource its {@link #close()} releases, as this repository's other client
 * wrappers are. A client is opened for each {@link #create(CreateReadSessionRequest)} and closed
 * when that call returns, rather than held until {@link #close()}, so minting a creator costs
 * nothing — a restore whose checkpoint already records an initialized session opens no client — and
 * no client is left open once the session exists. That last part is what keeps the JobManager alive
 * (#2237): at the end of a job Flink closes the enumerator on a thread it does not wait for before
 * it releases the job's class loader, so a client still open until then can tear its event-loop
 * group down on a closed loader, and under {@code cluster.uncaught-exception-handling: FAIL} the
 * JobManager exits (measured on Flink 2.2.1: 15 of 16 cancelled reading jobs whose client stayed
 * open until the enumerator closed). A session is created when the enumerator plans, which a
 * restore of a checkpoint that records an initialized session skips, so a client per call costs one
 * channel setup per plan.
 *
 * <p>One creator belongs to one enumerator: {@link DefaultReadSessionCreatorFactory} mints it, and
 * the source mints one per {@code createEnumerator} and {@code restoreEnumerator}. That is what
 * makes the one-way {@code closed} flag below correct — it ends this object rather than poisoning
 * one the next enumerator will also be handed ({@code docs/adr/0128}).
 *
 * <p>Creation and release are guarded, and not for tidiness: {@link
 * #create(CreateReadSessionRequest)} runs on a coordinator worker thread while {@link #close()}
 * runs on the thread Flink closes the coordinator on (a dedicated one in Flink 2.2.1, which a
 * coordinator's final close does not wait for), and the two race whenever a job is cancelled during
 * session creation. Unguarded, {@code close} reads a field the worker has not yet written, closes
 * nothing, and the client — a gRPC channel and its executor — is leaked in the JobManager with
 * nothing left to reach it. Guarded, {@code close} cancels the call in flight, retries included,
 * rather than waiting for an answer nothing would read; if the cancellation lands while gax is
 * starting a retry and misses it, the creating thread's wait still ends when Flink interrupts it.
 * The lock is held while the client is built and the call started, never while the call runs. The
 * monitor is this object's own rather than a lock field: one creator belongs to one enumerator, so
 * the only contention is between that enumerator's session creation and its teardown, and a
 * separate lock would name nothing this object does not.
 */
@Internal
public final class ReadClientSessionCreator implements ReadSessionCreator {

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * The client of the {@link #create(CreateReadSessionRequest)} call in flight, with the
     * event-loop lease covering it, or {@code null} between calls.
     */
    @Nullable private GrpcEventLoops.LeasedClient<BigQueryReadClient> inFlight;

    /**
     * The call {@link #create(CreateReadSessionRequest)} is waiting on, which {@link #close()}
     * cancels.
     */
    @Nullable private ApiFuture<ReadSession> inFlightCall;

    private boolean closed;

    /**
     * Creates the session creator.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public ReadClientSessionCreator(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(null, emulatorEndpoint);
    }

    /**
     * Creates the session creator.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public ReadClientSessionCreator(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public ReadSession create(CreateReadSessionRequest request) throws IOException {
        GrpcEventLoops.LeasedClient<BigQueryReadClient> opened;
        ApiFuture<ReadSession> call;
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The BigQuery read session creator was closed; the source is shutting"
                                + " down.");
            }
            opened = BigQueryReadClients.createForSessions(serviceAccountKeyFile, emulatorEndpoint);
            try {
                // Started under the monitor, so that close() always finds the call it cancels.
                call = opened.client().createReadSessionCallable().futureCall(request);
            } catch (RuntimeException | Error e) {
                Closers.closeAllSuppressing(e, opened);
                throw e;
            }
            inFlight = opened;
            inFlightCall = call;
        }
        // Closed before the call is forgotten, so a close() racing this finds it still in flight
        // and waits for the same close rather than returning while it runs, unless interrupted.
        try {
            ReadSession session;
            try {
                session = await(call);
            } catch (IOException | RuntimeException | Error e) {
                // A failure closing the client is attached to the call's rather than put in its
                // place, save for a JVM-fatal one, which Closers lets through.
                Closers.closeAllSuppressing(e, opened);
                throw e;
            }
            opened.close();
            return session;
        } catch (ApiException e) {
            String hint = viewHint(e);
            if (hint == null) {
                throw e;
            }
            throw new IOException(hint, e);
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
     * Waits for the call, interruptibly. The SDK's own synchronous wait is uninterruptible, and a
     * retry gax had scheduled can be dropped by the client's shutdown without completing the call,
     * so a thread waiting that way could wait forever; Flink interrupts the coordinator's worker
     * when it closes the source, and this wait answers that by cancelling the call.
     */
    private static ReadSession await(ApiFuture<ReadSession> call) throws IOException {
        try {
            return call.get();
        } catch (InterruptedException e) {
            call.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(
                    "Interrupted while creating a BigQuery read session; the source is shutting"
                            + " down.");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // The failure may have been raised on a gax thread; this records where it was
                // waited for, as the SDK's own synchronous call does.
                cause.addSuppressed(new Exception("The read session was awaited here"));
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        }
    }

    /**
     * Returns the sentence to put in front of a failure that looks like a read of a view, or {@code
     * null} for any other failure.
     *
     * <p>This connector's rule is to match status codes and never message text, and the rule is not
     * being bent here so much as reaching its edge: {@code INVALID_ARGUMENT} is also what a bad
     * projection, an unparsable row restriction and a snapshot outside the time-travel window
     * answer with, so the code alone identifies nothing. What makes matching the text acceptable is
     * that the result is <em>only</em> a sentence added to a failure that is being thrown either
     * way — never a decision about retrying, dropping or routing, which is what that rule protects.
     * If BigQuery rewords this, the hint stops appearing and the error the user sees is exactly the
     * one they would have seen without it.
     *
     * <p>Measured 2026-08-10: a logical view and a materialized view both answer {@code
     * CreateReadSession} with {@code INVALID_ARGUMENT: request failed: non-table entities cannot be
     * read with the storage API} — the same code and the same words, so one match covers both.
     */
    @Nullable
    @VisibleForTesting
    static String viewHint(ApiException e) {
        if (StatusCodes.codeOf(e) != StatusCode.Code.INVALID_ARGUMENT) {
            return null;
        }
        String message = e.getMessage();
        if (message == null || !message.toLowerCase(Locale.ROOT).contains("non-table entities")) {
            return null;
        }
        return "BigQuery refused to read this as a table. The Storage Read API reads storage, and"
                + " a logical or materialized view has none — if that is what this names, read it"
                + " with query(...) instead of table(...), for example"
                + " query(\"SELECT * FROM `project.dataset.the_view`\").";
    }

    @Override
    public void close() {
        GrpcEventLoops.LeasedClient<BigQueryReadClient> open;
        ApiFuture<ReadSession> call;
        synchronized (this) {
            closed = true;
            open = inFlight;
            call = inFlightCall;
            inFlight = null;
            inFlightCall = null;
        }
        if (call != null) {
            // Cancels the call and any retry gax has scheduled for it, so the client below has
            // nothing in flight to wait for and the waiting create() returns at once.
            call.cancel(true);
        }
        if (open != null) {
            open.close();
        }
    }
}
