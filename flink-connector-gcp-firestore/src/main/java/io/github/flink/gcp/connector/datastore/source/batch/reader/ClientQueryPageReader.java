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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.Credentials;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.LazyDatastoreClient;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Reads pages through the generated Datastore client, on the connector's gRPC event-loop group.
 *
 * <p>A page keeps the retry settings the library's {@code Datastore} applied, so a page that meets
 * a transient failure is retried with the same request, which reads the same entities at a fixed
 * read time. The read returns when the call succeeds, fails, or the retry settings' total timeout
 * runs out; an interrupt cancels it.
 *
 * <p>{@link #close()} ends every page still in flight before it closes the client: a fetcher can
 * outlive the reader's close timeout, which Flink does not interrupt, and the client's bounded
 * close ends with {@code shutdownNow()}, which drops a retry gax has scheduled on the client's
 * executor without completing the call's future (gax 2.91.0), so a fetcher waiting on that future
 * would wait for good. A page therefore waits on a future derived from the call's ({@link
 * ApiFutures#transform(ApiFuture, com.google.api.core.ApiFunction,
 * java.util.concurrent.Executor)}), which a cancellation completes at once and passes on to the
 * call; the call stops unless gax is between two attempts, when a cancellation can miss it ({@code
 * CallbackChainRetryingFuture}), but the page's wait ends either way. A page asked for after the
 * close is refused.
 */
@Internal
public final class ClientQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    private final LazyDatastoreClient client;
    private final PageCall pageCall;

    @GuardedBy("this")
    @Nullable
    private transient Set<ApiFuture<QueryResultBatch>> inFlight;

    @GuardedBy("this")
    private transient boolean closed;

    /**
     * Creates the reader.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPageReader(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyDatastoreClient("page reader", emulatorEndpoint);
        // A named class, not a lambda: the reader travels in the job graph (ADR-0125).
        this.pageCall = new GeneratedClientCall(client);
    }

    /** Creates a reader whose pages {@code pageCall} starts, for tests of its waits and close. */
    @VisibleForTesting
    ClientQueryPageReader(PageCall pageCall) {
        this.client = new LazyDatastoreClient("page reader", null);
        this.pageCall = pageCall;
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
    }

    @Override
    public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page)
            throws IOException {
        ApiFuture<QueryResultBatch> batch;
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The Datastore page reader for " + database + " was closed before use.");
            }
            // Started under the monitor, so that close() finds the page as soon as it exists.
            batch =
                    ApiFutures.transform(
                            pageCall.start(database, page),
                            RunQueryResponse::getBatch,
                            Runnable::run);
            if (inFlight == null) {
                inFlight = new HashSet<>();
            }
            inFlight.add(batch);
        }
        try {
            return batch.get();
        } catch (InterruptedException e) {
            batch.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(
                    "Interrupted while reading a page of Datastore entities.");
        } catch (CancellationException e) {
            throw new IOException(
                    "The Datastore page reader was closed while a page was being read.", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to read a page of Datastore entities.", e.getCause());
        } finally {
            synchronized (this) {
                inFlight.remove(batch);
            }
        }
    }

    /**
     * Ends the pages in flight, closes the client and refuses later pages.
     *
     * @throws IOException if the client fails to close
     */
    @Override
    public void close() throws IOException {
        List<ApiFuture<QueryResultBatch>> toCancel;
        synchronized (this) {
            closed = true;
            toCancel = inFlight == null ? List.of() : new ArrayList<>(inFlight);
        }
        for (ApiFuture<QueryResultBatch> batch : toCancel) {
            batch.cancel(true);
        }
        client.close();
    }

    /** Starts a page's call through the reader's generated client. */
    private static final class GeneratedClientCall implements PageCall {

        private static final long serialVersionUID = 1L;

        private final LazyDatastoreClient client;

        private GeneratedClientCall(LazyDatastoreClient client) {
            this.client = client;
        }

        @Override
        public ApiFuture<RunQueryResponse> start(DatabaseDestination database, RunQueryRequest page)
                throws IOException {
            return client.client(database).runQueryCallable().futureCall(page);
        }
    }

    /** Starts one page's call. */
    @VisibleForTesting
    @FunctionalInterface
    interface PageCall extends Serializable {

        /**
         * Starts the call.
         *
         * @param database the database the request addresses
         * @param page the request
         * @return the call's future
         * @throws IOException if the client cannot be created
         */
        ApiFuture<RunQueryResponse> start(DatabaseDestination database, RunQueryRequest page)
                throws IOException;
    }
}
