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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.LazyFirestoreClient;
import io.github.flink.gcp.connector.firestore.source.batch.ReadTimeQueries;

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
 * Reads pages through a {@code google-cloud-firestore} client, at the split's read time.
 *
 * <p>A read at a read time answers with the whole result rather than a stream, which is why every
 * page is bounded by a {@code limit} ({@link ReadTimeQueries}).
 *
 * <p>{@link #close()} ends every page still in flight before it closes the client: a fetcher can
 * outlive the reader's close timeout, which Flink does not interrupt, and the client closes with
 * {@code shutdownNow()} (ADR-0208), which drops a retry gax has scheduled on the client's executor
 * without ever notifying the query's stream observer (gax 2.91.0), so the read's future would never
 * complete and a fetcher waiting on it would wait for good. A page therefore waits on a future
 * derived from the read's ({@link ApiFutures#transform(ApiFuture, com.google.api.core.ApiFunction,
 * java.util.concurrent.Executor)}), which a cancellation completes at once and passes on to the
 * read's future; the read's stream itself ends only when the client closes. A page asked for after
 * the close is refused.
 */
@Internal
public final class ClientQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    private final LazyFirestoreClient client;
    private final PageRead pageRead;

    @GuardedBy("this")
    @Nullable
    private transient Set<ApiFuture<?>> inFlight;

    @GuardedBy("this")
    private transient boolean closed;

    /**
     * Creates the reader.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPageReader(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyFirestoreClient("page reader", emulatorEndpoint);
        // A named class, not a lambda: the reader travels in the job graph (ADR-0125).
        this.pageRead = new ReadAtReadTime();
    }

    /** Creates a reader whose pages {@code pageRead} starts, for tests of its waits and close. */
    @VisibleForTesting
    ClientQueryPageReader(PageRead pageRead) {
        this.client = new LazyFirestoreClient("page reader", null);
        this.pageRead = pageRead;
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
    }

    @Override
    public Query query(DatabaseDestination database, RunQueryRequest request) throws IOException {
        try {
            return Query.fromProto(client.get(database), request);
        } catch (IllegalArgumentException e) {
            throw new IOException("Cannot read a Firestore query of " + database + ".", e);
        }
    }

    @Override
    public List<? extends DocumentSnapshot> read(Query page, Timestamp readTime)
            throws IOException {
        ApiFuture<List<QueryDocumentSnapshot>> documents;
        synchronized (this) {
            if (closed) {
                throw new IOException("The Firestore page reader was closed before use.");
            }
            // Started under the monitor, so that close() finds the page as soon as it exists.
            documents =
                    ApiFutures.transform(
                            pageRead.start(page, readTime),
                            QuerySnapshot::getDocuments,
                            Runnable::run);
            if (inFlight == null) {
                inFlight = new HashSet<>();
            }
            inFlight.add(documents);
        }
        try {
            return documents.get();
        } catch (InterruptedException e) {
            documents.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(
                    "Interrupted while reading a page of Firestore documents.");
        } catch (CancellationException e) {
            throw new IOException(
                    "The Firestore page reader was closed while a page was being read.", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to read a page of Firestore documents.", e.getCause());
        } finally {
            synchronized (this) {
                inFlight.remove(documents);
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
        List<ApiFuture<?>> toCancel;
        synchronized (this) {
            closed = true;
            toCancel = inFlight == null ? List.of() : new ArrayList<>(inFlight);
        }
        for (ApiFuture<?> page : toCancel) {
            page.cancel(true);
        }
        client.close();
    }

    /** Starts the read of a page at a read time. */
    @VisibleForTesting
    @FunctionalInterface
    interface PageRead extends Serializable {

        /**
         * Starts the read.
         *
         * @param page the page's query
         * @param readTime the split's read time
         * @return the read's result, once it completes
         */
        ApiFuture<QuerySnapshot> start(Query page, Timestamp readTime);
    }

    /** Reads a page through {@link ReadTimeQueries#start(Query, Timestamp)}. */
    private static final class ReadAtReadTime implements PageRead {

        private static final long serialVersionUID = 1L;

        @Override
        public ApiFuture<QuerySnapshot> start(Query page, Timestamp readTime) {
            return ReadTimeQueries.start(page, readTime);
        }
    }
}
