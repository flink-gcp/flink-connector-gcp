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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Reads partitions through a {@code google-cloud-spanner} {@link BatchClient}.
 *
 * <p>Named after the client its {@link #close()} releases, which is the connector's convention for
 * the real implementation of a seam.
 *
 * <p>One service handle per reader, built on first use and shared by every partition that reader is
 * assigned. Rejoining a snapshot costs no round trip — the client builds the transaction handle
 * from the id alone — so a transaction is opened per partition rather than cached, which is what
 * keeps a partition's teardown from touching another's.
 */
@Internal
public final class BatchClientStructStreamOpener implements StructStreamOpener {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;

    /**
     * The client, built on first use.
     *
     * <p>Transient because this opener is serialized into the job graph. Built under the monitor
     * because a reader's split fetchers open reads from their own threads while {@link #close()}
     * runs on the task thread once those fetchers are down.
     */
    @Nullable private transient volatile Spanner spanner;

    @Nullable private transient GoogleCredentials credentialsOverride;

    private transient volatile boolean closed;

    /**
     * Creates the opener.
     *
     * @param database the database to read
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for the real service
     */
    public BatchClientStructStreamOpener(
            SpannerDatabase database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(database, emulatorEndpoint, null);
    }

    /** Creates the opener with an optional runtime-loaded service-account key path. */
    public BatchClientStructStreamOpener(
            SpannerDatabase database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.database = database;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    @Override
    public StructStream open(BatchTransactionId batchTransactionId, Partition partition)
            throws IOException {
        BatchClient batchClient =
                client().getBatchClient(
                                DatabaseId.of(
                                        database.getProject(),
                                        database.getInstance(),
                                        database.getDatabase()));
        BatchReadOnlyTransaction transaction =
                batchClient.batchReadOnlyTransaction(batchTransactionId);
        try {
            return new ResultSetStructStream(transaction, transaction.execute(partition));
        } catch (Throwable e) {
            // The handle is this method's until the stream takes it over; nothing else would ever
            // close it. Throwable rather than Exception for the reason every creation guard in
            // this project uses it: a first classload can fail with an Error.
            Closers.closeAllSuppressing(e, transaction);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        Spanner toClose;
        synchronized (this) {
            closed = true;
            toClose = spanner;
            spanner = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    /**
     * Returns the client, building it on first use.
     *
     * <p>The monitor is this object rather than a lock field, because a lock field would have to
     * travel in the job graph.
     */
    private Spanner client() throws IOException {
        Spanner existing = spanner;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The Spanner partition reader for "
                                + database
                                + " was closed before it was used.");
            }
            if (spanner == null) {
                spanner = SpannerClients.open(database, settings());
            }
            return spanner;
        }
    }

    /** Loads credentials when a TaskManager creates the source reader. */
    public void loadCredentials() throws IOException {
        credentials();
    }

    /** Builds settings exposed for verifying that the reader injects runtime credentials. */
    @VisibleForTesting
    SpannerOptions settings() throws IOException {
        return SpannerClients.settings(database, emulatorEndpoint, credentials());
    }

    @Nullable
    private GoogleCredentials credentials() throws IOException {
        if (credentialsOverride == null && serviceAccountKeyFile != null) {
            credentialsOverride = SpannerCredentials.load(serviceAccountKeyFile);
        }
        return credentialsOverride;
    }

    /**
     * One partition's rows, over the client's {@code ResultSet}.
     *
     * <p>The transaction handle is closed with the stream, and {@code cleanup()} is deliberately
     * not called: it would release the session the whole batch read shares.
     */
    private static final class ResultSetStructStream implements StructStream {

        private final BatchReadOnlyTransaction transaction;
        private final ResultSet resultSet;

        private ResultSetStructStream(BatchReadOnlyTransaction transaction, ResultSet resultSet) {
            this.transaction = transaction;
            this.resultSet = resultSet;
        }

        @Override
        public Struct next() {
            return resultSet.next() ? resultSet.getCurrentRowAsStruct() : null;
        }

        @Override
        public void close() {
            // Both closes are unchecked, so nothing here can throw a checked exception the
            // interface does not declare. The result set first: closing it is what cancels a read
            // that was not consumed to its end.
            try (BatchReadOnlyTransaction toClose = transaction) {
                resultSet.close();
            }
        }
    }
}
