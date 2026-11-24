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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.cloud.datastore.v1.DatastoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * A Datastore client built on first use and closed once, held by the readers' page reader.
 *
 * <p>The Firestore root's {@code LazyFirestoreClient} shape. The client is built under this
 * object's monitor, on the connector's gRPC event-loop group ({@link DatastoreClients#open}), and a
 * close is one-way: a {@link #client(DatabaseDestination)} that arrives after it — a fetcher that
 * outlived the reader's close timeout — is refused rather than building a client nothing would
 * close.
 *
 * <p>The client keeps the retry settings the library's {@code Datastore} applied to every call
 * ({@link DatastoreClients#libraryRetrySettings()}): the source sends only queries, which are
 * idempotent at a fixed read time.
 *
 * <p>The client and the credentials are {@code transient}: the page reader holding this travels in
 * the job graph, and each runtime component pushes in the credentials it loaded.
 */
@Internal
public final class LazyDatastoreClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String owner;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    @Nullable private transient volatile GrpcEventLoops.LeasedClient<DatastoreClient> client;
    @Nullable private transient volatile Credentials credentials;
    private transient volatile boolean closed;

    /**
     * Creates the holder.
     *
     * @param owner how the holder names its seam in the closed-before-use failure
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public LazyDatastoreClient(String owner, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.owner = Preconditions.checkNotNull(owner, "owner must not be null");
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /**
     * Returns the client, building it on first use.
     *
     * @param database the database the client reaches
     * @return the client, which this holder owns: closing this holder closes it
     * @throws IOException if the holder was closed, or the client cannot be created
     */
    public DatastoreClient client(DatabaseDestination database) throws IOException {
        GrpcEventLoops.LeasedClient<DatastoreClient> existing = client;
        if (existing == null) {
            synchronized (this) {
                if (closed) {
                    throw new IOException(
                            "The Datastore "
                                    + owner
                                    + " for "
                                    + database
                                    + " was closed before use.");
                }
                if (client == null) {
                    client = DatastoreClients.open(database, emulatorEndpoint, settings());
                }
                existing = client;
            }
        }
        return existing.client();
    }

    /** Returns the settings the client is built from: the library's retry settings. */
    @VisibleForTesting
    DatastoreSettings.Builder settings() {
        return DatastoreClients.settings(
                emulatorEndpoint, credentials, DatastoreClients.libraryRetrySettings());
    }

    /**
     * Takes the credentials the owner loaded, or {@code null} to keep application-default
     * credentials.
     *
     * @param credentials the credentials
     * @throws IllegalStateException if the holder is already closed
     */
    public synchronized void useCredentials(@Nullable Credentials credentials) {
        Preconditions.checkState(
                !closed, "The Datastore %s was closed before credentials were supplied.", owner);
        this.credentials = credentials;
    }

    /**
     * Closes the client if one was built, within {@link GrpcEventLoops.LeasedClient}'s bounds, and
     * refuses later use.
     *
     * @throws IOException if the client fails to close
     */
    public void close() throws IOException {
        GrpcEventLoops.LeasedClient<DatastoreClient> toClose;
        synchronized (this) {
            closed = true;
            toClose = client;
            client = null;
            credentials = null;
        }
        if (toClose == null) {
            return;
        }
        try {
            toClose.close();
        } catch (RuntimeException e) {
            throw new IOException("Failed to close the Datastore " + owner + "'s client.", e);
        }
    }
}
