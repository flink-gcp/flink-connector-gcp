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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * A Firestore client built on first use and closed once, held by a seam implementation of the
 * source: the enumerator's planner and the readers' page reader.
 *
 * <p>The Bigtable module's {@code LazyBigtableDataClient} shape. The client is built under this
 * object's monitor, and a close is one-way: a {@link #get(DatabaseDestination)} that arrives after
 * it — a planning call still running on the enumerator's worker thread when the coordinator closes
 * the enumerator, or a fetcher that outlived the reader's close timeout — is refused rather than
 * building a client nothing would close.
 *
 * <p>The client and the credentials are {@code transient}: the page reader holding this travels in
 * the job graph, and each runtime component pushes in the credentials it loaded.
 */
@Internal
public final class LazyFirestoreClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String owner;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    @Nullable private transient volatile Firestore client;
    @Nullable private transient volatile Credentials credentials;
    private transient volatile boolean closed;

    /**
     * Creates the holder.
     *
     * @param owner how the holder names its seam in the closed-before-use failure
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public LazyFirestoreClient(String owner, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.owner = Preconditions.checkNotNull(owner, "owner must not be null");
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /**
     * Returns the client, building it on first use.
     *
     * @param database the database the client reaches
     * @return the client, which this holder owns
     * @throws IOException if the holder was closed, or the client cannot be created
     */
    public Firestore get(DatabaseDestination database) throws IOException {
        Firestore existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The Firestore " + owner + " for " + database + " was closed before use.");
            }
            if (client == null) {
                client =
                        FirestoreClients.open(
                                database,
                                FirestoreClients.settings(
                                        database, emulatorEndpoint, credentials, null));
            }
            return client;
        }
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
                !closed, "The Firestore %s was closed before credentials were supplied.", owner);
        this.credentials = credentials;
    }

    /**
     * Closes the client if one was built, and refuses later use.
     *
     * @throws IOException if the client fails to close
     */
    public void close() throws IOException {
        Firestore toClose;
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
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close the Firestore " + owner + "'s client.", e);
        }
    }
}
