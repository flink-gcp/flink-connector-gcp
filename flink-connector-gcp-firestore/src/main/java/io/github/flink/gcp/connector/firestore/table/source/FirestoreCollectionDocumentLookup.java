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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.FirestoreClients;
import io.github.flink.gcp.connector.firestore.FirestoreCredentials;
import io.github.flink.gcp.connector.firestore.table.FirestoreConnectorOptions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads documents of one collection through a client owned by one lookup function instance, each
 * projected to the fields of the columns read.
 *
 * <p>{@link #close()} ends every read still in flight before it closes the client: the client
 * closes with {@code shutdownNow()} (ADR-0208), which drops a retry gax has scheduled on the
 * client's executor without ever completing the read's future, so a lookup waiting on it would wait
 * for good. A read answers with a future derived from the library's ({@link
 * ApiFutures#transform(ApiFuture, com.google.api.core.ApiFunction,
 * java.util.concurrent.Executor)}), which a cancellation completes at once whatever the library's
 * own future does with it.
 */
@Internal
final class FirestoreCollectionDocumentLookup implements FirestoreDocumentLookup {
    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final String collection;
    private final String[] fields;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private transient GrpcEventLoops.LeasedClient<Firestore> client;

    @GuardedBy("this")
    @Nullable
    private transient CollectionReference documents;

    @GuardedBy("this")
    @Nullable
    private transient FieldMask mask;

    @GuardedBy("this")
    @Nullable
    private transient Set<ApiFuture<DocumentSnapshot>> inFlight;

    /**
     * Creates the lookup.
     *
     * @param database the database
     * @param collection the collection path
     * @param fields the field paths to read, each in the client library's encoded form ({@code
     *     FieldPath.toString()}); an empty array reads none
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the service
     * @param serviceAccountKeyFile the key file, or {@code null} for application default ones
     */
    FirestoreCollectionDocumentLookup(
            DatabaseDestination database,
            String collection,
            String[] fields,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.database = database;
        this.collection = collection;
        this.fields = fields.clone();
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    /** Returns the collection path documents are read from. */
    @VisibleForTesting
    String collection() {
        return collection;
    }

    /** Returns the encoded field paths a read asks for. */
    @VisibleForTesting
    String[] fields() {
        return fields.clone();
    }

    /** Returns the serialized credential path without reading it. */
    @VisibleForTesting
    @Nullable
    String serviceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public void open() throws Exception {
        EmulatorEndpoint endpoint = endpoint();
        client = FirestoreClients.open(database, endpoint, settings(endpoint));
        synchronized (this) {
            documents = client.client().collection(collection);
            mask = FieldMask.of(maskPaths(fields));
        }
    }

    /** Builds the settings the one client is opened on; tests inspect them without a client. */
    @VisibleForTesting
    FirestoreOptions.Builder settings() throws Exception {
        return settings(endpoint());
    }

    private FirestoreOptions.Builder settings(@Nullable EmulatorEndpoint endpoint)
            throws Exception {
        return FirestoreClients.settings(
                database,
                endpoint,
                endpoint == null ? FirestoreCredentials.load(serviceAccountKeyFile) : null,
                null);
    }

    @Nullable
    private EmulatorEndpoint endpoint() {
        return emulatorEndpoint == null
                ? null
                : EmulatorEndpoint.parse(
                        emulatorEndpoint, FirestoreConnectorOptions.EMULATOR_ENDPOINT.key());
    }

    /**
     * The field mask of a read: each encoded path decoded, so a name containing a dot stays one
     * segment. A read of no field asks for the document's name alone.
     */
    @VisibleForTesting
    static FieldPath[] maskPaths(String[] fields) {
        if (fields.length == 0) {
            return new FieldPath[] {FieldPath.documentId()};
        }
        FieldPath[] paths = new FieldPath[fields.length];
        for (int i = 0; i < fields.length; i++) {
            paths[i] = FieldPath.fromServerFormat(fields[i]);
        }
        return paths;
    }

    @Override
    public synchronized ApiFuture<DocumentSnapshot> readAsync(String id) {
        if (documents == null || mask == null) {
            throw new IllegalStateException("The Firestore document lookup has not been opened.");
        }
        ApiFuture<DocumentSnapshot> read =
                ApiFutures.transform(
                        documents.document(id).get(mask), snapshot -> snapshot, Runnable::run);
        if (inFlight == null) {
            inFlight = new HashSet<>();
        }
        inFlight.add(read);
        read.addListener(() -> forget(read), Runnable::run);
        return read;
    }

    private synchronized void forget(ApiFuture<DocumentSnapshot> read) {
        if (inFlight != null) {
            inFlight.remove(read);
        }
    }

    @Override
    public void close() throws Exception {
        List<ApiFuture<DocumentSnapshot>> toCancel;
        synchronized (this) {
            // Refuses later reads, then ends the ones in flight before the client closes.
            documents = null;
            mask = null;
            toCancel = inFlight == null ? List.of() : new ArrayList<>(inFlight);
        }
        for (ApiFuture<DocumentSnapshot> read : toCancel) {
            read.cancel(true);
        }
        if (client != null) {
            // Closes the client, then releases its event-loop lease.
            client.close();
            client = null;
        }
    }
}
