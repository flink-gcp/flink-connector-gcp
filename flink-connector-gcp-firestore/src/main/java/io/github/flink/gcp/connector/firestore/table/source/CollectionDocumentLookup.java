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
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.FirestoreClients;
import io.github.flink.gcp.connector.firestore.FirestoreCredentials;
import io.github.flink.gcp.connector.firestore.table.FirestoreConnectorOptions;

import javax.annotation.Nullable;

/**
 * Reads documents of one collection through a client owned by one lookup function instance, each
 * projected to the fields of the columns read.
 */
@Internal
final class CollectionDocumentLookup implements DocumentLookup {
    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final String collection;
    private final String[] fields;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private transient Firestore firestore;
    @Nullable private transient CollectionReference documents;
    @Nullable private transient FieldMask mask;

    /**
     * Creates the lookup.
     *
     * @param database the database
     * @param collection the collection path
     * @param fields the field names to read, each one literal segment; an empty array reads none
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the service
     * @param serviceAccountKeyFile the key file, or {@code null} for application default ones
     */
    CollectionDocumentLookup(
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

    /** Returns the field names a read asks for. */
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
        firestore = FirestoreClients.open(database, settings());
        documents = firestore.collection(collection);
        mask = FieldMask.of(maskPaths(fields));
    }

    /** Builds the settings the one client is opened on; tests inspect them without a client. */
    @VisibleForTesting
    FirestoreOptions settings() throws Exception {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint,
                                FirestoreConnectorOptions.EMULATOR_ENDPOINT.key());
        return FirestoreClients.settings(
                database,
                endpoint,
                endpoint == null ? FirestoreCredentials.load(serviceAccountKeyFile) : null,
                null);
    }

    /**
     * The field mask of a read: each name one literal segment, so a name containing a dot is one
     * field. A read of no field asks for the document's name alone.
     */
    @VisibleForTesting
    static FieldPath[] maskPaths(String[] fields) {
        if (fields.length == 0) {
            return new FieldPath[] {FieldPath.documentId()};
        }
        FieldPath[] paths = new FieldPath[fields.length];
        for (int i = 0; i < fields.length; i++) {
            paths[i] = FieldPath.of(fields[i]);
        }
        return paths;
    }

    @Override
    public ApiFuture<DocumentSnapshot> readAsync(String id) {
        if (documents == null || mask == null) {
            throw new IllegalStateException("The Firestore document lookup has not been opened.");
        }
        return documents.document(id).get(mask);
    }

    @Override
    public void close() throws Exception {
        if (firestore != null) {
            firestore.close();
            firestore = null;
            documents = null;
            mask = null;
        }
    }
}
