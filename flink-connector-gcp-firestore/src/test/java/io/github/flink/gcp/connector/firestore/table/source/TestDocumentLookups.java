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

import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;

import java.util.concurrent.TimeUnit;

/** Opens the table source's document lookup from outside its package, for tests elsewhere. */
public final class TestDocumentLookups {

    private TestDocumentLookups() {}

    /** An open lookup of one collection. */
    public interface OpenLookup extends AutoCloseable {

        /**
         * Reads a document by id.
         *
         * @param id the document id
         * @return the snapshot, which does not exist when no document has the id
         * @throws Exception if the read fails
         */
        DocumentSnapshot read(String id) throws Exception;
    }

    /**
     * Opens a lookup of a collection on an emulator, reading only the document names.
     *
     * @param database the database
     * @param collection the collection path
     * @param emulatorEndpoint the emulator endpoint
     * @return the open lookup; close it to close its client
     * @throws Exception if the lookup cannot be opened
     */
    public static OpenLookup open(
            DatabaseDestination database, String collection, String emulatorEndpoint)
            throws Exception {
        FirestoreCollectionDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        database, collection, new String[0], emulatorEndpoint, null);
        try {
            lookup.open();
        } catch (Exception e) {
            try {
                lookup.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        return new OpenLookup() {
            @Override
            public DocumentSnapshot read(String id) throws Exception {
                return lookup.readAsync(id).get(30, TimeUnit.SECONDS);
            }

            @Override
            public void close() throws Exception {
                lookup.close();
            }
        };
    }
}
