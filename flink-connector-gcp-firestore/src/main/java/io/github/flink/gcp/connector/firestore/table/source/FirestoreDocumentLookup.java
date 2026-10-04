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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentSnapshot;

import java.io.Serializable;

/** Reads one document of the table's collection by id, the seam a lookup function reads through. */
@Internal
interface FirestoreDocumentLookup extends Serializable, AutoCloseable {

    /** Opens the client, once per lookup function instance. */
    void open() throws Exception;

    /**
     * Reads the document with this id. The client library has no blocking read; the blocking lookup
     * waits on this future.
     *
     * @param id a document id {@link
     *     FirestoreDocumentLookups#documentId(org.apache.flink.table.data.RowData)} accepted
     * @return the snapshot's future; the snapshot does not exist when there is no such document
     */
    ApiFuture<DocumentSnapshot> readAsync(String id);

    @Override
    void close() throws Exception;
}
