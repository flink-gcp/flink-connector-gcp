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
import org.apache.flink.table.data.RowData;

import com.google.cloud.firestore.DocumentSnapshot;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** What both lookup functions share: which keys can name a document, and the row a read makes. */
@Internal
final class DocumentLookups {

    /** The longest document id the service stores, in UTF-8 bytes. */
    private static final int MAX_ID_BYTES = 1500;

    private DocumentLookups() {}

    /**
     * Returns the document id a lookup key names, or {@code null} when it can name no document of
     * the collection, so the lookup joins no row without a read: a NULL key, or one the service
     * never stores as an id (empty, {@code .}, {@code ..}, {@code __...__}, over 1,500 bytes) or
     * that holds {@code '/'}, which would address a document of another collection ({@code
     * "a/sub/b"}) or this one under another id ({@code "a/"} reads {@code a}).
     */
    @Nullable
    static String documentId(RowData key) {
        if (key.isNullAt(0)) {
            return null;
        }
        String id = key.getString(0).toString();
        boolean storable =
                !id.isEmpty()
                        && id.indexOf('/') < 0
                        && !id.equals(".")
                        && !id.equals("..")
                        && !(id.length() >= 4 && id.startsWith("__") && id.endsWith("__"))
                        && id.getBytes(StandardCharsets.UTF_8).length <= MAX_ID_BYTES;
        return storable ? id : null;
    }

    /** Converts a read: no row for a document that does not exist, else its one row. */
    static Collection<RowData> rows(
            RowDataDeserializationSchema deserializer, DocumentSnapshot snapshot)
            throws IOException {
        return snapshot.exists()
                ? Collections.singletonList(deserializer.read(snapshot))
                : Collections.emptyList();
    }

    /**
     * Waits for a read, rethrowing the client library's failure as it is, so the classifier sees
     * its status.
     */
    static DocumentSnapshot await(Future<DocumentSnapshot> read, String id) {
        try {
            return read.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading document '" + id + "'.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Failed to read document '" + id + "'.", cause);
        }
    }
}
