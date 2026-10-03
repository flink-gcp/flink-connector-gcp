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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.BulkWriterOptions;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Precondition;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * The production {@link FirestoreDatabaseAccess}: a {@code BulkWriter} over a Firestore client,
 * replaced on request.
 */
@Internal
final class BulkWriterDatabaseAccess implements FirestoreDatabaseAccess {

    private final Firestore firestore;
    private final ScheduledExecutorService executor;
    private final BulkWriterOptions options;
    private final BulkWriterRetryPolicy retryPolicy;

    private BulkWriter bulkWriter;

    BulkWriterDatabaseAccess(
            Firestore firestore,
            ScheduledExecutorService executor,
            BulkWriterOptions options,
            BulkWriterRetryPolicy retryPolicy) {
        this.firestore = firestore;
        this.executor = executor;
        this.options = options;
        this.retryPolicy = retryPolicy;
        this.bulkWriter = newBulkWriter();
    }

    private BulkWriter newBulkWriter() {
        BulkWriter created = firestore.bulkWriter(options);
        created.addWriteErrorListener(retryPolicy);
        return created;
    }

    @Override
    public ApiFuture<WriteResult> submit(FirestoreWrite write) {
        DocumentReference document = firestore.document(write.getDocumentPath());
        Map<String, Object> fields = replaceReferences(write.getFields(), firestore::document);
        switch (write.getOperation()) {
            case SET:
                return bulkWriter.set(document, fields);
            case SET_MERGE:
                return bulkWriter.set(document, fields, SetOptions.merge());
            case CREATE:
                return bulkWriter.create(document, fields);
            case UPDATE:
                return update(document, fields, write);
            case DELETE:
                return write.getLastUpdateTime() == null
                        ? bulkWriter.delete(document)
                        : bulkWriter.delete(
                                document, Precondition.updatedAt(write.getLastUpdateTime()));
            default:
                throw new IllegalArgumentException("Unknown operation " + write.getOperation());
        }
    }

    /**
     * Updates through the field-path overload, one {@link FieldPath#of} per key, so that a key is a
     * literal top-level field name as it is in every other operation; the map overload would split
     * each key on dots.
     */
    private ApiFuture<WriteResult> update(
            DocumentReference document, Map<String, Object> fields, FirestoreWrite write) {
        Iterator<Map.Entry<String, Object>> entries = fields.entrySet().iterator();
        Map.Entry<String, Object> first = entries.next();
        Object[] rest = new Object[(fields.size() - 1) * 2];
        for (int i = 0; entries.hasNext(); i += 2) {
            Map.Entry<String, Object> entry = entries.next();
            rest[i] = FieldPath.of(entry.getKey());
            rest[i + 1] = entry.getValue();
        }
        FieldPath firstPath = FieldPath.of(first.getKey());
        return write.getLastUpdateTime() == null
                ? bulkWriter.update(document, firstPath, first.getValue(), rest)
                : bulkWriter.update(
                        document,
                        Precondition.updatedAt(write.getLastUpdateTime()),
                        firstPath,
                        first.getValue(),
                        rest);
    }

    /**
     * Returns the fields with every {@link FirestoreDocumentReference}, at any depth, replaced by
     * what {@code document} makes of its path: in production a {@link DocumentReference} of this
     * client, which is what the library encodes as a reference value. A map or list holding no
     * reference is returned as it is, so a write without one is not copied.
     *
     * @param fields a write's fields
     * @param document makes the library's reference from a document path
     * @return the fields to hand to the library
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> replaceReferences(
            Map<String, Object> fields, Function<String, ?> document) {
        return (Map<String, Object>) replaceReferencesIn(fields, document);
    }

    private static Object replaceReferencesIn(Object value, Function<String, ?> document) {
        if (value instanceof FirestoreDocumentReference) {
            return document.apply(((FirestoreDocumentReference) value).getDocumentPath());
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<Object, Object> replaced = null;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object element = entry.getValue();
                Object library = replaceReferencesIn(element, document);
                if (replaced == null && library != element) {
                    replaced = new LinkedHashMap<>(map);
                }
                if (replaced != null) {
                    replaced.put(entry.getKey(), library);
                }
            }
            return replaced == null ? map : replaced;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> replaced = null;
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                Object library = replaceReferencesIn(element, document);
                if (replaced == null && library != element) {
                    replaced = new ArrayList<>(list);
                }
                if (replaced != null) {
                    replaced.set(i, library);
                }
            }
            return replaced == null ? list : replaced;
        }
        return value;
    }

    @Override
    public void sendOutstanding() {
        // The returned future completes once everything sent so far is answered and never fails;
        // the writer waits on its own ledger instead, since a write the library mishandled would
        // keep this one pending forever.
        bulkWriter.flush();
    }

    @Override
    public void replaceBulkWriter() {
        bulkWriter = newBulkWriter();
    }

    @Override
    public void close() throws Exception {
        // The executor first: shutting it down cancels every batch send and backoff retry still
        // scheduled on it, so nothing more reaches the client being closed after it.
        Closers.closeAll(executor::shutdownNow, firestore);
    }
}
