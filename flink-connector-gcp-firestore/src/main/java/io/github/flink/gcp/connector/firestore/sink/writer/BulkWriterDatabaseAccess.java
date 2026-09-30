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
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

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
        Map<String, Object> fields = write.getFields();
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
