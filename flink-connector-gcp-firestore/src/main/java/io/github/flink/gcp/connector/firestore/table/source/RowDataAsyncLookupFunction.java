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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentSnapshot;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Looks a document up by id through the client library's asynchronous API. Public because the
 * planner refuses a function class that is not.
 */
@Internal
public final class RowDataAsyncLookupFunction extends AsyncLookupFunction {
    private static final long serialVersionUID = 1L;

    private final RowDataDeserializationSchema deserializer;
    private final int maxRetries;
    private final DocumentLookup lookup;

    RowDataAsyncLookupFunction(
            RowDataDeserializationSchema deserializer, int maxRetries, DocumentLookup lookup) {
        this.deserializer = deserializer;
        this.maxRetries = maxRetries;
        this.lookup = lookup;
    }

    @VisibleForTesting
    DocumentLookup documentLookup() {
        return lookup;
    }

    @VisibleForTesting
    int maxRetries() {
        return maxRetries;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        deserializer.open(null);
        lookup.open();
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        String id = DocumentLookups.documentId(keyRow);
        if (id == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        new LookupAttempt(id, result).schedule();
        return result;
    }

    /**
     * One lookup and its retries. A retry is scheduled from the failed read's callback, which may
     * run on the calling thread when the read fails at once; the work counter turns that recursion
     * into a loop, so a long retry budget cannot grow the task thread's stack.
     *
     * <p>Nothing cancels a read: Flink only waits on the result, and cancelling the client
     * library's future would not stop its {@code BatchGetDocuments} stream.
     */
    private final class LookupAttempt {
        private final String id;
        private final CompletableFuture<Collection<RowData>> result;
        private final AtomicInteger work = new AtomicInteger();
        private int retry;

        private LookupAttempt(String id, CompletableFuture<Collection<RowData>> result) {
            this.id = id;
            this.result = result;
        }

        private void schedule() {
            if (work.getAndIncrement() != 0) {
                return;
            }
            int pending = 1;
            do {
                if (!result.isDone()) {
                    issue();
                }
                pending = work.addAndGet(-pending);
            } while (pending != 0);
        }

        private void issue() {
            final ApiFuture<DocumentSnapshot> future;
            try {
                future = lookup.readAsync(id);
            } catch (RuntimeException failure) {
                handleFailure(failure);
                return;
            }
            ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<DocumentSnapshot>() {
                        @Override
                        public void onFailure(Throwable failure) {
                            handleFailure(failure);
                        }

                        @Override
                        public void onSuccess(DocumentSnapshot snapshot) {
                            try {
                                result.complete(DocumentLookups.rows(deserializer, snapshot));
                            } catch (Exception failure) {
                                result.completeExceptionally(failure);
                            }
                        }
                    },
                    Runnable::run);
        }

        private void handleFailure(Throwable failure) {
            if (result.isDone()) {
                return;
            }
            if (retry < maxRetries && LookupErrorClassifier.isTransient(failure)) {
                retry++;
                schedule();
            } else {
                result.completeExceptionally(failure);
            }
        }
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
