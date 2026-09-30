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
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

/**
 * The narrow view of the Firestore client the writer needs: hand a write to the current {@code
 * BulkWriter}, send what it holds, and replace it.
 *
 * <p>It exists so the writer can be tested against a hand-written fake rather than the client
 * library, whose {@code Firestore} and {@code DocumentReference} are extensible only within it.
 * Every method is called on the task thread.
 */
@Internal
public interface FirestoreDatabaseAccess extends AutoCloseable {

    /**
     * Hands one write to the current {@code BulkWriter}.
     *
     * <p>The write is only queued: the library sends a request once it holds a batch's worth, or on
     * {@link #sendOutstanding()}, and on no timer.
     *
     * @param write the write
     * @return the write's outcome, completed on a library thread
     * @throws RuntimeException if the library refuses the write synchronously — after which the
     *     {@code BulkWriter} may hold an operation it will never answer
     */
    ApiFuture<WriteResult> submit(FirestoreWrite write);

    /** Asks the current {@code BulkWriter} to send every write it holds; does not wait. */
    void sendOutstanding();

    /**
     * Replaces the current {@code BulkWriter} with a fresh one. Called only when no write is in
     * flight: the old one is dropped, not closed, and still answers what it was sent.
     */
    void replaceBulkWriter();

    /**
     * Releases the client and the executor the {@code BulkWriter}s run on, abandoning whatever they
     * still hold. Never sends anything.
     */
    @Override
    void close() throws Exception;
}
