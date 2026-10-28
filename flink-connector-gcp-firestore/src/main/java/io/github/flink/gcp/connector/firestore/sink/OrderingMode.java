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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Whether the sink applies writes to one document in the order a sink subtask received them.
 *
 * <p>Submission order is worth keeping only when the input already has an order per document and
 * every write for a document reaches the same sink subtask: a Table API changelog that Flink
 * shuffles by its primary key, or a stream keyed by document path before the sink. Each subtask
 * orders only what it receives, so input that spreads one document's writes over several subtasks
 * stays unordered whatever this mode says.
 *
 * @see FirestoreWriterOptions.Builder#orderingMode(OrderingMode)
 */
@PublicEvolving
public enum OrderingMode {

    /**
     * No order between writes to one document (default), tuned for throughput. The client library
     * sends several requests at once and retries a write in a later request, and Firestore applies
     * the writes of one request in no guaranteed order, so a {@code set} followed by a {@code
     * delete} of the same document can leave the document in place.
     */
    NONE("none"),

    /**
     * Applies writes to one document in the order the subtask received them. A write whose document
     * already has an earlier write unanswered is held in the writer and sent only once that write
     * is applied or handed to the failure handler. Writes to other documents are not held back.
     *
     * <p>Each held write costs one round trip after the one before it, so a document written many
     * times in a row is written at one write per round trip. Held writes count against {@link
     * FirestoreWriterOptions.Builder#maxInFlightWrites(int)} and {@link
     * FirestoreWriterOptions.Builder#maxInFlightBytes(long)}. A document receiving writes faster
     * than that can fill the bounds and slow the subtask's other documents with it. A write created
     * by {@link FirestoreWrite#add(String, java.util.Map)} is never held, because its drawn id
     * names a document no other write addresses.
     */
    PER_KEY("per-key");

    private final String value;

    OrderingMode(String value) {
        this.value = value;
    }

    /** Returns the spelling a {@code WITH} clause uses. */
    @Override
    public String toString() {
        return value;
    }
}
