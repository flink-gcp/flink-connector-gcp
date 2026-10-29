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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.Experimental;

/**
 * Whether the sink applies mutations of one row in the order a sink subtask received them.
 *
 * <p>Submission order is worth keeping only when the input already has an order per row and every
 * mutation of a row reaches the same sink subtask: a Table API changelog that Flink shuffles by its
 * primary key, or a stream keyed by primary key before the sink. Each subtask orders only what it
 * receives, so input that spreads one row's mutations over several subtasks stays unordered
 * whatever this mode says.
 *
 * @see SpannerWriterOptions.Builder#orderingMode(OrderingMode)
 */
@Experimental
public enum OrderingMode {

    /**
     * No order between mutations of one row within a batch write request (default). Spanner applies
     * the mutation groups of one request in an unspecified order, so an {@code insertOrUpdate}
     * followed by a {@code delete} of the same row in one request can leave the row in place.
     * Requests are sent one at a time, so mutations in different requests keep their order, unless
     * the service applies an attempt that timed out on the client after a later request.
     */
    NONE("none"),

    /**
     * Applies mutations of one row in the order the subtask received them. The writer sends its
     * batch before a mutation whose row the batch already holds, so no request carries a row twice.
     * Mutations of other rows still share requests.
     *
     * <p>The row is the table and the primary-key values, with the primary key and its column types
     * read from the database's {@code INFORMATION_SCHEMA} when the writer opens. A mutation whose
     * row the writer cannot name, such as a delete of a key range, is ordered against every
     * mutation of its table instead, and a mutation of a table the schema did not list, such as a
     * synonym, against every mutation of the batch. A row written many times in a row costs one
     * request per mutation, and an attempt that timed out on the client can still be applied by the
     * service after a later request.
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
