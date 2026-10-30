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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Experimental;

import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

/**
 * Whether the sink applies writes to one row in the order a sink subtask received them.
 *
 * <p>Submission order is worth keeping only when the input already has an order per row and every
 * write for a row reaches the same sink subtask: a Table API changelog that Flink shuffles by its
 * primary key, or a stream keyed by row key before the sink. Each subtask orders only what it
 * receives, so input that spreads one row's writes over several subtasks stays unordered whatever
 * this mode says. A row is a row key in one table: the same key in two tables is two rows.
 *
 * @see BigtableWriterOptions.Builder#orderingMode(OrderingMode)
 * @see BigtableRequestOptions.Builder#orderingMode(OrderingMode)
 */
@Experimental
public enum OrderingMode {

    /**
     * No order between writes to one row (default), tuned for throughput. The client keeps several
     * {@code MutateRows} requests in flight and Bigtable may apply them, and even the entries of
     * one request, in any order; the single-row sinks keep several requests in flight too. Writes
     * to one row that travel in different requests have been measured applied out of order, so a
     * {@code setCell} followed by a row delete can leave the row in place.
     */
    NONE("none"),

    /**
     * Applies writes to one row in the order the subtask received them. A write whose row already
     * has an earlier write unanswered is held in the writer and sent only once that write is
     * applied or handed to the failure handler. Writes to other rows are not held back. A turn ends
     * when the client answers, so an attempt the client abandoned at its own timeout and retried
     * may in principle still be applied after the row's next write.
     *
     * <p>Each held write costs one round trip after the one before it, so a row written many times
     * in a row is written at one write per round trip. Held writes count against the writer's
     * in-flight bounds, so a row receiving writes faster than that can fill them and slow the
     * subtask's other rows with it.
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
