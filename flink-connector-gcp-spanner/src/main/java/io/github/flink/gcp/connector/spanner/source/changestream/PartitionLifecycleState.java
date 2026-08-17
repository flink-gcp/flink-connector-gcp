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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;

/**
 * Lifecycle of one Spanner Change Streams partition in the checkpointed coordinator ledger.
 *
 * <p>The order is the only path: a partition is discovered, becomes eligible, is handed to a
 * reader, and ends. What each state means is a statement about the <em>coordinator's</em> knowledge
 * rather than about Spanner, and {@link #CREATED} is the one that carries the protocol.
 */
@Internal
public enum PartitionLifecycleState {

    /**
     * Discovered from a parent's child-partitions record, and <b>not yet eligible</b>: at least one
     * parent that names it has not finished. This is where the lineage's ordering lives — reading a
     * child before its parents have ended would read changes out of order for the key range they
     * share.
     */
    CREATED,

    /** Every parent has finished, so the partition is queued for the next reader that asks. */
    SCHEDULED,

    /** Handed to a reader. A reader failure returns it here through {@code addSplitsBack}. */
    RUNNING,

    /**
     * The reader's query ended successfully. Only this state promotes children, and only a
     * successful end reaches it — a failed query leaves the partition {@link #RUNNING} with its
     * split retained.
     */
    FINISHED
}
