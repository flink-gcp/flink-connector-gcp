/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

/** The mechanism used to write records to BigQuery. */
@PublicEvolving
public enum WriteMethod {

    /**
     * Appends to the destination table's Storage Write API default stream.
     *
     * <p>At-least-once semantics. Supports dynamic per-record table destinations; connection
     * multiplexing across destination tables is delegated to the BigQuery Storage client's
     * connection pool.
     */
    STORAGE_API_AT_LEAST_ONCE,

    /**
     * Writes through application-created Storage Write API buffered streams committed with a
     * two-phase commit protocol on Flink checkpoints: rows are appended at explicit offsets and
     * become visible only when a completed checkpoint's commit flushes them.
     *
     * <p>Exactly-once semantics. Requires a fixed {@code destination(...)} (dynamic destinations
     * are not supported yet) and {@code bufferedStreamOptions(...)}. Each subtask reuses one
     * buffered stream across checkpoints (tracked in writer state). Streaming execution requires
     * exactly-once checkpointing with checkpoints-after-tasks-finish; batch execution commits at
     * end of input. The table schema is pinned at stream creation — mid-stream schema evolution is
     * not supported.
     */
    STORAGE_API_EXACTLY_ONCE,

    /**
     * Stages records as files on Cloud Storage and imports them with BigQuery load jobs.
     *
     * <p>Always exactly-once. Batch execution loads everything at end of input; streaming execution
     * loads each checkpoint's files (checkpointing required, {@link WriteDisposition#WRITE_APPEND}
     * only, and mind BigQuery's 1,500 load jobs per table per day — checkpoint intervals of 2-5
     * minutes or more).
     */
    FILE_LOADS
}
