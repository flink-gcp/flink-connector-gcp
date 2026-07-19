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
     * two-phase commit protocol on Flink checkpoints.
     *
     * <p>Exactly-once semantics. Requires checkpointing to be enabled.
     */
    STORAGE_API_EXACTLY_ONCE,

    /**
     * Stages records as files on Cloud Storage and imports them with BigQuery load jobs.
     *
     * <p>Batch execution only; always exactly-once.
     */
    FILE_LOADS
}
