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

import java.io.IOException;
import java.io.Serializable;

/**
 * Pluggable policy for rows that terminally fail to be written to BigQuery (row-level errors: rows
 * rejected by the Storage Write API with per-row error details, rows that fail serialization, and
 * rows exceeding the per-row size limit).
 *
 * <p>Only row-level failures reach the handler. Transient append failures are retried (by the SDK
 * and by the sink's bounded retry budget) without involving the handler, and terminal request
 * failures such as {@code PERMISSION_DENIED} always fail the job.
 *
 * <p>The handler is invoked on the Flink task thread only, so implementations need not be
 * thread-safe. Throwing from {@link #handle} fails the ongoing write or checkpoint — the built-in
 * {@link #failJob()} policy (the default) does exactly that. Returning normally drops the row.
 */
@PublicEvolving
@FunctionalInterface
public interface FailedRowHandler extends Serializable {

    /**
     * Handles one terminally failed row. Returning normally drops the row; throwing fails the
     * ongoing write or checkpoint.
     *
     * @param row the failed row
     * @throws IOException to fail the job instead of dropping the row
     */
    void handle(FailedRow row) throws IOException;

    /** Releases resources held by the handler when the sink writer closes. */
    default void close() throws Exception {}

    /**
     * Returns the default policy: every row-level failure fails the job.
     *
     * @return the fail-job handler
     */
    static FailedRowHandler failJob() {
        return FailedRowHandlers.FailJob.INSTANCE;
    }

    /**
     * Returns the policy that logs each failed row at WARN level and drops it, letting the pipeline
     * continue.
     *
     * @return the log-and-drop handler
     */
    static FailedRowHandler logAndDrop() {
        return FailedRowHandlers.LogAndDrop.INSTANCE;
    }

    /**
     * Returns a policy that routes each failed row to the given dead-letter queue; a failure of the
     * queue itself fails the job.
     *
     * @param deadLetterQueue the dead-letter queue
     * @return the dead-letter-queue handler
     */
    static FailedRowHandler sendToDeadLetterQueue(DeadLetterQueue deadLetterQueue) {
        return new FailedRowHandlers.SendToDeadLetterQueue(deadLetterQueue);
    }
}
