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

import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.io.Serializable;

/**
 * Destination for rows that terminally failed to be written to BigQuery, used through {@link
 * FailedRowHandler#sendToDeadLetterQueue(DeadLetterQueue)}.
 *
 * <p>This is a stub for the cross-connector dead-letter-queue standardization (issue #37): the
 * lifecycle contract (open/close, checkpoint integration) and the extraction into a shared module
 * are decided there. Until then the interface is a minimal, evolving extension point with two
 * caveats implementations must account for themselves:
 *
 * <ul>
 *   <li>there is no flush/close hook yet, so an implementation that buffers rows asynchronously can
 *       lose them when the job stops — write through synchronously (throwing on failure) to stay
 *       reliable;
 *   <li>rows are offered before the ongoing checkpoint completes, so a restart can replay the same
 *       records and offer them again — dead-letter output is at-least-once.
 * </ul>
 */
@Experimental
public interface DeadLetterQueue extends Serializable {

    /**
     * Accepts one terminally failed row.
     *
     * @param row the failed row
     * @throws IOException if the row cannot be accepted; this fails the job
     */
    void offer(FailedRow row) throws IOException;
}
