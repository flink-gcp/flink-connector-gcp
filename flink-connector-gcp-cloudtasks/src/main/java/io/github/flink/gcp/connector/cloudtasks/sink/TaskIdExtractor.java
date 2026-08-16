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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Extracts the deduplication key of a record, opting the sink into named tasks.
 *
 * <p>Set through {@code CloudTasksSinkBuilder#taskIdExtractor(TaskIdExtractor)}; without one the
 * sink creates unnamed tasks and a replayed record produces a second call to the endpoint. With
 * one, a repeated create for a key Cloud Tasks has already seen fails with {@code ALREADY_EXISTS},
 * which the sink treats as success.
 *
 * <p>The returned key is <em>not</em> the task id: the sink hashes it with SHA-256 and uses the
 * digest, because Google documents that sequential task ids — an event id, an offset, a timestamp,
 * exactly what a key extractor tends to return — increase latency and error rates across all task
 * commands. Deduplication is unaffected, since the same key always hashes the same way.
 *
 * <p>The extractor lives on the sink builder rather than on the serialization schema because a
 * {@code Task} carries no task-id field, only the full {@code
 * projects/P/locations/L/queues/Q/tasks/ID} name, which needs the resolved queue the schema never
 * sees.
 *
 * @param <T> type of the records written by the sink
 */
@Public
@FunctionalInterface
public interface TaskIdExtractor<T> extends Serializable {

    /**
     * Returns the deduplication key of the given record.
     *
     * @param element the record
     * @return the key; must be neither {@code null} nor empty (the writer fails the record
     *     otherwise, since silently falling back to an unnamed task would drop deduplication
     *     without saying so)
     */
    String extractTaskId(T element);
}
