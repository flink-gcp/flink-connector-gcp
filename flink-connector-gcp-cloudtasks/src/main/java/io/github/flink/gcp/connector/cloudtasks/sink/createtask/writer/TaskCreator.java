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

package io.github.flink.gcp.connector.cloudtasks.sink.createtask.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;

/**
 * Creates Cloud Tasks tasks, one RPC per task.
 *
 * <p>An interface (instead of the concrete {@code CloudTasksClient}) so the writer can be
 * unit-tested against fakes. One instance serves every destination queue: Cloud Tasks has no
 * per-queue connection or stream, so there is nothing to key by destination.
 *
 * <p>There is deliberately no batch method. {@code BatchCreateTasks} and {@code BufferTask} are
 * REST-only and absent from {@code google-cloud-tasks} 2.94.0, and no method in its settings is
 * configured with batching, so the writer owns batching, backpressure and concurrency outright.
 */
@Internal
public interface TaskCreator extends AutoCloseable {

    /**
     * Creates the requested task asynchronously.
     *
     * @param request the create request
     * @return a future completing with the created task, or exceptionally when the creation fails
     */
    ApiFuture<Task> createTask(CreateTaskRequest request);

    /** Shuts the underlying client down, waiting a bounded time for termination. */
    @Override
    void close() throws Exception;
}
