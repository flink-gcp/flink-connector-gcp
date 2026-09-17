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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.grpc.Deadline;

/** Benchmark-only 128-bit name distribution control over the production eager writer. */
@Internal
final class RandomNameControl implements TaskCreator {
    private final TaskCreator delegate;

    RandomNameControl(TaskCreator delegate) {
        this.delegate = delegate;
    }

    @Override
    public ApiFuture<Task> createTask(CreateTaskRequest request) {
        return delegate.createTask(project(request));
    }

    @Override
    public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
        return delegate.createTask(project(request), deadline);
    }

    private static CreateTaskRequest project(CreateTaskRequest request) {
        String prefix = request.getParent() + "/tasks/";
        String name = request.getTask().getName();
        if (!name.startsWith(prefix) || !name.substring(prefix.length()).matches("[a-f0-9]{64}")) {
            throw new IllegalArgumentException(
                    "The random-name control requires the eager SHA-256 name");
        }
        // A fixed prefix of a SHA-256 digest supplies 128 distributed bits and remains identical
        // on retry. This control matches the random staged name width, not its generation cost.
        return request.toBuilder()
                .setTask(
                        request.getTask().toBuilder()
                                .setName(name.substring(0, prefix.length() + 32)))
                .build();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
