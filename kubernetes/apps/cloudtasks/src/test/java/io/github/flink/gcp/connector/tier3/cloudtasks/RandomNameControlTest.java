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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.grpc.Deadline;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RandomNameControlTest {
    @Test
    void preservesBodyAndDeadlineAndProjectsRetriesToOneName() throws Exception {
        List<CreateTaskRequest> sent = new ArrayList<>();
        List<Deadline> deadlines = new ArrayList<>();
        var control =
                new RandomNameControl(
                        new TaskCreator() {
                            @Override
                            public ApiFuture<Task> createTask(CreateTaskRequest request) {
                                sent.add(request);
                                return ApiFutures.immediateFuture(request.getTask());
                            }

                            @Override
                            public ApiFuture<Task> createTask(
                                    CreateTaskRequest request, Deadline deadline) {
                                deadlines.add(deadline);
                                return createTask(request);
                            }

                            @Override
                            public void close() {}
                        });
        String parent = "projects/test/locations/test/queues/test";
        String digest = "0123456789abcdef".repeat(4);
        var original =
                CreateTaskRequest.newBuilder()
                        .setParent(parent)
                        .setTask(
                                Task.newBuilder()
                                        .setName(parent + "/tasks/" + digest)
                                        .setHttpRequest(
                                                HttpRequest.newBuilder()
                                                        .setBody(
                                                                MeasurementPayload.create(
                                                                        65536, 1))))
                        .build();
        Deadline deadline = Deadline.after(10, TimeUnit.SECONDS);
        assertThat(control.createTask(original, deadline).get().getName())
                .isEqualTo(parent + "/tasks/" + digest.substring(0, 32));
        control.createTask(original);
        assertThat(sent).hasSize(2);
        assertThat(sent.get(0)).isEqualTo(sent.get(1));
        assertThat(sent.get(0).getTask().getHttpRequest())
                .isEqualTo(original.getTask().getHttpRequest());
        assertThat(original.getTask().getName()).endsWith(digest);
        assertThat(deadlines).containsExactly(deadline);
        assertThatThrownBy(
                        () ->
                                control.createTask(
                                        original.toBuilder()
                                                .setTask(original.getTask().toBuilder().clearName())
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sent).hasSize(2);
    }
}
