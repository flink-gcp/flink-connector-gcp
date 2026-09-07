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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksStagedWriter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;
import io.grpc.Deadline;
import io.grpc.Status;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** Serialized production sink with process-local clock and service fault controls. */
final class StagedCreationTestSink extends CloudTasksStagedCreateTaskSink<String> {
    private static final long serialVersionUID = 1L;
    static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();
    private final String runId;

    StagedCreationTestSink(String runId, CloudTasksStagedOptions options) {
        super(config(runId), options);
        this.runId = runId;
    }

    private static CloudTasksSinkConfig<String> config(String runId) {
        return ((CloudTasksCreateTaskSink<String>)
                        CloudTasksSink.<String>builder()
                                .queue(QueueDestination.of("p", "l", "q"))
                                .emulatorEndpoint("localhost:1")
                                .writerOptions(
                                        CloudTasksWriterOptions.builder()
                                                .maxInFlightTasks(1)
                                                .build())
                                .serializer(
                                        value -> {
                                            if (value.equals("skip")) {
                                                return null;
                                            }
                                            PROBES.get(runId).serialized.incrementAndGet();
                                            return Task.newBuilder()
                                                    .setHttpRequest(
                                                            HttpRequest.newBuilder()
                                                                    .setUrl(
                                                                            "https://example.com/task")
                                                                    .setHttpMethod(HttpMethod.POST)
                                                                    .setBody(
                                                                            ByteString.copyFromUtf8(
                                                                                    value)))
                                                    .build();
                                        })
                                .build())
                .getConfig();
    }

    @Override
    protected CloudTasksStagedWriter<String> createStagedWriter(
            CloudTasksSinkConfig<String> config,
            CloudTasksStagingConfig staging,
            WriterInitContext context) {
        Probe probe = PROBES.get(runId);
        return new CloudTasksStagedWriter<>(
                config,
                staging,
                context.metricGroup(),
                committerClock(),
                new SecureRandom() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void nextBytes(byte[] bytes) {
                        ByteBuffer.wrap(bytes)
                                .putLong(0)
                                .putLong(probe.identities.incrementAndGet());
                    }
                });
    }

    @Override
    protected TimeSource committerClock() {
        Probe probe = PROBES.get(runId);
        return new TimeSource() {
            @Override
            public long currentTimeMillis() {
                return probe.wall.get();
            }

            @Override
            public void sleep(long millis) {
                throw new AssertionError("Unexpected wall-clock sleep");
            }
        };
    }

    @Override
    public Committer<CloudTasksCommittable> createCommitter(CommitterInitContext context)
            throws java.io.IOException {
        PROBES.get(runId).metrics.add(context.metricGroup());
        return super.createCommitter(context);
    }

    @Override
    protected TaskCreator createTaskCreator(DefaultTaskCreatorFactory factory) {
        Probe probe = PROBES.get(runId);
        return new TaskCreator() {
            private boolean closed;

            @Override
            public ApiFuture<Task> createTask(CreateTaskRequest request) {
                throw new AssertionError("Absolute deadline required");
            }

            @Override
            public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
                if (closed) {
                    throw new AssertionError("Old creator reused");
                }
                probe.requests.add(request);
                probe.arrivals.add(request);
                var action = probe.actions.poll();
                return action == null ? probe.accept(request) : action.apply(request);
            }

            @Override
            public void close() {
                closed = true;
            }
        };
    }

    static final class Probe {
        final List<SinkCommitterMetricGroup> metrics = new CopyOnWriteArrayList<>();
        final AtomicLong wall = new AtomicLong(1000);
        final AtomicLong identities = new AtomicLong();
        final AtomicInteger serialized = new AtomicInteger();
        final List<CreateTaskRequest> requests = new CopyOnWriteArrayList<>();
        final Map<String, Task> created = new ConcurrentHashMap<>();
        final BlockingQueue<CreateTaskRequest> arrivals = new LinkedBlockingQueue<>();
        final Queue<Function<CreateTaskRequest, ApiFuture<Task>>> actions =
                new ConcurrentLinkedQueue<>();

        ApiFuture<Task> accept(CreateTaskRequest request) {
            Task previous = created.putIfAbsent(request.getTask().getName(), request.getTask());
            return previous == null
                    ? ApiFutures.immediateFuture(request.getTask())
                    : ApiFutures.immediateFailedFuture(Status.ALREADY_EXISTS.asRuntimeException());
        }
    }
}
