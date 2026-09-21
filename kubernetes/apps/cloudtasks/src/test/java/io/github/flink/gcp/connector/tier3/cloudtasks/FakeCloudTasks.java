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

import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * An in-process {@code CreateTask} server, so a test can run the whole job without an emulator.
 *
 * <p>It answers the one method the measurement path calls: it names an unnamed task, refuses a name
 * it has already accepted with {@code ALREADY_EXISTS}, and records what it took. That is enough for
 * the connector's eager and staged writers alike, and it keeps this module free of testcontainers
 * and of Docker.
 */
final class FakeCloudTasks implements AutoCloseable {
    private final Map<Long, Task> accepted = new ConcurrentHashMap<>();
    private final Set<String> names = ConcurrentHashMap.newKeySet();
    private final Server server;

    FakeCloudTasks() throws IOException {
        MethodDescriptor<CreateTaskRequest, Task> method =
                MethodDescriptor.<CreateTaskRequest, Task>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName("google.cloud.tasks.v2.CloudTasks/CreateTask")
                        .setRequestMarshaller(
                                ProtoUtils.marshaller(CreateTaskRequest.getDefaultInstance()))
                        .setResponseMarshaller(ProtoUtils.marshaller(Task.getDefaultInstance()))
                        .build();
        server =
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                ServerServiceDefinition.builder("google.cloud.tasks.v2.CloudTasks")
                                        .addMethod(method, ServerCalls.asyncUnaryCall(this::create))
                                        .build())
                        .build()
                        .start();
    }

    private void create(CreateTaskRequest request, io.grpc.stub.StreamObserver<Task> response) {
        var origin = MeasurementPayload.read(request.getTask().getHttpRequest().getBody());
        Task task = request.getTask();
        if (task.getName().isEmpty()) {
            task =
                    task.toBuilder()
                            .setName(request.getParent() + "/tasks/" + UUID.randomUUID())
                            .build();
        }
        if (!names.add(task.getName())) {
            response.onError(Status.ALREADY_EXISTS.asRuntimeException());
            return;
        }
        accepted.putIfAbsent(origin.sequence(), task);
        response.onNext(task);
        response.onCompleted();
    }

    /** The endpoint a sink's {@code emulatorEndpoint} takes. */
    String endpoint() {
        return "127.0.0.1:" + server.getPort();
    }

    /** One entry per distinct sequence the server accepted. */
    Map<Long, Task> accepted() {
        return accepted;
    }

    /** Every task name accepted, which is one per physical creation. */
    Set<String> names() {
        return names;
    }

    /**
     * Shuts the server down and insists it actually terminated: netty builds its own event loop
     * groups here, and a module run starts fifteen of these, so a wedged one is a leak worth
     * hearing about rather than a thread nobody counts.
     */
    @Override
    public void close() throws InterruptedException {
        server.shutdownNow();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("FakeCloudTasks did not terminate");
        }
    }
}
