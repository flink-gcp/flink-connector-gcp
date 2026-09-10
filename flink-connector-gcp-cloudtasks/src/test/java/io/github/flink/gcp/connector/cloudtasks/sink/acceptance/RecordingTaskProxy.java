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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Forwards real requests; only the selected downstream response is replaced or withheld. */
final class RecordingTaskProxy implements AutoCloseable {
    interface Service {
        Task create(CreateTaskRequest request, Deadline deadline) throws Exception;
    }

    enum Fault {
        NONE,
        LOSE_RESPONSE,
        HOLD_RESPONSE,
        HOLD_BEFORE_FORWARD
    }

    private final Service service;
    private final AcceptanceEvidence evidence;
    private final Server server;
    private final java.util.concurrent.ExecutorService handlers =
            java.util.concurrent.Executors.newCachedThreadPool();
    private final List<CreateTaskRequest> requests = new ArrayList<>();
    private final List<Task> responses = new ArrayList<>();
    private final AtomicInteger ordinal = new AtomicInteger();
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch held = new CountDownLatch(1);
    private volatile Fault fault = Fault.NONE;
    private volatile int faultAt = 1;
    private volatile int incarnation;
    private volatile Throwable failure;

    RecordingTaskProxy(Service service, AcceptanceEvidence evidence) throws Exception {
        this.service = service;
        this.evidence = evidence;
        var method =
                MethodDescriptor.<CreateTaskRequest, Task>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName("google.cloud.tasks.v2.CloudTasks/CreateTask")
                        .setRequestMarshaller(
                                ProtoUtils.marshaller(CreateTaskRequest.getDefaultInstance()))
                        .setResponseMarshaller(ProtoUtils.marshaller(Task.getDefaultInstance()))
                        .build();
        server =
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .executor(handlers)
                        .addService(
                                ServerServiceDefinition.builder("google.cloud.tasks.v2.CloudTasks")
                                        .addMethod(
                                                method, ServerCalls.asyncUnaryCall(this::forward))
                                        .build())
                        .build()
                        .start();
    }

    String endpoint() {
        return "127.0.0.1:" + server.getPort();
    }

    void inject(Fault selected, int requestOrdinal) {
        fault = selected;
        faultAt = requestOrdinal;
    }

    void nextIncarnation() {
        incarnation++;
    }

    void awaitHeld() throws InterruptedException {
        if (!held.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("The old incarnation never reached the selected fault");
        }
    }

    void releaseOldRequest() {
        release.countDown();
    }

    synchronized List<CreateTaskRequest> requests() {
        checkFailure();
        return List.copyOf(requests);
    }

    synchronized List<Task> responses() {
        checkFailure();
        return List.copyOf(responses);
    }

    private void checkFailure() {
        if (failure != null) {
            throw new AssertionError("Forwarding harness failed", failure);
        }
    }

    private void forward(CreateTaskRequest request, StreamObserver<Task> downstream) {
        int attempt = ordinal.incrementAndGet();
        int owner = incarnation;
        Fault selected = attempt == faultAt ? fault : Fault.NONE;
        Deadline deadline = Context.current().getDeadline();
        try {
            if (deadline == null) {
                throw new AssertionError("Connector request carries no absolute deadline");
            }
            synchronized (this) {
                requests.add(request);
            }
            evidence.record(
                    "request",
                    Map.of(
                            "incarnation",
                            owner,
                            "attempt",
                            attempt,
                            "remainingNanos",
                            deadline.timeRemaining(TimeUnit.NANOSECONDS),
                            "requestBase64",
                            Base64.getEncoder().encodeToString(request.toByteArray())));
            if (selected == Fault.HOLD_BEFORE_FORWARD) {
                held.countDown();
                if (!release.await(
                        Math.max(0, deadline.timeRemaining(TimeUnit.NANOSECONDS)),
                        TimeUnit.NANOSECONDS)) {
                    downstream.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                    return;
                }
            }
            if (deadline.isExpired()) {
                downstream.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                return;
            }
            // Detach cancellation only: the original deadline still bounds the outgoing call.
            // This models an old client's request whose response the replacement cannot receive.
            Task task = Context.ROOT.call(() -> service.create(request, deadline));
            evidence.record(
                    "create-response",
                    Map.of(
                            "incarnation",
                            owner,
                            "attempt",
                            attempt,
                            "taskBase64",
                            Base64.getEncoder().encodeToString(task.toByteArray()),
                            "name",
                            task.getName(),
                            "createTime",
                            task.getCreateTime().toString()));
            synchronized (this) {
                responses.add(task);
            }
            if (selected == Fault.HOLD_RESPONSE) {
                held.countDown();
                return;
            }
            if (selected == Fault.LOSE_RESPONSE) {
                evidence.record(
                        "response-suppressed",
                        Map.of("name", task.getName(), "incarnation", owner));
                downstream.onError(
                        Status.UNAVAILABLE
                                .withDescription("Captured success; injected response loss")
                                .asRuntimeException());
            } else {
                downstream.onNext(task);
                downstream.onCompleted();
            }
        } catch (Throwable error) {
            Status status = Status.fromThrowable(error);
            if (status.getCode() == Status.Code.UNKNOWN) {
                failure = error;
            }
            evidence.record(
                    "rpc-outcome",
                    Map.of(
                            "incarnation",
                            owner,
                            "attempt",
                            attempt,
                            "status",
                            status.getCode().name()));
            downstream.onError(status.asRuntimeException());
        }
    }

    @Override
    public void close() throws Exception {
        release.countDown();
        server.shutdownNow();
        if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new AssertionError("Forwarding server did not stop");
        }
        // Transport termination does not await cancellation-detached application callbacks.
        handlers.shutdown();
        if (!handlers.awaitTermination(30, TimeUnit.SECONDS)) {
            handlers.shutdownNow();
            throw new AssertionError("Forwarding callbacks did not stop");
        }
        checkFailure();
    }
}
