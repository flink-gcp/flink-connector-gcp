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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what the production client factory's emulator arm reports as {@code CANCELLED}, through the
 * real gax and gRPC stack against a loopback server. The classifier retries {@code CANCELLED}
 * because the call reports it, never because this sink cancelled the future; if a dependency update
 * makes a caller's cancel arrive as an {@code ApiException} with that status, the second test fails
 * and ADR-0048's {@code CANCELLED} bullet needs revisiting.
 */
@Timeout(60)
class CreateTaskCancellationTest {

    private Server server;

    @AfterEach
    void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aStreamResetAfterTheServiceTookTheRequestIsATransientCancelled() throws Exception {
        // The server reads the request and then fails to encode its answer, which resets the
        // stream with RST_STREAM CANCEL: the service may have acted on a request whose caller
        // sees only CANCELLED, the ambiguity a retry resolves for a named task. gRPC logs that
        // encoding failure as a WARNING with its stack trace; it is this fixture, not a defect.
        AtomicInteger received = new AtomicInteger();
        server =
                start(
                        unencodable(),
                        (request, response) -> {
                            received.incrementAndGet();
                            response.onNext(Task.getDefaultInstance());
                            response.onCompleted();
                        });

        try (TaskCreator creator = creator()) {
            ApiFuture<Task> call =
                    creator.createTask(
                            CreateTaskRequest.getDefaultInstance(),
                            Deadline.after(30, TimeUnit.SECONDS));

            Throwable failure =
                    assertThatThrownBy(call::get)
                            .isInstanceOf(ExecutionException.class)
                            .actual()
                            .getCause();
            assertThat(CloudTasksErrorClassifier.statusCode(failure))
                    .isEqualTo(StatusCode.Code.CANCELLED);
            assertThat(CloudTasksErrorClassifier.transientCode(failure))
                    .isEqualTo(StatusCode.Code.CANCELLED);
            assertThat(failure).hasMessageContaining("RST_STREAM");
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void aCallerCancelReachesTheCallbackWithoutAStatus() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        server =
                start(
                        ProtoUtils.marshaller(Task.getDefaultInstance()),
                        (request, response) -> received.countDown());

        try (TaskCreator creator = creator()) {
            ApiFuture<Task> call =
                    creator.createTask(
                            CreateTaskRequest.getDefaultInstance(),
                            Deadline.after(30, TimeUnit.SECONDS));
            CompletableFuture<Throwable> reported = new CompletableFuture<>();
            ApiFutures.addCallback(
                    call,
                    new ApiFutureCallback<Task>() {
                        @Override
                        public void onSuccess(Task task) {
                            reported.complete(null);
                        }

                        @Override
                        public void onFailure(Throwable failure) {
                            reported.complete(failure);
                        }
                    },
                    Runnable::run);
            assertThat(received.await(30, TimeUnit.SECONDS)).isTrue();

            assertThat(call.cancel(true)).isTrue();

            Throwable failure = reported.get(30, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(CancellationException.class);
            assertThat(CloudTasksErrorClassifier.statusCode(failure)).isNull();
        }
    }

    private TaskCreator creator() throws Exception {
        return new DefaultTaskCreatorFactory(
                        null,
                        EmulatorEndpoint.parse("127.0.0.1:" + server.getPort(), "emulatorEndpoint"),
                        null)
                .create();
    }

    private static Server start(
            MethodDescriptor.Marshaller<Task> responses,
            ServerCalls.UnaryMethod<CreateTaskRequest, Task> handler)
            throws Exception {
        MethodDescriptor<CreateTaskRequest, Task> method =
                MethodDescriptor.<CreateTaskRequest, Task>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName("google.cloud.tasks.v2.CloudTasks/CreateTask")
                        .setRequestMarshaller(
                                ProtoUtils.marshaller(CreateTaskRequest.getDefaultInstance()))
                        .setResponseMarshaller(responses)
                        .build();
        return NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(
                        ServerServiceDefinition.builder("google.cloud.tasks.v2.CloudTasks")
                                .addMethod(method, ServerCalls.asyncUnaryCall(handler))
                                .build())
                .build()
                .start();
    }

    /** A response marshaller that cannot encode, so the server resets the stream instead. */
    private static MethodDescriptor.Marshaller<Task> unencodable() {
        MethodDescriptor.Marshaller<Task> delegate =
                ProtoUtils.marshaller(Task.getDefaultInstance());
        return new MethodDescriptor.Marshaller<>() {
            @Override
            public InputStream stream(Task value) {
                throw new IllegalStateException("response cannot be encoded");
            }

            @Override
            public Task parse(InputStream stream) {
                return delegate.parse(stream);
            }
        };
    }
}
