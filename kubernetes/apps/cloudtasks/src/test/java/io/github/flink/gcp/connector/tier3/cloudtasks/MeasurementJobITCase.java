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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(60)
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_OUT)
class MeasurementJobITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir Path temporary;

    @ParameterizedTest
    @EnumSource(MeasurementOptions.Arm.class)
    void actualJobUsesTheObservedProductionRpcPath(MeasurementOptions.Arm arm) throws Exception {
        Map<Long, Task> accepted = new ConcurrentHashMap<>();
        Set<String> acceptedNames = ConcurrentHashMap.newKeySet();
        MethodDescriptor<CreateTaskRequest, Task> method =
                MethodDescriptor.<CreateTaskRequest, Task>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName("google.cloud.tasks.v2.CloudTasks/CreateTask")
                        .setRequestMarshaller(
                                ProtoUtils.marshaller(CreateTaskRequest.getDefaultInstance()))
                        .setResponseMarshaller(ProtoUtils.marshaller(Task.getDefaultInstance()))
                        .build();
        Server server =
                NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(
                                ServerServiceDefinition.builder("google.cloud.tasks.v2.CloudTasks")
                                        .addMethod(
                                                method,
                                                ServerCalls.asyncUnaryCall(
                                                        (request, response) -> {
                                                            var origin =
                                                                    MeasurementPayload.read(
                                                                            request.getTask()
                                                                                    .getHttpRequest()
                                                                                    .getBody());
                                                            Task task = request.getTask();
                                                            if (task.getName().isEmpty()) {
                                                                task =
                                                                        task.toBuilder()
                                                                                .setName(
                                                                                        request
                                                                                                        .getParent()
                                                                                                + "/tasks/"
                                                                                                + UUID
                                                                                                        .randomUUID())
                                                                                .build();
                                                            }
                                                            if (!acceptedNames.add(
                                                                    task.getName())) {
                                                                response.onError(
                                                                        io.grpc.Status
                                                                                .ALREADY_EXISTS
                                                                                .asRuntimeException());
                                                                return;
                                                            }
                                                            accepted.putIfAbsent(
                                                                    origin.sequence(), task);
                                                            response.onNext(task);
                                                            response.onCompleted();
                                                        }))
                                        .build())
                        .build()
                        .start();
        ByteArrayOutputStream evidence = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(evidence, true, StandardCharsets.UTF_8));
        try {
            var options =
                    MeasurementOptions.parse(MeasurementOptionsTest.arguments("--arm", arm.name()));
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            config.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                    temporary.resolve("checkpoints").toUri().toString());
            config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
            var environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
            CloudTasksMeasurementJob.configure(
                    environment, options, "127.0.0.1:" + server.getPort());
            var job = environment.executeAsync("measurement-path-" + arm);
            try {
                job.getJobExecutionResult().get(45, TimeUnit.SECONDS);
            } finally {
                if (!job.getJobExecutionResult().isDone()) {
                    job.cancel().get(10, TimeUnit.SECONDS);
                }
            }
            assertThat(accepted).hasSize(100);
            assertThat(acceptedNames).hasSize(100);
            var rows =
                    evidence.toString(StandardCharsets.UTF_8)
                            .lines()
                            .filter(line -> line.startsWith("CT1246,"))
                            .toList();
            assertThat(rows).hasSize(100);
            for (String row : rows) {
                String[] fields = row.split(",", -1);
                assertThat(fields).hasSize(16);
                assertThat(fields[14]).isEqualTo("OK");
                assertThat(Long.parseLong(fields[13])).isGreaterThanOrEqualTo(0);
            }
            for (long sequence = 0; sequence < 100; sequence++) {
                var task = accepted.get(sequence);
                assertThat(task).isNotNull();
                assertThat(task.getHttpRequest().getBody().size()).isEqualTo(1024);
                if (arm == MeasurementOptions.Arm.STAGED_RANDOM
                        || arm == MeasurementOptions.Arm.NAMED_RANDOM_CONTROL) {
                    assertThat(task.getName()).matches(".*/tasks/[a-f0-9]{32}");
                } else if (arm != MeasurementOptions.Arm.UNNAMED) {
                    assertThat(task.getName()).matches(".*/tasks/[a-f0-9]{64}");
                }
            }
        } finally {
            System.setOut(original);
            server.shutdownNow();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
