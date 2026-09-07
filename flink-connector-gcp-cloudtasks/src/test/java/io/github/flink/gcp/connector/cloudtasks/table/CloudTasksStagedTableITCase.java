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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the production SQL factory, transport and committer against controlled RPC outcomes.
 */
@Timeout(120)
class CloudTasksStagedTableITCase {
    private static final InMemoryReporter REPORTER = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(REPORTER.addToConfiguration(new Configuration()))
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir Path temporary;
    private final List<JobClient> jobs = new ArrayList<>();
    private final List<CreateTaskRequest> received = new CopyOnWriteArrayList<>();
    private final Map<String, Task> created = new ConcurrentHashMap<>();
    private final AtomicBoolean loseResponse = new AtomicBoolean();
    private final AtomicBoolean holdResponse = new AtomicBoolean();
    private Server service;

    private String endpoint() throws Exception {
        if (service == null) {
            var method =
                    MethodDescriptor.<CreateTaskRequest, Task>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName("google.cloud.tasks.v2.CloudTasks/CreateTask")
                            .setRequestMarshaller(
                                    ProtoUtils.marshaller(CreateTaskRequest.getDefaultInstance()))
                            .setResponseMarshaller(ProtoUtils.marshaller(Task.getDefaultInstance()))
                            .build();
            service =
                    NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                            .addService(
                                    ServerServiceDefinition.builder(
                                                    "google.cloud.tasks.v2.CloudTasks")
                                            .addMethod(
                                                    method,
                                                    ServerCalls.asyncUnaryCall(this::create))
                                            .build())
                            .build()
                            .start();
        }
        return "127.0.0.1:" + service.getPort();
    }

    private void create(CreateTaskRequest request, StreamObserver<Task> response) {
        received.add(request);
        Task previous = created.putIfAbsent(request.getTask().getName(), request.getTask());
        if (holdResponse.get()) {
            return;
        }
        if (loseResponse.compareAndSet(true, false)) {
            response.onError(Status.UNAVAILABLE.asRuntimeException());
        } else if (previous != null) {
            response.onError(Status.ALREADY_EXISTS.asRuntimeException());
        } else {
            response.onNext(request.getTask());
            response.onCompleted();
        }
    }

    @AfterEach
    void close() throws Exception {
        try {
            for (var job : jobs) {
                if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                    job.cancel().get(10, TimeUnit.SECONDS);
                }
                try {
                    job.getJobExecutionResult().get(20, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException expected) {
                    // Failed and canceled recovery attempts are part of these scenarios.
                }
            }
        } finally {
            if (service != null) {
                service.shutdownNow();
                assertThat(service.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointCommitRestoresTheAcceptedNameAfterResponseLoss(
            boolean appEngine, @InjectMiniCluster MiniCluster cluster) throws Exception {
        loseResponse.set(true);
        JobClient first = start(null, "fail", false, false, appEngine, false);
        awaitGauge(first, "stagedTasks", 2);
        assertThat(received).isEmpty();
        assertThat(gauge(first, "oldestStagedTaskAgeMillis")).isGreaterThanOrEqualTo(0);
        String checkpoint = checkpoint(cluster, first);
        assertThatThrownBy(() -> first.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("UNAVAILABLE");
        assertThat(created).hasSize(1);
        Task accepted = created.values().iterator().next();
        assertThat(accepted.getScheduleTime().getSeconds()).isEqualTo(1577836800);
        assertReadable(checkpoint);
        JobClient restored = start(checkpoint, "fail", false, false, appEngine, false);
        awaitCounter(restored, "tasksDeduplicated", 1);
        awaitCounter(restored, "successfulCommittables", 1);
        assertThat(created).hasSize(2);
        assertThat(received).hasSize(3);
        assertThat(received.get(1)).isEqualTo(received.get(0));
        assertThat(created.get(accepted.getName())).isEqualTo(accepted);
        assertThat(counter(restored, "expiredEnvelopesFailed")).isZero();
        assertThat(counter(restored, "expiredEnvelopesAssumedCommitted")).isZero();
        assertThat(created.values())
                .allSatisfy(
                        task -> {
                            assertThat(task.hasAppEngineHttpRequest()).isEqualTo(appEngine);
                            assertThat(task.getScheduleTime())
                                    .isEqualTo(accepted.getScheduleTime());
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"assume-committed", "drop", "create-anyway"})
    void expiredStateIsRetainedAndOnlyTheExplicitRecoveryPolicyCanReleaseIt(
            String policy, @InjectMiniCluster MiniCluster cluster) throws Exception {
        JobClient first = start(null, "fail", true, false, false, false);
        awaitGauge(first, "stagedTasks", 2);
        awaitGauge(first, "stagedReplayBudgetMillis", 0);
        String checkpoint = checkpoint(cluster, first);
        assertThatThrownBy(() -> first.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("envelope expired");
        assertReadable(checkpoint);
        assertThat(received).isEmpty();
        assertThat(counter(first, "expiredEnvelopesFailed")).isEqualTo(1);
        JobClient repeated = start(checkpoint, "fail", true, false, false, false);
        assertThatThrownBy(() -> repeated.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("envelope expired");
        assertReadable(checkpoint);
        assertThat(received).isEmpty();
        // The persisted expired deadline still governs with relaxed current settings.
        JobClient overridden = start(checkpoint, policy, false, false, false, false);
        String metric =
                policy.equals("assume-committed")
                        ? "expiredEnvelopesAssumedCommitted"
                        : policy.equals("drop")
                                ? "expiredEnvelopesDropped"
                                : "expiredEnvelopeCreatesAuthorized";
        awaitCounter(overridden, metric, 2);
        awaitCounter(
                overridden,
                policy.equals("assume-committed")
                        ? "alreadyCommittedCommittables"
                        : "successfulCommittables",
                2);
        assertThat(created).hasSize(policy.equals("create-anyway") ? 2 : 0);
        assertThat(counter(overridden, "tasksDeduplicated")).isZero();
    }

    @Test
    void blockedRpcExposesCurrentCommitTimeAndRetainedPendingCount(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        holdResponse.set(true);
        JobClient job = start(null, "fail", false, false, false, false);
        awaitGauge(job, "stagedTasks", 2);
        checkpoint(cluster, job);
        Awaits.await("first accepted request", Duration.ofSeconds(20), () -> created.size() == 1);
        assertThat(gauge(job, "currentCommitOldestTaskAgeMillis")).isGreaterThanOrEqualTo(0);
        assertThat(gauge(job, "currentCommitReplayBudgetMillis")).isBetween(1L, 3_280_000L);
        assertThat(gauge(job, "pendingCommittables")).isEqualTo(2);
        assertThat(gauge(job, "stagedReplayBudgetMillis")).isEqualTo(-1);
        assertThat(received).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void boundedStreamingCommitsItsTailAndStableMetadataKeepsCollisionSemantics(boolean stableKey)
            throws Exception {
        JobClient job = start(null, "fail", false, stableKey, false, true);
        job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        assertThat(created).hasSize(stableKey ? 1 : 2);
        assertThat(received).hasSize(2);
        assertThat(counter(job, "tasksDeduplicated")).isEqualTo(stableKey ? 1 : 0);
        assertThat(counter(job, "expiredEnvelopesFailed")).isZero();
    }

    private JobClient start(
            String restore,
            String policy,
            boolean shortWindow,
            boolean stableKey,
            boolean appEngine,
            boolean finite)
            throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 0);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (restore != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
        }
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofHours(1).toMillis());
        var source =
                new DataGeneratorSource<Long>(
                        value -> value,
                        finite ? 2 : Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(20),
                        Types.LONG);
        var records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                        .uid("records")
                        .filter(value -> value < 2)
                        .map(value -> "same")
                        .returns(Types.STRING);
        var table = StreamTableEnvironment.create(env);
        table.createTemporaryView("records", table.fromDataStream(records).as("payload"));
        String target =
                appEngine
                        ? "'target.type'='app-engine', 'app-engine.relative-uri'='/task',"
                        : "'http.url'='https://example.com/task',";
        table.executeSql(
                "CREATE TABLE tasks (payload STRING,"
                        + " scheduled TIMESTAMP_LTZ(3) METADATA FROM 'schedule-time'"
                        + (stableKey ? ", task_key STRING METADATA FROM 'task-id'" : "")
                        + ") WITH ('connector'='cloud-tasks','project'='p','location'='l','queue'='q',"
                        + "'format'='json',"
                        + target
                        + "'emulator-endpoint'='"
                        + endpoint()
                        + "',"
                        + "'sink.delivery-guarantee'='exactly-once',"
                        + "'sink.in-flight.max-tasks'='1','sink.recovery.max-attempts'='1',"
                        + "'sink.staged.expired-envelope-policy'='"
                        + policy
                        + "'"
                        + (shortWindow
                                ? ",'sink.staged.name-retention'='100 ms',"
                                        + "'sink.staged.clock-skew-allowance'='0 ms','sink.staged.request-timeout'='10 ms'"
                                : "")
                        + ")");
        var result =
                table.executeSql(
                        "INSERT INTO tasks SELECT payload,"
                                + " TO_TIMESTAMP_LTZ(1577836800000, 3)"
                                + (stableKey ? ", 'logical-task'" : "")
                                + " FROM records");
        var job = result.getJobClient().orElseThrow();
        jobs.add(job);
        return job;
    }

    private String checkpoint(MiniCluster cluster, JobClient job) throws Exception {
        return cluster.triggerCheckpoint(job.getJobID()).get(30, TimeUnit.SECONDS);
    }

    private static void assertReadable(String pointer) throws Exception {
        Path path = Path.of(java.net.URI.create(pointer));
        assertThat(Files.isReadable(Files.isDirectory(path) ? path.resolve("_metadata") : path))
                .isTrue();
    }

    private static long gauge(JobClient job, String name) {
        return REPORTER.findMetric(job.getJobID(), ".*\\." + name + "$")
                .map(metric -> ((Number) ((Gauge<?>) metric).getValue()).longValue())
                .orElse(Long.MIN_VALUE);
    }

    private static long counter(JobClient job, String name) {
        var registered = REPORTER.findMetrics(job.getJobID(), ".*\\." + name + "$");
        if (registered.isEmpty()) {
            return Long.MIN_VALUE;
        }
        return registered.values().stream()
                .map(Counter.class::cast)
                .mapToLong(Counter::getCount)
                .sum();
    }

    private static void awaitGauge(JobClient job, String name, long expected)
            throws InterruptedException {
        Awaits.await(
                name + "=" + expected, Duration.ofSeconds(20), () -> gauge(job, name) == expected);
    }

    private static void awaitCounter(JobClient job, String name, long expected)
            throws InterruptedException {
        Awaits.await(
                name + "=" + expected,
                Duration.ofSeconds(20),
                () -> counter(job, name) == expected);
    }
}
