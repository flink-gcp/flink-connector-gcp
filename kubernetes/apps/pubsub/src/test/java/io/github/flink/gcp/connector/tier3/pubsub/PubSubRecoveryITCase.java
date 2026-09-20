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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.PubSubEmulatorContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(300)
@Execution(ExecutionMode.SAME_THREAD)
class PubSubRecoveryITCase {
    static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @TempDir Path temporary;

    @BeforeAll
    static void startEmulator() {
        EMULATOR.start();
    }

    @AfterAll
    static void stopEmulator() {
        EMULATOR.stop();
    }

    @ParameterizedTest
    @CsvSource({"1,2", "2,1"})
    void productionSourceAndSinkRestoreAndReachBothSubscriptions(int from, int to)
            throws Exception {
        RecoveryOptions initial =
                new RecoveryOptions("it-" + UUID.randomUUID(), 2, from, "initial", false);
        RecoveryOptions upgrade = new RecoveryOptions(initial.runId, 2, to, "upgrade", true);
        try (PubSubTestClients clients =
                        PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
                AutoCloseable resources = () -> deleteResources(clients, initial)) {
            createResources(clients, initial);
            PubSubRecoveryReport report = new PubSubRecoveryReport(initial);
            publish(clients, initial, 0);
            JobClient first = submit(initial, null);
            try (AutoCloseable firstCleanup = () -> cancelIfRunning(first)) {
                collect(clients, initial, report, 2, false);
                String savepoint =
                        first.stopWithSavepoint(
                                        false,
                                        temporary.toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                .get(60, TimeUnit.SECONDS);
                publish(clients, initial, 1);
                JobClient second = submit(upgrade, savepoint);
                try (AutoCloseable secondCleanup = () -> cancelIfRunning(second)) {
                    collect(clients, upgrade, report, 4, true);
                    assertThat(second.getJobStatus().get(10, TimeUnit.SECONDS))
                            .isEqualTo(JobStatus.RUNNING);
                    assertThat(report.complete()).startsWith("logical_inputs=4 ");
                }
            }
        }
    }

    @Test
    void taskManagerLossRestoresACompletedCheckpoint(@InjectMiniCluster MiniCluster cluster)
            throws Exception {
        RecoveryOptions options =
                new RecoveryOptions("it-" + UUID.randomUUID(), 2, 2, "initial", false);
        try (PubSubTestClients clients =
                        PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
                AutoCloseable resources = () -> deleteResources(clients, options)) {
            createResources(clients, options);
            publish(clients, options, 0);
            JobClient job = submit(options, null, true);
            try (AutoCloseable jobCleanup = () -> cancelIfRunning(job)) {
                PubSubRecoveryReport report = new PubSubRecoveryReport(options);
                collect(clients, options, report, 2, false);
                cluster.triggerCheckpoint(job.getJobID(), CheckpointType.FULL)
                        .get(60, TimeUnit.SECONDS);
                // Restart both TMs so this does not depend on the scheduler's slot placement.
                cluster.terminateTaskManager(0).get(10, TimeUnit.SECONDS);
                cluster.terminateTaskManager(1).get(10, TimeUnit.SECONDS);
                cluster.startTaskManager();
                cluster.startTaskManager();
                publish(clients, options, 1);
                collect(clients, options, report, 4, true);
                assertThat(job.getJobStatus().get(10, TimeUnit.SECONDS))
                        .isEqualTo(JobStatus.RUNNING);
                assertThat(report.complete()).startsWith("logical_inputs=4 ");
            }
        }
    }

    @Test
    void freshUpgradeFailsWithoutLosingItsCauseDuringCleanup() throws Exception {
        RecoveryOptions options =
                new RecoveryOptions("it-" + UUID.randomUUID(), 1, 1, "upgrade", true);
        try (PubSubTestClients clients =
                        PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
                AutoCloseable resources = () -> deleteResources(clients, options)) {
            createResources(clients, options);
            JobClient job = submit(options, null);
            try (AutoCloseable jobCleanup = () -> cancelIfRunning(job)) {
                assertThatThrownBy(() -> job.getJobExecutionResult().get(60, TimeUnit.SECONDS))
                        .hasStackTraceContaining("--require-restored=true");
                assertThat(job.getJobStatus().get(10, TimeUnit.SECONDS))
                        .isEqualTo(JobStatus.FAILED);
            }
        }
    }

    private static void cancelIfRunning(JobClient job) throws Exception {
        if (job == null || job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            return;
        }
        try {
            job.cancel().get(10, TimeUnit.SECONDS);
        } catch (ExecutionException failure) {
            if (ExceptionUtils.findThrowable(
                            failure, FlinkJobTerminatedWithoutCancellationException.class)
                    .isPresent()) {
                return;
            }
            throw failure;
        }
        // Cancellation acknowledgement precedes task termination in the shared cluster.
        try {
            job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
        } catch (ExecutionException failure) {
            if (!ExceptionUtils.findThrowable(failure, JobCancellationException.class)
                    .isPresent()) {
                throw failure;
            }
        }
    }

    private static void createResources(PubSubTestClients clients, RecoveryOptions options) {
        for (String name : List.of(options.input(0), options.input(1), options.output())) {
            clients.topicAdmin().createTopic(TopicName.of(RecoveryOptions.PROJECT, name));
            clients.subscriptionAdmin()
                    .createSubscription(
                            SubscriptionName.of(RecoveryOptions.PROJECT, name),
                            TopicName.of(RecoveryOptions.PROJECT, name),
                            PushConfig.getDefaultInstance(),
                            10);
        }
    }

    private static void deleteResources(PubSubTestClients clients, RecoveryOptions options) {
        for (String name : List.of(options.input(0), options.input(1), options.output())) {
            clients.subscriptionAdmin()
                    .deleteSubscription(SubscriptionName.of(RecoveryOptions.PROJECT, name));
            clients.topicAdmin().deleteTopic(TopicName.of(RecoveryOptions.PROJECT, name));
        }
    }

    private JobClient submit(RecoveryOptions options, String restore) throws Exception {
        return submit(options, restore, false);
    }

    private JobClient submit(RecoveryOptions options, String restore, boolean restart)
            throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, restart ? "fixed-delay" : "none");
        if (restart) {
            config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
            config.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    Duration.ofSeconds(1));
        }
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        if (restore != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(options.parallelism);
        env.setMaxParallelism(128);
        env.enableCheckpointing(restart ? 600000 : 500);
        PubSubRecoveryJob.attach(env, options, EMULATOR.getEmulatorEndpoint());
        return env.executeAsync();
    }

    private static void publish(PubSubTestClients clients, RecoveryOptions options, int sequence)
            throws Exception {
        for (int subscription = 0; subscription < 2; subscription++) {
            clients.publishOrdered(
                    TopicName.of(RecoveryOptions.PROJECT, options.input(subscription)),
                    null,
                    null,
                    RecoveryPayload.input(options, subscription, sequence));
        }
    }

    private static void collect(
            PubSubTestClients clients,
            RecoveryOptions options,
            PubSubRecoveryReport report,
            int count,
            boolean restored)
            throws Exception {
        Set<Integer> restoredInputs = new HashSet<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        await(
                "output from both input subscriptions",
                Duration.ofSeconds(60),
                () -> {
                    for (PubsubMessage message : pullOutputs(clients, options, deadline)) {
                        String value = message.getData().toStringUtf8();
                        report.accept(message.getMessageId(), value);
                        String[] fields = value.split("\\|", -1);
                        if (fields[3].equals("1")
                                && fields[7].equals(options.phase)
                                && fields[8].equals("true")) {
                            restoredInputs.add(Integer.parseInt(fields[2]));
                        }
                    }
                    return report.distinctInputs() >= count
                            && (!restored || restoredInputs.size() == 2);
                },
                () ->
                        "Observed logical inputs="
                                + report.distinctInputs()
                                + ", restored inputs="
                                + restoredInputs);
    }

    private static List<PubsubMessage> pullOutputs(
            PubSubTestClients clients, RecoveryOptions options, long deadline) {
        String subscription = SubscriptionName.format(RecoveryOptions.PROJECT, options.output());
        PullResponse response;
        try {
            response =
                    awaitRpc(
                            clients.subscriptionAdmin()
                                    .getStub()
                                    .pullCallable()
                                    .futureCall(
                                            PullRequest.newBuilder()
                                                    .setSubscription(subscription)
                                                    .setMaxMessages(10)
                                                    .build()),
                            deadline);
        } catch (TimeoutException timeout) {
            // No response is no evidence; the enclosing await reports its missing-ID diagnosis.
            return List.of();
        }
        List<PubsubMessage> messages = new ArrayList<>();
        AcknowledgeRequest.Builder ack =
                AcknowledgeRequest.newBuilder().setSubscription(subscription);
        for (ReceivedMessage received : response.getReceivedMessagesList()) {
            messages.add(received.getMessage());
            ack.addAckIds(received.getAckId());
        }
        if (!messages.isEmpty()) {
            try {
                awaitRpc(
                        clients.subscriptionAdmin()
                                .getStub()
                                .acknowledgeCallable()
                                .futureCall(ack.build()),
                        deadline);
            } catch (TimeoutException timeout) {
                throw new IllegalStateException(
                        "Output evidence acknowledgement timed out", timeout);
            }
        }
        return messages;
    }

    private static <T> T awaitRpc(ApiFuture<T> pending, long deadline) throws TimeoutException {
        try {
            return pending.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Output evidence RPC was interrupted", interrupted);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Output evidence RPC failed", failure.getCause());
        } finally {
            if (!pending.isDone()) {
                pending.cancel(true);
            }
        }
    }
}
