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
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedCreateTaskSink;
import io.github.flink.gcp.connector.testutils.Awaits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the deployed graph against an in-process server and checks names the way the oracle will.
 */
@Timeout(180)
@Execution(ExecutionMode.SAME_THREAD)
class RecoveryJobITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir Path temporary;

    @Test
    void everySequenceBecomesTheTaskItsKeyNames() throws Exception {
        try (var server = server()) {
            var options = RecoveryOptions.parse(RecoveryOptionsTest.arguments());
            // The eager writer would make the same names; only the staged sink commits per
            // checkpoint.
            assertThat(CloudTasksRecoveryJob.sink(options, server.endpoint()))
                    .isInstanceOf(CloudTasksStagedCreateTaskSink.class);
            var job = start(options, server, null);
            try {
                job.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            } finally {
                cancel(job);
            }
            assertEverySequenceNamedOnce(server, options);
            // Without a restore nothing is committed twice.
            assertThat(server.refused()).isZero();
        }
    }

    @Test
    void savepointUpgradeResumesTheInputAndEndsWithEveryName() throws Exception {
        try (var server = server()) {
            String[] slow = {"--records", "300", "--records-per-second", "20"};
            var initial = RecoveryOptions.parse(RecoveryOptionsTest.arguments(slow));
            var first = start(initial, server, null);
            String savepoint;
            try {
                Awaits.await(
                        "tasks committed before the savepoint",
                        Duration.ofSeconds(30),
                        () -> server.names().size() >= 20,
                        () -> "created=" + server.names().size());
                savepoint =
                        first.stopWithSavepoint(
                                        false,
                                        temporary.resolve("savepoints").toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                .get(30, TimeUnit.SECONDS);
            } finally {
                cancel(first);
            }
            assertThat(server.names().size()).isLessThan(300);
            var upgrade =
                    RecoveryOptions.parse(
                            RecoveryOptionsTest.arguments(
                                    "--records",
                                    "300",
                                    "--records-per-second",
                                    "20",
                                    "--phase",
                                    "upgrade",
                                    "--require-restored",
                                    "true"));
            var second = start(upgrade, server, savepoint);
            try {
                second.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            } finally {
                cancel(second);
            }
            assertEverySequenceNamedOnce(server, initial);
        }
    }

    private static FakeCloudTasks server() throws Exception {
        return new FakeCloudTasks(
                task -> {
                    String key = task.getHttpRequest().getBody().toString(StandardCharsets.UTF_8);
                    return Long.parseLong(key.substring(key.lastIndexOf('/') + 1));
                });
    }

    /** Every sequence was accepted once, under the name its own key derives, and nothing else. */
    private static void assertEverySequenceNamedOnce(
            FakeCloudTasks server, RecoveryOptions options) {
        assertThat(server.accepted().keySet())
                .containsExactlyInAnyOrderElementsOf(
                        LongStream.range(0, options.records).boxed().toList());
        server.accepted()
                .forEach(
                        (sequence, task) ->
                                assertThat(task.getName())
                                        .as("task name of sequence %d", sequence)
                                        .isEqualTo(expectedName(options, sequence)));
        assertThat(server.names()).hasSize((int) options.records);
    }

    /** What the oracle derives: the queue's task name for the SHA-256 of a sequence's key. */
    private static String expectedName(RecoveryOptions options, long sequence) {
        byte[] key = (options.runId + "/" + sequence).getBytes(StandardCharsets.UTF_8);
        try {
            return "projects/"
                    + RecoveryOptions.PROJECT
                    + "/locations/"
                    + RecoveryOptions.LOCATION
                    + "/queues/"
                    + options.queue
                    + "/tasks/"
                    + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private JobClient start(
            RecoveryOptions options, FakeCloudTasks server, @Nullable String savepoint)
            throws Exception {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepoint != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        CloudTasksRecoveryJob.configure(
                env,
                options,
                CloudTasksRecoveryJob.sink(options, server.endpoint()),
                Duration.ofMillis(200));
        return env.executeAsync("Cloud Tasks recovery " + options.phase);
    }

    private static void cancel(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            job.cancel().get(10, TimeUnit.SECONDS);
        }
    }
}
