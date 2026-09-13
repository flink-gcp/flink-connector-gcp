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

package io.github.flink.gcp.connector.tier3.smoke;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(90)
class SmokeRecoveryITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    // The embedded cluster ships Capture into tasks in this JVM.
    private static final BlockingQueue<String> EVENTS = new LinkedBlockingQueue<>();
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    @TempDir Path temporary;

    @BeforeEach
    void resetEvidence() {
        EVENTS.clear();
        FAILED.set(false);
    }

    @Test
    void sourceAndVerifierRecoverFromACompletedCheckpoint() throws Exception {
        JobClient job = start("initial", false, null, true, 18_000);
        try {
            String initial = event(line -> line.contains("restored=false"));
            String recovered = event(line -> line.contains("restored=true"));
            assertThat(FAILED).isTrue();
            assertSameLineage(initial, recovered);
            assertThat(Long.parseLong(SmokeVerifierTest.field(recovered, "processed")))
                    .isGreaterThan(1);
            assertProgressContinues(job, recovered);
        } finally {
            cancelIfRunning(job);
        }
    }

    @Test
    void savepointUpgradeContinuesTheSameSequence() throws Exception {
        JobClient initialJob = start("initial", false, null, false, 18_000);
        String first;
        String savepoint;
        try {
            first = event(line -> line.contains("restored=false"));
            savepoint =
                    initialJob
                            .stopWithSavepoint(
                                    false,
                                    temporary.resolve("savepoints").toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(30, TimeUnit.SECONDS);
            initialJob.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        } finally {
            cancelIfRunning(initialJob);
        }
        EVENTS.clear();
        JobClient upgraded = start("upgrade", true, savepoint, false, 18_000);
        try {
            String recovered = event(line -> line.contains("phase=upgrade"));
            assertSameLineage(first, recovered);
            assertThat(recovered).contains("restored=true");
            assertThat(Long.parseLong(SmokeVerifierTest.field(recovered, "processed")))
                    .isGreaterThan(1);
            assertProgressContinues(upgraded, recovered);
        } finally {
            cancelIfRunning(upgraded);
        }
    }

    @Test
    void boundedInputFinishesAfterEveryRecordIsVerified() throws Exception {
        JobClient job = start("initial", false, null, false, 30);
        try {
            job.getJobExecutionResult().get(40, TimeUnit.SECONDS);
            assertThat(event(line -> line.contains("processed=30 "))).contains("sequence=29");
        } finally {
            cancelIfRunning(job);
        }
    }

    @Test
    void upgradeWithoutSavepointFailsInTheRuntime() throws Exception {
        JobClient job = start("upgrade", true, null, false, 18_000);
        try {
            assertThatThrownBy(() -> job.getJobExecutionResult().get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("--require-restored=true");
            assertThat(EVENTS).isEmpty();
        } finally {
            cancelIfRunning(job);
        }
    }

    private static void cancelIfRunning(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            try {
                job.cancel().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                if (!ExceptionUtils.findThrowable(
                                failure, FlinkJobTerminatedWithoutCancellationException.class)
                        .isPresent()) {
                    throw failure;
                }
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
    }

    private JobClient start(
            String phase, boolean requireRestored, String savepoint, boolean fail, long records)
            throws Exception {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        config.set(RestartStrategyOptions.RESTART_STRATEGY, fail ? "fixed-delay" : "none");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        if (savepoint != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(100);
        SmokeOptions options =
                SmokeOptions.parse(
                        "--run-id",
                        "smoke-it",
                        "--phase",
                        phase,
                        "--records",
                        Long.toString(records),
                        "--require-restored",
                        Boolean.toString(requireRestored));
        SmokeJob.progress(env, options)
                .map(new Capture(fail))
                .uid("capture")
                .sinkTo(new DiscardingSink<>())
                .uid("discard");
        return env.executeAsync("smoke-recovery");
    }

    private static String event(Predicate<String> predicate) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < end) {
            String event = EVENTS.poll(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
            assertThat(event).as("smoke progress before the deadline").isNotNull();
            if (predicate.test(event)) {
                return event;
            }
        }
        throw new AssertionError("No matching smoke progress");
    }

    private static void assertProgressContinues(JobClient job, String recovered) throws Exception {
        long restoredCount = Long.parseLong(SmokeVerifierTest.field(recovered, "processed"));
        String continued =
                event(
                        line ->
                                Long.parseLong(SmokeVerifierTest.field(line, "processed"))
                                        > restoredCount);
        assertSameLineage(recovered, continued);
        assertThat(Long.parseLong(SmokeVerifierTest.field(continued, "sequence")))
                .isEqualTo(Long.parseLong(SmokeVerifierTest.field(continued, "processed")) - 1);
        assertThat(job.getJobStatus().get(10, TimeUnit.SECONDS)).isEqualTo(JobStatus.RUNNING);
    }

    private static void assertSameLineage(String initial, String restored) {
        assertThat(SmokeVerifierTest.field(restored, "lineage"))
                .isEqualTo(SmokeVerifierTest.field(initial, "lineage"));
    }

    private static final class Capture extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private final boolean fail;
        private transient boolean sawProgress;
        private transient Map<Long, Boolean> snapshots;

        Capture(boolean fail) {
            this.fail = fail;
        }

        @Override
        public String map(String value) {
            EVENTS.add(value);
            sawProgress = true;
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            snapshots.put(context.getCheckpointId(), sawProgress);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            snapshots = new HashMap<>();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            boolean coveredProgress = Boolean.TRUE.equals(snapshots.get(checkpointId));
            snapshots.keySet().removeIf(id -> id <= checkpointId);
            if (fail && coveredProgress && FAILED.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "Smoke test failure after a checkpoint covered progress");
            }
        }
    }
}
