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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks source/identity state recovery with a discard sink, independently of BigQuery semantics.
 */
@Timeout(90)
class RecoverySourceITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static final BlockingQueue<Long> VALUES = new LinkedBlockingQueue<>();
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static final AtomicBoolean RESTORED = new AtomicBoolean();
    @TempDir Path temporary;

    @BeforeEach
    void reset() {
        VALUES.clear();
        FAILED.set(false);
        RESTORED.set(false);
    }

    @Test
    void completedCheckpointRestoresTheActualSourceAndIdentityOperator() throws Exception {
        var job = start(null, true);
        try {
            job.getJobExecutionResult().get(45, TimeUnit.SECONDS);
            assertThat(FAILED).isTrue();
            assertThat(RESTORED).isTrue();
            assertThat(VALUES).contains(0L, 99L);
            assertThat(VALUES.stream().distinct().count()).isEqualTo(100);
        } finally {
            cancel(job);
        }
    }

    @Test
    void savepointUpgradeResumesTheSameFiniteInput() throws Exception {
        var initial = start(null, false);
        String savepoint;
        try {
            Long first = VALUES.poll(30, TimeUnit.SECONDS);
            assertThat(first).isEqualTo(0);
            savepoint =
                    initial.stopWithSavepoint(
                                    false,
                                    temporary.resolve("savepoints").toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(30, TimeUnit.SECONDS);
        } finally {
            cancel(initial);
        }
        long lastInitial = VALUES.stream().mapToLong(Long::longValue).max().orElse(0);
        VALUES.clear();
        var upgrade = start(savepoint, false);
        try {
            jobResult(upgrade);
            assertThat(RESTORED).isTrue();
            assertThat(VALUES.peek()).isEqualTo(lastInitial + 1);
            assertThat(VALUES).contains(99L);
        } finally {
            cancel(upgrade);
        }
    }

    private void jobResult(JobClient job) throws Exception {
        job.getJobExecutionResult().get(45, TimeUnit.SECONDS);
    }

    private JobClient start(String savepoint, boolean fail) throws Exception {
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
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--records",
                                "100",
                                "--bytes-per-second",
                                "10240",
                                "--phase",
                                savepoint == null ? "initial" : "upgrade",
                                "--require-restored",
                                Boolean.toString(savepoint != null)));
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        BigQueryRecoveryJob.input(env, options)
                .map(new Capture(fail))
                .uid("capture")
                .sinkTo(new DiscardingSink<>())
                .uid("discard");
        return env.executeAsync("BigQuery input recovery");
    }

    private static void cancel(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            job.cancel().get(10, TimeUnit.SECONDS);
        }
    }

    private static final class Capture extends RichMapFunction<Long, Long>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private final boolean fail;
        private transient boolean progress;
        private transient Map<Long, Boolean> snapshots;

        Capture(boolean fail) {
            this.fail = fail;
        }

        @Override
        public Long map(Long value) {
            VALUES.add(value);
            progress = true;
            return value;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            snapshots = new HashMap<>();
            if (context.isRestored()) {
                RESTORED.set(true);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            snapshots.put(context.getCheckpointId(), progress);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            boolean covered = Boolean.TRUE.equals(snapshots.get(checkpointId));
            snapshots.keySet().removeIf(id -> id <= checkpointId);
            if (fail && covered && FAILED.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "Injected failure after a checkpoint covered input");
            }
        }
    }
}
