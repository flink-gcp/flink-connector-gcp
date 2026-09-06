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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions.ExpiredEnvelopePolicy;
import io.github.flink.gcp.connector.testutils.Awaits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real checkpoint storage and job recovery, with deterministic service effects in the same JVM. */
@Timeout(120)
class CloudTasksStagedRecoveryITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static final Map<String, Control> CONTROLS = new ConcurrentHashMap<>();
    @TempDir Path temporary;
    private final String runId = UUID.randomUUID().toString();
    private final StagedCreationTestSink.Probe probe = new StagedCreationTestSink.Probe();
    private final Control control = new Control();
    private final List<JobClient> jobs = new ArrayList<>();

    CloudTasksStagedRecoveryITCase() {
        StagedCreationTestSink.PROBES.put(runId, probe);
        CONTROLS.put(runId, control);
    }

    @AfterEach
    void cancelJobsAndReleaseRegistries() throws Exception {
        try {
            for (JobClient job : jobs) {
                if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                    job.cancel().get(10, TimeUnit.SECONDS);
                }
                try {
                    job.getJobExecutionResult().get(15, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException ignored) {
                    /* Expected failed/canceled jobs. */
                }
            }
        } finally {
            StagedCreationTestSink.PROBES.remove(runId);
            CONTROLS.remove(runId);
        }
    }

    @Test
    void cancellationRetainsPendingTasksAndRestoresTheirExactIdentity(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        var pending = SettableApiFuture.<Task>create();
        probe.actions.add(request -> pending);
        control.limit.set(2);
        JobClient first = start(null, ExpiredEnvelopePolicy.FAIL, false, false);
        awaitAccepted(2);
        String checkpoint = checkpoint(cluster, first);
        assertThat(probe.arrivals.poll(20, TimeUnit.SECONDS)).isNotNull();
        var originalRequest = probe.requests.get(0);
        first.cancel().get(10, TimeUnit.SECONDS);
        cluster.requestJobResult(first.getJobID()).get(20, TimeUnit.SECONDS);
        assertThat(pending.isCancelled()).isTrue();
        assertReadable(checkpoint);
        assertThat(probe.created).isEmpty();
        JobClient second = start(checkpoint, ExpiredEnvelopePolicy.FAIL, false, false);
        awaitCreated(2);
        assertThat(probe.requests.get(1)).isEqualTo(originalRequest);
        assertThat(probe.serialized.get()).isEqualTo(2);
        assertThat(probe.identities.get()).isEqualTo(2);
        assertThat(second.getJobStatus().get(10, TimeUnit.SECONDS)).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void terminalExpiryRetainsReadableStateAndFailsEveryRestoreBeforeSending(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        control.limit.set(2);
        JobClient first = start(null, ExpiredEnvelopePolicy.FAIL, false, false);
        awaitAccepted(2);
        probe.wall.set(3_281_000);
        String checkpoint = checkpoint(cluster, first);
        assertThatThrownBy(() -> first.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("Cloud Tasks envelope expired");
        assertReadable(checkpoint);
        JobClient second = start(checkpoint, ExpiredEnvelopePolicy.FAIL, false, false);
        assertThatThrownBy(() -> second.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("Cloud Tasks envelope expired");
        assertReadable(checkpoint);
        assertThat(probe.requests).isEmpty();
        assertThat(probe.identities.get()).isEqualTo(2);
    }

    @Test
    void finishedStopWithSavepointReplaysWithinTheWindowAndNeedsAnExplicitOverrideAfterIt(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        control.limit.set(2);
        JobClient first = start(null, ExpiredEnvelopePolicy.FAIL, false, false);
        awaitAccepted(2);
        String savepoint =
                first.stopWithSavepoint(
                                false,
                                temporary.resolve("savepoints").toUri().toString(),
                                SavepointFormatType.CANONICAL)
                        .get(30, TimeUnit.SECONDS);
        first.getJobExecutionResult().get(20, TimeUnit.SECONDS);
        assertThat(first.getJobStatus().get(10, TimeUnit.SECONDS)).isEqualTo(JobStatus.FINISHED);
        assertThat(probe.created).hasSize(2);
        assertReadable(savepoint);
        JobClient within = start(savepoint, ExpiredEnvelopePolicy.FAIL, false, false);
        Awaits.await(
                "savepoint replay by original name",
                Duration.ofSeconds(20),
                () -> probe.requests.size() == 4);
        assertThat(probe.created).hasSize(2);
        within.cancel().get(10, TimeUnit.SECONDS);
        cluster.requestJobResult(within.getJobID()).get(20, TimeUnit.SECONDS);
        probe.wall.set(3_281_000);
        JobClient expired = start(savepoint, ExpiredEnvelopePolicy.FAIL, false, false);
        assertThatThrownBy(() -> expired.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("envelope expired");
        JobClient overridden =
                start(savepoint, ExpiredEnvelopePolicy.ASSUME_COMMITTED, false, false);
        Awaits.await(
                "the override job to run",
                Duration.ofSeconds(20),
                () -> status(overridden) == JobStatus.RUNNING);
        checkpoint(cluster, overridden);
        assertThat(probe.requests).hasSize(4);
        assertThat(probe.created).hasSize(2);
    }

    @Test
    void downgradeRefusesUnmappedStateAndTheExplicitFlagLosesOnlyTheOwnedTasks(
            @InjectMiniCluster MiniCluster cluster) throws Exception {
        probe.actions.add(request -> SettableApiFuture.create());
        control.limit.set(2);
        JobClient first = start(null, ExpiredEnvelopePolicy.FAIL, false, false);
        awaitAccepted(2);
        String checkpoint = checkpoint(cluster, first);
        assertThat(probe.arrivals.poll(20, TimeUnit.SECONDS)).isNotNull();
        first.cancel().get(10, TimeUnit.SECONDS);
        cluster.requestJobResult(first.getJobID()).get(20, TimeUnit.SECONDS);
        assertReadable(checkpoint);
        JobClient refused = start(checkpoint, ExpiredEnvelopePolicy.FAIL, true, false);
        assertThatThrownBy(() -> refused.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("allowNonRestoredState");
        JobClient allowed = start(checkpoint, ExpiredEnvelopePolicy.FAIL, true, true);
        Awaits.await(
                "the explicit state-dropping downgrade to run",
                Duration.ofSeconds(20),
                () -> status(allowed) == JobStatus.RUNNING);
        checkpoint(cluster, allowed);
        assertThat(probe.created).isEmpty();
        assertThat(probe.requests).hasSize(1);
        assertThat(control.replayedRecords.get()).isZero();
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void failedStopIsNonRecoverableAndOnlyItsSavepointPreservesTheOwnedIdentities(
            boolean unrelatedFailure, boolean followRunbook, @InjectMiniCluster MiniCluster cluster)
            throws Exception {
        control.autoRestart = true;
        JobClient first = start(null, ExpiredEnvelopePolicy.FAIL, false, false);
        String baseline = checkpoint(cluster, first);
        Awaits.await(
                "baseline completion notification",
                Duration.ofSeconds(20),
                () -> control.completed.get() > 0);
        assertReadable(baseline);
        if (unrelatedFailure) {
            control.failNotification.set(true);
        } else {
            probe.actions.add(probe::accept);
            probe.actions.add(
                    request ->
                            com.google.api.core.ApiFutures.immediateFailedFuture(
                                    io.grpc.Status.INVALID_ARGUMENT.asRuntimeException()));
        }
        control.limit.set(2);
        awaitAccepted(2);
        Path savepointDirectory = temporary.resolve("failed-stop");
        var stopped =
                first.stopWithSavepoint(
                        false,
                        savepointDirectory.toUri().toString(),
                        SavepointFormatType.CANONICAL);
        assertThatThrownBy(() -> stopped.get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThatThrownBy(() -> first.getJobExecutionResult().get(20, TimeUnit.SECONDS))
                .hasStackTraceContaining("The failure is not recoverable")
                .hasStackTraceContaining("StopWithSavepointStoppingException");
        assertThat(status(first)).isEqualTo(JobStatus.FAILED);
        assertThat(control.initializations.get()).isEqualTo(1);
        assertThat(probe.serialized.get()).isEqualTo(2);
        Path saved;
        try (var files = Files.walk(savepointDirectory)) {
            saved =
                    files.filter(path -> path.getFileName().toString().equals("_metadata"))
                            .findFirst()
                            .orElseThrow()
                            .getParent();
        }
        assertReadable(saved.toUri().toString());
        assertThat(probe.created).hasSize(unrelatedFailure ? 2 : 1);
        if (followRunbook) {
            var originalRequests = List.copyOf(probe.requests);
            JobClient recovered =
                    start(saved.toUri().toString(), ExpiredEnvelopePolicy.FAIL, false, false);
            Awaits.await(
                    "saved envelopes to be replayed by the recovery job",
                    Duration.ofSeconds(20),
                    () -> probe.requests.size() == originalRequests.size() + 2);
            checkpoint(cluster, recovered);
            assertThat(probe.requests.subList(originalRequests.size(), probe.requests.size()))
                    .containsExactlyElementsOf(originalRequests);
            assertThat(probe.serialized.get()).isEqualTo(2);
            assertThat(probe.identities.get()).isEqualTo(2);
            awaitCreated(2);
            assertThat(
                            probe.created.values().stream()
                                    .map(task -> task.getHttpRequest().getBody().toStringUtf8())
                                    .distinct()
                                    .count())
                    .isEqualTo(2);
        } else {
            JobClient wrongRestore = start(baseline, ExpiredEnvelopePolicy.FAIL, false, false);
            awaitAccepted(4);
            checkpoint(cluster, wrongRestore);
            awaitCreated(unrelatedFailure ? 4 : 3);
            assertThat(
                            probe.created.values().stream()
                                    .filter(
                                            task ->
                                                    task.getHttpRequest()
                                                            .getBody()
                                                            .toStringUtf8()
                                                            .equals("body-0"))
                                    .count())
                    .isEqualTo(2);
        }
    }

    private JobClient start(
            String restoredPath,
            ExpiredEnvelopePolicy policy,
            boolean eager,
            boolean allowNonRestoredState)
            throws Exception {
        Configuration config = new Configuration();
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                control.autoRestart ? "fixed-delay" : "none");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                org.apache.flink.configuration.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        if (restoredPath != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restoredPath);
            config.set(
                    StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, allowNonRestoredState);
        }
        var environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.setParallelism(1);
        // Tests trigger checkpoints explicitly; no periodic barrier can overtake a fault stimulus.
        environment.enableCheckpointing(Duration.ofHours(1).toMillis());
        environment.disableOperatorChaining();
        var ticks =
                new DataGeneratorSource<Long>(
                        value -> value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(100),
                        Types.LONG);
        var records =
                environment
                        .fromSource(ticks, WatermarkStrategy.noWatermarks(), "ticks")
                        .uid("ticks")
                        .map(new ControlledRecords(runId))
                        .uid("records");
        String observerId = runId;
        if (eager) {
            records.sinkTo(
                            CloudTasksSink.<String>builder()
                                    .queue(QueueDestination.of("p", "l", "q"))
                                    .emulatorEndpoint("localhost:1")
                                    .serializer(
                                            value -> {
                                                if (!value.equals("skip")) {
                                                    CONTROLS.get(observerId)
                                                            .replayedRecords
                                                            .incrementAndGet();
                                                }
                                                return null;
                                            })
                                    .build())
                    .uid("tasks");
        } else {
            records.sinkTo(
                            new StagedCreationTestSink(
                                    runId,
                                    CloudTasksStagedOptions.builder()
                                            .expiredEnvelopePolicy(policy)
                                            .build()))
                    .uid("tasks");
        }
        JobClient job = environment.executeAsync("staged-recovery");
        jobs.add(job);
        return job;
    }

    private String checkpoint(MiniCluster cluster, JobClient job) throws Exception {
        Awaits.await(
                "all checkpoint-triggering tasks to run",
                Duration.ofSeconds(20),
                () -> {
                    try {
                        if (status(job) != JobStatus.RUNNING) {
                            return false;
                        }
                        for (var vertex :
                                cluster.getExecutionGraph(job.getJobID())
                                        .get(5, TimeUnit.SECONDS)
                                        .getAllExecutionVertices()) {
                            var state = vertex.getExecutionState();
                            if (state != org.apache.flink.runtime.execution.ExecutionState.RUNNING
                                    && state
                                            != org.apache.flink.runtime.execution.ExecutionState
                                                    .FINISHED) {
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception failure) {
                        throw new AssertionError(failure);
                    }
                });
        return cluster.triggerCheckpoint(job.getJobID()).get(30, TimeUnit.SECONDS);
    }

    private void awaitAccepted(int records) throws InterruptedException {
        Awaits.await(
                "writer to stage " + records + " records",
                Duration.ofSeconds(20),
                () -> probe.serialized.get() >= records,
                () -> "serialized=" + probe.serialized + ", requests=" + probe.requests.size());
    }

    private void awaitCreated(int records) throws InterruptedException {
        Awaits.await(
                "service to create " + records + " tasks",
                Duration.ofSeconds(20),
                () -> probe.created.size() == records,
                () -> "created=" + probe.created.size() + ", requests=" + probe.requests.size());
    }

    private static void assertReadable(String pointer) {
        Path path = pointer.startsWith("file:") ? Path.of(URI.create(pointer)) : Path.of(pointer);
        assertThat(Files.isReadable(path.resolve("_metadata")))
                .as("retained checkpoint metadata at %s", path)
                .isTrue();
    }

    private static JobStatus status(JobClient job) {
        try {
            return job.getJobStatus().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("Could not read the test job status", e);
        }
    }

    private static final class Control {
        private volatile boolean autoRestart;
        private final AtomicBoolean failNotification = new AtomicBoolean();
        private final AtomicLong completed = new AtomicLong();
        private final AtomicInteger limit = new AtomicInteger();
        private final AtomicInteger initializations = new AtomicInteger();
        private final AtomicInteger replayedRecords = new AtomicInteger();
    }

    private static final class ControlledRecords extends RichMapFunction<Long, String>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private final String runId;
        private int emitted;
        private transient ListState<Integer> state;

        private ControlledRecords(String runId) {
            this.runId = runId;
        }

        @Override
        public String map(Long tick) {
            return emitted < CONTROLS.get(runId).limit.get() ? "body-" + emitted++ : "skip";
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(List.of(emitted));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            Control current = CONTROLS.get(runId);
            current.completed.set(checkpointId);
            if (current.failNotification.compareAndSet(true, false)) {
                Awaits.await(
                        "all savepoint-owned tasks to be accepted before the unrelated failure",
                        Duration.ofSeconds(20),
                        () -> StagedCreationTestSink.PROBES.get(runId).created.size() == 2);
                throw new java.io.IOException("injected unrelated operator completion failure");
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("emitted", Integer.class));
            for (int restored : state.get()) {
                emitted = restored;
            }
            CONTROLS.get(runId).initializations.incrementAndGet();
        }
    }
}
