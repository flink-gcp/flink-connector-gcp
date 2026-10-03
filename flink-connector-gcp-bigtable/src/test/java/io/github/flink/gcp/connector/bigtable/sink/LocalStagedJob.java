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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Owns one embedded MiniCluster job; the caller owns its temporary checkpoint directory. */
final class LocalStagedJob implements AutoCloseable {
    /**
     * An interval no held job reaches, so only an explicit checkpoint or savepoint completes one.
     * Flink draws the first periodic trigger uniformly between the minimum pause and the interval,
     * so a job built with this interval also gets an equal pause, which pins that draw to the
     * interval; without it the first periodic checkpoint can complete, and notify its committer,
     * within a second of submission. A harness that sets its own {@code minPauseMillis} keeps that
     * pause instead.
     */
    static final long HELD_INTERVAL_MILLIS = 3_600_000;

    final org.apache.flink.runtime.minicluster.MiniCluster cluster;
    final JobClient client;
    final java.util.concurrent.CompletableFuture<org.apache.flink.api.common.JobExecutionResult>
            result;
    final LocalStagedHarness run;
    final long started = System.nanoTime();

    LocalStagedJob(
            LocalStagedHarness run,
            Path directory,
            boolean staged,
            boolean emulator,
            int parallelism,
            long records,
            long intervalMillis,
            boolean hold,
            String restorePath,
            boolean restart)
            throws Exception {
        this(
                run,
                directory,
                staged,
                emulator,
                parallelism,
                records,
                intervalMillis,
                hold,
                restorePath,
                restart,
                null);
    }

    LocalStagedJob(
            LocalStagedHarness run,
            Path directory,
            boolean staged,
            boolean emulator,
            int parallelism,
            long records,
            long intervalMillis,
            boolean hold,
            String restorePath,
            boolean restart,
            org.apache.flink.api.connector.sink2.Sink<Long> productionSink)
            throws Exception {
        this.run = run;
        Configuration configuration = new Configuration();
        configuration.set(org.apache.flink.configuration.RestOptions.PORT, 0);
        configuration.set(org.apache.flink.configuration.RestOptions.BIND_PORT, "0");
        configuration.set(org.apache.flink.configuration.RestOptions.BIND_ADDRESS, "127.0.0.1");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, restart ? "fixed-delay" : "none");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        configuration.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                org.apache.flink.configuration.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION);
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                directory.resolve("checkpoints").toUri().toString());
        if (restorePath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, restorePath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.setMaxParallelism(16);
        env.enableCheckpointing(intervalMillis);
        env.getCheckpointConfig().setCheckpointTimeout(run.checkpointTimeoutMillis());
        if (run.minPauseMillis > 0) {
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(run.minPauseMillis);
        } else if (intervalMillis == HELD_INTERVAL_MILLIS) {
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(intervalMillis);
        }
        env.fromSource(run.source(records, hold), WatermarkStrategy.noWatermarks(), "local-input")
                .uid("local-input")
                .sinkTo(
                        productionSink != null
                                ? productionSink
                                : staged
                                        ? run instanceof Stage2Harness
                                                ? new Stage2ProductionSink(
                                                        (Stage2Harness) run, emulator)
                                                : new LocalStagedHarness.StagedSink(run, emulator)
                                        : new LocalStagedHarness.BulkSink(run, emulator))
                .uid("local-sink");
        var streamGraph = env.getStreamGraph();
        if (staged && run instanceof Stage2Harness && ((Stage2Harness) run).timed) {
            Stage2NotificationOperatorFactory.install(streamGraph, run.id);
        }
        streamGraph.setJobName("local-bigtable-" + (staged ? "staged" : "bulk"));
        var graph = streamGraph.getJobGraph();
        if (restorePath != null) {
            graph.setSavepointRestoreSettings(
                    org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath(
                            restorePath));
        }
        cluster =
                new org.apache.flink.runtime.minicluster.MiniCluster(
                        new org.apache.flink.runtime.minicluster.MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(parallelism)
                                .build());
        try {
            cluster.start();
            cluster.submitJob(graph).get(30, TimeUnit.SECONDS);
            client =
                    new org.apache.flink.runtime.minicluster.MiniClusterJobClient(
                            graph.getJobID(),
                            cluster,
                            getClass().getClassLoader(),
                            org.apache.flink.runtime.minicluster.MiniClusterJobClient
                                    .JobFinalizationBehavior.NOTHING);
            result = client.getJobExecutionResult();
        } catch (Exception failure) {
            try {
                cluster.closeAsync().get(30, TimeUnit.SECONDS);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    void awaitAdmissions(int count) throws InterruptedException {
        await(
                "local writer admissions",
                Duration.ofSeconds(30),
                () -> healthy() && run.admissions.size() == count,
                () ->
                        "admissions="
                                + run.admissions.size()
                                + ", ack="
                                + run.acknowledgements.size());
    }

    void awaitAcks(int count) throws InterruptedException {
        await(
                "local write acknowledgements",
                Duration.ofSeconds(30),
                () -> healthy() && run.acknowledgements.size() == count,
                () ->
                        "admissions="
                                + run.admissions.size()
                                + ", ack="
                                + run.acknowledgements.size());
    }

    private boolean healthy() {
        if (result.isDone()) {
            result.join();
            return true;
        }
        JobStatus status = client.getJobStatus().join();
        if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
            result.join();
            throw new AssertionError("Unexpected local job termination: " + status);
        }
        return true;
    }

    /** Waits until every vertex is RUNNING, which for a restored committer follows its replay. */
    void awaitRunning() throws InterruptedException {
        await(
                "local vertices running",
                Duration.ofMillis(run.checkpointOperationTimeoutMillis()),
                this::allRunning,
                () ->
                        "status="
                                + client.getJobStatus().join()
                                + " vertices="
                                + java.util.stream.StreamSupport.stream(
                                                cluster.getExecutionGraph(client.getJobID())
                                                        .join()
                                                        .getAllExecutionVertices()
                                                        .spliterator(),
                                                false)
                                        .map(
                                                vertex ->
                                                        vertex.getTaskNameWithSubtaskIndex()
                                                                + "="
                                                                + vertex.getExecutionState())
                                        .collect(java.util.stream.Collectors.joining(", ")));
    }

    /**
     * True once the job and every vertex are RUNNING; rethrows a terminated job's failure. The job
     * status is checked first because the dispatcher answers an execution-graph request for a job
     * whose JobManager is still initializing with a graph that has no vertices yet, which an
     * all-vertices loop alone would accept.
     */
    boolean allRunning() {
        if (result.isDone()) {
            result.join();
        }
        if (client.getJobStatus().join() != JobStatus.RUNNING) {
            return false;
        }
        int vertices = 0;
        for (var vertex :
                cluster.getExecutionGraph(client.getJobID()).join().getAllExecutionVertices()) {
            vertices++;
            if (vertex.getExecutionState() != ExecutionState.RUNNING) {
                return false;
            }
        }
        return vertices > 0;
    }

    /**
     * Triggers a savepoint once every vertex is RUNNING as the JobManager sees it. A task reports
     * RUNNING asynchronously after it has started on the TaskManager, so a signal raised inside the
     * task, such as a reader starting or a record being admitted, can arrive first; the trigger
     * would then fail because not all required tasks are running.
     *
     * <p>A stop that fails or times out is rethrown with each execution's state and failure info in
     * its message, because the {@code StopWithSavepointStoppingException} it reports names neither
     * the execution that failed nor why. They are read as they stand rather than after the job
     * ends: Flink reports a failure during stopping only once every execution has terminated, so
     * the failed one's cause is already recorded, and a savepoint that expires leaves the job
     * running.
     */
    String savepoint(Path directory, boolean stop) throws Exception {
        awaitRunning();
        if (!stop) {
            return client.triggerSavepoint(
                            directory.toUri().toString(), SavepointFormatType.CANONICAL)
                    .get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        try {
            String path =
                    client.stopWithSavepoint(
                                    false,
                                    directory.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
            result.get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
            return path;
        } catch (ExecutionException | TimeoutException failure) {
            String state;
            try {
                state = jobState(null, null);
            } catch (Exception unreadable) {
                if (unreadable instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                failure.addSuppressed(unreadable);
                throw failure;
            }
            throw new IllegalStateException(
                    "Stop-with-savepoint failed; the job's state:\n" + state, failure);
        }
    }

    /**
     * Waits for the job to terminate and returns every failure text it left behind: the job status,
     * the caller's direct exception, the job result's failure and each execution's state and own
     * failure info. A stop-with-savepoint that fails during stopping reports a {@code
     * StopWithSavepointStoppingException} on both the operation and the job result; among what the
     * archived execution graph exposes, the task's cause survives only on the failed execution's
     * failure info, so a rejection message must be read from there. Call it before the cluster
     * closes; a job that does not terminate fails the assertion naming its state.
     */
    String failureText(@Nullable Throwable direct) throws Exception {
        Throwable terminal;
        try {
            terminal =
                    result.handle((value, failure) -> failure)
                            .get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            throw new AssertionError(
                    "The job did not terminate: status="
                            + jobStatus()
                            + " admissions="
                            + run.admissions.size()
                            + " acknowledgements="
                            + run.acknowledgements.size(),
                    timeout);
        }
        return jobState(direct, terminal);
    }

    /**
     * The job status, the given failures, and each execution's state and failure info as they
     * stand.
     */
    private String jobState(@Nullable Throwable direct, @Nullable Throwable terminal)
            throws Exception {
        StringBuilder text = new StringBuilder("jobStatus=" + jobStatus()).append('\n');
        if (direct != null) {
            text.append(ExceptionUtils.stringifyException(direct)).append('\n');
        }
        if (terminal != null) {
            text.append(ExceptionUtils.stringifyException(terminal)).append('\n');
        }
        var graph = cluster.getExecutionGraph(client.getJobID()).get(10, TimeUnit.SECONDS);
        if (graph.getFailureInfo() != null) {
            text.append(graph.getFailureInfo().getExceptionAsString()).append('\n');
        }
        for (var vertex : graph.getAllExecutionVertices()) {
            text.append(vertex.getTaskNameWithSubtaskIndex())
                    .append('=')
                    .append(vertex.getExecutionState())
                    .append('\n');
            vertex.getCurrentExecutionAttempt()
                    .getFailureInfo()
                    .ifPresent(info -> text.append(info.getExceptionAsString()).append('\n'));
        }
        return text.toString();
    }

    private JobStatus jobStatus() throws Exception {
        return client.getJobStatus().get(10, TimeUnit.SECONDS);
    }

    /**
     * Triggers a checkpoint once every vertex is RUNNING, for the reason {@link #savepoint(Path,
     * boolean)} gives.
     */
    String checkpoint() throws Exception {
        awaitRunning();
        return cluster.triggerCheckpoint(client.getJobID())
                .get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    String checkpointStats() throws Exception {
        java.net.URI address = cluster.getRestAddress().get(10, TimeUnit.SECONDS);
        java.net.http.HttpResponse<String> response =
                java.net.http.HttpClient.newHttpClient()
                        .send(
                                java.net.http.HttpRequest.newBuilder(
                                                address.resolve(
                                                        "/jobs/"
                                                                + client.getJobID()
                                                                + "/checkpoints"))
                                        .timeout(Duration.ofSeconds(10))
                                        .build(),
                                java.net.http.HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Local checkpoint stats: " + response.statusCode());
        }
        return response.body();
    }

    void finish() throws Exception {
        run.releaseSource();
        result.get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        try {
            if (!result.isDone()) {
                try {
                    client.cancel().get(30, TimeUnit.SECONDS);
                } catch (ExecutionException failure) {
                    // Terminal job status can reach the dispatcher before its result reaches us.
                    if (!(failure.getCause()
                            instanceof
                            org.apache.flink.runtime.messages
                                    .FlinkJobTerminatedWithoutCancellationException)) {
                        throw failure;
                    }
                }
            }
            result.handle((value, failure) -> null).get(30, TimeUnit.SECONDS);
        } finally {
            cluster.closeAsync().get(30, TimeUnit.SECONDS);
        }
    }
}
