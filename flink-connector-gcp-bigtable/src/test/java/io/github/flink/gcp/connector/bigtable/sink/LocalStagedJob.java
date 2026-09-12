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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/** Owns one embedded MiniCluster job; the caller owns its temporary checkpoint directory. */
final class LocalStagedJob implements AutoCloseable {
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
        env.fromSource(run.source(records, hold), WatermarkStrategy.noWatermarks(), "local-input")
                .uid("local-input")
                .sinkTo(
                        productionSink != null
                                ? productionSink
                                : staged
                                        ? new LocalStagedHarness.StagedSink(run, emulator)
                                        : new LocalStagedHarness.BulkSink(run, emulator))
                .uid("local-sink");
        var streamGraph = env.getStreamGraph();
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

    String savepoint(Path directory, boolean stop) throws Exception {
        String path =
                (stop
                                ? client.stopWithSavepoint(
                                        false,
                                        directory.toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                : client.triggerSavepoint(
                                        directory.toUri().toString(),
                                        SavepointFormatType.CANONICAL))
                        .get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
        if (stop) {
            result.get(run.checkpointOperationTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        return path;
    }

    String checkpoint() throws Exception {
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
                } catch (java.util.concurrent.ExecutionException failure) {
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
