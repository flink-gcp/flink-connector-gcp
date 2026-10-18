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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.test.junit5.MiniClusterExtension;

import io.github.flink.gcp.connector.testutils.Awaits;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the deployed graph with in-memory connectors: the input, its restore and every rescale.
 *
 * <p>Neither the staged sink nor the change-stream source runs against the Bigtable emulator: the
 * staged sink reads its app profile through the instance admin API, which the emulator does not
 * serve, and the source has no emulator endpoint. The connector modules' own tests cover their
 * behavior; this checks the graph the trial deploys around them.
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

    /**
     * The rig's observer reads the job as four vertices: the input chain at parallelism one, each
     * sink mode, and the change stream chained to its relay, each at the trial's parallelism.
     */
    @ParameterizedTest(name = "parallelism {0}")
    @ValueSource(ints = {1, 2})
    void theProductionGraphIsTheInputBothSinksAndTheRelayChain(int parallelism) {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--parallelism", String.valueOf(parallelism)));
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        BigtableRecoveryJob.configure(
                env,
                options,
                BigtableRecoveryJob.Components.production(options),
                Duration.ofSeconds(30));
        var graph = env.getStreamGraph();
        // Savepoint upgrades and rescales restore every operator's state by these.
        assertThat(graph.getStreamNodes().stream().map(StreamNode::getTransformationUID))
                .contains(
                        "bt1316-source-v1",
                        "bt1316-input-v1",
                        "bt1316-eo-sink-v1",
                        "bt1316-alo-sink-v1",
                        "bt1316-change-stream-v1",
                        "bt1316-delivery-v1",
                        "bt1316-relay-sink-v1");
        JobVertex[] jobVertices =
                StreamingJobGraphGenerator.createJobGraph(graph).getVerticesAsArray();
        Map<String, Integer> vertices =
                Arrays.stream(jobVertices)
                        .collect(Collectors.toMap(JobVertex::getName, JobVertex::getParallelism));
        assertThat(vertices)
                .containsOnlyKeys(
                        "Source: Bigtable input -> Bigtable input identity",
                        "Bigtable EXACTLY_ONCE: Writer -> Bigtable EXACTLY_ONCE: Committer",
                        "Bigtable AT_LEAST_ONCE: Writer",
                        "Source: Bigtable change stream -> Bigtable relay delivery"
                                + " -> Bigtable relay: Writer");
        assertThat(vertices)
                .allSatisfy(
                        (name, actual) ->
                                assertThat(actual)
                                        .as(name)
                                        .isEqualTo(
                                                name.startsWith("Source: Bigtable input")
                                                        ? 1
                                                        : parallelism));
        // Every rescale restores into the same key-group range.
        assertThat(jobVertices).extracting(JobVertex::getMaxParallelism).containsOnly(1, 128);
    }

    /**
     * The trial's upgrades: one keeping two subtasks, and the two rescales, which restore the state
     * of two subtasks into one and of one into two.
     */
    @ParameterizedTest(name = "parallelism {0} to {1}")
    @CsvSource({"2,2", "2,1", "1,2"})
    void savepointUpgradeResumesTheInputAndEveryConnectorEndsWithEveryRecord(
            String before, String after) throws Exception {
        String[] input = {"--records", "300", "--records-per-second", "20"};
        var initial =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(concat(input, "--parallelism", before)));
        var connectors = new Connectors(initial);
        var first = start(initial, connectors, null);
        String savepoint;
        try {
            Awaits.await(
                    "records delivered before the savepoint",
                    Duration.ofSeconds(30),
                    () -> connectors.exactlyOnce.received().size() >= 20,
                    () -> "delivered=" + connectors.exactlyOnce.received().size());
            savepoint =
                    first.stopWithSavepoint(
                                    false,
                                    temporary.resolve("savepoints").toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(30, TimeUnit.SECONDS);
        } finally {
            cancel(first);
        }
        assertThat(connectors.exactlyOnce.received()).hasSizeLessThan(300);
        var upgrade =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                concat(
                                        input,
                                        "--phase",
                                        "upgrade",
                                        "--require-restored",
                                        "true",
                                        "--parallelism",
                                        after)));
        var second = start(upgrade, connectors, savepoint);
        try {
            second.getJobExecutionResult().get(60, TimeUnit.SECONDS);
        } finally {
            cancel(second);
        }
        var sequences = LongStream.range(0, initial.records).boxed().toList();
        // A stop-with-savepoint leaves nothing to replay, so each sink saw each record once.
        assertThat(connectors.exactlyOnce.received())
                .containsExactlyInAnyOrderElementsOf(sequences);
        assertThat(connectors.atLeastOnce.received())
                .containsExactlyInAnyOrderElementsOf(sequences);
        List<Tuple3<String, String, Long>> relayed = connectors.relay.received();
        assertThat(relayed)
                .extracting(delivery -> delivery.f0)
                .containsExactlyInAnyOrderElementsOf(
                        sequences.stream().map(initial::rowKey).toList());
        // Deliveries before the savepoint name the initial phase, those after it the upgrade.
        assertThat(relayed)
                .extracting(delivery -> delivery.f1.substring(0, delivery.f1.indexOf('/')))
                .contains("initial", "upgrade")
                .containsOnly("initial", "upgrade");
    }

    /**
     * In-memory connectors shared by both phases. The change stream is a rate-limited generator of
     * the eo rows, standing in for what the source would read; its splits restore like the input's.
     */
    private static final class Connectors {
        final FakeSink<Long> exactlyOnce = new FakeSink<>();
        final FakeSink<Long> atLeastOnce = new FakeSink<>();
        final FakeSink<Tuple3<String, String, Long>> relay = new FakeSink<>();
        final RecoveryOptions options;

        Connectors(RecoveryOptions options) {
            this.options = options;
        }

        BigtableRecoveryJob.Components components() {
            var rows = options;
            return new BigtableRecoveryJob.Components(
                    exactlyOnce,
                    atLeastOnce,
                    new DataGeneratorSource<Tuple2<String, Long>>(
                            sequence -> Tuple2.of(rows.rowKey(sequence), sequence),
                            options.records,
                            RateLimiterStrategy.perSecond(options.recordsPerSecond),
                            ChangeRelay.CHANGE),
                    relay);
        }
    }

    private JobClient start(
            RecoveryOptions options, Connectors connectors, @Nullable String savepoint)
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
        BigtableRecoveryJob.configure(
                env, options, connectors.components(), Duration.ofMillis(200));
        return env.executeAsync("Bigtable recovery " + options.phase);
    }

    private static String[] concat(String[] first, String... rest) {
        String[] all = new String[first.length + rest.length];
        System.arraycopy(first, 0, all, 0, first.length);
        System.arraycopy(rest, 0, all, first.length, rest.length);
        return all;
    }

    private static void cancel(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            job.cancel().get(10, TimeUnit.SECONDS);
        }
    }
}
