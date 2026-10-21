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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.spanner.SpannerEmulatorContainers;
import io.github.flink.gcp.connector.testutils.spanner.SpannerTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the deployed graph with the production connectors against the Spanner emulator, through the
 * trial's savepoint upgrade and both rescales.
 *
 * <p>The emulator is a convenience, never evidence about the service: it says nothing about
 * workload identity, service partition changes, batch partition planning or a bound in the future
 * on the real service, which the deployed trials measure.
 */
@Testcontainers
@Timeout(300)
@Execution(ExecutionMode.SAME_THREAD)
class RecoveryJobITCase {
    private static final String PROJECT = RecoveryOptions.PROJECT;
    private static final long RECORDS = 300;
    private static final long SEED_ROWS = 300;
    private static final AtomicInteger INSTANCES = new AtomicInteger();

    @Container
    static final SpannerEmulatorContainer EMULATOR = SpannerEmulatorContainers.newContainer();

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static Spanner spanner;

    @TempDir Path temporary;

    @BeforeAll
    static void startClient() {
        spanner = SpannerTestClients.forEmulator(EMULATOR.getEmulatorGrpcEndpoint(), PROJECT);
    }

    @AfterAll
    static void stopClient() {
        if (spanner != null) {
            spanner.close();
        }
    }

    /**
     * The rig's observer reads the job as four vertices: the input chain at parallelism one, the
     * input sink, and each source chained to its delivery and sink, each at the trial's
     * parallelism.
     */
    @ParameterizedTest(name = "parallelism {0}")
    @ValueSource(ints = {1, 2})
    void theProductionGraphIsTheInputItsSinkAndBothSourceChains(int parallelism) {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--parallelism", String.valueOf(parallelism)));
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        SpannerRecoveryJob.configure(
                env,
                options,
                SpannerRecoveryJob.Components.production(options),
                Duration.ofSeconds(30));
        var graph = env.getStreamGraph();
        // Savepoint upgrades and rescales restore every operator's state by these.
        assertThat(graph.getStreamNodes().stream().map(StreamNode::getTransformationUID))
                .contains(
                        "sp1360-source-v1",
                        "sp1360-input-v1",
                        "sp1360-input-sink-v1",
                        "sp1360-change-stream-v1",
                        "sp1360-relay-delivery-v1",
                        "sp1360-relay-sink-v1",
                        "sp1360-seed-v1",
                        "sp1360-seed-pace-v1",
                        "sp1360-seed-delivery-v1",
                        "sp1360-batch-sink-v1");
        JobVertex[] jobVertices =
                StreamingJobGraphGenerator.createJobGraph(graph).getVerticesAsArray();
        Map<String, Integer> vertices =
                Arrays.stream(jobVertices)
                        .collect(Collectors.toMap(JobVertex::getName, JobVertex::getParallelism));
        assertThat(vertices)
                .containsOnlyKeys(
                        "Source: Spanner input -> Spanner input identity",
                        "Spanner input: Writer",
                        "Source: Spanner change stream -> Spanner relay delivery"
                                + " -> Spanner relay: Writer",
                        "Source: Spanner seed -> Spanner seed pace -> Spanner seed delivery"
                                + " -> Spanner batch copy: Writer");
        assertThat(vertices)
                .allSatisfy(
                        (name, actual) ->
                                assertThat(actual)
                                        .as(name)
                                        .isEqualTo(
                                                name.startsWith("Source: Spanner input")
                                                        ? 1
                                                        : parallelism));
        // Every rescale restores into the same key-group range.
        assertThat(jobVertices).extracting(JobVertex::getMaxParallelism).containsOnly(1, 128);
    }

    /**
     * The trial's upgrades: one keeping two subtasks, and the two rescales, which restore the state
     * of two subtasks into one and of one into two. Every connector ends with every record, and the
     * relayed and copied rows name both phases. Keys are compared as sets: a restore can deliver
     * the change stream's inclusive boundary again and re-reads every batch partition still open,
     * as both sources document.
     */
    @ParameterizedTest(name = "parallelism {0} to {1}")
    @CsvSource({"2,2", "2,1", "1,2"})
    void savepointUpgradeResumesEveryComponentAndEachEndsWithEveryRecord(
            String before, String after) throws Exception {
        String instance = "sp1360-it-" + INSTANCES.incrementAndGet();
        // After the input's last commit in either phase, and soon enough to end the test.
        Instant end = Instant.now().plusSeconds(60);
        DatabaseClient client = createDatabase(instance);
        String[] trial = {
            "--instance",
            instance,
            "--records",
            String.valueOf(RECORDS),
            "--records-per-second",
            "20",
            "--seed-rows-per-second",
            "20",
            "--change-stream-end",
            end.toString()
        };
        var initial =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(concat(trial, "--parallelism", before)));
        var first = start(initial, null);
        String savepoint;
        try {
            Awaits.await(
                    "rows relayed and copied before the savepoint",
                    Duration.ofSeconds(60),
                    () -> count(client, "relay") >= 20 && count(client, "batch_out") >= 20,
                    () ->
                            "relay="
                                    + count(client, "relay")
                                    + " batch_out="
                                    + count(client, "batch_out"));
            savepoint =
                    first.stopWithSavepoint(
                                    false,
                                    temporary.resolve("savepoints").toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(60, TimeUnit.SECONDS);
        } finally {
            cancel(first);
        }
        assertThat(count(client, "input"))
                .as("the first phase outlasted the input")
                .isLessThan(RECORDS);
        assertThat(count(client, "batch_out"))
                .as("the first phase outlasted the seed copy")
                .isLessThan(SEED_ROWS);
        // The upgrade's input, at most 15 seconds, must commit before the change stream's end.
        assertThat(Instant.now())
                .as("time left before the change stream's end")
                .isBefore(end.minusSeconds(20));
        var upgrade =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                concat(
                                        trial,
                                        "--phase",
                                        "upgrade",
                                        "--require-restored",
                                        "true",
                                        "--parallelism",
                                        after)));
        var second = start(upgrade, savepoint);
        try {
            second.getJobExecutionResult().get(120, TimeUnit.SECONDS);
        } finally {
            cancel(second);
        }

        Map<String, Long> input = sequences(client, "SELECT k, n FROM input");
        assertThat(input)
                .containsExactlyInAnyOrderEntriesOf(
                        LongStream.range(0, RECORDS)
                                .boxed()
                                .collect(Collectors.toMap(initial::rowKey, n -> n)));
        assertPhases(client, "relay", input.keySet().stream().toList());
        assertPhases(
                client,
                "batch_out",
                LongStream.range(0, SEED_ROWS).mapToObj(RecoveryJobITCase::seedKey).toList());
    }

    /** Every key reaches the table, and deliveries before and after the savepoint both did. */
    private static void assertPhases(DatabaseClient client, String table, List<String> keys) {
        List<String[]> rows = new ArrayList<>();
        try (ResultSet result =
                client.singleUse().executeQuery(Statement.of("SELECT k, delivery FROM " + table))) {
            while (result.next()) {
                rows.add(new String[] {result.getString(0), result.getString(1)});
            }
        }
        assertThat(rows.stream().map(row -> row[0]).distinct())
                .as(table)
                .containsExactlyInAnyOrderElementsOf(keys);
        assertThat(rows.stream().map(row -> row[1].substring(0, row[1].indexOf('/'))).distinct())
                .as(table)
                .containsExactlyInAnyOrder("initial", "upgrade");
    }

    private JobClient start(RecoveryOptions options, @Nullable String savepoint) throws Exception {
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
        SpannerRecoveryJob.configure(
                env,
                options,
                SpannerRecoveryJob.Components.connectors(
                        options, EMULATOR.getEmulatorGrpcEndpoint()),
                Duration.ofMillis(200));
        return env.executeAsync("Spanner recovery " + options.phase);
    }

    /** The layout the rig's runner creates, with the seed rows it writes before the job starts. */
    private static DatabaseClient createDatabase(String instance) throws Exception {
        spanner.getInstanceAdminClient()
                .createInstance(
                        InstanceInfo.newBuilder(InstanceId.of(PROJECT, instance))
                                .setInstanceConfigId(
                                        InstanceConfigId.of(PROJECT, "emulator-config"))
                                .setNodeCount(1)
                                .setDisplayName(instance)
                                .build())
                .get();
        spanner.getDatabaseAdminClient()
                .createDatabase(
                        instance,
                        RecoveryOptions.DATABASE,
                        List.of(
                                "CREATE TABLE input (k STRING(MAX) NOT NULL, n INT64 NOT NULL)"
                                        + " PRIMARY KEY (k)",
                                "CREATE CHANGE STREAM input_changes FOR input",
                                "CREATE TABLE relay (k STRING(MAX) NOT NULL,"
                                        + " delivery STRING(MAX) NOT NULL,"
                                        + " commit_micros INT64 NOT NULL) PRIMARY KEY (k, delivery)",
                                "CREATE TABLE seed (k STRING(MAX) NOT NULL, n INT64 NOT NULL)"
                                        + " PRIMARY KEY (k)",
                                "CREATE TABLE batch_out (k STRING(MAX) NOT NULL,"
                                        + " delivery STRING(MAX) NOT NULL, n INT64 NOT NULL)"
                                        + " PRIMARY KEY (k, delivery)"))
                .get();
        DatabaseClient client =
                spanner.getDatabaseClient(
                        DatabaseId.of(PROJECT, instance, RecoveryOptions.DATABASE));
        List<Mutation> seed = new ArrayList<>();
        for (long n = 0; n < SEED_ROWS; n++) {
            seed.add(
                    Mutation.newInsertBuilder("seed")
                            .set("k")
                            .to(seedKey(n))
                            .set("n")
                            .to(n)
                            .build());
        }
        client.write(seed);
        return client;
    }

    private static String seedKey(long n) {
        return String.format("s%x/%07d", n % 16, n);
    }

    private static long count(DatabaseClient client, String table) {
        try (ResultSet result =
                client.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM " + table))) {
            result.next();
            return result.getLong(0);
        }
    }

    private static Map<String, Long> sequences(DatabaseClient client, String sql) {
        Map<String, Long> rows = new HashMap<>();
        try (ResultSet result = client.singleUse().executeQuery(Statement.of(sql))) {
            while (result.next()) {
                rows.put(result.getString(0), result.getLong(1));
            }
        }
        return rows;
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
