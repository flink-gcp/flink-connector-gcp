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

package io.github.flink.gcp.connector.tier3.firestore;

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

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.firestore.FirestoreEmulatorContainers;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
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
 * Runs the deployed graph with the production connectors against the Firestore emulator in both
 * modes, through the trial's savepoint upgrade and both rescales.
 *
 * <p>The emulator is a convenience, never evidence about the service: it implements neither {@code
 * PartitionQuery} nor {@code __scatter__} sampling, so each read here is one split, and it says
 * nothing about workload identity or the service's read-time window, which the deployed trials
 * measure.
 */
@Testcontainers
@Timeout(300)
@Execution(ExecutionMode.SAME_THREAD)
class RecoveryJobITCase {
    private static final String PROJECT = RecoveryOptions.PROJECT;
    private static final long RECORDS = 300;
    private static final long SEED_RECORDS = 300;
    private static final AtomicInteger RUNS = new AtomicInteger();

    @Container
    static final FirestoreEmulatorContainer NATIVE = FirestoreEmulatorContainers.newContainer();

    @Container
    static final FirestoreEmulatorContainer DATASTORE =
            FirestoreEmulatorContainers.newContainer().withFlags("--database-mode=datastore-mode");

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(6)
                            .build());

    @TempDir Path temporary;

    /**
     * The rig's observer reads the job as five vertices: the input chain at parallelism one, each
     * input sink, and each source chained to its pace, delivery and copy sink, each at the trial's
     * parallelism.
     */
    @ParameterizedTest(name = "parallelism {0}")
    @ValueSource(ints = {1, 2})
    void theProductionGraphIsTheInputItsSinksAndBothSourceChains(int parallelism) {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--parallelism", String.valueOf(parallelism)));
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        FirestoreRecoveryJob.configure(
                env,
                options,
                FirestoreRecoveryJob.Components.production(options),
                Duration.ofSeconds(30));
        var graph = env.getStreamGraph();
        // Savepoint upgrades and rescales restore every operator's state by these.
        assertThat(graph.getStreamNodes().stream().map(StreamNode::getTransformationUID))
                .contains(
                        "fs1879-source-v1",
                        "fs1879-input-v1",
                        "fs1879-native-input-sink-v1",
                        "fs1879-datastore-input-sink-v1",
                        "fs1879-native-seed-v1",
                        "fs1879-native-seed-pace-v1",
                        "fs1879-native-seed-delivery-v1",
                        "fs1879-native-batch-sink-v1",
                        "fs1879-datastore-seed-v1",
                        "fs1879-datastore-seed-pace-v1",
                        "fs1879-datastore-seed-delivery-v1",
                        "fs1879-datastore-batch-sink-v1");
        JobVertex[] jobVertices =
                StreamingJobGraphGenerator.createJobGraph(graph).getVerticesAsArray();
        Map<String, Integer> vertices =
                Arrays.stream(jobVertices)
                        .collect(Collectors.toMap(JobVertex::getName, JobVertex::getParallelism));
        assertThat(vertices)
                .containsOnlyKeys(
                        "Source: Firestore input -> Firestore input identity",
                        "Firestore native input: Writer",
                        "Firestore datastore input: Writer",
                        "Source: Firestore native seed -> Firestore native seed pace"
                                + " -> Firestore native seed delivery"
                                + " -> Firestore native batch copy: Writer",
                        "Source: Firestore datastore seed -> Firestore datastore seed pace"
                                + " -> Firestore datastore seed delivery"
                                + " -> Firestore datastore batch copy: Writer");
        assertThat(vertices)
                .allSatisfy(
                        (name, actual) ->
                                assertThat(actual)
                                        .as(name)
                                        .isEqualTo(
                                                name.startsWith("Source: Firestore input")
                                                        ? 1
                                                        : parallelism));
        // Every rescale restores into the same key-group range.
        assertThat(jobVertices).extracting(JobVertex::getMaxParallelism).containsOnly(1, 128);
    }

    /**
     * The trial's upgrades: one keeping two subtasks, and the two rescales, which restore the state
     * of two subtasks into one and of one into two. Every sink ends with every record, and each
     * copy holds every seed record with deliveries from both phases. Keys are compared as sets: a
     * restore re-reads what a split emitted after the restored checkpoint.
     */
    @ParameterizedTest(name = "parallelism {0} to {1}")
    @CsvSource({"2,2", "2,1", "1,2"})
    void savepointUpgradeResumesEveryComponentAndEachEndsWithEveryRecord(
            String before, String after) throws Exception {
        String run = "it-" + RUNS.incrementAndGet();
        String nativeDatabase = "fs1879-" + run + "-n";
        String datastoreDatabase = "fs1879-" + run + "-d";
        String[] trial = {
            "--run-id",
            run,
            "--native-database",
            nativeDatabase,
            "--datastore-database",
            datastoreDatabase,
            "--records",
            String.valueOf(RECORDS),
            "--records-per-second",
            "20",
            "--seed-records-per-second",
            "20"
        };
        var initial =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(concat(trial, "--parallelism", before)));
        try (Firestore firestore = firestore(nativeDatabase);
                Datastore datastore = datastore(datastoreDatabase)) {
            seed(firestore, datastore, initial);
            var first = start(initial, null);
            String savepoint;
            try {
                Awaits.await(
                        "records copied before the savepoint",
                        Duration.ofSeconds(60),
                        () ->
                                documents(firestore, RecoveryOptions.BATCH).size() >= 20
                                        && entities(datastore, RecoveryOptions.BATCH).size() >= 20,
                        () ->
                                "native="
                                        + documents(firestore, RecoveryOptions.BATCH).size()
                                        + " datastore="
                                        + entities(datastore, RecoveryOptions.BATCH).size());
                savepoint =
                        first.stopWithSavepoint(
                                        false,
                                        temporary.resolve("savepoints").toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                .get(60, TimeUnit.SECONDS);
            } finally {
                cancel(first);
            }
            assertThat(documents(firestore, RecoveryOptions.INPUT).size())
                    .as("the first phase outlasted the input")
                    .isLessThan((int) RECORDS);
            assertThat(documents(firestore, RecoveryOptions.BATCH).size())
                    .as("the first phase outlasted the native copy")
                    .isLessThan((int) SEED_RECORDS);
            assertThat(entities(datastore, RecoveryOptions.BATCH).size())
                    .as("the first phase outlasted the datastore copy")
                    .isLessThan((int) SEED_RECORDS);
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

            Map<String, Long> expectedInput =
                    LongStream.range(0, RECORDS)
                            .boxed()
                            .collect(Collectors.toMap(initial::key, n -> n));
            assertThat(sequences(documents(firestore, RecoveryOptions.INPUT)))
                    .as("native input")
                    .containsExactlyInAnyOrderEntriesOf(expectedInput);
            Map<String, Long> datastoreInput = new HashMap<>();
            for (Entity entity : entities(datastore, RecoveryOptions.INPUT)) {
                datastoreInput.put(entity.getKey().getName(), entity.getLong("n"));
            }
            assertThat(datastoreInput)
                    .as("datastore input")
                    .containsExactlyInAnyOrderEntriesOf(expectedInput);

            List<String> seedKeys =
                    LongStream.range(0, SEED_RECORDS).mapToObj(RecoveryJobITCase::seedKey).toList();
            List<String[]> nativeCopies = new ArrayList<>();
            for (DocumentSnapshot copy : documents(firestore, RecoveryOptions.BATCH)) {
                nativeCopies.add(new String[] {copy.getString("k"), copy.getString("delivery")});
                assertThat(copy.getId())
                        .isEqualTo(copy.getString("k") + "@" + copy.getString("delivery"));
                assertThat(copy.getLong("n")).isEqualTo(seedSequence(copy.getString("k")));
            }
            assertPhases("native copy", nativeCopies, seedKeys);
            List<String[]> datastoreCopies = new ArrayList<>();
            for (Entity copy : entities(datastore, RecoveryOptions.BATCH)) {
                datastoreCopies.add(new String[] {copy.getString("k"), copy.getString("delivery")});
                assertThat(copy.getKey().getName())
                        .isEqualTo(copy.getString("k") + "@" + copy.getString("delivery"));
                assertThat(copy.getLong("n")).isEqualTo(seedSequence(copy.getString("k")));
            }
            assertPhases("datastore copy", datastoreCopies, seedKeys);
        }
    }

    /**
     * Every seed key was copied, deliveries before and after the savepoint both were, and the
     * upgrade resumed the split rather than reading it again from its start: it copied fewer keys
     * than the seed holds, because the first phase had copied some.
     */
    private static void assertPhases(String what, List<String[]> copies, List<String> keys) {
        assertThat(copies.stream().map(copy -> copy[0]).distinct())
                .as(what)
                .containsExactlyInAnyOrderElementsOf(keys);
        assertThat(
                        copies.stream()
                                .map(copy -> copy[1].substring(0, copy[1].indexOf(':')))
                                .distinct())
                .as(what)
                .containsExactlyInAnyOrder("initial", "upgrade");
        assertThat(
                        copies.stream()
                                .filter(copy -> copy[1].startsWith("upgrade:"))
                                .map(copy -> copy[0])
                                .distinct()
                                .count())
                .as(what + ": keys the upgrade copied")
                .isLessThan(keys.size());
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
        FirestoreRecoveryJob.configure(
                env,
                options,
                FirestoreRecoveryJob.Components.connectors(
                        options, NATIVE.getEmulatorEndpoint(), DATASTORE.getEmulatorEndpoint()),
                Duration.ofMillis(200));
        return env.executeAsync("Firestore recovery " + options.phase);
    }

    /** The seed the rig's runner writes in both databases before the job starts. */
    private static void seed(Firestore firestore, Datastore datastore, RecoveryOptions options)
            throws Exception {
        WriteBatch batch = firestore.batch();
        List<Entity> entities = new ArrayList<>();
        for (long n = 0; n < SEED_RECORDS; n++) {
            batch.set(
                    firestore.collection(RecoveryOptions.SEED).document(seedKey(n)),
                    Map.of("n", n));
            entities.add(
                    Entity.newBuilder(options.datastoreKey(RecoveryOptions.SEED, seedKey(n)))
                            .set("n", n)
                            .build());
        }
        batch.commit().get(30, TimeUnit.SECONDS);
        datastore.put(entities.toArray(new Entity[0]));
    }

    private static String seedKey(long n) {
        return String.format("s%x-%07d", n % 16, n);
    }

    private static long seedSequence(String key) {
        return Long.parseLong(key.substring(key.indexOf('-') + 1));
    }

    private static Firestore firestore(String database) {
        return FirestoreOptions.newBuilder()
                .setProjectId(PROJECT)
                .setDatabaseId(database)
                .setEmulatorHost(NATIVE.getEmulatorEndpoint())
                .build()
                .getService();
    }

    private static Datastore datastore(String database) {
        return DatastoreOptions.newBuilder()
                .setProjectId(PROJECT)
                .setDatabaseId(database)
                .setHost(DATASTORE.getEmulatorEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }

    private static List<DocumentSnapshot> documents(Firestore firestore, String collection) {
        try {
            return new ArrayList<>(
                    firestore
                            .collection(collection)
                            .get()
                            .get(30, TimeUnit.SECONDS)
                            .getDocuments());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Entity> entities(Datastore datastore, String kind) {
        List<Entity> entities = new ArrayList<>();
        datastore
                .run(Query.newEntityQueryBuilder().setKind(kind).build())
                .forEachRemaining(entities::add);
        return entities;
    }

    private static Map<String, Long> sequences(List<DocumentSnapshot> documents) {
        Map<String, Long> sequences = new HashMap<>();
        for (DocumentSnapshot document : documents) {
            sequences.put(document.getId(), document.getLong("n"));
        }
        return sequences;
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
