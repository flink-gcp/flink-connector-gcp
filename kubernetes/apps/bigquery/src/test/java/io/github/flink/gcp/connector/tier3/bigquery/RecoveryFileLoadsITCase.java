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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the application's FILE_LOADS mode through a savepoint upgrade against <b>real</b> BigQuery
 * and Cloud Storage: the BigQuery emulator has no load jobs and no storage endpoint, so the
 * connector refuses emulator endpoints for this write method.
 *
 * <p>The initial job is stopped with a savepoint once a load has committed and while input remains,
 * so the savepoint carries pending committables and the upgrade restores the input mid-stream. The
 * restored job must finish the same finite input with every sequence in exactly one row of its
 * destination table, and leave no staged file behind.
 *
 * <p>Writes run-unique tables into {@code BQ_IT_DATASET} and stages under {@code BQ_IT_GCS_BUCKET},
 * both removed afterwards; the dataset's and bucket's one-day expiry is the backstop for a crashed
 * run.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 900, threadMode = ThreadMode.SEPARATE_THREAD)
class RecoveryFileLoadsITCase {
    private static final int DESTINATIONS = 10;
    private static final long RECORDS = 3000;
    // 20 one-KiB rows a second: 150 seconds of input, against a first load that measured about
    // ten seconds after the start, leaves the savepoint well inside the input.
    private static final long BYTES_PER_SECOND = 20 * 1024;
    private static final Duration INPUT = Duration.ofSeconds(RECORDS * 1024 / BYTES_PER_SECOND);
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(2);

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @TempDir Path temporary;
    private final String runId = "fl-it-" + TestNames.runId();
    // The parent of every run prefix, as the deployed state bucket's runs/ is.
    private static final String RUNS = "tier3-bigquery-it";
    private String location;
    private final List<String> tables = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        Closers.closeAll(
                () -> RealBigQuery.deleteTables(tables.toArray(String[]::new)),
                () -> RealGcs.deletePrefix(RUNS + "/" + runId + "/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"AVRO", "PARQUET"})
    void savepointUpgradeLoadsEverySequenceExactlyOnce(String stagingFormat) throws Exception {
        // The sink names the dataset's own location, as the deployed application names its own.
        location = RealBigQuery.datasetLocation();
        var initial = options(stagingFormat, false);
        createTables(initial);
        String savepoint;
        long started = System.nanoTime();
        var initialJob = start(initial, null);
        try {
            awaitFirstLoad(initial, initialJob);
            savepoint =
                    initialJob
                            .stopWithSavepoint(
                                    false,
                                    temporary.resolve("savepoints").toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(120, TimeUnit.SECONDS);
        } finally {
            cancel(initialJob);
        }
        Duration stopped = Duration.ofNanos(System.nanoTime() - started);
        // What the initial job loaded, including whatever the savepoint committed, is short of
        // the input: the rest can only come from the upgrade restoring mid-stream.
        assertThat(loaded(initial))
                .as("rows loaded before the upgrade, stopped %s into %s of input", stopped, INPUT)
                .isLessThan(RECORDS);
        var upgrade = options(stagingFormat, true);
        var upgradeJob = start(upgrade, savepoint);
        try {
            upgradeJob.getJobExecutionResult().get(INPUT.toSeconds() + 300, TimeUnit.SECONDS);
        } finally {
            cancel(upgradeJob);
        }

        Set<Long> seen = new HashSet<>();
        for (int destination = 0; destination < DESTINATIONS; destination++) {
            for (FieldValueList row :
                    RealBigQuery.queryRows(
                            "SELECT run_id, sequence, destination FROM "
                                    + path(upgrade, destination))) {
                long sequence = row.get("sequence").getLongValue();
                assertThat(row.get("run_id").getStringValue()).isEqualTo(runId);
                assertThat(row.get("destination").getLongValue()).isEqualTo(destination);
                assertThat(sequence % DESTINATIONS).isEqualTo(destination);
                assertThat(seen.add(sequence)).as("sequence %s loaded twice", sequence).isTrue();
            }
        }
        assertThat(seen)
                .isEqualTo(LongStream.range(0, RECORDS).boxed().collect(Collectors.toSet()));
        String staging = RUNS + "/" + runId + "/staging";
        assertThat(upgrade.fileLoads.getStagingPath()).isEqualTo(RealGcs.uri(staging));
        assertThat(RealGcs.list(staging + "/")).isEmpty();
    }

    private RecoveryOptions options(String stagingFormat, boolean upgrade) {
        var target =
                new RecoveryOptions.Target(
                        RealBigQuery.project(),
                        RealBigQuery.dataset(),
                        location,
                        RealGcs.uri(RUNS),
                        CHECKPOINT_INTERVAL);
        return RecoveryOptions.parse(
                target,
                "--run-id",
                runId,
                "--mode",
                "FILE_LOADS",
                "--destinations",
                Integer.toString(DESTINATIONS),
                "--records",
                Long.toString(RECORDS),
                "--bytes-per-second",
                Long.toString(BYTES_PER_SECOND),
                "--staging-format",
                stagingFormat,
                "--phase",
                upgrade ? "upgrade" : "initial",
                "--require-restored",
                Boolean.toString(upgrade));
    }

    private void createTables(RecoveryOptions options) {
        var schema =
                Schema.of(
                        Field.of("run_id", StandardSQLTypeName.STRING),
                        Field.of("sequence", StandardSQLTypeName.INT64),
                        Field.of("destination", StandardSQLTypeName.INT64),
                        Field.of("payload", StandardSQLTypeName.BYTES));
        for (int destination = 0; destination < DESTINATIONS; destination++) {
            String table = options.table(destination).getTable();
            RealBigQuery.createTable(table, schema);
            tables.add(table);
        }
    }

    private JobClient start(RecoveryOptions options, String savepoint) throws Exception {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("checkpoints").toUri().toString());
        // A failing load must fail the test rather than restart until the timeout.
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (savepoint != null) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        BigQueryRecoveryJob.configure(env, options, BigQueryRecoveryJob.sink(options, null, null));
        return env.executeAsync("BigQuery FILE_LOADS recovery " + options.runId);
    }

    /** Waits until a checkpoint's load has committed rows, while the job still runs. */
    private void awaitFirstLoad(RecoveryOptions options, JobClient job) throws Exception {
        long deadline = System.nanoTime() + INPUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                // A failed job rethrows its cause here; a finished one ran out of input first.
                job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
                throw new AssertionError("The initial job finished before a load committed");
            }
            if (loaded(options) > 0) {
                return;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError("No load committed within the " + INPUT + " of input");
    }

    private static long loaded(RecoveryOptions options) throws InterruptedException {
        String union =
                LongStream.range(0, DESTINATIONS)
                        .mapToObj(d -> "SELECT COUNT(*) AS c FROM " + path(options, (int) d))
                        .collect(Collectors.joining(" UNION ALL "));
        return RealBigQuery.queryLongs("SELECT SUM(c) FROM (" + union + ")").get(0);
    }

    private static String path(RecoveryOptions options, int destination) {
        return RealBigQuery.tablePath(options.table(destination).getTable());
    }

    /**
     * Cancels a job still running and waits until it is terminal, so cleanup does not list the
     * staging prefix while a writer or the committer is still finishing. A job that ends between
     * the status read and the cancel is not an error, and must not replace the test's failure.
     */
    private static void cancel(JobClient job) throws Exception {
        if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            try {
                job.cancel().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                    throw e;
                }
            }
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Job " + job.getJobID() + " did not terminate");
            }
            Thread.sleep(500);
        }
    }
}
