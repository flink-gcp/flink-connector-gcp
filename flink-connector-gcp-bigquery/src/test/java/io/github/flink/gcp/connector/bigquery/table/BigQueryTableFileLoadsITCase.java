/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.RealGcs;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code sink.write-method} = {@code file-loads} against real BigQuery and real Cloud Storage.
 *
 * <p>Gated because there is no half of it an emulator can carry: the rows travel through staged
 * files on Cloud Storage, which nothing in this repository stands in for, and the builder therefore
 * refuses an emulator endpoint under this write method at all — {@code
 * BigQueryTableWriteMethodsPlanTest} asserts that refusal, which is all the emulator suite can say.
 *
 * <p>Two tests, one per execution mode, because the write method behaves differently in each and
 * only one of them consults {@code sink.file-loads.write-disposition}: streaming loads each
 * checkpoint's files and accepts {@code write-append} only, batch loads everything at end of input
 * and takes any disposition. The DataStream counterparts are {@code BigQueryFileLoadsITCase} and
 * {@code BigQueryFileLoadsStreamingITCase}; what these add is that the DDL reaches them.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET} and {@code BQ_IT_GCS_BUCKET} are
 * set.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryTableFileLoadsITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String STREAMING_TABLE = "table_file_loads_streaming_" + RUN_ID;
    private static final String BATCH_TABLE = "table_file_loads_batch_" + RUN_ID;

    // One staging directory per test: the streaming one asserts its own is empty after the load,
    // and a failing sibling keeps its staged files by design.
    private static final String STAGING_ROOT = "flink-table-file-loads-it/" + RUN_ID;
    private static final String STREAMING_PREFIX = STAGING_ROOT + "/streaming";
    private static final String BATCH_PREFIX = STAGING_ROOT + "/batch";

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(STREAMING_TABLE, BATCH_TABLE);
        RealGcs.deletePrefix(STAGING_ROOT);
    }

    private static String withOptions(String table, String stagingPrefix, String... keysAndValues) {
        String[] fixed = {
            "sink.write-method",
            "file-loads",
            "sink.file-loads.staging-path",
            RealGcs.uri(stagingPrefix)
        };
        return TableDdl.withOptions(
                RealBigQuery.project(),
                RealBigQuery.dataset(),
                table,
                TableDdl.concat(fixed, keysAndValues));
    }

    @Test
    void streamingLoadsEachCheckpointsFilesAndCleansTheStagingPrefix() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a
        // permanently failing append or load would loop until the timeout instead of failing
        // fast, on a run that is paying for real BigQuery.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());

        tEnv.executeSql(
                "CREATE TABLE events (name STRING, amount BIGINT) "
                        + withOptions(
                                STREAMING_TABLE,
                                STREAMING_PREFIX,
                                // Explicit opt-in to fast checkpoints for this short-lived job:
                                // BigQuery allows 1,500 load jobs per table per day and the
                                // connector's own floor is two minutes, which would refuse the
                                // interval above when the job graph is built.
                                "sink.file-loads.min-checkpoint-interval",
                                "1 s"));

        tEnv.executeSql("INSERT INTO events VALUES ('alice', 1), ('bob', 2)").await();

        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(STREAMING_TABLE)))
                .containsExactly(2L);
        // The load consumed the staged files; nothing is left to pay for.
        assertThat(RealGcs.list(STREAMING_PREFIX)).isEmpty();
    }

    @Test
    void batchTruncatesWhenTheWriteDispositionSaysSo() throws Exception {
        // The only place sink.file-loads.write-disposition has an effect: streaming execution
        // refuses anything but write-append, since every checkpoint issues its own load job.
        loadInBatch("('alice', 1), ('bob', 2)", "write-append");
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(BATCH_TABLE)))
                .containsExactly(2L);

        loadInBatch("('carol', 3)", "write-truncate");
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(BATCH_TABLE)))
                .containsExactly(1L);
    }

    private static void loadInBatch(String values, String writeDisposition) throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE TABLE events (name STRING, amount BIGINT) "
                        + withOptions(
                                BATCH_TABLE,
                                BATCH_PREFIX,
                                "sink.file-loads.write-disposition",
                                writeDisposition));
        tEnv.executeSql("INSERT INTO events VALUES " + values).await();
    }
}
