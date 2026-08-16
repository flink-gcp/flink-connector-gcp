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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code sink.write-method} = {@code storage-api-exactly-once} against real BigQuery: a SQL job
 * spanning several checkpoints writes each row exactly once.
 *
 * <p>Gated because the emulator cannot answer this at all, which was measured rather than assumed
 * (2026-08-06, goccy 0.8.1): it assigns its own append offsets instead of honoring the requested
 * one, and it keeps no flush cursor, so a second flush re-inserts every row the first made visible.
 * {@code BigQueryTableWriteMethodsPlanTest} records both. The source is paced to span several
 * checkpoints for exactly that reason — one bounded {@code VALUES} insert commits once, and a
 * duplicating second commit is what this is here to rule out.
 *
 * <p>What the assertion actually establishes is worth stating: <b>the row count</b>, which the
 * pacing arranges to be a multi-commit row count but does not itself prove was one. Flink exposes
 * no completed-checkpoint count through {@code TableResult}, so a run whose datagen source finished
 * inside one checkpoint interval would pass as a single-commit job rather than fail. Pacing is
 * therefore the mechanism and the count is the verdict; if this ever needs to be airtight, the
 * committer's own {@code loadJobsSubmitted}-style counter is the thing to read.
 *
 * <p>The DataStream counterpart is {@code BigQueryBufferedStreamExactlyOnceITCase}, which covers
 * restart-after-failure. What this adds is the SQL surface: the DDL selects the buffered sink, the
 * {@code sink.buffered-stream.*} family reaches it, and the planner's own checkpointing carries the
 * two-phase commit.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryTableExactlyOnceITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String TABLE = "table_exactly_once_" + RUN_ID;

    /** Rows the datagen source emits, slowly enough to span several checkpoints. */
    private static final int ROWS = 20;

    private static final int ROWS_PER_SECOND = 4;

    @AfterAll
    static void dropTables() {
        RealBigQuery.deleteTables(TABLE);
    }

    private static String withOptions(String table, String... keysAndValues) {
        return TableDdl.withOptions(
                RealBigQuery.project(), RealBigQuery.dataset(), table, keysAndValues);
    }

    @Test
    void everyRowLandsExactlyOnceAcrossSeveralCheckpoints() throws Exception {
        Configuration configuration = new Configuration();
        // Well under the job's own duration, so the sink flushes and commits more than once — the
        // thing a bounded VALUES insert cannot produce. A TableEnvironment has no method for this.
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
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

        // A sequence rather than random values: a duplicate is then a repeated id, which counting
        // distinct ids detects, and a lost row is a missing one.
        tEnv.executeSql(
                "CREATE TABLE src (id BIGINT) WITH ("
                        + "'connector' = 'datagen',"
                        + " 'fields.id.kind' = 'sequence',"
                        + " 'fields.id.start' = '1',"
                        + " 'fields.id.end' = '"
                        + ROWS
                        + "',"
                        + " 'rows-per-second' = '"
                        + ROWS_PER_SECOND
                        + "')");
        tEnv.executeSql(
                "CREATE TABLE events (id BIGINT) "
                        + withOptions(
                                TABLE,
                                // Two writers, not the planner's default of one per core.
                                // Measured 2026-08-06: at ten, every subtask races to create the
                                // same missing table and BigQuery answers "Exceeded rate limits:
                                // too many table update operations for this table". The writers'
                                // recovery schedule absorbs it and the job still succeeds — this
                                // is about the test's own cost, not about a defect — but two is
                                // enough to prove one stream per subtask.
                                "sink.parallelism",
                                "2",
                                "sink.write-method",
                                "storage-api-exactly-once",
                                // One knob of the family, so the DDL that runs here is the shape a
                                // tuned job has. What its value becomes is the factory test's;
                                // what this adds is that the SDK accepts the retry settings it
                                // produces, which only the real client can say.
                                "sink.buffered-stream.retry.max-attempts",
                                "7"));

        tEnv.executeSql("INSERT INTO events SELECT id FROM src").await();

        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + RealBigQuery.tablePath(TABLE)))
                .containsExactly((long) ROWS);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(DISTINCT id) FROM " + RealBigQuery.tablePath(TABLE)))
                .containsExactly((long) ROWS);
    }
}
