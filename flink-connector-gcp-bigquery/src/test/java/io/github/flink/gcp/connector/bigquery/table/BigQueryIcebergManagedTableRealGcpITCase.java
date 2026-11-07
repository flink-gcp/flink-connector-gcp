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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink and the source against an Iceberg managed table the user created beforehand (#1994).
 *
 * <p>Real GCP because what BigQuery accepts for these tables is the service's answer and the
 * emulator has no such tables. The connector does nothing Iceberg-specific: it neither detects
 * these tables nor refuses a write method for them, so this class records which configurations work
 * and that the ones the service refuses fail the job with the service's own message. A failure of
 * one of the refusal cases means the service changed and the documented table needs updating.
 *
 * <p>Requires application-default credentials plus {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET},
 * {@code BQ_IT_GCS_BUCKET} (FILE_LOADS staging), {@code BQ_IT_ICEBERG_CONNECTION} (the Cloud
 * resource connection, as {@code project.location.connection}) and {@code BQ_IT_ICEBERG_BUCKET}
 * (where BigQuery writes the tables' files); skipped when they are absent. Each table takes a
 * prefix of its own in that bucket, because two tables on one prefix delete each other's files.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_ICEBERG_CONNECTION", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_ICEBERG_BUCKET", matches = ".+")
@Timeout(value = 900, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryIcebergManagedTableRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String DEFAULT_STREAM = "iceberg_it_ds_" + RUN_ID;
    private static final String EXACTLY_ONCE = "iceberg_it_eo_" + RUN_ID;
    private static final String FILE_LOADS = "iceberg_it_append_" + RUN_ID;
    private static final String FILE_LOADS_TRUNCATE = "iceberg_it_truncate_" + RUN_ID;
    private static final String CDC = "iceberg_it_cdc_" + RUN_ID;
    private static final String STAGING_ROOT = "flink-iceberg-it/" + RUN_ID;

    @BeforeAll
    static void createIcebergTables() throws Exception {
        for (String table :
                new String[] {DEFAULT_STREAM, EXACTLY_ONCE, FILE_LOADS, FILE_LOADS_TRUNCATE}) {
            createIcebergTable(table, "id INT64 NOT NULL, name STRING", "");
        }
        // CDC needs a primary key and max_staleness on the table, so a CDC user creates it so.
        createIcebergTable(
                CDC,
                "id INT64 NOT NULL, name STRING, PRIMARY KEY (id) NOT ENFORCED",
                ", max_staleness = INTERVAL 0 MINUTE");
    }

    private static void createIcebergTable(String table, String columns, String extraOptions)
            throws Exception {
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(table)
                        + " ("
                        + columns
                        + ") WITH CONNECTION `"
                        + System.getenv("BQ_IT_ICEBERG_CONNECTION")
                        + "` OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG',"
                        + " storage_uri = 'gs://"
                        + System.getenv("BQ_IT_ICEBERG_BUCKET")
                        + "/"
                        + RUN_ID
                        + "/"
                        + table
                        + "'"
                        + extraOptions
                        + ")");
        // Without this, a DDL that made a standard table would let every case pass vacuously.
        assertThat(RealBigQuery.tableDefinition(table).getBigLakeConfiguration())
                .as("%s is an Iceberg managed table", table)
                .isNotNull()
                .satisfies(config -> assertThat(config.getTableFormat()).isEqualTo("ICEBERG"));
    }

    /**
     * Deletes the tables; their files stay in the Iceberg bucket, whose lifecycle rule removes them
     * (deleting a table does not garbage-collect its files).
     */
    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                DEFAULT_STREAM, EXACTLY_ONCE, FILE_LOADS, FILE_LOADS_TRUNCATE, CDC),
                () -> RealGcs.deletePrefix(STAGING_ROOT));
    }

    private static TableEnvironment streaming() {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        // A refused write must fail the job rather than restart it until the timeout.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        return TableEnvironment.create(
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build());
    }

    private static void createSinkTable(TableEnvironment tEnv, String table, String... options) {
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, name STRING) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(), RealBigQuery.dataset(), table, options));
    }

    private static String stagingPath(String table) {
        return RealGcs.uri(STAGING_ROOT + "/" + table);
    }

    /** Writes two rows, then reads the table back through the connector's source. */
    private static void assertWrittenAndReadBack(String table, String... options) throws Exception {
        TableEnvironment tEnv = streaming();
        createSinkTable(tEnv, table, options);

        tEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await();

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> read = tEnv.executeSql("SELECT id, name FROM t").collect()) {
            read.forEachRemaining(rows::add);
        }
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1L, "a"), Row.of(2L, "b"));
    }

    @Test
    void theDefaultStreamWritesAndTheSourceReadsBack() throws Exception {
        assertWrittenAndReadBack(DEFAULT_STREAM);
    }

    @Test
    void theExactlyOnceMethodWritesAndTheSourceReadsBack() throws Exception {
        assertWrittenAndReadBack(
                EXACTLY_ONCE,
                "sink.parallelism",
                "1",
                "sink.write-method",
                "storage-api-exactly-once");
    }

    @Test
    void fileLoadsAppendWritesAndTheSourceReadsBack() throws Exception {
        assertWrittenAndReadBack(
                FILE_LOADS,
                "sink.write-method",
                "file-loads",
                "sink.file-loads.staging-path",
                stagingPath(FILE_LOADS),
                "sink.file-loads.min-checkpoint-interval",
                "1 s");
    }

    /**
     * The service refuses a truncating load into these tables; the connector passes the refusal on
     * rather than predicting it. The copy and query that finish an oversized commit are refused the
     * same way, which this case does not reach.
     */
    @Test
    void aTruncatingLoadFailsWithTheServicesMessage() {
        // Batch, because the connector itself refuses a truncating disposition in streaming.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inBatchMode()
                                .withConfiguration(configuration)
                                .build());
        createSinkTable(
                tEnv,
                FILE_LOADS_TRUNCATE,
                "sink.write-method",
                "file-loads",
                "sink.file-loads.staging-path",
                stagingPath(FILE_LOADS_TRUNCATE),
                "sink.file-loads.write-disposition",
                "write-truncate");

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await())
                .satisfies(
                        failure ->
                                assertThat(ExceptionUtils.stringifyException(failure))
                                        .contains("do not support load with truncating"));
    }

    /**
     * Google documents change data capture into these tables as a Preview, but on 2026-11-07 the
     * Storage Write API refused the UPSERT stream for them in this project; no row is written.
     */
    @Test
    void changeDataCaptureFailsWithTheServicesMessage() throws Exception {
        TableEnvironment tEnv = streaming();
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, name STRING, "
                        + "sequence STRING METADATA FROM 'change-sequence-number', "
                        + "PRIMARY KEY (id) NOT ENFORCED) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                CDC,
                                "sink.cdc.enabled",
                                "true",
                                "sink.create-disposition",
                                "create-never"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO t VALUES (1, 'a', '1')").await())
                .satisfies(
                        failure ->
                                assertThat(ExceptionUtils.stringifyException(failure))
                                        .contains("UPSERT stream is not supported"));
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + RealBigQuery.tablePath(CDC)))
                .containsExactly(0L);
    }
}
