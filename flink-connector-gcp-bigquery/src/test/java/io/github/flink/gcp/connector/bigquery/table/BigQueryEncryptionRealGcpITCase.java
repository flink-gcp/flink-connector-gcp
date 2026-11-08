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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two Cloud KMS key options of the SQL connector against BigQuery itself (#1995): a table the
 * Storage Write API sink creates, and a source query's result in BigQuery's anonymous dataset, each
 * read back for the key that encrypts it.
 *
 * <p>Requires application-default credentials plus {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET} and
 * {@code BQ_IT_KMS_KEY}, a key in the dataset's location that BigQuery's encryption service account
 * may use.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_KMS_KEY", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryEncryptionRealGcpITCase {

    private static final String RUN = TestNames.runId();
    private static final String KEY = System.getenv("BQ_IT_KMS_KEY");
    private static final String CREATED = "cmek_sql_created_" + RUN;

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(CREATED);
    }

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    @Test
    void theSinkCreatesTheTableEncryptedWithTheConfiguredKey() throws Exception {
        TableEnvironment tEnv = batch();
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, name STRING) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                CREATED,
                                "sink.table-create.kms-key-name",
                                KEY));

        tEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

        assertThat(RealBigQuery.tableKmsKeyName(CREATED)).isEqualTo(KEY);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(CREATED)))
                .containsExactly(1L);
    }

    @Test
    void aQueryResultInTheAnonymousDatasetIsEncryptedWithTheConfiguredKey() throws Exception {
        long since = System.currentTimeMillis() - 60_000;
        // A comment, because the DDL's option value cannot carry a quote unescaped: it finds this
        // run's job among the project's, and keeps the result out of any other run's cache.
        String marker = "cmek_query_" + RUN;
        TableEnvironment tEnv = batch();
        tEnv.executeSql(
                "CREATE TABLE q (n BIGINT) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(),
                                RealBigQuery.dataset(),
                                "unused_" + RUN,
                                "scan.query",
                                "SELECT 7 AS n /* " + marker + " */",
                                // A query naming no table runs in the US multi-region, where
                                // BigQuery refuses a regional key (measured).
                                "scan.query-location",
                                RealBigQuery.datasetLocation(),
                                "scan.query-result-kms-key-name",
                                KEY));

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> read = tEnv.executeSql("SELECT n FROM q").collect()) {
            read.forEachRemaining(rows::add);
        }

        assertThat(rows).containsExactly(Row.of(7L));
        assertThat(RealBigQuery.queryResultKmsKeyName(marker, since)).isEqualTo(KEY);
    }
}
