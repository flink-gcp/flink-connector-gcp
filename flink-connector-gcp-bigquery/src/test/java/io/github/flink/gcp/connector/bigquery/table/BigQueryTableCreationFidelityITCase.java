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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Does BigQuery <em>accept</em> the create request {@code sink.table-create.*} builds?
 *
 * <p>The emulator answers a different question. It stores the create request's JSON and hands it
 * back unchanged, so {@code BigQueryTableCreateOptionsITCase} can prove the settings survive the
 * mapper — but it validates nothing, and a request the service would refuse passes there exactly as
 * one it accepts. This class is the measurement, and until it existed nothing in this repository
 * had made it: every other partitioning assertion is against a locally built {@code TableInfo} or a
 * recording fake, so the claim that {@code BigQueryTableAdmin} builds a request BigQuery honours
 * rested on reading the API documentation.
 *
 * <p>It runs through the planner because that is the surface this issue adds, and the {@code
 * BigQueryTableAdmin} path underneath is the same one the DataStream API takes — so the answer
 * covers {@code tableCreateOptions(...)} too.
 *
 * <p>Skipped unless {@code BQ_IT_PROJECT} and {@code BQ_IT_DATASET} are set (no bucket needed —
 * nothing is staged).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryTableCreationFidelityITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String PARTITIONED_TABLE = "table_create_partitioned_" + RUN_ID;
    private static final String INGESTION_TIME_TABLE = "table_create_ingestion_" + RUN_ID;

    @AfterAll
    static void dropTables() {
        RealBigQuery.deleteTables(PARTITIONED_TABLE, INGESTION_TIME_TABLE);
    }

    private static String withOptions(String table, String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigQueryDynamicTableFactory.IDENTIFIER);
        options.put("project", RealBigQuery.project());
        options.put("dataset", RealBigQuery.dataset());
        options.put("table", table);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return options.entrySet().stream()
                .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }

    @Test
    void bigQueryAcceptsAColumnPartitionedAndClusteredCreateRequest() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE events (name STRING, event_ts TIMESTAMP_LTZ(6), region STRING) "
                        + withOptions(
                                PARTITIONED_TABLE,
                                "sink.table-create.time-partitioning.type",
                                "day",
                                "sink.table-create.time-partitioning.field",
                                "event_ts",
                                "sink.table-create.time-partitioning.expiration",
                                "90 d",
                                "sink.table-create.clustered-fields",
                                "region;name"));

        tEnv.executeSql(
                        "INSERT INTO events"
                                // Relative, not a literal: the partition carries a 90-day
                                // expiration, so a hardcoded date would start landing in an
                                // already-expired partition once the calendar passed it.
                                + " VALUES ('alice', CURRENT_TIMESTAMP, 'jp')")
                .await();

        StandardTableDefinition definition = RealBigQuery.tableDefinition(PARTITIONED_TABLE);
        TimePartitioning partitioning = definition.getTimePartitioning();
        assertThat(partitioning).isNotNull();
        assertThat(partitioning.getType()).isEqualTo(TimePartitioning.Type.DAY);
        assertThat(partitioning.getField()).isEqualTo("event_ts");
        assertThat(partitioning.getExpirationMs()).isEqualTo(Duration.ofDays(90).toMillis());
        assertThat(definition.getClustering()).isNotNull();
        assertThat(definition.getClustering().getFields()).containsExactly("region", "name");

        assertThat(
                        RealBigQuery.queryRows(
                                "SELECT name FROM " + RealBigQuery.tablePath(PARTITIONED_TABLE)))
                .hasSize(1);
    }

    @Test
    void bigQueryAcceptsAnIngestionTimePartitionedCreateRequest() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE events (name STRING) "
                        + withOptions(
                                INGESTION_TIME_TABLE,
                                "sink.table-create.time-partitioning.type",
                                "month"));

        tEnv.executeSql("INSERT INTO events VALUES ('alice')").await();

        StandardTableDefinition definition = RealBigQuery.tableDefinition(INGESTION_TIME_TABLE);
        assertThat(definition.getTimePartitioning()).isNotNull();
        assertThat(definition.getTimePartitioning().getType())
                .isEqualTo(TimePartitioning.Type.MONTH);
        // The half PARTITIONED BY could never express, and the reason these are options.
        assertThat(definition.getTimePartitioning().getField()).isNull();
    }
}
