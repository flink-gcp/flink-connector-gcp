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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code sink.table-create.*} options end to end: a table the planner creates comes back from
 * the service partitioned and clustered as the DDL asked.
 *
 * <p>What the emulator (0.8.1) can and cannot show here. It stores the whole create request as JSON
 * and returns it unchanged from {@code tables.get}, so the settings round-trip field for field —
 * which is what these tests assert, and it is a real assertion: it fails if the mapper drops a
 * value on the way to {@code BigQueryTableAdmin}. What it does <b>not</b> do is <em>validate</em>
 * the request — a partitioning column of the wrong type, or a fifth clustering column, would be
 * stored here and refused by BigQuery — or give the settings any <em>effect</em>: no partition
 * pruning, no expiry, no clustered storage. Whether the service accepts the request at all is
 * measured by {@code BigQueryTableCreationFidelityITCase} against real BigQuery, which is the only
 * thing that can measure it.
 */
class BigQueryTableCreateOptionsITCase extends BigQueryTableTestBase {

    private static final String COLUMNS = "name STRING, event_ts TIMESTAMP_LTZ(6), region STRING";

    @Test
    void createsAColumnPartitionedAndClusteredTable() throws Exception {
        String table = "table_create_partitioned";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events ("
                        + COLUMNS
                        + ") "
                        + withOptions(
                                table,
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
                                + " VALUES ('alice', TIMESTAMP '2026-08-06 00:00:00', 'jp')")
                .await();

        StandardTableDefinition definition = definitionOf(table);
        TimePartitioning partitioning = definition.getTimePartitioning();
        assertThat(partitioning).isNotNull();
        assertThat(partitioning.getType()).isEqualTo(TimePartitioning.Type.DAY);
        assertThat(partitioning.getField()).isEqualTo("event_ts");
        assertThat(partitioning.getExpirationMs()).isEqualTo(Duration.ofDays(90).toMillis());
        assertThat(definition.getClustering()).isNotNull();
        assertThat(definition.getClustering().getFields()).containsExactly("region", "name");
        assertThat(queryNames(table)).containsExactly("alice");
    }

    @Test
    void createsAnIngestionTimePartitionedTable() throws Exception {
        String table = "table_create_ingestion_time";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events (name STRING) "
                        + withOptions(table, "sink.table-create.time-partitioning.type", "month"));

        tEnv.executeSql("INSERT INTO events VALUES ('alice')").await();

        StandardTableDefinition definition = definitionOf(table);
        assertThat(definition.getTimePartitioning()).isNotNull();
        assertThat(definition.getTimePartitioning().getType())
                .isEqualTo(TimePartitioning.Type.MONTH);
        // Ingestion time is the granularity without a column — the case PARTITIONED BY could never
        // have expressed, and the reason these options are not that clause.
        assertThat(definition.getTimePartitioning().getField()).isNull();
        assertThat(definition.getClustering()).isNull();
    }

    @Test
    void leavesAnUnconfiguredTablePlain() throws Exception {
        String table = "table_create_plain";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql("CREATE TABLE events (name STRING) " + withOptions(table));

        tEnv.executeSql("INSERT INTO events VALUES ('alice')").await();

        StandardTableDefinition definition = definitionOf(table);
        assertThat(definition.getTimePartitioning()).isNull();
        assertThat(definition.getClustering()).isNull();
    }

    @Test
    void refusesCreationSettingsBesideAnExplicitCreateNever() {
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events (name STRING) "
                        + withOptions(
                                "table_create_never",
                                "sink.create-disposition",
                                "create-never",
                                "sink.table-create.time-partitioning.type",
                                "day"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')").await())
                // The first needle is a phrase only this connector's message carries; the option
                // key and 'create-never' alone would also match FactoryUtil's own dump of the
                // WITH clause, which it attaches to everything the factory throws.
                .hasStackTraceContaining("configure a table this sink never creates")
                .hasStackTraceContaining("sink.table-create.time-partitioning.type")
                .hasStackTraceContaining("create-never");
    }

    @Test
    void refusesAPartitioningColumnTheTableDoesNotDeclare() {
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events ("
                        + COLUMNS
                        + ") "
                        + withOptions(
                                "table_create_unknown_column",
                                "sink.table-create.time-partitioning.type",
                                "day",
                                "sink.table-create.time-partitioning.field",
                                "created_at"));

        // Through the planner, not only in the factory unit test: this is the failure that would
        // otherwise reach the service, where the emulator accepts it and BigQuery does not.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO events"
                                                + " VALUES ('alice', TIMESTAMP '2026-08-06"
                                                + " 00:00:00', 'jp')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("which the table does not declare")
                .hasStackTraceContaining("created_at");
    }

    private static StandardTableDefinition definitionOf(String table) {
        Table live = restClient.getTable(TableId.of(DATASET, table));
        assertThat(live).as("table %s exists", table).isNotNull();
        return live.getDefinition();
    }
}
