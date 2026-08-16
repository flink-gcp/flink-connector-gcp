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

import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code bigquery} table sink end to end: {@code CREATE TABLE} and {@code INSERT INTO} through
 * the planner, read back over REST.
 *
 * <p>Column types are chosen around what the emulator (0.8.1) implements: it has neither the packed
 * civil-time encoding nor the decimal byte encoding, and it rejects every insert into a table
 * carrying an {@code ARRAY&lt;JSON&gt;} column — so {@code TIME}, {@code DATETIME}, {@code NUMERIC}
 * and repeated JSON are covered by the unit tests and, where a value has to be seen, by the gated
 * real-GCP suite. What is exercised here is the layer this issue adds: the factory, the planner
 * path and the {@code RowData} serializer.
 */
class BigQueryTableSinkITCase extends BigQueryTableTestBase {

    @Test
    void writesRowsThroughThePlannerIntoATableItCreates() throws Exception {
        String table = "table_sink_plain";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events (\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  ok BOOLEAN\n"
                        + ") "
                        + withOptions(table));

        tEnv.executeSql("INSERT INTO events VALUES ('alice', 1, true), ('bob', 2, false)").await();

        assertThat(queryNames(table)).containsExactly("alice", "bob");
        assertThat(
                        query(
                                "SELECT amount FROM `"
                                        + PROJECT
                                        + "."
                                        + DATASET
                                        + "."
                                        + table
                                        + "` ORDER BY amount"))
                .containsExactly("1", "2");
    }

    @Test
    void writesANestedRowAndAMarkedJsonColumn() throws Exception {
        String table = "table_sink_nested";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events (\n"
                        + "  name STRING,\n"
                        + "  inner_row ROW<a BIGINT, b STRING>,\n"
                        + "  doc ROW<k STRING>\n"
                        + ") "
                        + withOptions(table, "sink.json-field-paths", "doc"));

        tEnv.executeSql("INSERT INTO events VALUES ('alice', ROW(1, 'x'), ROW('v'))").await();

        assertThat(queryNames(table)).containsExactly("alice");
        assertThat(query("SELECT inner_row.b FROM `" + PROJECT + "." + DATASET + "." + table + "`"))
                .containsExactly("x");
        // The marked ROW landed as JSON text rather than as a STRUCT.
        assertThat(
                        query(
                                "SELECT TO_JSON_STRING(doc) FROM `"
                                        + PROJECT
                                        + "."
                                        + DATASET
                                        + "."
                                        + table
                                        + "`"))
                .containsExactly("{\"k\":\"v\"}");
    }

    @Test
    void refusesToCreateTheTableUnderCreateNever() {
        String table = "table_sink_create_never";
        TableEnvironment tEnv = streamingTableEnvironment();

        tEnv.executeSql(
                "CREATE TABLE events (name STRING) "
                        + withOptions(table, "sink.create-disposition", "create-never"));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO events VALUES ('alice')").await())
                .hasStackTraceContaining("CREATE_NEVER");
    }

    @Test
    void refusesAnUpdatingQueryAtPlanTime() {
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events (name STRING, total BIGINT) "
                        + withOptions("table_sink_updating"));
        tEnv.executeSql(
                "CREATE TABLE source_rows (name STRING) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '1')");

        // Only the table name is asserted, not the planner's wording: the weekly matrix builds
        // against an unreleased Flink, where a rephrasing upstream would turn this red for no
        // reason. The connector-owned half — insertOnly() — is pinned in the factory test.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO events"
                                                + " SELECT name, COUNT(*) FROM source_rows"
                                                + " GROUP BY name"))
                .hasStackTraceContaining("events");
    }

    @Test
    void refusesAPartitionedByClauseAtPlanTime() {
        TableEnvironment tEnv = streamingTableEnvironment();
        // PARTITIONED BY models Hive-style value partitioning, which BigQuery time partitioning is
        // not — so the sink does not implement SupportsPartitioning and the clause fails loudly
        // rather than being silently ignored. Table creation goes through sink.table-create.*.
        tEnv.executeSql(
                "CREATE TABLE events (name STRING, part STRING) PARTITIONED BY (part) "
                        + withOptions("table_sink_partitioned"));

        assertThatThrownBy(
                        () -> tEnv.executeSql("INSERT INTO events VALUES ('alice', 'p')").await())
                .hasStackTraceContaining("artition");
    }

    private static List<String> query(String sql) throws Exception {
        List<String> values = new ArrayList<>();
        for (FieldValueList row : restClient.query(QueryJobConfiguration.of(sql)).iterateAll()) {
            values.add(row.get(0).getStringValue());
        }
        return values;
    }
}
