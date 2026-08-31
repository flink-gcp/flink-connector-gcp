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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Planner-level coverage for BigQuery source abilities. */
class BigQueryTablePlanTest {

    @Test
    void aSafeFilterIsPushedAndAlsoRetainedAboveTheSource() {
        TableEnvironment table = tableEnvironment();
        createDirectSource(table, "events");

        String plan = table.explainSql("SELECT name FROM events WHERE id >= 7");

        assertThat(plan)
                .contains("filter=[>=(id")
                .contains("project=[id, name]")
                .contains("Calc(select=[name], where=[>=(id");
    }

    @Test
    void aDoubleFilterIsPushedAndAlsoRetainedAboveTheSource() {
        TableEnvironment table = tableEnvironment();
        createDirectSource(table, "events");

        String plan = table.explainSql("SELECT name FROM events WHERE score > 0.5");

        assertThat(plan)
                .contains("filter=[>(score")
                .contains("project=[name, score]")
                .contains("Calc(select=[name], where=[>(score");
    }

    @Test
    void projectionSafeAndStringFiltersCompose() {
        TableEnvironment table = tableEnvironment();
        createDirectSource(table, "events");

        String plan = table.explainSql("SELECT name FROM events WHERE id >= 7 AND name = 'alice'");

        assertThat(plan)
                // Flink renders AND in lower case through 2.3 and in upper case on 2.4-SNAPSHOT.
                // Which spelling it picks is not what this test is about, so both are matched.
                .containsIgnoringCase("filter=[and(>=(id")
                .contains("project=[id, name]")
                .containsIgnoringCase("where=[and(>=(id")
                .contains("=(name");
    }

    @Test
    void addedScalarTypesReachTheSourceAsLiteralComparisons() {
        TableEnvironment table = tableEnvironment();
        createDirectSource(table, "events");

        assertPushed(table, "name = 'alice'", "=(name");
        assertPushed(table, "amount = 7.250000000", "=(amount");
        assertPushed(table, "single_score <= CAST(1.0 AS FLOAT)", "<=(single_score");
        assertPushed(
                table, "civil_time >= TIMESTAMP '2026-08-29 12:34:56.123456'", ">=(civil_time");
        assertPushed(
                table,
                "event_time >= CAST(TIMESTAMP '2026-08-29 03:34:56.123456' AS TIMESTAMP_LTZ(6))",
                ">=(event_time");
    }

    @Test
    void deliberatelyUnsupportedShapesRemainOnlyAboveTheSource() {
        TableEnvironment table = tableEnvironment();
        createDirectSource(table, "events");

        assertResidual(table, "fixed > 'aaaa'", "fixed");
        assertResidual(table, "fixed IS NULL", "fixed");
        assertResidual(table, "name <> 'alice'", "name");
        assertResidual(table, "millis_time >= TIMESTAMP '2026-08-29 12:34:56.123'", "millis_time");
        assertResidual(table, "millis_time IS NOT NULL", "millis_time");
        assertResidual(table, "UPPER(name) = 'ALICE'", "name");
        assertResidual(table, "id = other_id", "id");
    }

    @Test
    void querySourcesExposeTheSameFilterAbility() {
        TableEnvironment table = tableEnvironment();
        table.executeSql(
                "CREATE TABLE query_events (id BIGINT, name STRING, score DOUBLE) WITH ("
                        + "'connector'='bigquery', 'project'='p', "
                        + "'scan.query'='SELECT id, name, score FROM `p.d.t`')");

        String plan = table.explainSql("SELECT name FROM query_events WHERE id = 7");

        assertThat(plan)
                .contains("filter=[=(id")
                .contains("project=[id, name]")
                .contains("Calc(select=[name], where=[=(id");
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static void createDirectSource(TableEnvironment table, String name) {
        table.executeSql(
                "CREATE TABLE "
                        + name
                        + " (id BIGINT, other_id BIGINT, name STRING, score DOUBLE, "
                        + "amount DECIMAL(38, 9), single_score FLOAT, fixed CHAR(4), "
                        + "civil_time TIMESTAMP(6), event_time TIMESTAMP_LTZ(6), "
                        + "millis_time TIMESTAMP(3)) WITH ("
                        + "'connector'='bigquery', 'project'='p', 'dataset'='d', 'table'='t')");
    }

    private static void assertPushed(TableEnvironment table, String predicate, String marker) {
        String plan = table.explainSql("SELECT name FROM events WHERE " + predicate);
        assertThat(plan)
                .contains("filter=[")
                .doesNotContain("filter=[]")
                .contains(marker)
                .contains("Calc(");
    }

    private static void assertResidual(TableEnvironment table, String predicate, String marker) {
        String plan = table.explainSql("SELECT name FROM events WHERE " + predicate);
        assertThat(plan).contains("filter=[]").contains("Calc(").contains(marker);
    }
}
