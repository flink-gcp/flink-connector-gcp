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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins which statements push a limit into the {@code datastore} scan (with or without property
 * filter push-down) and that Flink keeps its own limit above it ({@code docs/adr/0201}). Plans
 * only, in batch and streaming mode alike; the rows are {@code
 * DatastoreTableLimitPushDownITCase}'s.
 */
class DatastoreTableLimitPushDownPlanTest {

    private static String scan(boolean batch, String query) {
        TableEnvironment table =
                TableEnvironment.create(
                        batch
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE TABLE t (id STRING NOT NULL, s STRING, n BIGINT, PRIMARY KEY (id) NOT"
                        + " ENFORCED) WITH ('connector' = 'datastore', 'project' = 'p',"
                        + " 'kind' = 'K', 'scan.filter-push-down.enabled' = 'true')");
        table.executeSql(
                "CREATE TABLE u (id STRING NOT NULL, s STRING, n BIGINT, PRIMARY KEY (id) NOT"
                        + " ENFORCED) WITH ('connector' = 'datastore', 'project' = 'p',"
                        + " 'kind' = 'K')");
        String plan = table.explainSql(query);
        // The optimized physical plan alone: the abstract syntax tree repeats the statement.
        return plan.substring(
                plan.indexOf("== Optimized Physical Plan =="),
                plan.indexOf("== Optimized Execution Plan =="));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SELECT * FROM t LIMIT 5",
                "SELECT s FROM t LIMIT 5",
                "SELECT UPPER(s) FROM t LIMIT 5",
                // A filter above the limit stays above it.
                "SELECT * FROM (SELECT * FROM t LIMIT 5) WHERE s = 'x'",
                "SELECT * FROM u LIMIT 5",
            })
    void aLimitDirectlyAboveTheScanIsPushedAndKept(String query) {
        for (boolean batch : new boolean[] {true, false}) {
            String plan = scan(batch, query);

            assertThat(plan).as("batch=%s", batch).containsPattern("limit=\\[\\d+\\]");
            assertThat(plan).as("batch=%s", batch).contains("Limit(");
        }
    }

    @Test
    void aFilterAboveALimitedSubqueryStaysAboveTheScan() {
        for (boolean batch : new boolean[] {true, false}) {
            String plan = scan(batch, "SELECT * FROM (SELECT * FROM t LIMIT 5) WHERE s = 'x'");

            assertThat(plan).as("batch=%s", batch).contains("limit=[5]").doesNotContain("filter=[");
        }
    }

    @Test
    void anOffsetIsPushedWithTheFetch() {
        for (boolean batch : new boolean[] {true, false}) {
            assertThat(scan(batch, "SELECT * FROM t LIMIT 5 OFFSET 3"))
                    .as("batch=%s", batch)
                    .contains("limit=[8]");
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // Every predicate stays a residual, so a filter, pushed or not, sits between.
                "SELECT * FROM t WHERE s = 'x' LIMIT 5",
                "SELECT * FROM t WHERE n > 1 LIMIT 5",
                "SELECT * FROM t ORDER BY n LIMIT 5",
                "SELECT s, COUNT(*) FROM t GROUP BY s LIMIT 5",
                "SELECT DISTINCT s FROM t LIMIT 5",
                // A key equality is pushed without the option.
                "SELECT * FROM u WHERE id = 'k' LIMIT 5",
                "SELECT * FROM u WHERE s = 'x' LIMIT 5",
            })
    void aLimitAboveAnotherOperatorIsNotPushed(String query) {
        for (boolean batch : new boolean[] {true, false}) {
            assertThat(scan(batch, query)).as("batch=%s", batch).doesNotContain("limit=[");
        }
    }
}
