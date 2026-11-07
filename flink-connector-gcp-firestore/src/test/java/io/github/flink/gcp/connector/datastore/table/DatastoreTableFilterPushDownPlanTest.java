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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins which predicates the {@code datastore} scan pushes into its query with {@code
 * scan.filter-push-down.enabled}, that it pushes none without it, and that every predicate stays a
 * residual Flink evaluates ({@code docs/adr/0184}). Plans only; the rows are {@code
 * DatastoreTableFilterPushDownITCase}'s.
 */
class DatastoreTableFilterPushDownPlanTest {

    private static final String COLUMNS =
            "id STRING, s STRING, n BIGINT, d DOUBLE, b BOOLEAN, y BYTES, ts TIMESTAMP_LTZ(6),"
                    + " ts9 TIMESTAMP_LTZ(9), ts5 TIMESTAMP_LTZ(5), c CHAR(3), bin BINARY(2),"
                    + " u STRING, `a.b` STRING, `a``b` STRING, arr ARRAY<STRING>,"
                    + " e ROW<x BIGINT>, loc ROW<latitude DOUBLE, longitude DOUBLE>,"
                    + " owner ROW<kind STRING, name STRING, id BIGINT>,"
                    + " keyName STRING METADATA FROM 'key-name' VIRTUAL";

    private static final String COMMON =
            "'connector' = 'datastore', 'project' = 'p', 'kind' = 'K',"
                    + " 'sink.unindexed-columns' = 'u', 'geo-point-field-paths' = 'loc',"
                    + " 'reference-field-paths' = 'owner'";

    private TableEnvironment table;

    @BeforeEach
    void createTables() {
        table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE TABLE f ("
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + COMMON
                        + ", 'scan.filter-push-down.enabled' = 'true')");
        table.executeSql(
                "CREATE TABLE off ("
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + COMMON
                        + ")");
    }

    private String scan(String query) {
        String plan = table.explainSql(query);
        // The optimized physical plan alone: the abstract syntax tree repeats the predicate.
        return plan.substring(
                plan.indexOf("== Optimized Physical Plan =="),
                plan.indexOf("== Optimized Execution Plan =="));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = 'x'",
                "'x' = s",
                "n = 5",
                "5 = n",
                "b",
                "NOT b",
                "b = FALSE",
                "y = x'0102'",
                "ts = TO_TIMESTAMP_LTZ(1000, 3)",
                "ts9 = TO_TIMESTAMP_LTZ(1000, 3)"
            })
    void anEqualityOnAScalarColumnIsPushedAndStaysAResidual(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        assertThat(plan).containsPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = 'x'",
                "n = 5",
                "b",
                "s = 'x' AND n = 5",
            })
    void withoutTheOptionNothingIsPushed(String predicate) {
        String plan = scan("SELECT id FROM off WHERE " + predicate);

        // Flink 2.4 prints a declined result as an empty filter=[]; earlier versions print none.
        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // The key is not a property.
                "id = 'k'",
                // Written excluded from indexes, so no filter on it can match.
                "u = 'x'",
                // The service would read the name as a path into an embedded entity.
                "`a.b` = 'q'",
                "`a``b` = 'q'",
                // The converter reads a stored integer into DOUBLE, which 5.0 does not match.
                "d = 5.5",
                "d = 5",
                // Fixed-length types, unmeasured.
                "c = 'ab'",
                "bin = x'0102'",
                // The converter truncates a stored timestamp below the service's precision.
                "ts5 = TO_TIMESTAMP_LTZ(1000, 3)",
                // A missing property reads as NULL and matches no filter.
                "s IS NULL",
                "s IS NOT NULL",
                "s <> 'x'",
                "n > 5",
                "s = 'x' OR n = 5",
                "s IN ('x', 'y')",
                "n = CAST(d AS BIGINT)",
                "arr = ARRAY['x']",
                "e = ROW(1)",
                "e.x = 1",
                "s LIKE 'x%'",
                "keyName = 'k'",
                // Past the 1,500 bytes the service indexes of a value.
                "s = REPEAT('x', 1501)",
                // The limit counts UTF-8 bytes: 751 two-byte characters are 1,502 bytes.
                "s = REPEAT('é', 751)",
                // Before the earliest timestamp the service represents.
                "ts = TO_TIMESTAMP_LTZ(-62135596801, 0)",
                // Finer than the microseconds a stored timestamp holds.
                "ts9 = CAST(TIMESTAMP '1970-01-01 00:00:01.000000001' AS TIMESTAMP_LTZ(9))"
            })
    void anythingElseIsNotPushed(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = REPEAT('x', 1500)",
                "s = REPEAT('é', 750)",
                "ts = TO_TIMESTAMP_LTZ(-62135596800, 0)",
                "ts = TO_TIMESTAMP_LTZ(253402300799, 0)",
                "ts9 = CAST(TIMESTAMP '1970-01-01 00:00:01.000001' AS TIMESTAMP_LTZ(9))"
            })
    void theBoundaryValuesArePushed(String predicate) {
        assertThat(scan("SELECT id FROM f WHERE " + predicate)).containsPattern("filter=\\[[^\\]]");
    }

    @Test
    void aBytesLiteralIsPushedUpToTheIndexedLengthOnly() {
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1500) + "'"))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1501) + "'"))
                .doesNotContainPattern("filter=\\[[^\\]]");
    }

    @Test
    void theTranslatableConjunctsOfAConjunctionArePushedAndTheWholeStaysResidual() {
        String plan = scan("SELECT id FROM f WHERE s = 'x' AND n > 5 AND b");

        assertThat(plan)
                .containsPattern("filter=\\[[^\\]]*=\\(s, ")
                .doesNotContainPattern("filter=\\[[^\\]]*>\\(n")
                .containsPattern("where=\\[[^\\]]*>\\(n, 5\\)")
                .containsPattern("where=\\[[^\\]]*=\\(s, 'x'\\)");
    }
}
