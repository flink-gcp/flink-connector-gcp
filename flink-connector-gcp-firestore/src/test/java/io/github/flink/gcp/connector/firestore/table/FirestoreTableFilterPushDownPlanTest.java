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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins which predicates the {@code firestore} scan pushes into its query and that every predicate
 * stays a residual Flink evaluates ({@code docs/adr/0197}). Plans only; the rows are {@code
 * FirestoreTableFilterPushDownITCase}'s.
 */
class FirestoreTableFilterPushDownPlanTest {

    private static final String COLUMNS =
            "id STRING, s STRING, n BIGINT, d DOUBLE, b BOOLEAN, y BYTES, ts TIMESTAMP_LTZ(6),"
                    + " ts9 TIMESTAMP_LTZ(9), ts5 TIMESTAMP_LTZ(5), ts3 TIMESTAMP_LTZ(3), c CHAR(3),"
                    + " bin BINARY(2), r STRING,"
                    + " `a.b` STRING, arr ARRAY<STRING>";

    private TableEnvironment table;

    @BeforeEach
    void createTables() {
        table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE TABLE f ("
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore',"
                        + " 'project' = 'p', 'collection' = 'c', 'reference-field-paths' = 'r')");
        table.executeSql(
                "CREATE TABLE g ("
                        + COLUMNS
                        + ") WITH ('connector' = 'firestore', 'project' = 'p', 'collection' = 'c',"
                        + " 'scan.collection-group' = 'true')");
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
                "d = 5.5",
                "d = 5",
                "b",
                "NOT b",
                "b = FALSE",
                "y = x'0102'",
                "ts = TO_TIMESTAMP_LTZ(1000, 3)",
                "ts9 = TO_TIMESTAMP_LTZ(1000, 3)",
                "`a.b` = 'q'"
            })
    void anEqualityOnAScalarColumnIsPushedAndStaysAResidual(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        assertThat(plan).containsPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // The document id is not a field.
                "id = 'k'",
                // A reference never equals a string.
                "r = 'c/x'",
                // Fixed-length types, whose comparison semantics against a stored value are
                // unmeasured.
                "c = 'ab'",
                // The converter truncates a stored timestamp to a precision below the service's.
                "ts3 = TO_TIMESTAMP_LTZ(1000, 3)",
                "ts5 = TO_TIMESTAMP_LTZ(1000, 3)",
                "bin = x'0102'",
                // A missing field reads as NULL and matches no filter.
                "s IS NULL",
                "s IS NOT NULL",
                "s <> 'x'",
                "n > 5",
                "s = 'x' OR n = 5",
                "s IN ('x', 'y')",
                "n = CAST(d AS BIGINT)",
                "arr = ARRAY['x']",
                "s LIKE 'x%'",
                // Past the 1,500 bytes a Standard-edition database considers of a value in a query.
                "s = REPEAT('x', 1501)",
                // The limit counts UTF-8 bytes: 751 two-byte characters are 1,502 bytes.
                "s = REPEAT('\u00e9', 751)",
                // Before the earliest timestamp the service represents.
                "ts = TO_TIMESTAMP_LTZ(-62135596801, 0)"
            })
    void anythingElseIsNotPushed(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        // Flink 2.4 prints a declined result as an empty filter=[]; earlier versions print none.
        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s = REPEAT('x', 1500)",
                "s = REPEAT('\u00e9', 750)",
                "ts = TO_TIMESTAMP_LTZ(-62135596800, 0)"
            })
    void theBoundaryValuesArePushed(String predicate) {
        assertThat(scan("SELECT id FROM f WHERE " + predicate)).containsPattern("filter=\\[[^\\]]");
    }

    @Test
    void aBytesLiteralIsPushedUpToTheIndexedPrefixOnly() {
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1500) + "'"))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1501) + "'"))
                .doesNotContainPattern("filter=\\[[^\\]]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"s = 'x'", "n = 5 AND b"})
    void aCollectionGroupScanPushesNothing(String predicate) {
        String plan = scan("SELECT id FROM g WHERE " + predicate);

        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
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
