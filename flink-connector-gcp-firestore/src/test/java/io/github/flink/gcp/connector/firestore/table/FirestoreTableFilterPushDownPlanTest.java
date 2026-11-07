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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
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
                    + " `a.b` STRING, arr ARRAY<STRING>,"
                    + " e ROW<x BIGINT, flag BOOLEAN, `y.z` ROW<w STRING>, link STRING>,"
                    + " loc ROW<latitude DOUBLE, longitude DOUBLE>";

    private TableEnvironment table;

    @BeforeEach
    void createTables() {
        table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE TABLE f ("
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore',"
                        + " 'project' = 'p', 'collection' = 'c', 'reference-field-paths' = 'r;e.link',"
                        + " 'geo-point-field-paths' = 'loc')");
        table.executeSql(
                "CREATE TABLE g ("
                        + COLUMNS
                        + ") WITH ('connector' = 'firestore', 'project' = 'p', 'collection' = 'c',"
                        + " 'scan.collection-group' = 'true', 'geo-point-field-paths' = 'loc')");
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
                "`a.b` = 'q'",
                // The document id, as a filter on the document's name.
                "id = 'k'",
                "'k' = id",
                // A fixed-length column beside a literal of its length, which the planner
                // presents without a cast.
                "c = 'abc'",
                "bin = x'0102'",
                // A field nested in ROW columns, as a path of literal segments.
                "e.x = 1",
                "e.flag",
                "NOT e.flag",
                "e.`y.z`.w = 'q'",
                // A timestamp read below microseconds, as the range of stored values that
                // truncate to the literal.
                "ts3 = TO_TIMESTAMP_LTZ(1000, 3)",
                "ts5 = TO_TIMESTAMP_LTZ(1000, 3)",
                // Inequalities, either way round, and their negations the planner folds.
                "n > 5",
                "5 < n",
                "n >= 5",
                "n < 5",
                "n <= 5",
                "NOT (n > 5)",
                "n BETWEEN 3 AND 7",
                "d > 5",
                "d < 5.5",
                "s > 'a'",
                "s < '\uD7FF'",
                "s <> '\uFFFD'",
                "y > x'01'",
                "b > FALSE",
                "ts > TO_TIMESTAMP_LTZ(1000, 3)",
                "ts3 <= TO_TIMESTAMP_LTZ(1000, 3)",
                "e.x > 1",
                "s <> 'x'",
                "n NOT IN (1, 2)",
                "s IS NOT NULL",
                "e.x IS NOT NULL",
                "c IS NOT NULL",
                // IN and an OR of equalities on one column, which the planner presents alike.
                "s IN ('x', 'y')",
                "n = 1 OR n = 2",
                "e.x IN (1, 2)",
                // The document id's ranges and IN.
                "id > 'k'",
                "id <= 'k'",
                "id IN ('a', 'b')",
            })
    void aComparisonOnAScalarColumnIsPushedAndStaysAResidual(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        assertThat(plan).containsPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // An id the service cannot store in the collection names no document.
                "id = 'k/x'",
                "id = ''",
                "id = '__k__'",
                "id = '.'",
                "id = REPEAT('x', 1501)",
                // A reference never equals a string.
                "r = 'c/x'",
                "e.link = 'c/x'",
                // A geographical point is one value, not a map.
                "loc.latitude = 1.5",
                // The planner presents a fixed-length column beside a shorter literal as a cast.
                "c = 'ab'",
                "bin = x'01'",
                // A missing field reads as NULL and matches no filter.
                "s IS NULL",
                // An OR across columns, or holding anything but equalities.
                "s = 'x' OR n = 5",
                "n = 1 OR n > 5",
                "n = CAST(d AS BIGINT)",
                // A range literal holding a character UTF-8 and UTF-16 could order unlike each
                // other: Flink compares two Java strings by UTF-16 code units.
                "s > '\uFFFD'",
                "s < '\uD83D\uDE00'",
                "s >= '\uD7FF\uE000'",
                "id > '\uFFFD'",
                // No ordering of fixed-length values was measured.
                "c > 'abc'",
                "bin > x'0102'",
                // A document always has its id.
                "id <> 'k'",
                "id > ''",
                "id > 'k/x'",
                // Whether an empty array or map matches != null is unmeasured.
                "arr IS NOT NULL",
                "e IS NOT NULL",
                "r IS NOT NULL",
                "loc IS NOT NULL",
                "r > 'c/x'",
                "loc.latitude > 1.5",
                // A range literal of 1,500 bytes could equal the prefix of a longer value.
                "s > REPEAT('x', 1500)",
                // More disjuncts than a query takes.
                "n IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,"
                        + " 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)",
                "arr = ARRAY['x']",
                // The planner presents no ARRAY_CONTAINS to a source.
                "ARRAY_CONTAINS(arr, 'x')",
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
                "ts = TO_TIMESTAMP_LTZ(-62135596800, 0)",
                "id = REPEAT('x', 1500)",
                // Three underscores cannot be read as a leading and a trailing double underscore.
                "id = '___'"
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
    @ValueSource(strings = {"s = 'x'", "n = 5 AND b", "e.x = 1"})
    void aCollectionGroupScanPushesNothing(String predicate) {
        String plan = scan("SELECT id FROM g WHERE " + predicate);

        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @Test
    void aNotNullFieldIsPushedOnlyWhereNoNullableRowLiesAboveIt() {
        // Flink 1.20.5 drops a ROW field's NOT NULL from DDL, so the table is built from data
        // types, which every supported version keeps. Flink takes loose.a to be never NULL,
        // although a NULL loose makes it so; held.a is never NULL in Flink or in the document.
        table.createTemporaryTable(
                "nested",
                TableDescriptor.forConnector("firestore")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING().notNull())
                                        .column(
                                                "loose",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.BIGINT().notNull())))
                                        .column(
                                                "held",
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "a",
                                                                        DataTypes.BIGINT()
                                                                                .notNull()))
                                                        .notNull())
                                        .primaryKey("id")
                                        .build())
                        .option("project", "p")
                        .option("collection", "c")
                        .build());

        assertThat(scan("SELECT id FROM nested WHERE loose.a = 1"))
                .doesNotContainPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM nested WHERE held.a = 1"))
                .containsPattern("filter=\\[[^\\]]");
    }

    @Test
    void theTranslatableConjunctsOfAConjunctionArePushedAndTheWholeStaysResidual() {
        String plan = scan("SELECT id FROM f WHERE s = 'x' AND s LIKE 'x%' AND n > 5");

        assertThat(plan)
                .containsPattern("filter=\\[[^\\]]*=\\(s, ")
                .containsPattern("filter=\\[[^\\]]*>\\(n, 5\\)")
                .doesNotContainPattern("filter=\\[[^\\]]*LIKE")
                .containsPattern("where=\\[[^\\]]*LIKE\\(s")
                .containsPattern("where=\\[[^\\]]*>\\(n, 5\\)")
                .containsPattern("where=\\[[^\\]]*=\\(s, 'x'\\)");
    }

    @Test
    void onlyTheFirstNotEqualIsPushed() {
        String plan = scan("SELECT id FROM f WHERE n <> 5 AND s <> 'x'");

        assertThat(plan)
                .containsPattern("filter=\\[[^\\]]*<>\\(n, 5")
                .doesNotContainPattern("filter=\\[[^\\]]*<>\\(s");
    }

    @ParameterizedTest
    @ValueSource(strings = {"n > 5", "s IN ('x', 'y')", "s IS NOT NULL", "id > 'k'", "s = 'x'"})
    void withoutTheOptionOnlyAnIdEqualityIsPushed(String predicate) {
        table.executeSql(
                "CREATE TABLE off ("
                        + COLUMNS
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore',"
                        + " 'project' = 'p', 'collection' = 'c',"
                        + " 'scan.filter-push-down.enabled' = 'false')");

        assertThat(scan("SELECT id FROM off WHERE " + predicate))
                .doesNotContainPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM off WHERE id = 'k'")).containsPattern("filter=\\[[^\\]]");
    }
}
