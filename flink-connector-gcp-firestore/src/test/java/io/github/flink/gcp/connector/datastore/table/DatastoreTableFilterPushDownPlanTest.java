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
 * Pins which predicates the {@code datastore} scan pushes into its query with {@code
 * scan.filter-push-down.enabled}, that it pushes only a key equality without it, and that every
 * predicate stays a residual Flink evaluates ({@code docs/adr/0184}). Plans only; the rows are
 * {@code DatastoreTableFilterPushDownITCase}'s.
 */
class DatastoreTableFilterPushDownPlanTest {

    private static final String COLUMNS =
            "id STRING, s STRING, n BIGINT, d DOUBLE, b BOOLEAN, y BYTES, ts TIMESTAMP_LTZ(6),"
                    + " ts9 TIMESTAMP_LTZ(9), ts5 TIMESTAMP_LTZ(5), c CHAR(3), bin BINARY(2),"
                    + " u STRING, `a.b` STRING, `a``b` STRING, arr ARRAY<STRING>,"
                    + " e ROW<x BIGINT, flag BOOLEAN, `y.z` ROW<w STRING>>,"
                    + " loc ROW<latitude DOUBLE, longitude DOUBLE>,"
                    + " owner ROW<kind STRING, name STRING, id BIGINT>, ue ROW<x BIGINT>,"
                    + " `a\\b` STRING,"
                    + " keyName STRING METADATA FROM 'key-name' VIRTUAL";

    private static final String COMMON =
            "'connector' = 'datastore', 'project' = 'p', 'kind' = 'K',"
                    + " 'sink.unindexed-columns' = 'u;ue', 'geo-point-field-paths' = 'loc',"
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
        table.executeSql(
                "CREATE TABLE numbered (id BIGINT, s STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector' = 'datastore', 'project' = 'p', 'kind' = 'K')");
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
                "ts9 = TO_TIMESTAMP_LTZ(1000, 3)",
                "id = 'k'",
                // A literal no stored integer equals, which the converter could read as one.
                "d = 5.5",
                "d = -0.5",
                // A fixed-length column beside a literal of its length, which the planner
                // presents without a cast.
                "c = 'abc'",
                "bin = x'0102'",
                // A name holding a dot reads as a path, which also matches the property of that
                // name; a backquote is no quoting character in a filter's property.
                "`a.b` = 'q'",
                "`a``b` = 'q'",
                "`a\\b` = 'q'",
                // A field of an embedded entity, as a property path.
                "e.x = 1",
                "e.flag",
                "NOT e.flag",
                "e.`y.z`.w = 'q'",
            })
    void anEqualityOnAScalarColumnIsPushedAndStaysAResidual(String predicate) {
        String plan = scan("SELECT id FROM f WHERE " + predicate);

        assertThat(plan).containsPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(strings = {"s = 'x'", "n = 5", "b", "s = 'x' AND n = 5", "e.x = 1"})
    void withoutTheOptionNoPropertyIsPushed(String predicate) {
        String plan = scan("SELECT id FROM off WHERE " + predicate);

        // Flink 2.4 prints a declined result as an empty filter=[]; earlier versions print none.
        assertThat(plan).doesNotContainPattern("filter=\\[[^\\]]").contains("where=[");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // A key name no root entity of the kind has.
                "id = ''",
                "id = '__k__'",
                // Four underscores: the emulator stores the name, the service is unmeasured.
                "id = '____'",
                "id = REPEAT('x', 1501)",
                // Written excluded from indexes, so no filter on it can match.
                "u = 'x'",
                "ue.x = 1",
                // The converter reads a stored integer into DOUBLE, which 5.0 does not match.
                "d = 5",
                "d = 0.0",
                "d = -0.0",
                // A geographical point and a key value are each one value, not an entity.
                "loc.latitude = 1.5",
                "owner.name = 'x'",
                // The planner presents a fixed-length column beside a shorter literal as a cast.
                "c = 'ab'",
                "bin = x'01'",
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
                // The planner presents no ARRAY_CONTAINS to a source.
                "ARRAY_CONTAINS(arr, 'x')",
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
                "ts9 = CAST(TIMESTAMP '1970-01-01 00:00:01.000001' AS TIMESTAMP_LTZ(9))",
                "id = REPEAT('x', 1500)",
                "id = '___'"
            })
    void theBoundaryValuesArePushed(String predicate) {
        assertThat(scan("SELECT id FROM f WHERE " + predicate)).containsPattern("filter=\\[[^\\]]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"id = 'k'", "'k' = id", "id = 'k' AND s = 'x'"})
    void aKeyEqualityIsPushedWithoutTheOption(String predicate) {
        String plan = scan("SELECT id FROM off WHERE " + predicate);

        assertThat(plan)
                .containsPattern("filter=\\[[^\\]]*\\bid\\b")
                .doesNotContainPattern("filter=\\[[^\\]]*\\bs\\b")
                .contains("where=[");
    }

    @Test
    void anIdKeyEqualityIsPushedUnlessTheIdNamesNoEntity() {
        assertThat(scan("SELECT s FROM numbered WHERE id = 5")).containsPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT s FROM numbered WHERE id = -5"))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT s FROM numbered WHERE id = 0"))
                .doesNotContainPattern("filter=\\[[^\\]]");
    }

    @Test
    void aBytesLiteralIsPushedUpToTheIndexedLengthOnly() {
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1500) + "'"))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM f WHERE y = x'" + "00".repeat(1501) + "'"))
                .doesNotContainPattern("filter=\\[[^\\]]");
    }

    @Test
    void aNotNullFieldIsPushedOnlyWhereNoNullableRowLiesAboveIt() {
        // Flink 1.20.5 drops a ROW field's NOT NULL from DDL, so the table is built from data
        // types, which every supported version keeps. Flink takes loose.a to be never NULL,
        // although a NULL loose makes it so; held.a is never NULL in Flink or in the entity.
        table.createTemporaryTable(
                "nested",
                TableDescriptor.forConnector("datastore")
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
                        .option("kind", "K")
                        .option("scan.filter-push-down.enabled", "true")
                        .build());

        assertThat(scan("SELECT id FROM nested WHERE loose.a = 1"))
                .doesNotContainPattern("filter=\\[[^\\]]");
        assertThat(scan("SELECT id FROM nested WHERE held.a = 1"))
                .containsPattern("filter=\\[[^\\]]");
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
