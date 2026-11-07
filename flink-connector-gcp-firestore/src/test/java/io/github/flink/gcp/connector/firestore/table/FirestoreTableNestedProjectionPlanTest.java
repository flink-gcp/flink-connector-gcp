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
 * Pins which projections the {@code firestore} scan receives as nested paths ({@code
 * docs/adr/0179}, the revision for #1839). Plans only; the rows are {@code
 * FirestoreTableNestedProjectionITCase}'s, and the field mask is {@code
 * FirestoreNestedProjectionTest}'s.
 */
class FirestoreTableNestedProjectionPlanTest {

    private TableEnvironment table;

    @BeforeEach
    void createTables() {
        table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        String columns =
                "id STRING, r ROW<a BIGINT, b STRING, c ROW<x BIGINT>>,"
                        + " `p.q` ROW<`u.v` STRING>, arr ARRAY<ROW<a BIGINT>>,"
                        + " m MAP<STRING, BIGINT>, n ROW<a BIGINT NOT NULL> NOT NULL";
        table.executeSql(
                "CREATE TABLE f ("
                        + columns
                        + ", PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore',"
                        + " 'project' = 'p', 'collection' = 'c')");
        table.executeSql(
                "CREATE TABLE g ("
                        + columns
                        + ") WITH ('connector' = 'firestore', 'project' = 'p', 'collection' = 'c',"
                        + " 'scan.collection-group' = 'true')");
        // Through the Table API: Flink 1.20 drops a NOT NULL that DDL declares on a ROW's field.
        table.createTemporaryTable(
                "strict",
                TableDescriptor.forConnector("firestore")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING().notNull())
                                        .column(
                                                "r",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.BIGINT().notNull()),
                                                        DataTypes.FIELD("b", DataTypes.STRING())))
                                        .column("s", DataTypes.STRING())
                                        .primaryKey("id")
                                        .build())
                        .option("project", "p")
                        .option("collection", "c")
                        .build());
    }

    private String scan(String query) {
        String plan = table.explainSql(query);
        return plan.substring(
                plan.indexOf("== Optimized Physical Plan =="),
                plan.indexOf("== Optimized Execution Plan =="));
    }

    @ParameterizedTest
    @ValueSource(strings = {"f", "g"})
    void aFieldOfARowIsProjectedAsItsPath(String name) {
        assertThat(scan("SELECT r.a FROM " + name)).contains("project=[r_a]");
        assertThat(scan("SELECT r.c.x, r.b FROM " + name)).contains("project=[r_c_x, r_b]");
        assertThat(scan("SELECT `p.q`.`u.v` FROM " + name)).contains("project=[p.q_u.v]");
        assertThat(scan("SELECT n.a FROM " + name)).contains("project=[n_a]");
    }

    @Test
    void aFilterOnANestedFieldIsPushedByItsPathNotUnderTheProjectedName() {
        // The projected field is named r_a. Filters are pushed before the projection, against
        // the declared columns, so a predicate on r.a names the path r.a (docs/adr/0197's #2066
        // revision) and one on the column r_a names that column.
        table.executeSql(
                "CREATE TABLE h (id STRING, r ROW<a BIGINT>, r_a BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'firestore',"
                        + " 'project' = 'p', 'collection' = 'c')");

        assertThat(scan("SELECT r.a FROM h WHERE r.a = 5"))
                .containsPattern("filter=\\[=\\(r\\.a, 5")
                .doesNotContainPattern("filter=\\[[^\\]]*\\br_a\\b");
        assertThat(scan("SELECT r.a FROM h WHERE r_a = 5"))
                .containsPattern("filter=\\[=\\(r_a, 5")
                .containsPattern("project=\\[[^\\]]*\\br_a\\b");
    }

    @Test
    void aWholeRowNamedBesideItsFieldIsProjectedWhole() {
        assertThat(scan("SELECT r.a, r FROM f")).contains("project=[r]");
    }

    @Test
    void aLookupReadsWholeColumns() {
        TableEnvironment stream = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        stream.executeSql(
                "CREATE TABLE d (id STRING, r ROW<a BIGINT, b STRING>, PRIMARY KEY (id) NOT"
                        + " ENFORCED) WITH ('connector' = 'firestore', 'project' = 'p',"
                        + " 'collection' = 'c')");
        stream.executeSql(
                "CREATE TABLE k (k STRING, pt AS PROCTIME()) WITH ('connector' = 'datagen')");

        String plan =
                stream.explainSql(
                        "SELECT k.k, d.r.a FROM k JOIN d FOR SYSTEM_TIME AS OF k.pt ON k.k = d.id");

        assertThat(plan)
                .contains("LookupJoin(")
                .contains("select=[k, id, r]")
                .doesNotContain("r_a");
    }

    @Test
    void anArrayOrMapIsProjectedWhole() {
        assertThat(scan("SELECT arr[1].a, m['k'] FROM f")).contains("project=[arr, m]");
    }

    @Test
    void aTableWithANotNullFieldInANullableRowProjectsWholeColumns() {
        // Flink would produce r_a NOT NULL although r may be NULL.
        assertThat(table.from("strict").getResolvedSchema().toString())
                .contains("`a` BIGINT NOT NULL");
        assertThat(scan("SELECT r.a FROM strict")).contains("project=[r]");
        assertThat(scan("SELECT r.b, s FROM strict")).contains("project=[r, s]");
    }
}
