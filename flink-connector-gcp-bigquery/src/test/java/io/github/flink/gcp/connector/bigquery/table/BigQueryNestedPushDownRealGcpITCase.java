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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nested projection and nested-field restrictions against the real Storage Read API: each pushed
 * read returns the rows its unpushed counterpart returns (ADR-0100).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryNestedPushDownRealGcpITCase {

    private static final String TABLE = "nested_pushdown_" + TestNames.runId();
    private static final String VIEW = "nested_pushdown_view_" + TestNames.runId();
    private static final String COLUMNS =
            "(id BIGINT, r ROW<a BIGINT, b STRING, c ROW<d BIGINT, e STRING>>,"
                    + " span ROW<`start` DATE, `end` DATE>) ";

    // A NULL STRUCT (1), a NULL field of a present STRUCT (2), a NULL inner STRUCT (4), a NULL
    // RANGE (1) and an unbounded RANGE start (3) are the shapes where Flink's and BigQuery's NULL
    // handling must agree.
    @BeforeAll
    static void seed() throws Exception {
        try {
            RealBigQuery.queryRows(
                    "CREATE TABLE "
                            + RealBigQuery.tablePath(TABLE)
                            + " AS SELECT id, IF(id = 1, NULL, STRUCT(IF(id = 2, NULL, id) AS a,"
                            + " REPEAT('wide-', 200) AS b, IF(id = 4, NULL, STRUCT(id * 10 AS d,"
                            + " CONCAT('e', CAST(id AS STRING)) AS e)) AS c)) AS r,"
                            + " CASE id WHEN 1 THEN NULL"
                            + " WHEN 3 THEN RANGE(CAST(NULL AS DATE), DATE '2026-02-01')"
                            + " ELSE RANGE(DATE '2026-01-01', DATE '2026-02-01') END AS span"
                            + " FROM UNNEST(GENERATE_ARRAY(1, 6)) AS id");
            RealBigQuery.queryRows(
                    "CREATE VIEW "
                            + RealBigQuery.tablePath(VIEW)
                            + " AS SELECT * FROM "
                            + RealBigQuery.tablePath(TABLE));
        } catch (Exception e) {
            RealBigQuery.deleteTables(VIEW, TABLE);
            throw e;
        }
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(VIEW, TABLE);
    }

    @Test
    void nestedProjectionReturnsWhatTheWholeColumnsHold() throws Exception {
        for (String source : sources()) {
            TableEnvironment table = tableEnvironment(source);
            String pushed = "SELECT id, r.a, r.c.e, span.`start` FROM nested";
            assertThat(table.explainSql(pushed)).contains("project=[id, r_a, r_c_e, span_start]");

            List<String> fromPaths = strings(rows(table, pushed));
            List<String> fromWholeColumns = new ArrayList<>();
            for (Row row : rows(table, "SELECT id, r, span FROM nested")) {
                Row r = (Row) row.getField(1);
                Row c = r == null ? null : (Row) r.getField(2);
                Row span = (Row) row.getField(2);
                fromWholeColumns.add(
                        Row.of(
                                        row.getField(0),
                                        r == null ? null : r.getField(0),
                                        c == null ? null : c.getField(1),
                                        span == null ? null : span.getField(0))
                                .toString());
            }

            assertThat(fromPaths).as(source).isEqualTo(sorted(fromWholeColumns));
            assertThat(fromPaths)
                    .as(source)
                    .contains(
                            Row.of(1L, null, null, null).toString(),
                            Row.of(2L, null, "e2", LocalDate.of(2026, 1, 1)).toString(),
                            Row.of(3L, 3L, "e3", null).toString(),
                            Row.of(4L, 4L, null, LocalDate.of(2026, 1, 1)).toString());
        }
    }

    @Test
    void nestedRestrictionsReturnWhatFlinkAloneReturns() throws Exception {
        String[] predicates = {
            "r.a = 5",
            "r.a > 3",
            "r.a <> 5",
            "r.a IS NULL",
            "r.a IS NOT NULL",
            "r.c.d IS NULL",
            "r.c.d >= 30",
            "r.c.e = 'e5'",
            "r.a = 3 OR r.c.d = 60",
            "r.a >= 3 AND r.c.e = 'e5'",
            "span.`start` IS NULL",
        };
        for (String source : sources()) {
            TableEnvironment pushed = tableEnvironment(source);
            // The unpushed side: the same rows read without a predicate, then filtered by Flink
            // alone over values no source can push into. Flink 2.x has no switch that turns
            // predicate pushdown off.
            TableEnvironment unpushed = tableEnvironment(source);
            List<Row> whole = rows(unpushed, "SELECT * FROM nested");
            // An empty read would make every comparison below agree vacuously.
            assertThat(whole).as(source).hasSize(6);
            unpushed.createTemporaryView(
                    "oracle",
                    unpushed.fromValues(
                            unpushed.from("nested").getResolvedSchema().toPhysicalRowDataType(),
                            whole));
            for (String predicate : predicates) {
                String sql = "SELECT id, r.b IS NULL FROM nested WHERE " + predicate;
                if (!predicate.startsWith("span")) {
                    assertThat(pushed.explainSql(sql))
                            .as("%s: %s", source, predicate)
                            .doesNotContain("filter=[]");
                }
                assertThat(strings(rows(pushed, sql)))
                        .as("%s: %s", source, predicate)
                        .isEqualTo(
                                strings(rows(unpushed, sql.replace("FROM nested", "FROM oracle"))));
            }
        }
    }

    private static List<String> sources() {
        String project = RealBigQuery.project();
        String dataset = RealBigQuery.dataset();
        return Arrays.asList(
                TableDdl.withOptions(project, dataset, TABLE),
                "WITH ('connector' = 'bigquery', 'project' = '"
                        + project
                        + "', 'scan.query' = 'SELECT * FROM `"
                        + project
                        + "."
                        + dataset
                        + "."
                        + TABLE
                        + "`')",
                TableDdl.withOptions(project, dataset, VIEW, "scan.materialize-views", "true"));
    }

    private static TableEnvironment tableEnvironment(String options) {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql("CREATE TABLE nested " + COLUMNS + options);
        return table;
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<String> strings(List<Row> rows) {
        List<String> text = new ArrayList<>();
        for (Row row : rows) {
            text.add(row.toString());
        }
        return sorted(text);
    }

    private static List<String> sorted(List<String> values) {
        List<String> copy = new ArrayList<>(values);
        copy.sort(null);
        return copy;
    }
}
