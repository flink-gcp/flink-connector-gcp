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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.bigquery.source.AbstractBigQuerySourceEmulatorITCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The {@code bigquery} table's full lookup cache, planned and executed against the emulator. */
class BigQueryTableFullCacheITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String DIM = "full_cache_dim";
    private static final String GUARDED = "full_cache_guarded";
    private static final String RELOADED = "full_cache_reloaded";

    private static final Duration ROW_DEADLINE = Duration.ofSeconds(60);

    private static final String DIM_COLUMNS =
            "id BIGINT, name STRING, tier STRING, code INT, seen TIMESTAMP(3),"
                    + " attrs MAP<STRING, BIGINT>";

    @BeforeAll
    static void seed() throws Exception {
        createTable(
                DIM,
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("tier", StandardSQLTypeName.STRING),
                Field.of("code", StandardSQLTypeName.INT64),
                Field.of("seen", StandardSQLTypeName.DATETIME),
                Field.newBuilder(
                                "attrs",
                                LegacySQLTypeName.RECORD,
                                Field.of("key", StandardSQLTypeName.STRING),
                                Field.of("value", StandardSQLTypeName.INT64))
                        .setMode(Field.Mode.REPEATED)
                        .build());
        insert(
                DIM,
                "id, name, tier, code, seen, attrs",
                "(1, 'Ada', 'gold', 7, DATETIME '2026-01-02 03:04:05.123', "
                        + "[STRUCT('a' AS key, 1 AS value)]), "
                        + "(2, 'Bob', 'gold', 8, DATETIME '2026-01-02 03:04:05.456', "
                        + "[STRUCT('b' AS key, 2 AS value)]), "
                        + "(3, 'Cy', 'silver', 7, DATETIME '2026-01-02 03:04:06', []), "
                        + "(4, 'Dee', 'bronze', 9, DATETIME '2026-01-02 03:04:07', [])");
        // The emulator fails a read of a NULL in a NULLABLE column and of any DATETIME value
        // ("cannot encode binary"), so no row here holds a NULL and no join reads 'seen'; a NULL
        // probe value still shows that NULL matches nothing.
        // A row whose 'small' value no Flink INT holds, so a load that reads it fails; a load whose
        // read session carries 'n > 1' leaves it out.
        createTable(
                GUARDED,
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("n", StandardSQLTypeName.INT64),
                Field.of("small", StandardSQLTypeName.INT64));
        insert(
                GUARDED,
                "id, name, n, small",
                "(1, 'Ada', 1, 1), (2, 'Ada', 2, 2), (3, 'Ada', 0, 1099511627776)");
        createTable(
                RELOADED,
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING));
        insert(RELOADED, "id, name", "(1, 'Ada'), (2, 'Bob')");
    }

    private static TableEnvironment batch() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    private static TableEnvironment streaming() {
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        return table;
    }

    /** The table's {@code WITH} clause under a full cache reloaded hourly, plus the extras. */
    private static String with(String table, String... extra) {
        String[] options = {
            "emulator-endpoint", grpcEndpoint(),
            "lookup.cache", "FULL",
            "lookup.full-cache.periodic-reload.interval", "1 h"
        };
        return TableDdl.withOptions(PROJECT, DATASET, table, TableDdl.concat(options, extra));
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void aFullCacheJoinsOnAnyColumnAndKeepsEveryMatchingRow() throws Exception {
        TableEnvironment table = batch();
        table.executeSql("CREATE TABLE dim (" + DIM_COLUMNS + ") " + with(DIM));
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT k, tier, code, PROCTIME() AS t FROM (VALUES"
                        + " (CAST(1 AS BIGINT), 'gold', 7), (CAST(9 AS BIGINT), 'gold', 8),"
                        + " (CAST(4 AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS INT)))"
                        + " AS v(k, tier, code)");

        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name, d.attrs FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.id"))
                .containsExactlyInAnyOrder(
                        Row.of(1L, "Ada", Map.of("a", 1L)),
                        Row.of(9L, null, null),
                        Row.of(4L, "Dee", Collections.emptyMap()));
        // A column that is no key of the table matches every row holding the value, and a NULL
        // probe value matches nothing, as SQL equality says.
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p LEFT JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.tier = d.tier"))
                .containsExactlyInAnyOrder(
                        Row.of(1L, "Ada"),
                        Row.of(1L, "Bob"),
                        Row.of(9L, "Ada"),
                        Row.of(9L, "Bob"),
                        Row.of(4L, null));
        // An INT key, alone and with a second key.
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.code = d.code"))
                .containsExactlyInAnyOrder(Row.of(1L, "Ada"), Row.of(1L, "Cy"), Row.of(9L, "Bob"));
        assertThat(
                        collect(
                                table,
                                "SELECT p.k, d.name FROM probe AS p JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d"
                                        + " ON p.code = d.code AND p.tier = d.tier"))
                .containsExactlyInAnyOrder(Row.of(1L, "Ada"), Row.of(9L, "Bob"));
    }

    @Test
    void aConditionOnTheDimensionTableNarrowsAStreamingLoadButNotABatchLoad() throws Exception {
        String ddl = "CREATE TABLE guarded (id BIGINT, name STRING, n BIGINT, small INT) ";
        String join =
                // 'small' is selected so that the load reads it: projection pushdown would leave an
                // unused column out of the read session.
                "SELECT p.k, d.id, d.small FROM probe AS p JOIN guarded"
                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.name WHERE d.n > 1";
        String probe = "CREATE TEMPORARY VIEW probe AS SELECT 'Ada' AS k, PROCTIME() AS t";

        // Streaming mode offers the condition to the lookup source, so the load's read session
        // carries it as a row restriction, and Flink still applies it to the cached rows.
        TableEnvironment streaming = streaming();
        streaming.executeSql(ddl + with(GUARDED));
        streaming.executeSql(probe);
        assertThat(collect(streaming, join)).containsExactly(Row.of("Ada", 2L, 2));
        // A condition that holds only after the join, which a padded row satisfies, must stay on
        // the join. Every row 'restricted' loads has a non-null 'n', so the join keeps no row;
        // pushed into the load, the condition would empty it and pad 'Ada' instead. Each load here
        // succeeds, so the first-load race of a failed load does not apply.
        streaming.executeSql(
                ddl.replace("TABLE guarded", "TABLE restricted")
                        + with(GUARDED, "scan.row-restriction", "n > 0"));
        assertThat(
                        collect(
                                streaming,
                                "SELECT p.k, d.id FROM probe AS p LEFT JOIN restricted"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.name"
                                        + " WHERE d.n IS NULL"))
                .isEmpty();
        // The control: without the condition the same join keeps both loaded rows.
        assertThat(
                        collect(
                                streaming,
                                "SELECT p.k, d.id FROM probe AS p LEFT JOIN restricted"
                                        + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = d.name"))
                .containsExactlyInAnyOrder(Row.of("Ada", 1L), Row.of("Ada", 2L));

        // Batch mode keeps the condition on the join, so the load reads row 3 and fails on it.
        TableEnvironment batch = batch();
        batch.executeSql(ddl + with(GUARDED));
        batch.executeSql(probe);
        assertLoadFails(batch, join);
        // 'scan.row-restriction' narrows every load whatever the mode.
        batch.executeSql(
                ddl.replace("TABLE guarded", "TABLE restricted")
                        + with(GUARDED, "scan.row-restriction", "n > 0"));
        assertThat(collect(batch, join.replace("JOIN guarded", "JOIN restricted")))
                .containsExactly(Row.of("Ada", 2L, 2));
    }

    /**
     * Asserts that the query's full-cache load failed on the row whose value no INT holds. Flink
     * releases the first-load latch before it publishes the load's failure (#2225), so a probe can
     * see the empty cache instead of the exception; a load that succeeded would have joined row 2,
     * so either outcome shows the load read row 3. That a conversion failure fails a load rather
     * than leaving its rows out is held by {@code
     * BigQueryFullCacheInputFormatTest#aRowThatCannotBeConvertedFailsTheLoad}.
     */
    private static void assertLoadFails(TableEnvironment table, String query) {
        List<Row> rows;
        try {
            rows = collect(table, query);
        } catch (Exception e) {
            assertThat(e)
                    .as(query)
                    .hasStackTraceContaining("A BigQuery integer value does not fit INT");
            return;
        }
        assertThat(rows).as("rows of a failed load: %s", query).isEmpty();
    }

    @Test
    void aLookupWithoutAFullCacheIsRefusedWhileTheSameTableStillScans() throws Exception {
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE plain (id BIGINT, name STRING) "
                        + TableDdl.withOptions(
                                PROJECT,
                                DATASET,
                                RELOADED,
                                new String[] {"emulator-endpoint", grpcEndpoint()}));
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT CAST(1 AS BIGINT) AS k, PROCTIME() AS t");

        assertThatThrownBy(
                        () ->
                                table.executeSql(
                                        "SELECT p.k, d.name FROM probe AS p JOIN plain"
                                                + " FOR SYSTEM_TIME AS OF p.t AS d"
                                                + " ON p.k = d.id"))
                .hasRootCauseInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("only through Flink's full cache")
                .hasMessageContaining("'lookup.cache' is NONE");
        assertThat(collect(table, "SELECT name FROM plain WHERE id = 2"))
                .containsExactly(Row.of("Bob"));
    }

    @Test
    void aLookupOfAQueryOrASnapshotOrAnInexactKeyIsRefusedWhileTheJoinIsPlanned() {
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TEMPORARY VIEW probe AS SELECT CAST(1 AS BIGINT) AS k,"
                        + " CAST(1.0 AS DOUBLE) AS s, PROCTIME() AS t");
        table.executeSql(
                "CREATE TABLE queried (id BIGINT, name STRING) WITH ('connector' = 'bigquery',"
                        + " 'scan.parent-project' = '"
                        + PROJECT
                        + "', 'scan.query' = 'SELECT 1 AS id, \"x\" AS name',"
                        + " 'emulator-endpoint' = '"
                        + grpcEndpoint()
                        + "', 'emulator-rest-endpoint' = '"
                        + restEndpoint()
                        + "', 'lookup.cache' = 'FULL',"
                        + " 'lookup.full-cache.periodic-reload.interval' = '1 h')");
        table.executeSql(
                "CREATE TABLE snapshot (id BIGINT, name STRING) "
                        + with(RELOADED, "scan.snapshot-time", "2026-01-01T00:00:00Z"));
        table.executeSql("CREATE TABLE scored (id BIGINT, score DOUBLE) " + with(RELOADED));

        assertThatThrownBy(() -> table.explainSql(lookup("queried", "d.id")))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.query' cannot be set")
                .hasMessageContaining("every load would run the query again");
        assertThatThrownBy(() -> table.explainSql(lookup("snapshot", "d.id")))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'scan.snapshot-time' cannot be set");
        // A civil TIMESTAMP keys the cache at its declared precision; reading one is beyond the
        // emulator, so planning is what this test can hold.
        table.executeSql("CREATE TABLE dim (" + DIM_COLUMNS + ") " + with(DIM));
        table.executeSql(
                "CREATE TEMPORARY VIEW stamps AS SELECT CAST(TIMESTAMP '2026-01-02 03:04:05.123'"
                        + " AS TIMESTAMP(3)) AS seen, PROCTIME() AS t");
        assertThat(
                        table.explainSql(
                                "SELECT d.name FROM stamps AS s JOIN dim FOR SYSTEM_TIME AS OF"
                                        + " s.t AS d ON s.seen = d.seen"))
                .contains("LookupJoin");
        // A dimension column compared through a cast is no lookup key; with no other equality the
        // join keeps no key, and the source refuses it.
        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "SELECT d.name FROM stamps AS s JOIN dim FOR SYSTEM_TIME"
                                                + " AS OF s.t AS d"
                                                + " ON s.seen = CAST(d.seen AS TIMESTAMP(0))"))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No equality reached the source as a key");
        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        lookup("scored", "d.score").replace("p.k =", "p.s =")))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("unsupported equality type DOUBLE");
    }

    private static String lookup(String dim, String key) {
        return "SELECT p.k FROM probe AS p JOIN "
                + dim
                + " FOR SYSTEM_TIME AS OF p.t AS d ON p.k = "
                + key;
    }

    @Test
    void aPeriodicReloadObservesAChangedRow() throws Exception {
        TableEnvironment table = streaming();
        String options =
                with(RELOADED)
                        .replace(
                                "'lookup.full-cache.periodic-reload.interval' = '1 h'",
                                "'lookup.full-cache.periodic-reload.interval' = '1 s'");
        assertThat(options).as("the one-second reload reached the DDL").contains("'1 s'");
        table.executeSql("CREATE TABLE dim (id BIGINT, name STRING) " + options);
        // datagen refuses min == max, and a constant key would fold away; ids 1 and 2 are random.
        table.executeSql(
                "CREATE TABLE ticks (k BIGINT, t AS PROCTIME())"
                        + " WITH ('connector' = 'datagen', 'rows-per-second' = '20',"
                        + " 'fields.k.min' = '1', 'fields.k.max' = '2')");
        List<Row> seen = Collections.synchronizedList(new ArrayList<>());

        try (CloseableIterator<Row> rows =
                table.executeSql(
                                "SELECT k.k, d.name FROM ticks AS k JOIN dim"
                                        + " FOR SYSTEM_TIME AS OF k.t AS d ON k.k = d.id")
                        .collect()) {
            awaitRow(rows, row -> "Ada".equals(row.getField(1)), seen);
            restClient.query(
                    QueryJobConfiguration.of(
                            "UPDATE `"
                                    + DATASET
                                    + "."
                                    + RELOADED
                                    + "` SET name = 'Grace'"
                                    + " WHERE id = 1"));
            awaitRow(rows, row -> "Grace".equals(row.getField(1)), seen);
        } finally {
            restClient.query(
                    QueryJobConfiguration.of(
                            "UPDATE `"
                                    + DATASET
                                    + "."
                                    + RELOADED
                                    + "` SET name = 'Ada'"
                                    + " WHERE id = 1"));
        }
    }

    /**
     * Reads an unbounded query's rows until one matches, within {@link #ROW_DEADLINE}. The iterator
     * stays open for the caller's next wait; the caller closes it, which cancels the job.
     */
    private static void awaitRow(CloseableIterator<Row> rows, Predicate<Row> match, List<Row> seen)
            throws Exception {
        ExecutorService reader =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "it-await-row");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> reading =
                    reader.submit(
                            () -> {
                                while (rows.hasNext()) {
                                    Row row = rows.next();
                                    seen.add(row);
                                    if (match.test(row)) {
                                        return;
                                    }
                                }
                                throw new AssertionError(
                                        "The query ended after "
                                                + seen.size()
                                                + " rows without a matching row");
                            });
            try {
                reading.get(ROW_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                        "No matching row within "
                                + ROW_DEADLINE
                                + " after "
                                + seen.size()
                                + " rows; the last was "
                                + (seen.isEmpty() ? "none" : seen.get(seen.size() - 1)),
                        e);
            }
        } finally {
            reader.shutdownNow();
        }
    }
}
