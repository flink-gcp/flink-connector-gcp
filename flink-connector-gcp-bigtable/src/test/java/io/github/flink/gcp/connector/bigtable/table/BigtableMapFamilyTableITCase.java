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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigtable.data.v2.models.RowCell;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end SQL over a {@code MAP} column family against the emulator (ADR-0172): the map form
 * beside a {@code ROW} family in one table, the two forms reading the same cells alike, and the
 * merge and replace update modes.
 *
 * <p>Writes that must be ordered run as separate jobs, for the reason ADR-0086 records: two entries
 * for one row inside one {@code MutateRows} request have no defined winner.
 */
class BigtableMapFamilyTableITCase extends BigtableTableTestBase {

    private static List<Row> collect(TableEnvironment tEnv, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    /** The options every table here carries: plain inserts plan identically on every Flink. */
    private static String options(String tableId, String... keysAndValues) {
        String[] combined = new String[keysAndValues.length + 2];
        combined[0] = "sink.insert-only-input-mode";
        combined[1] = "insert-only";
        System.arraycopy(keysAndValues, 0, combined, 2, keysAndValues.length);
        return withOptions(tableId, combined);
    }

    /** Every cell of the table as {@code key/family:qualifier=value}, qualifier and value UTF-8. */
    private static List<String> cells(TableDestination destination) {
        List<String> cells = new ArrayList<>();
        for (com.google.cloud.bigtable.data.v2.models.Row row : readRows(destination)) {
            for (RowCell cell : row.getCells()) {
                cells.add(
                        row.getKey().toStringUtf8()
                                + '/'
                                + cell.getFamily()
                                + ':'
                                + cell.getQualifier().toStringUtf8()
                                + '='
                                + cell.getValue().toStringUtf8());
            }
        }
        return cells;
    }

    @Test
    void aMapFamilyIsWrittenBesideARowFamilyAndReadsBackThroughEitherForm() throws Exception {
        TableDestination destination = createTable("sql-map-beside-row", "cf", "m");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE mixed (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<name STRING>,\n"
                        + "  m MAP<STRING, STRING>\n"
                        + ") "
                        + options(destination.getTable()));
        tEnv.executeSql("INSERT INTO mixed VALUES ('r1', ROW('alice'), MAP['a', 'x', 'b', 'y'])")
                .await();

        // One cell per entry, and the ROW family's cell exactly as a ROW-only table writes it.
        assertThat(cells(destination))
                .containsExactlyInAnyOrder("r1/cf:name=alice", "r1/m:a=x", "r1/m:b=y");
        assertThat(collect(tEnv, "SELECT rowkey, cf.name, m FROM mixed"))
                .containsExactly(Row.of("r1", "alice", Map.of("a", "x", "b", "y")));

        tEnv.executeSql(
                "CREATE TABLE as_rows (\n"
                        + "  rowkey STRING,\n"
                        + "  m ROW<a STRING, b STRING>\n"
                        + ") "
                        + options(destination.getTable()));
        assertThat(collect(tEnv, "SELECT rowkey, m.a, m.b FROM as_rows"))
                .containsExactly(Row.of("r1", "x", "y"));
    }

    @Test
    void aMapReadsWhatTheEquivalentRowDdlReads() throws Exception {
        TableDestination destination = createTable("sql-map-reads-row", "cf", "counts");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE as_rows (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<name STRING, city STRING>,\n"
                        + "  counts ROW<a BIGINT, b BIGINT>\n"
                        + ") "
                        + options(destination.getTable()));
        tEnv.executeSql(
                        "INSERT INTO as_rows VALUES"
                                + " ('r1', ROW('alice', 'Tokyo'), ROW(CAST(7 AS BIGINT), CAST(-2"
                                + " AS BIGINT)))")
                .await();

        tEnv.executeSql(
                "CREATE TABLE as_maps (\n"
                        + "  rowkey STRING,\n"
                        + "  cf MAP<STRING, BYTES>,\n"
                        + "  counts MAP<BYTES, BIGINT>\n"
                        + ") "
                        + options(destination.getTable()));

        List<Row> fromRows =
                collect(tEnv, "SELECT rowkey, cf.name, cf.city, counts.a, counts.b FROM as_rows");
        // The raw GoogleSQL shape, cast in the query; and a typed value, which is how Flink gets
        // a number out of a cell, having no CAST from BYTES to BIGINT.
        List<Row> fromMaps =
                collect(
                        tEnv,
                        "SELECT rowkey, CAST(cf['name'] AS STRING), CAST(cf['city'] AS STRING),"
                                + " counts[x'61'], counts[x'62'] FROM as_maps");
        assertThat(fromRows).containsExactly(Row.of("r1", "alice", "Tokyo", 7L, -2L));
        assertThat(fromMaps).isEqualTo(fromRows);
        // Every cell the ROW DDL wrote is an entry, and nothing else is.
        assertThat(collect(tEnv, "SELECT CARDINALITY(cf), CARDINALITY(counts) FROM as_maps"))
                .containsExactly(Row.of(2, 2));
    }

    @Test
    void aBytesKeyedEntryHoldsOnlyItsLatestVersion() throws Exception {
        // Two versions of one qualifier are one entry. A byte[] key compares by identity in a
        // hash map, which would keep both — a map with a duplicate key.
        TableDestination destination = createTable("sql-map-bytes-versions", "n");
        writeCell(destination, "r1", "n", "q", 1_000L, "old");
        writeCell(destination, "r1", "n", "q", 2_000L, "new");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  n MAP<BYTES, STRING>\n"
                        + ") "
                        + options(destination.getTable()));

        assertThat(collect(tEnv, "SELECT rowkey, CARDINALITY(n), n[x'71'] FROM bt"))
                .containsExactly(Row.of("r1", 1, "new"));
    }

    @Test
    void aFamilyWithNoCellReadsAsNullAndIsFilteredByExistence() throws Exception {
        TableDestination destination = createTable("sql-map-existence", "cf", "m");
        writeCell(destination, "r1", "cf", "name", "alice");
        writeCell(destination, "r2", "m", "a", "x");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<name STRING>,\n"
                        + "  m MAP<STRING, STRING>\n"
                        + ") "
                        + options(destination.getTable()));

        // Read beside cf, so r1 is a member of the result and its map can read as NULL.
        assertThat(collect(tEnv, "SELECT rowkey, cf.name, m FROM bt"))
                .containsExactlyInAnyOrder(
                        Row.of("r1", "alice", null), Row.of("r2", null, Map.of("a", "x")));
        // Projected alone, the family decides row membership as a ROW family does (ADR-0092).
        assertThat(collect(tEnv, "SELECT rowkey, m FROM bt"))
                .containsExactly(Row.of("r2", Map.of("a", "x")));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt WHERE m IS NOT NULL"))
                .containsExactly(Row.of("r2"));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt WHERE m['a'] = 'x'"))
                .containsExactly(Row.of("r2"));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({"merge, 'a=3,b=2'", "replace, 'a=3'"})
    void anEntryTheNextMapOmitsSurvivesAMergeAndNotAReplace(String mode, String expected)
            throws Exception {
        TableDestination destination = createTable("sql-map-" + mode, "m");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  m MAP<STRING, STRING>\n"
                        + ") "
                        + options(destination.getTable(), "sink.map-family.update-mode", mode));

        // Two jobs, so the second write is ordered after the first.
        tEnv.executeSql("INSERT INTO bt VALUES ('r1', MAP['a', '1', 'b', '2'])").await();
        tEnv.executeSql("INSERT INTO bt VALUES ('r1', MAP['a', '3'])").await();

        List<Row> rows = collect(tEnv, "SELECT m FROM bt");
        assertThat(rows).hasSize(1);
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) rows.get(0).getField(0);
        assertThat(
                        map.entrySet().stream()
                                .map(entry -> entry.getKey() + '=' + entry.getValue())
                                .sorted()
                                .collect(Collectors.joining(",")))
                .isEqualTo(expected);
    }
}
