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

import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Executes read-modify-write SQL through the production factory and sink. */
class BigtableReadModifyWriteTableITCase extends BigtableTableTestBase {
    @Test
    void appendsTextAndBinaryAndLeavesNullCellsUntouched() throws Exception {
        TableDestination table = createTable("rmw-sql-append", "cf", "other");
        writeCell(table, "r", "cf", "text", "before");
        writeCell(table, "r", "cf", "untouched", "keep");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<text STRING, `raw` BYTES, untouched STRING>, other ROW<v STRING>) "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "append",
                                "null-string-literal",
                                "<null>"));
        env.executeSql(
                        "INSERT INTO bt VALUES ('r', ROW('-after', X'00FF', CAST(NULL AS STRING)), CAST(NULL AS ROW<v STRING>))")
                .await();
        com.google.cloud.bigtable.data.v2.models.Row row = readRows(table).get(0);
        assertThat(row.getCells("cf", "text").get(0).getValue().toStringUtf8())
                .isEqualTo("before-after");
        assertThat(row.getCells("cf", "raw").get(0).getValue().toByteArray())
                .containsExactly(0, -1);
        assertThat(row.getCells("cf", "untouched").get(0).getValue().toStringUtf8())
                .isEqualTo("keep");
        assertThat(row.getCells("other", "v")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedSameKeyInputsAreAllAppliedWithOrWithoutAPrimaryKey(boolean primaryKey)
            throws Exception {
        TableDestination table = createTable("rmw-sql-count-" + primaryKey);
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<n BIGINT>"
                        + (primaryKey ? ", PRIMARY KEY(k) NOT ENFORCED" : "")
                        + ") "
                        + withOptions(table.getTable(), "sink.write-mode", "increment"));
        env.executeSql(
                        "INSERT INTO bt VALUES ('r', ROW(3)), ('r', ROW(3)), ('r', ROW(-2)), ('r', ROW(0))")
                .await();
        com.google.cloud.bigtable.data.v2.models.Row row = readRows(table).get(0);
        assertThat(
                        ByteBuffer.wrap(row.getCells("cf", "n").get(0).getValue().toByteArray())
                                .getLong())
                .isEqualTo(4L);
    }

    @Test
    void mapFamiliesIncrementAndAppendPerEntryAndReadBackAsMaps() throws Exception {
        TableDestination table = createTable("rmw-sql-map", "counts", "tags");
        writeCell(table, "r", "tags", "a", "before");
        writeCell(table, "r", "tags", "untouched", "keep");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE counter (k STRING, counts MAP<STRING, BIGINT>) "
                        + withOptions(table.getTable(), "sink.write-mode", "increment"));
        // Repeated inputs for one row are separate requests; increments commute, so their order
        // does not matter. The null value adds no rule, so 'd3' is never created.
        env.executeSql(
                        "INSERT INTO counter VALUES ('r', MAP['d1', CAST(3 AS BIGINT), 'd2', CAST(5 AS BIGINT)]),"
                                + " ('r', MAP['d1', CAST(2 AS BIGINT), 'd3', CAST(NULL AS BIGINT)]),"
                                + " ('r', MAP['d2', CAST(-6 AS BIGINT)])")
                .await();
        env.executeSql(
                "CREATE TABLE appender (k STRING, tags MAP<STRING, STRING>) "
                        + withOptions(table.getTable(), "sink.write-mode", "append"));
        env.executeSql("INSERT INTO appender VALUES ('r', MAP['a', '-after', 'b', 'new'])").await();

        env.executeSql(
                "CREATE TABLE reader (k STRING, counts MAP<STRING, BIGINT>, tags MAP<STRING, STRING>) "
                        + withOptions(table.getTable()));
        List<Row> rows =
                collect(
                        env,
                        "SELECT counts['d1'], counts['d2'], CARDINALITY(counts), tags['a'],"
                                + " tags['b'], tags['untouched'], CARDINALITY(tags) FROM reader");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(5L);
        assertThat(rows.get(0).getField(1)).isEqualTo(-1L);
        assertThat(rows.get(0).getField(2)).isEqualTo(2);
        assertThat(rows.get(0).getField(3)).isEqualTo("before-after");
        assertThat(rows.get(0).getField(4)).isEqualTo("new");
        assertThat(rows.get(0).getField(5)).isEqualTo("keep");
        assertThat(rows.get(0).getField(6)).isEqualTo(3);
    }

    @Test
    void aMapHoldingOnlyNullValuesFailsTheRecordWithTheExistingMessage() throws Exception {
        TableDestination table = createTable("rmw-sql-map-all-null", "counts");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE counter (k STRING, counts MAP<STRING, BIGINT>) "
                        + withOptions(table.getTable(), "sink.write-mode", "increment"));
        assertThatThrownBy(
                        () ->
                                env.executeSql(
                                                "INSERT INTO counter VALUES ('r',"
                                                        + " MAP['d1', CAST(NULL AS BIGINT)])")
                                        .await())
                .hasStackTraceContaining("require at least one nonnull cell");
    }

    private static List<Row> collect(TableEnvironment env, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(query).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }
}
