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

import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs plain SQL conditional inserts through the production factory and public sink. */
class BigtableConditionalTableITCase extends BigtableTableTestBase {
    @Test
    void anUndeclaredFamilyMakesTheWholeRowExistAndALaterInsertCannotOverwrite() throws Exception {
        TableDestination table = createTable("conditional-sql-existing", "cf", "hidden");
        writeCell(table, "existing", "hidden", "q", "keep");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<v STRING>) "
                        + withOptions(table.getTable(), "sink.write-mode", "insert-if-absent"));
        env.executeSql("INSERT INTO bt VALUES ('existing', ROW('replace')), ('new', ROW('first'))")
                .await();
        env.executeSql(
                        "INSERT INTO bt SELECT k, ROW(v) FROM (VALUES ('new', 'second')) AS input(k, v)")
                .await();
        List<Row> rows = readRows(table);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getCells("cf", "v")).isEmpty();
        assertThat(rows.get(0).getCells("hidden", "q").get(0).getValue().toStringUtf8())
                .isEqualTo("keep");
        assertThat(rows.get(1).getCells("cf", "v")).hasSize(1);
        assertThat(rows.get(1).getCells("cf", "v").get(0).getValue().toStringUtf8())
                .isEqualTo("first");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void thePlannerPreservesRepeatedIdenticalInputsSoTheSecondSelectsTheEmptyBranch(
            boolean primaryKey) {
        TableDestination table = createTable("conditional-sql-repeat-" + primaryKey);
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<v STRING>"
                        + (primaryKey ? ", PRIMARY KEY(k) NOT ENFORCED" : "")
                        + ") "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "insert-if-absent",
                                "sink.conditional.empty-branch-policy",
                                "fail",
                                "sink.in-flight.max-requests",
                                "1"));
        assertThatThrownBy(
                        () ->
                                env.executeSql(
                                                "INSERT INTO bt VALUES ('r', ROW('v')), ('r', ROW('v'))")
                                        .await())
                .hasStackTraceContaining("EmptyBranchPolicy.FAIL");
        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void retainsNullCellEncodingAndTimestampMetadata() throws Exception {
        TableDestination table = createTable("conditional-sql-nulls", "cf", "other");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<v STRING, n BIGINT>, other ROW<v STRING>,"
                        + " ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp') "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "insert-if-absent",
                                "null-string-literal",
                                "<none>"));
        env.executeSql(
                        "INSERT INTO bt VALUES ('r', ROW(CAST(NULL AS STRING), CAST(NULL AS BIGINT)),"
                                + " CAST(NULL AS ROW<v STRING>), TO_TIMESTAMP_LTZ(1700000000000, 3))")
                .await();
        Row row = readRows(table).get(0);
        assertThat(row.getCells("cf", "v").get(0).getValue().toStringUtf8()).isEqualTo("<none>");
        assertThat(row.getCells("cf", "n").get(0).getValue()).isEmpty();
        assertThat(row.getCells("other", "v")).isEmpty();
        assertThat(row.getCells())
                .allSatisfy(
                        cell -> assertThat(cell.getTimestamp()).isEqualTo(1_700_000_000_000_000L));
    }
}
