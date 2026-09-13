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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableConditionalCommandITCase extends BigtableTableTestBase {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void latestValueComparisonIgnoresAMatchingHistoricalVersion(boolean batch) throws Exception {
        TableDestination table = createTable("commands-latest-" + batch, "cf", "audit");
        writeCell(table, "history", "cf", "q", 1000L, "pending");
        writeCell(table, "history", "cf", "q", 2000L, "active");
        writeCell(table, "match", "cf", "q", 1000L, "pending");
        TableEnvironment env = batch ? batchTableEnvironment() : streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE commands (k STRING, expected BYTES, replacement BYTES, reason BYTES) "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "conditional",
                                "sink.conditional.row-key-column",
                                "k",
                                "sink.conditional.predicate",
                                "latest-cell-value-equals",
                                "sink.conditional.predicate.family",
                                "cf",
                                "sink.conditional.predicate.qualifier",
                                "q",
                                "sink.conditional.predicate.value-column",
                                "expected",
                                "sink.conditional.then.0.operation",
                                "set-cell",
                                "sink.conditional.then.0.family",
                                "cf",
                                "sink.conditional.then.0.qualifier",
                                "q",
                                "sink.conditional.then.0.value-column",
                                "replacement",
                                "sink.conditional.otherwise.0.operation",
                                "set-cell",
                                "sink.conditional.otherwise.0.family",
                                "audit",
                                "sink.conditional.otherwise.0.qualifier",
                                "",
                                "sink.conditional.otherwise.0.value-column",
                                "reason"));
        env.executeSql(
                        "INSERT INTO commands VALUES ('history', CAST('pending' AS BYTES), CAST('wrong' AS BYTES), X'000AFF')")
                .await();
        env.executeSql(
                        "INSERT INTO commands SELECT k, CAST(e AS BYTES), CAST(v AS BYTES), CAST('' AS BYTES)"
                                + " FROM (VALUES ('match', 'pending', 'done')) AS src(k,e,v)")
                .await();
        Row history =
                readRows(table).stream()
                        .filter(r -> r.getKey().toStringUtf8().equals("history"))
                        .findFirst()
                        .orElseThrow();
        Row match =
                readRows(table).stream()
                        .filter(r -> r.getKey().toStringUtf8().equals("match"))
                        .findFirst()
                        .orElseThrow();
        assertThat(history.getCells("cf", "q").get(0).getValue().toStringUtf8())
                .isEqualTo("active");
        assertThat(history.getCells("audit", "").get(0).getValue().toByteArray())
                .containsExactly(0, 10, (byte) 255);
        assertThat(match.getCells("cf", "q").get(0).getValue().toStringUtf8()).isEqualTo("done");
        assertThat(match.getCells("audit", "")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedSameRowInputsAreNotRemoved(boolean select) {
        TableDestination table = createTable("commands-repeat-" + select, "cf");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE commands (k STRING) "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "conditional",
                                "sink.in-flight.max-requests",
                                "1",
                                "sink.conditional.row-key-column",
                                "k",
                                "sink.conditional.predicate",
                                "row-exists",
                                "sink.conditional.empty-branch-policy",
                                "fail",
                                "sink.conditional.otherwise.0.operation",
                                "set-cell",
                                "sink.conditional.otherwise.0.family",
                                "cf",
                                "sink.conditional.otherwise.0.qualifier",
                                "q",
                                "sink.conditional.otherwise.0.value-utf8",
                                "created"));
        String input =
                select ? "SELECT k FROM (VALUES ('r'), ('r')) AS src(k)" : "VALUES ('r'), ('r')";
        assertThatThrownBy(() -> env.executeSql("INSERT INTO commands " + input).await())
                .hasStackTraceContaining("EmptyBranchPolicy.FAIL");
        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void orderedDeletesAndSetsApplyAsOneBranchAndRangesStayHalfOpen() throws Exception {
        TableDestination table = createTable("commands-delete-order", "cf", "other");
        writeCell(table, "r", "cf", "q", 1000L, "one");
        writeCell(table, "r", "cf", "q", 2000L, "two");
        writeCell(table, "r", "cf", "q", 3000L, "three");
        writeCell(table, "r", "other", "q", "remove");
        TableEnvironment env = streamingTableEnvironment();
        env.executeSql(
                "CREATE TABLE commands (k STRING, start_time BIGINT, end_time BIGINT) "
                        + withOptions(
                                table.getTable(),
                                "sink.write-mode",
                                "conditional",
                                "sink.conditional.row-key-column",
                                "k",
                                "sink.conditional.predicate",
                                "cell-exists",
                                "sink.conditional.predicate.family",
                                "cf",
                                "sink.conditional.predicate.qualifier",
                                "q",
                                "sink.conditional.then.0.operation",
                                "delete-cells",
                                "sink.conditional.then.0.family",
                                "cf",
                                "sink.conditional.then.0.qualifier",
                                "q",
                                "sink.conditional.then.0.start-timestamp-column",
                                "start_time",
                                "sink.conditional.then.0.end-timestamp-column",
                                "end_time",
                                "sink.conditional.then.1.operation",
                                "delete-family",
                                "sink.conditional.then.1.family",
                                "other",
                                "sink.conditional.then.2.operation",
                                "set-cell",
                                "sink.conditional.then.2.family",
                                "other",
                                "sink.conditional.then.2.qualifier",
                                "q",
                                "sink.conditional.then.2.value-utf8",
                                "replacement"));
        env.executeSql("INSERT INTO commands VALUES ('r', 1000, 3000)").await();
        Row row = readRows(table).get(0);
        assertThat(row.getCells("cf", "q")).hasSize(1);
        assertThat(row.getCells("cf", "q").get(0).getTimestamp()).isEqualTo(3000);
        assertThat(row.getCells("other", "q")).hasSize(1);
        assertThat(row.getCells("other", "q").get(0).getValue().toStringUtf8())
                .isEqualTo("replacement");
    }
}
