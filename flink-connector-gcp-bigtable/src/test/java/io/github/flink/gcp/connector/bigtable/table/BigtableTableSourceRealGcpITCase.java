/*
 * Copyright 2026 laughingman7743
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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The table source against real Cloud Bigtable, driven through SQL with <b>no</b> {@code
 * emulator-endpoint} option.
 *
 * <p>Three things run nowhere else (ADR-0080). Split planning: the emulator models no tablets, so
 * only a pre-split real table makes a SQL scan actually plan several splits. {@code
 * scan.app-profile-id}: the emulator ignores profiles, so only the service can say whether the
 * option reached the wire. And the family filter's server-side answer: a declared family the table
 * lacks fails the read with {@code NOT_FOUND} rather than answering empty, while a row-key-only
 * projection — whose keys-only chain names no family — reads the same table fine, which is what
 * shows the pruning is served by the server and not by the converter. Filter pushdown: the emulator
 * can exercise the same filter proto, but only this suite proves that the service accepts the
 * conditional cell-existence filter composed with SQL row-key bounds and projection.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableTableSourceRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String APP_PROFILE = "flink-table-source-it";

    private static String ddl(String flinkTable, String columns, String tableId, String... opts) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put("project", PROJECT);
        options.put("instance", tableDestination(tableId).getInstance());
        options.put("table", tableId);
        for (int i = 0; i < opts.length; i += 2) {
            options.put(opts[i], opts[i + 1]);
        }
        return "CREATE TABLE "
                + flinkTable
                + " (\n  "
                + columns
                + ",\n  PRIMARY KEY (rowkey) NOT ENFORCED\n) "
                + options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }

    private static String declaredFamily() {
        return FAMILY + " ROW<q STRING>";
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    private static List<Row> collect(TableEnvironment tEnv, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void readsAPreSplitTableThroughSql() throws Exception {
        // The pre-split table makes SampleRowKeys answer with real tablet boundaries, which the
        // emulator cannot (ADR-0080). What the row assertion shows is that a SQL scan over such a
        // table is complete and correct; split planning itself — that those boundaries become
        // several splits — is pinned by BigtableSourceRealGcpITCase, which measures the samples
        // directly.
        TableDestination table = createTableWithSplits("table-source-splits", "b", "c");
        seedRows(table, "a1", "b1", "c1", "d1");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(ddl("bt", "rowkey STRING,\n  " + declaredFamily(), "table-source-splits"));

        assertThat(collect(tEnv, "SELECT rowkey, " + FAMILY + ".q FROM bt"))
                .containsExactlyInAnyOrder(
                        Row.of("a1", "a1"),
                        Row.of("b1", "b1"),
                        Row.of("c1", "c1"),
                        Row.of("d1", "d1"));
    }

    @Test
    void readsThroughTheConfiguredApplicationProfile() throws Exception {
        TableDestination table = createTable("table-source-app-profile");
        seedRows(table, "r1");
        createSingleClusterAppProfile(APP_PROFILE);
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl(
                        "bt",
                        "rowkey STRING,\n  " + declaredFamily(),
                        "table-source-app-profile",
                        "scan.app-profile-id",
                        APP_PROFILE));

        assertThat(collect(tEnv, "SELECT rowkey FROM bt")).containsExactly(Row.of("r1"));
    }

    @Test
    void failsWhenTheApplicationProfileDoesNotExist() throws Exception {
        // The load-bearing half: a factory that dropped the option would pass the test above by
        // reading through the instance's default profile, and fail only here. NOT_FOUND, asserted
        // on the status name in the chain's messages — Flink transports a task-side failure as
        // SerializedThrowable, which keeps the original class only as a message prefix, and
        // assertThrowableWithMessage rethrows the chain on a miss (#481's measured rule).
        TableDestination table = createTable("table-source-profile-missing");
        seedRows(table, "r1");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl(
                        "with_profile",
                        "rowkey STRING,\n  " + declaredFamily(),
                        "table-source-profile-missing",
                        "scan.app-profile-id",
                        "no-such-profile"));
        // The control: the same table reads fine through the default profile, so the only failure
        // left for the read below to earn is the profile's.
        tEnv.executeSql(
                ddl(
                        "without_profile",
                        "rowkey STRING,\n  " + declaredFamily(),
                        "table-source-profile-missing"));
        assertThat(collect(tEnv, "SELECT rowkey FROM without_profile"))
                .containsExactly(Row.of("r1"));

        assertThatThrownBy(() -> collect(tEnv, "SELECT rowkey FROM with_profile"))
                .satisfies(
                        thrown -> ExceptionUtils.assertThrowableWithMessage(thrown, "NOT_FOUND"));
    }

    @Test
    void aDeclaredFamilyTheTableLacksFailsTheScanAndTheKeysOnlyChainDoesNot() throws Exception {
        // The projection filter is served by the server, and this is the pair that shows it: the
        // same DDL over the same table fails a SELECT * with the service's NOT_FOUND — the family
        // filter names 'absent', which the source deliberately does not pre-validate — and answers
        // a SELECT rowkey, whose keys-only chain names no family at all.
        TableDestination table = createTable("table-source-absent-family");
        seedRows(table, "r1");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl(
                        "bt",
                        "rowkey STRING,\n  " + declaredFamily() + ",\n  absent ROW<q STRING>",
                        "table-source-absent-family"));

        assertThatThrownBy(() -> collect(tEnv, "SELECT * FROM bt"))
                .satisfies(
                        thrown -> ExceptionUtils.assertThrowableWithMessage(thrown, "NOT_FOUND"));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt")).containsExactly(Row.of("r1"));
    }

    @Test
    void combinesSqlRowKeyBoundsAndAQualifierPrefilterOnTheService() throws Exception {
        TableDestination table = createTable("table-source-filter-pushdown");
        seedRows(table, "a0", "b0", "c0", "d0");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl(
                        "bt",
                        "rowkey STRING,\n  " + declaredFamily(),
                        "table-source-filter-pushdown",
                        "scan.row-range.start-closed",
                        "a0",
                        "scan.row-range.end-open",
                        "d0"));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT rowkey FROM bt "
                                        + "WHERE rowkey >= 'b0' AND rowkey < 'd0' "
                                        + "AND "
                                        + FAMILY
                                        + ".q = 'b0'"))
                .containsExactly(Row.of("b0"));
    }
}
