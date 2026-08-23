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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The table source against real Cloud Bigtable, driven through SQL with <b>no</b> {@code
 * emulator-endpoint} option.
 *
 * <p>Four things run nowhere else (ADR-0080). Split planning: the emulator models no tablets, so
 * only a pre-split real table makes a SQL scan actually plan several splits. {@code
 * scan.app-profile-id}: the emulator ignores profiles, so only the service can say whether the
 * option reached the wire. And the family filter's server-side answer: a declared family the table
 * lacks fails the read with {@code NOT_FOUND} rather than answering empty, while a row-key-only
 * projection — whose keys-only chain names no family — reads the same table fine, which is what
 * shows the pruning is served by the server and not by the converter. Filter pushdown: the emulator
 * can exercise the same filter proto, but only this suite proves that the service accepts the
 * conditional cell-existence filter composed with SQL row-key bounds and projection. Change
 * Streams: the emulator implements no Change Streams RPC, so only the service can exercise the SQL
 * mutation envelope, its metadata, and its timestamp bounds end to end.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableTableSourceRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String APP_PROFILE = "flink-table-source-it";
    private static final String CHANGE_STREAM_APP_PROFILE = "flink-table-change-stream-it";

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

        assertThat(TableJobFailures.awaitFailure(tEnv, "SELECT rowkey FROM with_profile"))
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

        assertThat(TableJobFailures.awaitFailure(tEnv, "SELECT * FROM bt"))
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

    @Test
    void readsTheChangeStreamMutationEnvelopeAndMetadataThroughSql() throws Exception {
        TableDestination table = createChangeStreamTable("table-change-stream-source");
        String sourceCluster = createSingleClusterAppProfile(CHANGE_STREAM_APP_PROFILE);
        ByteString startMarkerRowKey = ByteString.copyFromUtf8("start-marker");
        mutateRow(table, startMarkerRowKey, mutation -> mutation.setCell(FAMILY, "q", "marker"));
        long startMicros = readRows(table).get(0).getCells(FAMILY, "q").get(0).getTimestamp();
        Instant start = instantFromMicros(startMicros);
        ByteString binaryRowKey = ByteString.copyFrom(new byte[] {0x00, (byte) 0x80, (byte) 0xff});
        ByteString binaryQualifier = ByteString.copyFrom(new byte[] {0x00, (byte) 0xff});
        ByteString binaryValue = ByteString.copyFrom(new byte[] {(byte) 0xff, 0x00});
        ByteString orderedDeleteRowKey = ByteString.copyFromUtf8("ordered-delete");

        mutateRow(
                table,
                binaryRowKey,
                mutation -> mutation.setCell(FAMILY, binaryQualifier, binaryValue));
        mutateRow(
                table,
                orderedDeleteRowKey,
                mutation ->
                        mutation.setCell(FAMILY, "q", "value")
                                .deleteCells(FAMILY, "q")
                                .deleteFamily(FAMILY));
        Instant end = Instant.now().plusSeconds(120);

        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(changeStreamTableDdl(table, start, end));
        List<Row> rows =
                collect(
                        tEnv,
                        "SELECT row_key, entry_index, kind, family, qualifier, entry_value, "
                                + "mutation_type, source_cluster, committed_at, tie_breaker, "
                                + "low_watermark "
                                + "FROM table_mutations CROSS JOIN UNNEST(entries) AS entry_table("
                                + "entry_index, kind, family, qualifier, entry_timestamp, "
                                + "entry_value, delete_range)");

        List<Row> binaryEntries = entriesFor(rows, binaryRowKey);
        assertThat(binaryEntries).hasSize(1);
        Row binaryEntry = binaryEntries.get(0);
        assertThat(binaryEntry.getField(1)).isEqualTo(0);
        assertThat(binaryEntry.getField(2)).isEqualTo("SET_CELL");
        assertThat(binaryEntry.getField(3)).isEqualTo(FAMILY);
        assertRawValue((Row) binaryEntry.getField(4), binaryQualifier);
        assertRawValue((Row) binaryEntry.getField(5), binaryValue);

        List<Row> orderedDeleteEntries = entriesFor(rows, orderedDeleteRowKey);
        assertThat(orderedDeleteEntries).hasSize(3);
        assertThat(orderedDeleteEntries)
                .extracting(row -> row.getField(1))
                .containsExactly(0, 1, 2);
        assertThat(orderedDeleteEntries)
                .extracting(row -> row.getField(2))
                .containsExactly("SET_CELL", "DELETE_CELLS", "DELETE_FAMILY");

        List<Row> acceptedEntries = new ArrayList<>(binaryEntries);
        acceptedEntries.addAll(orderedDeleteEntries);
        assertThat(acceptedEntries)
                .allSatisfy(
                        row -> {
                            assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
                            assertThat(row.getField(6)).isEqualTo("USER");
                            assertThat(row.getField(7)).isEqualTo(sourceCluster);
                            assertThat((Instant) row.getField(8)).isBetween(start, end);
                            assertThat(row.getField(9)).isInstanceOf(Integer.class);
                            assertThat(row.getField(10)).isInstanceOf(Instant.class);
                        });
    }

    private static void assertRawValue(Row value, ByteString expected) {
        assertThat(value.getField(0)).isEqualTo("RAW_VALUE");
        assertThat((byte[]) value.getField(1)).containsExactly(expected.toByteArray());
        assertThat(value.getField(2)).isNull();
    }

    private static List<Row> entriesFor(List<Row> rows, ByteString rowKey) {
        List<Row> matched = new ArrayList<>();
        for (Row row : rows) {
            if (Arrays.equals((byte[]) row.getField(0), rowKey.toByteArray())) {
                matched.add(row);
            }
        }
        matched.sort(Comparator.comparingInt(row -> (Integer) row.getField(1)));
        return matched;
    }

    private static Instant instantFromMicros(long micros) {
        return Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    private static String changeStreamTableDdl(TableDestination table, Instant start, Instant end) {
        return "CREATE TABLE table_mutations (\n"
                + "  row_key BYTES,\n"
                + "  entries ARRAY<ROW<\n"
                + "    entry_index INT,\n"
                + "    kind STRING,\n"
                + "    family STRING,\n"
                + "    qualifier ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,\n"
                + "    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,\n"
                + "    `value` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,\n"
                + "    delete_range ROW<start_bound STRING, start_micros BIGINT, "
                + "end_bound STRING, end_micros BIGINT>\n"
                + "  >>,\n"
                + "  mutation_type STRING NOT NULL METADATA FROM 'mutation-type' VIRTUAL,\n"
                + "  source_cluster STRING METADATA FROM 'source-cluster-id' VIRTUAL,\n"
                + "  committed_at TIMESTAMP_LTZ(9) NOT NULL METADATA FROM "
                + "'commit-timestamp' VIRTUAL,\n"
                + "  tie_breaker INT NOT NULL METADATA FROM 'tie-breaker' VIRTUAL,\n"
                + "  low_watermark TIMESTAMP_LTZ(9) NOT NULL METADATA FROM "
                + "'estimated-low-watermark' VIRTUAL\n"
                + ") WITH (\n"
                + "  'connector' = 'bigtable',\n"
                + "  'project' = '"
                + PROJECT
                + "',\n"
                + "  'instance' = '"
                + table.getInstance()
                + "',\n"
                + "  'table' = '"
                + table.getTable()
                + "',\n"
                + "  'scan.mode' = 'change-stream',\n"
                + "  'scan.change-stream.changelog-mode' = 'envelope',\n"
                + "  'scan.app-profile-id' = '"
                + CHANGE_STREAM_APP_PROFILE
                + "',\n"
                + "  'scan.startup.mode' = 'timestamp',\n"
                + "  'scan.startup.timestamp-millis' = '"
                + start.toEpochMilli()
                + "',\n"
                + "  'scan.bounded.timestamp-millis' = '"
                + end.toEpochMilli()
                + "'\n"
                + ")";
    }
}
