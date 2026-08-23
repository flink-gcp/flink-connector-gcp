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

import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SQL reads against the emulator, through the production factory.
 *
 * <p>Everything here goes through {@code CREATE TABLE} and {@code SELECT} rather than through a
 * hand-built source, which is the point: the DDL model, the factory, the projection pushdown, the
 * cell decoding and the DataStream source underneath are exercised as one. Rows are seeded either
 * through the SQL sink — whose own ITCase pins what it writes — for the round-trip cases, or with
 * the harness's own client where the test needs a shape the sink cannot produce.
 *
 * <p>What is deliberately <em>not</em> here (ADR-0080): split planning, which the emulator cannot
 * model, and {@code scan.app-profile-id}, which the emulator ignores. Both live in the gated
 * real-GCP suite.
 */
class BigtableTableSourceITCase extends BigtableTableTestBase {

    private static List<Row> collect(TableEnvironment tEnv, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static String[] append(String[] options, String... additions) {
        String[] combined = Arrays.copyOf(options, options.length + additions.length);
        System.arraycopy(additions, 0, combined, options.length, additions.length);
        return combined;
    }

    private static String readbackOptions(String tableId, String... keysAndValues) {
        return withOptions(
                tableId, append(keysAndValues, "sink.insert-only-input-mode", "insert-only"));
    }

    private static Stream<Arguments> pointLookupModes() {
        return Stream.of(
                Arguments.of("sync", new String[0]),
                Arguments.of("async", new String[] {"lookup.async", "true"}),
                Arguments.of(
                        "async-partial",
                        new String[] {
                            "lookup.async",
                            "true",
                            "lookup.cache",
                            "PARTIAL",
                            "lookup.partial-cache.max-rows",
                            "10"
                        }),
                Arguments.of(
                        "partial",
                        new String[] {
                            "lookup.cache", "PARTIAL", "lookup.partial-cache.max-rows", "10"
                        }),
                Arguments.of(
                        "full",
                        new String[] {
                            "lookup.cache",
                            "FULL",
                            "lookup.full-cache.periodic-reload.interval",
                            "1 h"
                        }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void aTemporalJoinReadsHitsAndMissesInEveryLookupMode(String mode, String[] lookupOptions)
            throws Exception {
        String tableId = "sql-lookup-" + mode;
        TableDestination destination = createTable(tableId, "cf1");
        writeCell(destination, "r1", "cf1", "name", "alice");
        writeCell(destination, "r2", "cf1", "name", "bob");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData("r1", "missing", "r2"),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  rowkey STRING,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(tableId, lookupOptions));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT f.f0, b.cf1.name FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.f0 = b.rowkey"))
                .containsExactlyInAnyOrder(
                        Row.of("r1", "alice"), Row.of("missing", null), Row.of("r2", "bob"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void everyLookupModeHonorsTheConfiguredScanRanges(String mode, String[] lookupOptions)
            throws Exception {
        String tableId = "sql-lookup-range-" + mode;
        TableDestination destination = createTable(tableId, "cf1");
        writeCell(destination, "tenant-a/r1", "cf1", "name", "alice");
        writeCell(destination, "tenant-b/r1", "cf1", "name", "bob");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData("tenant-a/r1", "tenant-b/r1"),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                tableId, append(lookupOptions, "scan.row-prefix", "tenant-a/")));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT f.f0, b.cf1.name FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.f0 = b.rowkey"))
                .containsExactlyInAnyOrder(
                        Row.of("tenant-a/r1", "alice"), Row.of("tenant-b/r1", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void everyLookupModeHonorsBase64BinaryPrefixes(String mode, String[] lookupOptions)
            throws Exception {
        String tableId = "sql-lookup-binary-" + mode;
        byte[] included = {0x00, (byte) 0x80};
        byte[] excluded = {0x01, (byte) 0x80};
        TableDestination destination = createTable(tableId, "cf1");
        writeCell(destination, ByteString.copyFrom(included), "cf1", "name", "included");
        writeCell(destination, ByteString.copyFrom(excluded), "cf1", "name", "excluded");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData(included, excluded),
                        Schema.newBuilder()
                                .column("f0", DataTypes.BYTES())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey BYTES,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                tableId,
                                append(
                                        lookupOptions,
                                        "scan.row-key-encoding",
                                        "BASE64",
                                        "scan.row-prefix",
                                        "AA==")));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT f.f0, b.cf1.name FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.f0 = b.rowkey"))
                .containsExactlyInAnyOrder(Row.of(included, "included"), Row.of(excluded, null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void everyLookupModeUsesTheSameResidualPredicateAndClosedRangeStart(
            String mode, String[] lookupOptions) throws Exception {
        String tableId = "sql-lookup-filter-" + mode;
        TableDestination destination = createTable(tableId, "cf1");
        writeCell(destination, "b", "cf1", "name", "keep");
        writeCell(destination, "c", "cf1", "name", "drop");
        writeCell(destination, "d", "cf1", "name", "keep");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData("b", "c", "d"),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                tableId,
                                append(
                                        lookupOptions,
                                        "scan.row-range.start-closed",
                                        "b",
                                        "scan.row-range.end-open",
                                        "d")));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT f.f0, b.cf1.name FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.f0 = b.rowkey AND b.cf1.name = 'keep'"))
                .containsExactlyInAnyOrder(
                        Row.of("b", "keep"), Row.of("c", null), Row.of("d", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void everyLookupModeHonorsTheSameMultipleRangeUnion(String mode, String[] lookupOptions)
            throws Exception {
        String tableId = "sql-lookup-ranges-" + mode;
        TableDestination destination = createTable(tableId, "cf1");
        for (String key : Arrays.asList("a", "b", "c", "d", "e")) {
            writeCell(destination, key, "cf1", "name", key);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData("a", "b", "c", "d", "e"),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                tableId, append(lookupOptions, "scan.row-ranges", "[a,c);[d,e)")));

        assertThat(
                        collect(
                                tEnv,
                                "SELECT f.f0, b.cf1.name FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.f0 = b.rowkey"))
                .containsExactlyInAnyOrder(
                        Row.of("a", "a"),
                        Row.of("b", "b"),
                        Row.of("c", null),
                        Row.of("d", "d"),
                        Row.of("e", null));
    }

    @Test
    void anInsertReadsBackThroughSelect() throws Exception {
        createTable("sql-select", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING, amount BIGINT, price DECIMAL(5, 2)>,\n"
                        + "  cf2 ROW<flag BOOLEAN, seen TIMESTAMP_LTZ(3)>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + readbackOptions("sql-select"));

        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('r1', ROW('alice', CAST(7 AS BIGINT), CAST(123.45 AS DECIMAL(5,"
                                + " 2))), ROW(true, TO_TIMESTAMP_LTZ(1700000000000, 3))),"
                                + " ('r2', ROW('bob', CAST(9 AS BIGINT), CAST(-1.00 AS DECIMAL(5, 2))),"
                                + " ROW(false, TO_TIMESTAMP_LTZ(1700000001000, 3)))")
                .await();

        assertThat(collect(tEnv, "SELECT * FROM bt"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                "r1",
                                Row.of("alice", 7L, new BigDecimal("123.45")),
                                Row.of(true, Instant.ofEpochMilli(1_700_000_000_000L))),
                        Row.of(
                                "r2",
                                Row.of("bob", 9L, new BigDecimal("-1.00")),
                                Row.of(false, Instant.ofEpochMilli(1_700_000_001_000L))));
    }

    @Test
    void nullsReadBackAsNulls() throws Exception {
        createTable("sql-select-null", "cf1");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING, amount BIGINT>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + readbackOptions("sql-select-null", "null-string-literal", "<none>"));

        // The sink writes a null string as the literal and a null BIGINT as an empty cell; both
        // must come back as SQL NULLs through the same option.
        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('r1', ROW(CAST(NULL AS STRING), CAST(NULL AS BIGINT)))")
                .await();

        assertThat(collect(tEnv, "SELECT * FROM bt"))
                .containsExactly(Row.of("r1", Row.of(null, null)));
    }

    @Test
    void aProjectionReadsOnlyItsColumns() throws Exception {
        createTable("sql-projection", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  cf2 ROW<amount BIGINT>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + readbackOptions("sql-projection"));
        tEnv.executeSql(
                        "INSERT INTO bt VALUES ('r1', ROW('alice'), ROW(CAST(7 AS BIGINT))),"
                                + " ('r2', ROW('bob'), ROW(CAST(9 AS BIGINT)))")
                .await();

        assertThat(collect(tEnv, "SELECT cf2, rowkey FROM bt"))
                .as("a reordered projection that drops cf1")
                .containsExactlyInAnyOrder(Row.of(Row.of(7L), "r1"), Row.of(Row.of(9L), "r2"));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt"))
                .as("a row-key-only projection, served by the keys-only chain")
                .containsExactlyInAnyOrder(Row.of("r1"), Row.of("r2"));

        // COUNT(*) in a batch environment, where the aggregate emits its one final row — in
        // streaming mode collect() would deliver the whole retract changelog. The projection it
        // pushes is *empty*: no column at all, one output row per Bigtable row.
        TableEnvironment batchEnv = batchTableEnvironment();
        batchEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  cf2 ROW<amount BIGINT>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-projection"));
        assertThat(collect(batchEnv, "SELECT COUNT(*) FROM bt")).containsExactly(Row.of(2L));
    }

    @Test
    void prefixesAreAdditive() throws Exception {
        TableDestination destination = createTable("sql-prefix");
        seedRows(destination, "other1", "user1", "user2", "web1");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-prefix", "scan.row-prefix", "user;web"));

        assertThat(collect(tEnv, "SELECT rowkey FROM bt"))
                .containsExactlyInAnyOrder(Row.of("user1"), Row.of("user2"), Row.of("web1"));
    }

    @Test
    void aRowRangeBoundsTheScan() throws Exception {
        TableDestination destination = createTable("sql-range");
        // Rows sitting exactly on the bounds, so the inclusivity assertions below can actually
        // fail: with only rows strictly between the bounds, every open/closed combination
        // produces the same result.
        seedRows(destination, "a1", "b", "c", "c1", "d", "d1");
        TableEnvironment tEnv = streamingTableEnvironment();
        String ddl =
                "CREATE TABLE %s (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") ";
        tEnv.executeSql(
                String.format(ddl, "two_sided")
                        + withOptions(
                                "sql-range",
                                "scan.row-range.start-closed",
                                "b",
                                "scan.row-range.end-open",
                                "d"));
        tEnv.executeSql(
                String.format(ddl, "start_only")
                        + withOptions("sql-range", "scan.row-range.start-closed", "c"));

        assertThat(collect(tEnv, "SELECT rowkey FROM two_sided"))
                .as(
                        "[b, d): the start is inclusive — row 'b' is in — and the end exclusive —"
                                + " row 'd' is out")
                .containsExactlyInAnyOrder(Row.of("b"), Row.of("c"), Row.of("c1"));
        assertThat(collect(tEnv, "SELECT rowkey FROM start_only"))
                .as("[c, *): a one-sided bound leaves the other end open")
                .containsExactlyInAnyOrder(Row.of("c"), Row.of("c1"), Row.of("d"), Row.of("d1"));
    }

    @Test
    void multipleRowRangesBoundOneSqlScan() throws Exception {
        TableDestination destination = createTable("sql-ranges");
        seedRows(destination, "a", "b", "c", "d", "e", "f");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-ranges", "scan.row-ranges", "[a,c);[e,)"));

        assertThat(collect(tEnv, "SELECT rowkey FROM bt"))
                .containsExactlyInAnyOrder(Row.of("a"), Row.of("b"), Row.of("e"), Row.of("f"));
    }

    @Test
    void aBase64RowRangeBoundsBinaryKeys() throws Exception {
        byte[] start = {0x00, 0x00};
        byte[] nonUtf8 = {0x00, (byte) 0xff};
        byte[] end = {0x01, 0x00};
        byte[] after = {(byte) 0x80, 0x00};
        TableDestination destination = createTable("sql-binary-range");
        writeCell(destination, ByteString.copyFrom(start), "cf", "q", "start");
        writeCell(destination, ByteString.copyFrom(nonUtf8), "cf", "q", "non-utf8");
        writeCell(destination, ByteString.copyFrom(end), "cf", "q", "end");
        writeCell(destination, ByteString.copyFrom(after), "cf", "q", "after");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey BYTES,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                "sql-binary-range",
                                "scan.row-key-encoding",
                                "BASE64",
                                "scan.row-range.start-closed",
                                "AAA=",
                                "scan.row-range.end-open",
                                "AQA="));

        assertThat(collect(tEnv, "SELECT rowkey FROM bt"))
                .containsExactlyInAnyOrder(Row.of(start), Row.of(nonUtf8));
    }

    @Test
    void theTrailingBytesPolicyDecidesAnOverlongRowKeyEndToEnd() throws Exception {
        // Issue #1037 through the whole stack — DDL, factory, scan plumbing, converter: a
        // nine-byte key on a BIGINT row-key column reads as its eight-byte prefix under the
        // default and fails the scan under 'reject', so a policy hardcoded anywhere along the
        // way breaks one of the two arms.
        byte[] overlongKey = {0, 0, 0, 0, 0, 0, 0, 7, 0x7f};
        TableDestination destination = createTable("sql-trailing-bytes");
        writeCell(destination, ByteString.copyFrom(overlongKey), "cf", "q", "value");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE ignoring (\n"
                        + "  rowkey BIGINT,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-trailing-bytes"));
        tEnv.executeSql(
                "CREATE TABLE rejecting (\n"
                        + "  rowkey BIGINT,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-trailing-bytes", "decode.trailing-bytes", "reject"));

        assertThat(collect(tEnv, "SELECT rowkey FROM ignoring")).containsExactly(Row.of(7L));
        assertThat(TableJobFailures.awaitFailure(tEnv, "SELECT rowkey FROM rejecting"))
                .hasStackTraceContaining("holds 9 byte(s)")
                .hasStackTraceContaining("row-key column type cannot decode");
        // The pushdown interaction ADR-0136 settles, remeasured after the independent review
        // found the two holes it closes: under 'ignore' equality is a prefix range and the
        // suffix-bearing key matches = and is excluded from <>; under 'reject' fixed-width key
        // predicates stay with Flink, so every one of these scans meets the malformed key and
        // fails — including <> which a pushed prefix-complement would have silently skipped, and
        // a projection that drops the key column, which decodes it anyway.
        assertThat(collect(tEnv, "SELECT rowkey FROM ignoring WHERE rowkey = 7"))
                .containsExactly(Row.of(7L));
        assertThat(collect(tEnv, "SELECT rowkey FROM ignoring WHERE rowkey <> 7")).isEmpty();
        assertThat(
                        TableJobFailures.awaitFailure(
                                tEnv, "SELECT rowkey FROM rejecting WHERE rowkey = 7"))
                .hasStackTraceContaining("holds 9 byte(s)");
        assertThat(
                        TableJobFailures.awaitFailure(
                                tEnv, "SELECT rowkey FROM rejecting WHERE rowkey <> 7"))
                .hasStackTraceContaining("holds 9 byte(s)");
        assertThat(TableJobFailures.awaitFailure(tEnv, "SELECT cf.q FROM rejecting"))
                .hasStackTraceContaining("holds 9 byte(s)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pointLookupModes")
    void everyLookupModeHonorsTheTrailingBytesPolicy(String mode, String[] lookupOptions)
            throws Exception {
        // The lookup-side arm of the policy, once per runtime shape: the five modes reach four
        // different constructors (sync, async, partial caches over each, and the full-cache input
        // format), and each carries the policy separately, so one hardcoded IGNORE among them
        // fails exactly its mode here. The malformed bytes sit in a cell rather than the key —
        // a point read can only hit a key the join input encoded, which is always exact.
        String tableId = "sql-lookup-trailing-" + mode;
        TableDestination destination = createTable(tableId, "cf1");
        writeCell(destination, "r1", "cf1", "score", "123456789");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView(
                "facts",
                tEnv.fromDataStream(
                        env.fromData("r1"),
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .columnByExpression("event_time", "PROCTIME()")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<score BIGINT>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                tableId, append(lookupOptions, "decode.trailing-bytes", "reject")));

        // The nine-byte string cell on a BIGINT qualifier is exactly the overlong shape.
        // Flink 1.20.4 can replace this cause with a TaskExecutor-shutdown failure on collect,
        // while Task logs the original failure before notifying the scheduler on both tested
        // lines (#1066).
        try (LogCapture capture = LogCapture.of(Task.class)) {
            assertThatThrownBy(
                    () ->
                            collect(
                                    tEnv,
                                    "SELECT f.f0, b.cf1.score FROM facts AS f "
                                            + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time"
                                            + " AS b ON f.f0 = b.rowkey"));
            assertThat(capture.getEvents())
                    .anySatisfy(
                            event ->
                                    assertThat(event.getThrowable())
                                            .hasStackTraceContaining("holds 9 byte(s)")
                                            .hasStackTraceContaining(
                                                    "declared column type cannot decode"));
        }
    }

    @Test
    void sqlRowKeyBoundsIntersectTheConfiguredRange() throws Exception {
        TableDestination destination = createTable("sql-filter-range");
        seedRows(destination, "a", "b", "c", "d", "e");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<q STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions(
                                "sql-filter-range",
                                "scan.row-range.start-closed",
                                "a",
                                "scan.row-range.end-open",
                                "e"));

        assertThat(collect(tEnv, "SELECT rowkey FROM bt WHERE rowkey >= 'b' AND rowkey < 'd'"))
                .containsExactlyInAnyOrder(Row.of("b"), Row.of("c"));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt WHERE rowkey = 'a' OR rowkey = 'd'"))
                .containsExactlyInAnyOrder(Row.of("a"), Row.of("d"));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt WHERE rowkey < 'a' AND rowkey >= 'e'"))
                .isEmpty();
    }

    @Test
    void qualifierPrefilterDoesNotReplaceTheSqlValuePredicate() throws Exception {
        TableDestination destination = createTable("sql-filter-qualifier", "cf1", "cf2");
        writeCell(destination, "match", "cf1", "name", "alice");
        writeCell(destination, "match", "cf2", "outcome", "projected");
        writeCell(destination, "missing-projected", "cf1", "name", "alice");
        writeCell(destination, "wrong", "cf1", "name", "bob");
        writeCell(destination, "sibling", "cf1", "other", "alice");
        writeCell(destination, "sibling", "cf2", "outcome", "not-a-match");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  cf2 ROW<outcome STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-filter-qualifier"));

        assertThat(collect(tEnv, "SELECT rowkey, cf2.outcome FROM bt WHERE cf1.name = 'alice'"))
                .containsExactlyInAnyOrder(
                        Row.of("match", "projected"), Row.of("missing-projected", null));
    }

    @Test
    void aRowAppearsWhereAReadFamilyHasACell() throws Exception {
        // The row-existence rule the docs page states (ADR-0092): a query reading families
        // returns the rows with at least one cell in a family it reads — the family filter is
        // served by the server, and a row every retained family is empty in has nothing for it to
        // return — while a query reading no family sees every physical row. Which columns a query
        // selects therefore decides which rows it sees. HBase has the same projection-dependent
        // membership at qualifier granularity; this connector's contract is family granularity.
        TableDestination destination = createTable("sql-row-existence", "cf1", "cf2");
        writeCell(destination, "both", "cf1", "a", "x");
        writeCell(destination, "both", "cf2", "b", "y");
        writeCell(destination, "cf2-only", "cf2", "b", "y");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<a STRING>,\n"
                        + "  cf2 ROW<b STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-row-existence"));

        assertThat(collect(tEnv, "SELECT * FROM bt"))
                .as("cf2-only has a cell in a read family, so it appears — with cf1 null")
                .containsExactlyInAnyOrder(
                        Row.of("both", Row.of("x"), Row.of("y")),
                        Row.of("cf2-only", null, Row.of("y")));
        assertThat(collect(tEnv, "SELECT rowkey, cf1 FROM bt"))
                .as("a projection to cf1 drops the row with no cf1 cell")
                .containsExactly(Row.of("both", Row.of("x")));
        assertThat(collect(tEnv, "SELECT rowkey FROM bt"))
                .as("a query reading no family sees every physical row")
                .containsExactlyInAnyOrder(Row.of("both"), Row.of("cf2-only"));
    }

    @Test
    void anUndeclaredFamilyAndQualifierAreIgnored() throws Exception {
        // The physical table has a family the DDL does not declare and a qualifier the declared
        // family does not: the family never leaves the server (the projection filter prunes it),
        // the qualifier arrives and is dropped by the converter.
        TableDestination destination = createTable("sql-undeclared", "cf1", "cf9");
        writeCell(destination, "r1", "cf1", "name", "alice");
        writeCell(destination, "r1", "cf1", "zzz", "ignored");
        writeCell(destination, "r1", "cf9", "name", "ignored");
        writeCell(destination, "r2", "cf1", "zzz", "ignored");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING, absent STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-undeclared"));

        assertThat(collect(tEnv, "SELECT * FROM bt"))
                .containsExactlyInAnyOrder(Row.of("r1", Row.of("alice", null)), Row.of("r2", null));
    }

    @Test
    void theLatestVersionOfACellWins() throws Exception {
        TableDestination destination = createTable("sql-versions", "cf1");
        writeCell(destination, "r1", "cf1", "name", 1_000_000L, "old");
        writeCell(destination, "r1", "cf1", "name", 2_000_000L, "new");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-versions"));

        assertThat(collect(tEnv, "SELECT * FROM bt")).containsExactly(Row.of("r1", Row.of("new")));
    }

    @Test
    void aRowKeyOnlyTableReadsItsKeys() throws Exception {
        // The table the sink refuses to write is read through the same DDL: the deliberate
        // asymmetry, end to end. Every row has cells — Bigtable has no other kind — and the
        // keys-only chain strips them.
        TableDestination destination = createTable("sql-keys-only");
        seedRows(destination, "r1", "r2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE keys_only (\n"
                        + "  rowkey STRING,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-keys-only"));

        assertThat(collect(tEnv, "SELECT * FROM keys_only"))
                .containsExactlyInAnyOrder(Row.of("r1"), Row.of("r2"));
    }
}
