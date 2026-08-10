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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SQL writes against the emulator, through the production factory.
 *
 * <p>Everything here goes through {@code CREATE TABLE} and {@code INSERT INTO} rather than through
 * a hand-built sink, which is the point: the DDL model, the factory, the cell encoding and the
 * DataStream sink underneath are exercised as one, and the rows are read back with the harness's
 * own client rather than with anything under test.
 */
class BigtableTableSinkITCase extends BigtableTableTestBase {

    private static String ddl(String withClause) {
        return "CREATE TABLE bt (\n"
                + "  rowkey STRING,\n"
                + "  cf1 ROW<name STRING, amount BIGINT>,\n"
                + "  cf2 ROW<flag BOOLEAN>,\n"
                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                + ") "
                + withClause;
    }

    private static String cell(Row row, String family, String qualifier) {
        return cellBytes(row, family, qualifier) == null
                ? null
                : new String(
                        cellBytes(row, family, qualifier), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] cellBytes(Row row, String family, String qualifier) {
        List<RowCell> cells = row.getCells(family, qualifier);
        assertThat(cells).as("cell %s:%s of row %s", family, qualifier, row.getKey()).hasSize(1);
        return cells.get(0).getValue().toByteArray();
    }

    @Test
    void writesEveryDeclaredCellOfEveryRow() throws Exception {
        TableDestination destination = createTable("sql-insert", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(ddl(withOptions("sql-insert")));

        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('r1', ROW('alice', CAST(7 AS BIGINT)), ROW(true)),"
                                + " ('r2', ROW('bob', CAST(9 AS BIGINT)), ROW(false))")
                .await();

        List<Row> rows = readRows(destination);
        assertThat(rows).extracting(row -> row.getKey().toStringUtf8()).containsExactly("r1", "r2");
        assertThat(cell(rows.get(0), "cf1", "name")).isEqualTo("alice");
        assertThat(cellBytes(rows.get(0), "cf1", "amount"))
                .isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 7});
        assertThat(cellBytes(rows.get(0), "cf2", "flag")).isEqualTo(new byte[] {(byte) 0xff});
        assertThat(cell(rows.get(1), "cf1", "name")).isEqualTo("bob");
        assertThat(cellBytes(rows.get(1), "cf2", "flag")).isEqualTo(new byte[] {0});
    }

    @Test
    void aNullStringCellTakesTheConfiguredLiteral() throws Exception {
        TableDestination destination = createTable("sql-null", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(ddl(withOptions("sql-null", "null-string-literal", "<none>")));

        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('r1', ROW(CAST(NULL AS STRING), CAST(NULL AS BIGINT)),"
                                + " ROW(CAST(NULL AS BOOLEAN)))")
                .await();

        List<Row> rows = readRows(destination);
        assertThat(cell(rows.get(0), "cf1", "name")).isEqualTo("<none>");
        assertThat(cellBytes(rows.get(0), "cf1", "amount")).isEmpty();
        assertThat(cellBytes(rows.get(0), "cf2", "flag")).isEmpty();
    }

    @Test
    void aLaterWriteForTheSameKeyOverwritesTheEarlierOne() throws Exception {
        TableDestination destination = createTable("sql-overwrite", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(ddl(withOptions("sql-overwrite")));

        // Two jobs, not two rows of one VALUES. Bigtable applies the entries of a MutateRows
        // request "in arbitrary order (even between entries for the same row)" — its own proto
        // contract — so two rows for one key inside a single batch have no defined winner, and a
        // test asserting one would be asserting the emulator's submission order. Sequential
        // requests do have one, and that is the property an upsert sink actually offers.
        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('first', CAST(1 AS BIGINT)), ROW(true))")
                .await();
        tEnv.executeSql(
                        "INSERT INTO bt VALUES ('r1', ROW('second', CAST(2 AS BIGINT)),"
                                + " ROW(false))")
                .await();

        List<Row> rows = readRows(destination);
        assertThat(rows).hasSize(1);
        // The family was created with the harness's default rule, so both versions are stored and
        // the read has to name the latest rather than assume a single cell.
        assertThat(rows.get(0).getCells("cf1", "name").get(0).getValue().toStringUtf8())
                .isEqualTo("second");
    }

    @Test
    void aRowWhoseFamiliesAreAllNullIsRefusedRatherThanSentEmpty() {
        // A partial column list is the ordinary way to reach it. Left to the service this is an
        // INVALID_ARGUMENT naming neither the row nor the reason.
        createTable("sql-empty-mutation", "cf1", "cf2");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(ddl(withOptions("sql-empty-mutation")));

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO bt (rowkey) VALUES ('r1')").await())
                .hasStackTraceContaining("Every column family of the row with key 'r1' is null");
    }

    @Test
    void aChangelogDeleteRemovesTheWholeRow() throws Exception {
        TableDestination destination = createTable("sql-delete", "cf1", "cf2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(ddl(withOptions("sql-delete")));
        tEnv.executeSql(
                        "INSERT INTO bt VALUES"
                                + " ('keep', ROW('alice', CAST(7 AS BIGINT)), ROW(true)),"
                                + " ('gone', ROW('bob', CAST(9 AS BIGINT)), ROW(false))")
                .await();
        assertThat(readRows(destination)).hasSize(2);

        // A changelog stream is how a -D reaches the sink at all: no source in this module emits
        // one, and the planner never sends UPDATE_BEFORE to an upsert sink, so this is the only
        // shape that exercises deleteRow end to end. The rows are written by a *previous* job so
        // that the insert and the delete for one key never share a MutateRows request, whose
        // entries the service may apply in any order.
        TypeInformation<org.apache.flink.types.Row> rowType =
                Types.ROW_NAMED(
                        new String[] {"k", "name", "amount", "flag"},
                        Types.STRING,
                        Types.STRING,
                        Types.LONG,
                        Types.BOOLEAN);
        DataStream<org.apache.flink.types.Row> changelog =
                env.fromData(
                        Arrays.asList(
                                org.apache.flink.types.Row.ofKind(
                                        RowKind.DELETE, "gone", "bob", 9L, false)),
                        rowType);
        tEnv.createTemporaryView(
                "src",
                tEnv.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("k", DataTypes.STRING().notNull())
                                .column("name", DataTypes.STRING())
                                .column("amount", DataTypes.BIGINT())
                                .column("flag", DataTypes.BOOLEAN())
                                .primaryKey("k")
                                .build(),
                        // A retract mode, not an upsert one, and that is what makes the test
                        // portable: an upsert source on the 1.20 LTS build makes the planner
                        // insert a stateful ChangelogNormalize, which starts empty and swallows a
                        // -D for a key this job never inserted. Measured — the test failed on
                        // 1.20.4 and passed on 2.2.1 until this changed.
                        ChangelogMode.all()));

        tEnv.executeSql(
                        "INSERT INTO bt SELECT k,"
                                + " CAST(ROW(name, amount) AS ROW<name STRING, amount BIGINT>),"
                                + " CAST(ROW(flag) AS ROW<flag BOOLEAN>) FROM src")
                .await();

        assertThat(readRows(destination))
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactly("keep");
    }

    @Test
    void createsTheTableAndItsFamiliesWhenAskedTo() throws Exception {
        // Nothing creates the table first: the DDL's ROW<...> columns are what the families are
        // made from, and the garbage-collection rule comes from the two keys.
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                ddl(
                        withOptions(
                                "sql-created",
                                "sink.create-disposition",
                                "create-if-needed",
                                "sink.table-create.gc-rule.max-versions",
                                "1")));

        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('alice', CAST(7 AS BIGINT)), ROW(true))")
                .await();

        assertThat(describeTable("sql-created").getColumnFamilies())
                .extracting(ColumnFamily::getId)
                .containsExactlyInAnyOrder("cf1", "cf2");
        assertThat(describeTable("sql-created").getColumnFamilies())
                .allSatisfy(
                        family ->
                                assertThat(family.getGCRule().toProto())
                                        .isEqualTo(GCRules.GCRULES.maxVersions(1).toProto()));
        assertThat(readRows(TableDestination.of(PROJECT, INSTANCE, "sql-created"))).hasSize(1);
    }

    @Test
    void aNonTextRowKeyIsWrittenInItsDeclaredEncoding() throws Exception {
        TableDestination destination = createTable("sql-bigint-key", "cf1");
        TableEnvironment tEnv = streamingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  k BIGINT,\n"
                        + "  cf1 ROW<name STRING>,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ") "
                        + withOptions("sql-bigint-key"));

        tEnv.executeSql("INSERT INTO bt VALUES (CAST(1 AS BIGINT), ROW('alice'))").await();

        List<Row> rows = readRows(destination);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getKey().toByteArray())
                .isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 1});
        assertThat(cell(rows.get(0), "cf1", "name")).isEqualTo("alice");
    }
}
