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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrimaryKeys}. */
class PrimaryKeysTest {

    private static final Type ROW_TYPE =
            Type.struct(
                    Type.StructField.of("table_schema", Type.string()),
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string()),
                    Type.StructField.of("ordinal_position", Type.int64()),
                    Type.StructField.of("spanner_type", Type.string()));

    private static final PrimaryKeys KEYS =
            PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                    .keyColumn("Orders", "Id", 1, "INT64")
                    .keyColumn("Lines", "LineNo", 2, "INT64")
                    .keyColumn("Lines", "OrderId", 1, "INT64")
                    .build();

    @Test
    void aWriteAndADeleteOfOneKeyShareARow() {
        assertThat(overlap(upsert("Orders", 7L), Mutation.delete("Orders", Key.of(7L)))).isTrue();
        assertThat(overlap(upsert("Orders", 7L), Mutation.delete("Orders", Key.of(8L)))).isFalse();
    }

    @Test
    void writesOfOtherTablesNeverShareARow() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Orders", "Id", 1, "INT64")
                        .keyColumn("Archive", "Id", 1, "INT64")
                        .build();

        assertThat(overlap(keys, upsert("Orders", 7L), upsert("Archive", 7L))).isFalse();
    }

    @Test
    void readsACompositeKeyInKeyOrderWhateverOrderTheColumnsArrived() {
        Mutation write =
                Mutation.newInsertOrUpdateBuilder("Lines")
                        .set("LineNo")
                        .to(2L)
                        .set("Note")
                        .to("x")
                        .set("OrderId")
                        .to(1L)
                        .build();

        assertThat(overlap(write, Mutation.delete("Lines", Key.of(1L, 2L)))).isTrue();
        assertThat(overlap(write, Mutation.delete("Lines", Key.of(2L, 1L)))).isFalse();
    }

    @Test
    void matchesGoogleSqlTableAndColumnNamesWithoutCase() {
        Mutation write = Mutation.newUpdateBuilder("ORDERS").set("ID").to(7L).build();

        assertThat(KEYS.footprintOf(write).isWholeTable()).isFalse();
        assertThat(overlap(write, upsert("orders", 7L))).isTrue();
    }

    @Test
    void foldsPostgresTableNamesButNotColumnNames() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.POSTGRESQL).keyColumn("t", "Id", 1, "bigint").build();

        // Whether the service matches a table name exactly is not measured, so the spellings meet.
        assertThat(
                        overlap(
                                keys,
                                Mutation.newUpdateBuilder("T").set("Id").to(1L).build(),
                                Mutation.delete("public.t", Key.of(1L))))
                .isTrue();
        // A column another case names is a missing key column: the whole table is the row.
        assertThat(
                        keys.footprintOf(Mutation.newUpdateBuilder("t").set("id").to(1L).build())
                                .isWholeTable())
                .isTrue();
    }

    @Test
    void twoPostgresTablesThatFoldTogetherAreOrderedAsAWhole() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.POSTGRESQL)
                        .keyColumn("Orders", "id", 1, "bigint")
                        .keyColumn("orders", "id", 1, "bigint")
                        .build();

        assertThat(
                        keys.footprintOf(
                                        Mutation.newUpdateBuilder("orders")
                                                .set("id")
                                                .to(1L)
                                                .build())
                                .isWholeTable())
                .isTrue();
    }

    @Test
    void keepsTablesOfNamedSchemasApart() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("sales", "Orders", "Id", 1, "INT64")
                        .keyColumn("", "Orders", "Id", 1, "INT64")
                        .build();

        assertThat(overlap(keys, upsert("sales.Orders", 7L), upsert("Orders", 7L))).isFalse();
        assertThat(keys.footprintOf(upsert("sales.Orders", 7L)).isWholeTable()).isFalse();
    }

    @Test
    void namesTheWholeTableWhenItCannotNameTheRow() {
        assertThat(KEYS.footprintOf(upsert("Unknown", 1L)).isWholeTable()).isTrue();
        assertThat(
                        KEYS.footprintOf(
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("Note")
                                                .to("no key")
                                                .build())
                                .isWholeTable())
                .isTrue();
        assertThat(KEYS.footprintOf(Mutation.delete("Orders", KeySet.all())).isWholeTable())
                .isTrue();
        assertThat(
                        KEYS.footprintOf(
                                        Mutation.delete(
                                                "Orders",
                                                KeySet.range(
                                                        KeyRange.closedOpen(
                                                                Key.of(1L), Key.of(5L)))))
                                .isWholeTable())
                .isTrue();
        assertThat(KEYS.footprintOf(Mutation.delete("Lines", Key.of(1L))).isWholeTable())
                .as("a key shorter than the table's")
                .isTrue();
        assertThat(
                        KEYS.footprintOf(
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("Id")
                                                .to(
                                                        Value.untyped(
                                                                com.google.protobuf.Value
                                                                        .newBuilder()
                                                                        .setStringValue("7")
                                                                        .build()))
                                                .build())
                                .isWholeTable())
                .isTrue();
        assertThat(PrimaryKeys.empty().footprintOf(upsert("Orders", 1L)).isWholeTable()).isTrue();
    }

    @Test
    void aTableTheSchemaDidNotListIsOrderedAgainstEveryTable() {
        // A synonym of Orders: it addresses Orders' rows under another name.
        Mutation viaSynonym = Mutation.delete("OldOrders", Key.of(7L));

        assertThat(KEYS.footprintOf(viaSynonym).isWholeDatabase()).isTrue();
        assertThat(overlap(upsert("Orders", 7L), viaSynonym)).isTrue();
        assertThat(overlap(viaSynonym, upsert("Lines", 7L))).isTrue();
    }

    @Test
    void aListedTableWhoseRowsCannotBeNamedIsStillItsOwnTable() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.POSTGRESQL)
                        .keyColumn("Orders", "id", 1, "bigint")
                        .keyColumn("orders", "id", 1, "bigint")
                        .keyColumn("lines", "id", 1, "bigint")
                        .build();

        assertThat(
                        keys.footprintOf(
                                        Mutation.newUpdateBuilder("orders")
                                                .set("id")
                                                .to(1L)
                                                .build())
                                .isWholeDatabase())
                .isFalse();
        assertThat(
                        overlap(
                                keys,
                                Mutation.newUpdateBuilder("orders").set("id").to(1L).build(),
                                Mutation.newUpdateBuilder("lines").set("id").to(1L).build()))
                .isFalse();
    }

    @Test
    void aKeyInAnotherRepresentationThanItsColumnsTypeNamesTheWholeTable() {
        // Spanner reads an untyped wire value by the column's type, so "7" may address row 7.
        assertThat(KEYS.footprintOf(Mutation.delete("Orders", Key.of("7"))).isWholeTable())
                .isTrue();
        assertThat(KEYS.footprintOf(upsert("Orders", "7")).isWholeTable()).isTrue();
        PrimaryKeys uuidKeys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Things", "Id", 1, "UUID")
                        .build();
        assertThat(
                        uuidKeys.footprintOf(
                                        Mutation.delete(
                                                "Things",
                                                Key.of("123E4567-E89B-12D3-A456-426614174000")))
                                .isWholeTable())
                .isTrue();
    }

    @Test
    void aKeyColumnOfATypeNoComparisonCoversNamesTheWholeTable() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Events", "Kind", 1, "ENUM<pkg.Kind>")
                        .build();

        assertThat(
                        keys.footprintOf(
                                        Mutation.newInsertOrUpdateBuilder("Events")
                                                .set("Kind")
                                                .to(Value.protoEnum(1L, "pkg.Kind"))
                                                .build())
                                .isWholeTable())
                .isTrue();
    }

    @Test
    void aDeleteOfSeveralKeysNamesEachOfThem() {
        Mutation delete =
                Mutation.delete(
                        "Orders",
                        KeySet.newBuilder().addKey(Key.of(1L)).addKey(Key.of(2L)).build());

        assertThat(KEYS.footprintOf(delete).rows()).hasSize(2);
        assertThat(overlap(delete, upsert("Orders", 2L))).isTrue();
        assertThat(overlap(delete, upsert("Orders", 3L))).isFalse();
    }

    @Test
    void aNullKeyValueIsARowOfItsOwn() {
        Mutation nullKey =
                Mutation.newInsertOrUpdateBuilder("Orders").set("Id").to((Long) null).build();

        assertThat(overlap(nullKey, Mutation.delete("Orders", Key.of((Object) null)))).isTrue();
        assertThat(overlap(nullKey, upsert("Orders", 0L))).isFalse();
    }

    @Test
    void theCommitTimestampPlaceholderIsOneRow() {
        // Two writes carrying it may commit in one transaction, so at one timestamp.
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Log", "At", 1, "TIMESTAMP")
                        .build();

        assertThat(keys.footprintOf(logAt(Value.timestamp(Value.COMMIT_TIMESTAMP))).isWholeTable())
                .isFalse();
        assertThat(
                        overlap(
                                keys,
                                logAt(Value.timestamp(Value.COMMIT_TIMESTAMP)),
                                logAt(Value.timestamp(Value.COMMIT_TIMESTAMP))))
                .isTrue();
        assertThat(
                        overlap(
                                keys,
                                logAt(Value.timestamp(Value.COMMIT_TIMESTAMP)),
                                logAt(Value.timestamp(Timestamp.ofTimeMicroseconds(1)))))
                .isFalse();
    }

    /**
     * Each case names one row of an {@code Id} key column of the given type: a write's value, and
     * the part a delete's {@code Key} would carry for the same row.
     */
    static Stream<Arguments> equalKeys() {
        Timestamp timestamp = Timestamp.parseTimestamp("2026-10-28T01:02:03.400Z");
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        return Stream.of(
                Arguments.of("INT64", Value.int64(7), 7L),
                Arguments.of("INT64", Value.int64(7), 7),
                Arguments.of(
                        "NUMERIC", Value.numeric(new BigDecimal("1.50")), new BigDecimal("1.5")),
                Arguments.of(
                        "NUMERIC", Value.numeric(new BigDecimal("100")), new BigDecimal("1E+2")),
                Arguments.of("NUMERIC", Value.numeric(new BigDecimal("-0.00")), BigDecimal.ZERO),
                Arguments.of("NUMERIC", Value.pgNumeric("1.50"), new BigDecimal("1.5")),
                Arguments.of("NUMERIC", Value.pgNumeric("1.50"), "1.5"),
                Arguments.of("NUMERIC", Value.pgNumeric("NaN"), "NaN"),
                Arguments.of("FLOAT64", Value.float64(-0.0), 0.0),
                Arguments.of("FLOAT64", Value.float64(2.5), 2.5),
                Arguments.of("FLOAT64", Value.float64(Double.NaN), Double.NaN),
                Arguments.of("FLOAT32", Value.float32(0.1f), 0.1),
                Arguments.of("FLOAT32", Value.float64(0.1), 0.1f),
                Arguments.of("STRING(64)", Value.string("a"), "a"),
                Arguments.of("BOOL", Value.bool(true), true),
                Arguments.of(
                        "BYTES(MAX)",
                        Value.bytes(ByteArray.copyFrom("1200")),
                        ByteArray.copyFrom(new byte[] {'1', '2', '0', '0'})),
                Arguments.of("TIMESTAMP", Value.timestamp(timestamp), timestamp),
                Arguments.of(
                        "DATE",
                        Value.date(Date.fromYearMonthDay(2026, 10, 28)),
                        Date.fromYearMonthDay(2026, 10, 28)),
                Arguments.of("UUID", Value.uuid(uuid), uuid));
    }

    @ParameterizedTest(name = "{0}: {1} and {2}")
    @MethodSource("equalKeys")
    void aWritesValueAndADeletesKeyPartForTheSameRowShareIt(
            String type, Value written, Object deleted) {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Things", "Id", 1, type)
                        .build();
        Mutation write = Mutation.newInsertOrUpdateBuilder("Things").set("Id").to(written).build();
        Mutation delete = Mutation.delete("Things", Key.of(deleted));

        assertThat(keys.footprintOf(write).isWholeTable()).isFalse();
        assertThat(keys.footprintOf(delete).isWholeTable()).isFalse();
        assertThat(overlap(keys, write, delete)).isTrue();
    }

    @Test
    void readsPostgresTypeSpellings() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.POSTGRESQL)
                        .keyColumn("t", "id", 1, "bigint")
                        .keyColumn("t", "name", 2, "character varying(64)")
                        .build();
        Mutation write =
                Mutation.newInsertOrUpdateBuilder("t").set("id").to(1L).set("name").to("a").build();

        assertThat(keys.footprintOf(write).isWholeTable()).isFalse();
        assertThat(overlap(keys, write, Mutation.delete("t", Key.of(1L, "a")))).isTrue();
    }

    @Test
    void distinctValuesStayDistinct() {
        PrimaryKeys strings =
                PrimaryKeys.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .keyColumn("Things", "Id", 1, "STRING(MAX)")
                        .build();
        assertThat(overlap(strings, upsert("Things", "a"), upsert("Things", "A"))).isFalse();
        assertThat(overlap(strings, upsert("Things", "1.0"), upsert("Things", "1"))).isFalse();
        assertThat(overlap(upsert("Orders", 1L), upsert("Orders", 2L))).isFalse();
    }

    @Test
    void aNumericThatCannotBeNormalizedNamesTheWholeTable() {
        PrimaryKeys keys =
                PrimaryKeys.builder(Dialect.POSTGRESQL).keyColumn("t", "id", 1, "numeric").build();

        // Stripping trailing zeros overflows the scale.
        assertThat(keys.footprintOf(Mutation.delete("t", Key.of("100e2147483647"))).isWholeTable())
                .isTrue();
        assertThat(keys.footprintOf(Mutation.delete("t", Key.of("abc"))).isWholeTable()).isTrue();
    }

    private static Mutation upsert(String table, long id) {
        return Mutation.newInsertOrUpdateBuilder(table).set("Id").to(id).build();
    }

    private static Mutation upsert(String table, String id) {
        return Mutation.newInsertOrUpdateBuilder(table).set("Id").to(id).build();
    }

    private static Mutation logAt(Value at) {
        return Mutation.newInsertOrUpdateBuilder("Log").set("At").to(at).build();
    }

    private static boolean overlap(Mutation first, Mutation second) {
        return overlap(KEYS, first, second);
    }

    private static boolean overlap(PrimaryKeys keys, Mutation first, Mutation second) {
        BatchedRows batch = new BatchedRows();
        batch.add(keys.footprintOf(first));
        return batch.overlaps(keys.footprintOf(second));
    }

    // ---------------------------------------------------------------- reading the schema

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void hasAQueryForEveryDialectTheClientLibraryKnows(Dialect dialect) {
        assertThat(PrimaryKeys.query(dialect)).isNotBlank();
    }

    @Test
    void readsOnlyThePrimaryKeyOfEveryGoogleSqlUserSchema() {
        assertThat(PrimaryKeys.query(Dialect.GOOGLE_STANDARD_SQL))
                .contains("INFORMATION_SCHEMA.INDEX_COLUMNS AS k")
                .contains("JOIN INFORMATION_SCHEMA.COLUMNS AS c")
                .contains("k.ORDINAL_POSITION, c.SPANNER_TYPE")
                .contains("k.TABLE_CATALOG = ''")
                .contains("k.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')")
                .contains("k.INDEX_NAME = 'PRIMARY_KEY'")
                .contains("k.INDEX_TYPE = 'PRIMARY_KEY'");
    }

    @Test
    void readsOnlyThePrimaryKeyOfEveryPostgresUserSchema() {
        assertThat(PrimaryKeys.query(Dialect.POSTGRESQL))
                .contains("information_schema.index_columns AS k")
                .contains("JOIN information_schema.columns AS c")
                .contains("k.ordinal_position, c.spanner_type")
                .contains(
                        "k.table_schema NOT IN"
                                + " ('pg_catalog', 'information_schema', 'spanner_sys')")
                .contains("k.index_name = 'PRIMARY_KEY'")
                .contains("k.index_type = 'PRIMARY_KEY'");
    }

    @Test
    void ordersEachKeyByPositionAndKeepsSchemasApart() {
        ResultSet resultSet =
                rows(
                        row("", "Lines", "LineNo", 2L, "INT64"),
                        row("", "Lines", "OrderId", 1L, "INT64"),
                        row("archive", "Lines", "Id", 1L, "INT64"));

        PrimaryKeys keys = PrimaryKeys.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(keys.tableCount()).isEqualTo(2);
        Mutation line =
                Mutation.newInsertOrUpdateBuilder("Lines")
                        .set("LineNo")
                        .to(2L)
                        .set("OrderId")
                        .to(1L)
                        .build();
        BatchedRows batch = new BatchedRows();
        batch.add(keys.footprintOf(line));
        assertThat(batch.overlaps(keys.footprintOf(Mutation.delete("Lines", Key.of(1L, 2L)))))
                .isTrue();
        assertThat(
                        keys.footprintOf(
                                        Mutation.newInsertOrUpdateBuilder("archive.Lines")
                                                .set("Id")
                                                .to(1L)
                                                .build())
                                .isWholeTable())
                .isFalse();
    }

    @Test
    void readsAPostgresTableInItsSchemaWithItsTypes() {
        PrimaryKeys keys =
                PrimaryKeys.read(
                        rows(row("public", "orders", "id", 1L, "character varying(64)")),
                        Dialect.POSTGRESQL);

        BatchedRows batch = new BatchedRows();
        batch.add(
                keys.footprintOf(
                        Mutation.newInsertOrUpdateBuilder("orders").set("id").to("a").build()));
        assertThat(batch.overlaps(keys.footprintOf(Mutation.delete("orders", Key.of("a")))))
                .isTrue();
        assertThat(batch.overlaps(keys.footprintOf(Mutation.delete("orders", Key.of("b")))))
                .isFalse();
    }

    @Test
    void aTableWithAKeyColumnReadInPartIsNotNamed() {
        // A key read without one of its columns would compare fewer columns than Spanner does.
        ResultSet resultSet =
                rows(
                        row("", "Orders", "Region", 1L, "STRING(8)"),
                        row("", "Orders", "Id", 2L, null),
                        row("", "Lines", "Id", 1L, "INT64"),
                        row("", "Notes", null, 1L, "INT64"),
                        row("", "Logs", "Id", null, "INT64"),
                        row("", null, "Id", 1L, "INT64"),
                        row(null, "Orders", "Id", 1L, "INT64"));

        PrimaryKeys keys = PrimaryKeys.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(keys.tableCount()).isEqualTo(1);
        assertThat(
                        keys.footprintOf(
                                        Mutation.newInsertOrUpdateBuilder("Orders")
                                                .set("Region")
                                                .to("eu")
                                                .build())
                                .isWholeTable())
                .isTrue();
        // Still a listed table: ordered as a table, not against every other one.
        PrimaryKeys.Footprint notes =
                keys.footprintOf(
                        Mutation.newInsertOrUpdateBuilder("Notes").set("Id").to(1L).build());
        assertThat(notes.isWholeTable()).isTrue();
        assertThat(notes.isWholeDatabase()).isFalse();
    }

    private static ResultSet rows(Struct... structs) {
        List<Struct> list = Arrays.asList(structs);
        return ResultSets.forRows(ROW_TYPE, list);
    }

    private static Struct row(
            @Nullable String schema,
            @Nullable String table,
            @Nullable String column,
            @Nullable Long position,
            @Nullable String spannerType) {
        return Struct.newBuilder()
                .set("table_schema")
                .to(schema)
                .set("table_name")
                .to(table)
                .set("column_name")
                .to(column)
                .set("ordinal_position")
                .to(position)
                .set("spanner_type")
                .to(spannerType)
                .build();
    }
}
