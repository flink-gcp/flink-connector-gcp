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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CellWeights}. */
class CellWeightsTest {

    private static final Type ROW_TYPE =
            Type.struct(
                    Type.StructField.of("table_schema", Type.string()),
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string()),
                    Type.StructField.of("index_name", Type.string()));

    @Test
    void withoutIndexesEveryWrittenColumnCostsOneCell() {
        Mutation mutation =
                Mutation.newInsertBuilder("Orders")
                        .set("OrderId")
                        .to(1L)
                        .set("Total")
                        .to(2L)
                        .set("Note")
                        .to("x")
                        .build();

        assertThat(CellWeights.empty().weigh(mutation)).isEqualTo(3);
    }

    @Test
    void anIndexedColumnCostsOneMoreCellPerIndexCoveringIt() {
        CellWeights weights =
                CellWeights.builder()
                        .indexColumn("Orders", "Total", "OrdersByTotal")
                        .indexColumn("Orders", "Total", "OrdersByTotalAndNote")
                        .indexColumn("Orders", "Note", "OrdersByTotalAndNote")
                        .build();
        Mutation mutation =
                Mutation.newInsertBuilder("Orders")
                        .set("OrderId")
                        .to(1L)
                        .set("Total")
                        .to(2L)
                        .set("Note")
                        .to("x")
                        .build();

        // OrderId 1, Total 1 + 2 indexes, Note 1 + 1 index.
        assertThat(weights.weigh(mutation)).isEqualTo(6);
    }

    @Test
    void anIndexCountsOnceForAColumnHoweverManyRowsMentionIt() {
        CellWeights weights =
                CellWeights.builder()
                        .indexColumn("Orders", "Total", "OrdersByTotal")
                        .indexColumn("Orders", "Total", "OrdersByTotal")
                        .build();

        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(2);
    }

    @Test
    void aColumnNoIndexCoversStillCostsOneCell() {
        CellWeights weights =
                CellWeights.builder().indexColumn("Orders", "Total", "OrdersByTotal").build();

        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Note").to("x").build()))
                .isEqualTo(1);
    }

    @Test
    void matchesNamesWithoutRegardToCase() {
        // Spanner will not let two tables differ only in case, so folding costs nothing — and it
        // stops a serializer spelling a table differently from the schema losing its weights.
        CellWeights weights =
                CellWeights.builder().indexColumn("Orders", "Total", "OrdersByTotal").build();

        assertThat(weights.weigh(Mutation.newInsertBuilder("ORDERS").set("total").to(1L).build()))
                .isEqualTo(2);
        assertThat(weights.knows("orders")).isTrue();
    }

    @Test
    void keepsNamedSchemaTablesDistinct() {
        CellWeights weights =
                CellWeights.builder(Dialect.GOOGLE_STANDARD_SQL)
                        .indexColumn("sales", "Orders", "Total", "OrdersByTotal")
                        .indexColumn("archive", "Orders", "Note", "OrdersByNote")
                        .build();

        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("sales.Orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(2);
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("archive.Orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(1);
    }

    @Test
    void distinguishesQuotedPostgresqlNames() {
        CellWeights weights =
                CellWeights.builder(Dialect.POSTGRESQL)
                        .indexColumn("Sales", "Orders", "Total", "ByTotal")
                        .indexColumn("sales", "orders", "note", "by_note")
                        .build();

        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("Sales.Orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(2);
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("sales.orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(1);
    }

    @Test
    void aTableTheWeightsDoNotKnowIsCountedWithoutIndexEntries() {
        CellWeights weights =
                CellWeights.builder().indexColumn("Orders", "Total", "OrdersByTotal").build();

        assertThat(weights.knows("Shipments")).isFalse();
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("Shipments")
                                        .set("ShipmentId")
                                        .to(1L)
                                        .set("Carrier")
                                        .to("x")
                                        .build()))
                .isEqualTo(2);
    }

    @Test
    void aDeleteCostsOneCellPlusTheTablesIndexEntries() {
        CellWeights weights =
                CellWeights.builder()
                        .indexColumn("Orders", "Total", "OrdersByTotal")
                        .indexColumn("Orders", "Note", "OrdersByNote")
                        .build();

        // A delete counts as one mutation whatever its columns, plus one entry per index on the
        // table — so unlike a write, the count does not depend on which columns exist.
        assertThat(weights.weigh(Mutation.delete("Orders", Key.of(1L)))).isEqualTo(3);
    }

    @Test
    void aDeleteOfAnUnindexedTableCostsOneCell() {
        assertThat(CellWeights.empty().weigh(Mutation.delete("Orders", Key.of(1L)))).isEqualTo(1);
    }

    @Test
    void aRangeDeleteIsCountedAsTheSingleRowItCannotKnowTheSizeOf() {
        CellWeights weights =
                CellWeights.builder().indexColumn("Orders", "Total", "OrdersByTotal").build();
        Mutation mutation =
                Mutation.delete(
                        "Orders",
                        KeySet.range(KeyRange.closedClosed(Key.of(1L), Key.of(1_000_000L))));

        // Nothing on this side can know how many rows a range matches, so the estimate is the
        // single-row one — which the batch-limit headroom is what covers.
        assertThat(weights.weigh(mutation)).isEqualTo(2);
    }

    @Test
    void countsTheIndexedTablesItKnows() {
        CellWeights weights =
                CellWeights.builder()
                        .indexColumn("Orders", "Total", "OrdersByTotal")
                        .indexColumn("Shipments", "Carrier", "ShipmentsByCarrier")
                        .build();

        assertThat(weights.indexedTableCount()).isEqualTo(2);
        assertThat(CellWeights.empty().indexedTableCount()).isZero();
    }

    @Test
    void aWriteMutationNamingNoColumnStillCostsOneCell() {
        assertThat(CellWeights.empty().weigh(Mutation.newInsertBuilder("Orders").build()))
                .isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void hasAQueryForEveryDialectTheClientLibraryKnows(Dialect dialect) {
        // A dialect added to the client library must fail here rather than silently read nothing,
        // which would undercount every mutation of every table.
        assertThat(CellWeights.query(dialect)).isNotBlank();
    }

    @Test
    void readsAllGoogleSqlUserSchemasAndSkipsThePrimaryKey() {
        String sql = CellWeights.query(Dialect.GOOGLE_STANDARD_SQL);

        assertThat(sql)
                .contains("INFORMATION_SCHEMA.INDEX_COLUMNS")
                .contains("TABLE_CATALOG = ''")
                .contains("TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')")
                .doesNotContain("TABLE_SCHEMA = ''")
                .contains("INDEX_NAME != 'PRIMARY_KEY'");
    }

    @Test
    void readsAllPostgresUserSchemasAndSkipsThePrimaryKey() {
        String sql = CellWeights.query(Dialect.POSTGRESQL);

        assertThat(sql)
                .contains("information_schema.index_columns")
                .contains(
                        "table_schema NOT IN"
                                + " ('pg_catalog', 'information_schema', 'spanner_sys')")
                .doesNotContain("table_schema = 'public'")
                .contains("index_name != 'PRIMARY_KEY'");
    }

    @Test
    void buildsWeightsFromTheRowsTheQueryReturns() {
        ResultSet resultSet =
                rows(
                        row("", "Orders", "Total", "OrdersByTotal"),
                        row("", "Orders", "Note", "OrdersByTotalAndNote"),
                        row("", "Orders", "Total", "OrdersByTotalAndNote"));

        CellWeights weights = CellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.knows("Orders")).isTrue();
        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(3);
    }

    @Test
    void buildsIndependentWeightsForEveryReturnedSchema() {
        ResultSet resultSet =
                rows(
                        row("sales", "Orders", "Total", "OrdersByTotal"),
                        row("archive", "Orders", "Note", "OrdersByNote"));

        CellWeights weights = CellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("sales.Orders")
                                        .set("Total")
                                        .to(1L)
                                        .build()))
                .isEqualTo(2);
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("archive.Orders")
                                        .set("Note")
                                        .to("kept")
                                        .build()))
                .isEqualTo(2);
    }

    @Test
    void skipsARowWithAMissingName() {
        // Nothing in either dialect's view is documented to be null here, so the guard is defence
        // in depth — but reading a null as a name would throw and take the whole job's startup
        // with it.
        ResultSet resultSet =
                rows(
                        row("", "Orders", null, "OrdersByTotal"),
                        row("", null, "Total", "OrdersByTotal"),
                        row(null, "Orders", "Total", "OrdersByTotal"),
                        row("", "Orders", "Total", null),
                        row("", "Orders", "Total", "OrdersByTotal"));

        CellWeights weights = CellWeights.read(resultSet, Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(2);
    }

    @Test
    void anEmptyResultMeansNoIndexesAtAll() {
        CellWeights weights = CellWeights.read(rows(), Dialect.GOOGLE_STANDARD_SQL);

        assertThat(weights.indexedTableCount()).isZero();
        assertThat(weights.weigh(Mutation.newInsertBuilder("Orders").set("Total").to(1L).build()))
                .isEqualTo(1);
    }

    private static ResultSet rows(Struct... structs) {
        List<Struct> list = structs.length == 0 ? Collections.emptyList() : Arrays.asList(structs);
        return ResultSets.forRows(ROW_TYPE, list);
    }

    private static Struct row(String schema, String table, String column, String index) {
        return Struct.newBuilder()
                .set("table_schema")
                .to(schema)
                .set("table_name")
                .to(table)
                .set("column_name")
                .to(column)
                .set("index_name")
                .to(index)
                .build();
    }
}
