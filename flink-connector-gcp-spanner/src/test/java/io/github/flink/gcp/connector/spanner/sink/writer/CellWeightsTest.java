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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CellWeights}. */
class CellWeightsTest {

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
}
