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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.SelectedCellTableSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the combinations the builder made expressible.
 *
 * <p>The two positional constructors this replaced could not state a half-configured selected-cell
 * source at all — the envelope one had no selected-cell parameters and the selected-cell one
 * required all of them — so the checks below guard configurations that only became possible when
 * the two lists became one builder. The factory's own tests cover what the DDL can produce.
 */
class BigtableChangeStreamDynamicSourceTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "i", "t");
    private static final DataType PHYSICAL_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("row_id", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("score", DataTypes.INT()));
    private static final SelectedCellTableSchema SELECTED_CELL_SCHEMA =
            SelectedCellTableSchema.of(PHYSICAL_DATA_TYPE, new int[] {0});

    @Test
    void anEnvelopeSourceRejectsASelectedCellValueLeftOnTheBuilder() {
        BigtableChangeStreamDynamicSource.Builder builder = envelope().selectedCellFamily("state");

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selectedCellSchema");
    }

    @Test
    void aSelectedCellSourceRejectsAPhysicalDataTypeTheSchemaAlreadySupplies() {
        BigtableChangeStreamDynamicSource.Builder builder =
                selectedCell().physicalDataType(PHYSICAL_DATA_TYPE);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("physicalDataType must not be set");
    }

    @Test
    void aSelectedCellSourceRequiresTheRestOfItsGroup() {
        assertThatThrownBy(() -> selectedCell().build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("decodingFormat must not be null");
    }

    @Test
    void anEnvelopeSourceIsInsertOnlyAndCopiesEqual() {
        BigtableChangeStreamDynamicSource source = envelope().build();

        assertThat(source.getChangelogMode().getContainedKinds())
                .containsExactly(org.apache.flink.types.RowKind.INSERT);
        assertThat(source.copy()).isEqualTo(source).hasSameHashCodeAs(source);
    }

    private static BigtableChangeStreamDynamicSource.Builder envelope() {
        return BigtableChangeStreamDynamicSource.builder()
                .destination(DESTINATION)
                .appProfileId("single-cluster-profile")
                .physicalDataType(PHYSICAL_DATA_TYPE);
    }

    private static BigtableChangeStreamDynamicSource.Builder selectedCell() {
        return BigtableChangeStreamDynamicSource.builder()
                .destination(DESTINATION)
                .appProfileId("single-cluster-profile")
                .selectedCellSchema(SELECTED_CELL_SCHEMA);
    }
}
