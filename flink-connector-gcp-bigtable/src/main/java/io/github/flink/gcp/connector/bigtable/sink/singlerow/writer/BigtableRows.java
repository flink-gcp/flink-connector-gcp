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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the client's {@code Row} into the connector-owned {@link BigtableRow}.
 *
 * <p>The one place a client row is read: the client marks {@code Row} and {@code RowCell} for
 * internal extension only, and this is where a shape change in a client release would surface.
 */
@Internal
public final class BigtableRows {

    private BigtableRows() {}

    /**
     * Copies a client row into the connector-owned type, cells in the client's order.
     *
     * @param row the client's row
     * @return the connector-owned row
     */
    public static BigtableRow fromRow(Row row) {
        List<RowCell> rowCells = row.getCells();
        List<BigtableRow.Cell> cells = new ArrayList<>(rowCells.size());
        for (RowCell cell : rowCells) {
            cells.add(
                    new BigtableRow.Cell(
                            cell.getFamily(),
                            cell.getQualifier(),
                            cell.getTimestamp(),
                            cell.getValue(),
                            cell.getLabels()));
        }
        return new BigtableRow(row.getKey(), cells);
    }
}
