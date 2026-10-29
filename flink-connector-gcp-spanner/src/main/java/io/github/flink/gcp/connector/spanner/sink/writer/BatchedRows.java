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

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The rows the writer's current batch addresses, so that {@link
 * io.github.flink.gcp.connector.spanner.sink.OrderingMode#PER_KEY} can send the batch before a
 * mutation of a row it already holds. Task-thread only.
 */
@Internal
final class BatchedRows {

    /** Table key to the rows the batch holds in it. */
    private final Map<String, Set<List<Object>>> rowsByTable = new HashMap<>();

    /** Tables a mutation of the batch touches as a whole. */
    private final Set<String> wholeTables = new HashSet<>();

    /** Whether a mutation of the batch may touch any table. */
    private boolean wholeDatabase;

    /** Returns whether the footprint shares a row with any mutation of the batch. */
    boolean overlaps(PrimaryKeys.Footprint footprint) {
        if (wholeDatabase) {
            return true;
        }
        if (footprint.isWholeDatabase()) {
            return !rowsByTable.isEmpty() || !wholeTables.isEmpty();
        }
        String table = footprint.table();
        if (wholeTables.contains(table)) {
            return true;
        }
        Set<List<Object>> rows = rowsByTable.get(table);
        if (rows == null) {
            return false;
        }
        if (footprint.isWholeTable()) {
            return true;
        }
        for (List<Object> row : footprint.rows()) {
            if (rows.contains(row)) {
                return true;
            }
        }
        return false;
    }

    /** Records a mutation that joined the batch. */
    void add(PrimaryKeys.Footprint footprint) {
        if (footprint.isWholeDatabase()) {
            wholeDatabase = true;
        } else if (footprint.isWholeTable()) {
            wholeTables.add(footprint.table());
        } else {
            rowsByTable
                    .computeIfAbsent(footprint.table(), t -> new HashSet<>())
                    .addAll(footprint.rows());
        }
    }

    /** Forgets the batch, which was sent or dropped. */
    void clear() {
        rowsByTable.clear();
        wholeTables.clear();
        wholeDatabase = false;
    }
}
