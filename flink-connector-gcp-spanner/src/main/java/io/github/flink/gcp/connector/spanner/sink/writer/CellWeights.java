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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.spanner.Mutation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * How many cells a mutation costs against Spanner's per-request mutation limit.
 *
 * <p>Spanner counts an insert or update "with the multiplicity of the number of columns they
 * affect", a delete as one "regardless of the number of columns affected", and — in both cases —
 * every secondary-index entry the write changes, individually. So a column costs one cell for the
 * table plus one for each index containing it, and the index part is what {@link
 * InformationSchemaCellWeights} reads out of the database.
 *
 * <p>Names are matched case-insensitively. Spanner will not let two tables (or two columns of one
 * table) differ only in case, so folding costs nothing and it stops a serializer that spells a
 * table {@code orders} while the schema says {@code Orders} from silently losing its index weights.
 *
 * <p>A table the weights do not know — one created after the writer opened, or living in a named
 * schema rather than the default one — is counted <em>without</em> its index entries, which
 * undercounts. That is what the default {@code maxBatchCells} headroom is for; see {@link
 * io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions}.
 *
 * <p>Instances are immutable.
 */
@Internal
public final class CellWeights {

    private static final CellWeights EMPTY = new CellWeights(new HashMap<>(), new HashMap<>());

    /** Lower-cased table name to lower-cased column name to the number of indexes containing it. */
    private final Map<String, Map<String, Integer>> indexesPerColumn;

    /** Lower-cased table name to the number of secondary indexes on it. */
    private final Map<String, Integer> indexesPerTable;

    private CellWeights(
            Map<String, Map<String, Integer>> indexesPerColumn,
            Map<String, Integer> indexesPerTable) {
        this.indexesPerColumn = indexesPerColumn;
        this.indexesPerTable = indexesPerTable;
    }

    /**
     * Returns weights that know no index at all, so every column costs exactly one cell.
     *
     * @return the empty weights
     */
    public static CellWeights empty() {
        return EMPTY;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Returns how many cells the mutation costs.
     *
     * @param mutation the mutation
     * @return the cell count, at least one
     */
    int weigh(Mutation mutation) {
        String table = fold(mutation.getTable());
        if (mutation.getOperation() == Mutation.Op.DELETE) {
            // One for the row, plus one index entry per index on the table. A delete over a key
            // *range* costs that much per row it matches, which nothing on this side can know —
            // the estimate is deliberately the single-row one.
            return 1 + indexesPerTable.getOrDefault(table, 0);
        }
        Map<String, Integer> columns = indexesPerColumn.get(table);
        int cells = 0;
        for (String column : mutation.getColumns()) {
            cells += 1;
            if (columns != null) {
                cells += columns.getOrDefault(fold(column), 0);
            }
        }
        // A write mutation always names its primary key, so this is only reachable through a
        // hand-built mutation with no columns at all; one keeps a batch's cell count moving.
        return Math.max(cells, 1);
    }

    /** Returns whether the weights carry index information for the table. */
    @VisibleForTesting
    boolean knows(String table) {
        return indexesPerTable.containsKey(fold(table));
    }

    /** Returns how many indexed tables the weights carry, for the writer's open-time log line. */
    int indexedTableCount() {
        return indexesPerTable.size();
    }

    private static String fold(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "CellWeights{tables=" + indexesPerTable.size() + "}";
    }

    /** Builder for {@link CellWeights}, fed one index-column row at a time. */
    @Internal
    static final class Builder {

        private final Map<String, Map<String, Set<String>>> rows = new HashMap<>();
        private final Map<String, Set<String>> tableIndexes = new HashMap<>();

        private Builder() {}

        /**
         * Records that {@code indexName} covers {@code column} of {@code table} — either as a key
         * column of the index or as a stored one, since Spanner rewrites the index entry for both.
         *
         * @param table the table
         * @param column the column
         * @param indexName the secondary index
         * @return this builder
         */
        Builder indexColumn(String table, String column, String indexName) {
            String foldedTable = fold(table);
            rows.computeIfAbsent(foldedTable, t -> new HashMap<>())
                    .computeIfAbsent(fold(column), c -> new HashSet<>())
                    .add(indexName);
            tableIndexes.computeIfAbsent(foldedTable, t -> new HashSet<>()).add(indexName);
            return this;
        }

        /**
         * Builds the weights.
         *
         * @return the weights
         */
        CellWeights build() {
            Map<String, Map<String, Integer>> indexesPerColumn = new HashMap<>();
            rows.forEach(
                    (table, columns) -> {
                        Map<String, Integer> counts = new HashMap<>();
                        columns.forEach((column, indexes) -> counts.put(column, indexes.size()));
                        indexesPerColumn.put(table, counts);
                    });
            Map<String, Integer> indexesPerTable = new HashMap<>();
            tableIndexes.forEach((table, indexes) -> indexesPerTable.put(table, indexes.size()));
            return new CellWeights(indexesPerColumn, indexesPerTable);
        }
    }
}
