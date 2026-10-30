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
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import io.github.flink.gcp.connector.spanner.SpannerTableName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * How many cells a mutation costs, counted the way Spanner counts a mutation.
 *
 * <p>Spanner counts an insert or update "with the multiplicity of the number of columns they
 * affect", a delete as one "regardless of the number of columns affected", and — in both cases —
 * every secondary-index entry the write changes, individually. So a column costs one cell for the
 * table plus one for each index containing it, and the index part is what {@link #query(Dialect)}
 * reads out of the database's {@code INFORMATION_SCHEMA} when the writer opens.
 *
 * <p>Names use dialect semantics: GoogleSQL names are matched case-insensitively, while PostgreSQL
 * unquoted names fold to lower case and quoted names preserve case. The schema is part of a table's
 * identity.
 *
 * <p>A table the weights do not know — one created after the writer opened, or living in a named
 * schema that appeared after the writer opened — is counted <em>without</em> its index entries,
 * which undercounts. That is what the default {@code maxBatchCells} headroom is for; see {@link
 * io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions}.
 */
@Internal
public final class CellWeights {

    private static final CellWeights EMPTY =
            new CellWeights(Dialect.GOOGLE_STANDARD_SQL, new HashMap<>(), new HashMap<>());

    private final Dialect dialect;

    /** Qualified table key to column key to the number of indexes containing it. */
    private final Map<String, Map<String, Integer>> indexesPerColumn;

    /** Qualified table key to the number of secondary indexes on it. */
    private final Map<String, Integer> indexesPerTable;

    private CellWeights(
            Dialect dialect,
            Map<String, Map<String, Integer>> indexesPerColumn,
            Map<String, Integer> indexesPerTable) {
        this.dialect = dialect;
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
        return builder(Dialect.GOOGLE_STANDARD_SQL);
    }

    /** Creates a builder whose keys follow the database dialect. */
    static Builder builder(Dialect dialect) {
        return new Builder(dialect);
    }

    /**
     * Returns the query listing (schema, table, column, index) for every visible secondary index.
     *
     * <p>Both dialects expose the same {@code INDEX_COLUMNS} view — a row per (index, column) pair,
     * key columns and {@code STORING} columns alike, which is exactly the set of columns whose
     * write rewrites an index entry. The query includes every visible user schema and retains the
     * schema name so equally named tables remain distinct.
     *
     * <p>The primary-key index is excluded by name rather than by {@code INDEX_TYPE}: both dialects
     * name it {@code PRIMARY_KEY}, that is the filter Apache Beam has shipped against both the
     * service and the emulator for years, and its cells are already counted by the columns
     * themselves.
     *
     * @param dialect the database's dialect
     * @return the SQL to run
     */
    static String query(Dialect dialect) {
        switch (dialect) {
            case POSTGRESQL:
                return "SELECT table_schema, table_name, column_name, index_name"
                        + " FROM information_schema.index_columns"
                        + " WHERE table_schema NOT IN"
                        + " ('pg_catalog', 'information_schema', 'spanner_sys')"
                        + " AND index_name != 'PRIMARY_KEY'";
            case GOOGLE_STANDARD_SQL:
                return "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, INDEX_NAME"
                        + " FROM INFORMATION_SCHEMA.INDEX_COLUMNS"
                        + " WHERE TABLE_CATALOG = ''"
                        + " AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
                        + " AND INDEX_NAME != 'PRIMARY_KEY'";
            default:
                // Unreachable today; a dialect added to the client library lands here rather than
                // silently reading nothing, which would undercount every mutation.
                throw new IllegalStateException(
                        "Unsupported Spanner dialect: "
                                + dialect
                                + ". This connector reads mutation-cell weights for"
                                + " GOOGLE_STANDARD_SQL and POSTGRESQL only.");
        }
    }

    /**
     * Reads the weights from the result of {@link #query(Dialect)}. Does not close the result set.
     *
     * @param resultSet the query result, positioned before the first row
     * @param dialect the database's dialect
     * @return the weights
     */
    static CellWeights read(ResultSet resultSet, Dialect dialect) {
        Builder weights = builder(dialect);
        while (resultSet.next()) {
            // Read positionally: the two dialects' queries select the same four values under
            // different spellings.
            if (resultSet.isNull(0)
                    || resultSet.isNull(1)
                    || resultSet.isNull(2)
                    || resultSet.isNull(3)) {
                continue;
            }
            weights.indexColumn(
                    resultSet.getString(0),
                    resultSet.getString(1),
                    resultSet.getString(2),
                    resultSet.getString(3));
        }
        return weights.build();
    }

    /**
     * Returns how many cells the mutation costs.
     *
     * @param mutation the mutation
     * @return the cell count, at least one
     */
    int weigh(Mutation mutation) {
        String table = tableKey(mutation.getTable());
        if (mutation.getOperation() == Mutation.Op.DELETE) {
            // One for the row, plus one index entry per index on the table. A delete over a key
            // *range* costs the one only once, plus an index entry per index per row it matches —
            // and how many rows that is, nothing on this side can know. So the estimate is
            // deliberately the single-row one: exact on a table with no secondary index, an
            // undercount of one index entry per extra row on a table with them.
            return 1 + indexesPerTable.getOrDefault(table, 0);
        }
        Map<String, Integer> columns = indexesPerColumn.get(table);
        int cells = 0;
        for (String column : mutation.getColumns()) {
            cells += 1;
            if (columns != null) {
                cells +=
                        columns.getOrDefault(
                                SpannerTableName.catalogIdentifierKey(column, dialect), 0);
            }
        }
        // A write mutation always names its primary key, so this is only reachable through a
        // hand-built mutation with no columns at all; one keeps a batch's cell count moving.
        return Math.max(cells, 1);
    }

    /** Returns whether the weights carry index information for the table. */
    @VisibleForTesting
    boolean knows(String table) {
        return indexesPerTable.containsKey(tableKey(table));
    }

    /** Returns how many indexed tables the weights carry, for the writer's open-time log line. */
    int indexedTableCount() {
        return indexesPerTable.size();
    }

    private String tableKey(String table) {
        try {
            return SpannerTableName.nativeApiKey(table, dialect);
        } catch (IllegalArgumentException ignored) {
            String defaultSchema = dialect == Dialect.POSTGRESQL ? "public" : "";
            return SpannerTableName.catalogKey(defaultSchema, table, dialect);
        }
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
        private final Dialect dialect;

        private Builder(Dialect dialect) {
            this.dialect = dialect;
        }

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
            String defaultSchema = dialect == Dialect.POSTGRESQL ? "public" : "";
            return indexColumn(defaultSchema, table, column, indexName);
        }

        /** Records one index-column row including its schema. */
        Builder indexColumn(String schema, String table, String column, String indexName) {
            String tableKey = SpannerTableName.catalogKey(schema, table, dialect);
            String columnKey = SpannerTableName.catalogIdentifierKey(column, dialect);
            String indexKey = SpannerTableName.catalogIdentifierKey(indexName, dialect);
            rows.computeIfAbsent(tableKey, t -> new HashMap<>())
                    .computeIfAbsent(columnKey, c -> new HashSet<>())
                    .add(indexKey);
            tableIndexes.computeIfAbsent(tableKey, t -> new HashSet<>()).add(indexKey);
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
            return new CellWeights(dialect, indexesPerColumn, indexesPerTable);
        }
    }
}
