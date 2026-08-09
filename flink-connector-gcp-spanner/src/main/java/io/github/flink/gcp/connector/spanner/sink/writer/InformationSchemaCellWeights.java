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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;

/**
 * Reads the per-column secondary-index coverage out of a database's {@code INFORMATION_SCHEMA}.
 *
 * <p>Both dialects expose the same {@code INDEX_COLUMNS} view — a row per (index, column) pair, key
 * columns and {@code STORING} columns alike, which is exactly the set of columns whose write
 * rewrites an index entry — but they scope it differently: GoogleSQL puts the default schema at the
 * empty catalog and empty schema, PostgreSQL at schema {@code public}.
 *
 * <p>The primary-key index is excluded by name rather than by {@code INDEX_TYPE}: both dialects
 * name it {@code PRIMARY_KEY}, that is the filter Apache Beam has shipped against both the service
 * and the emulator for years, and its cells are already counted by the columns themselves.
 *
 * <p>What this deliberately does not cover: tables in a named schema. Their rows are filtered out
 * with everything else outside the default schema, so their mutations are weighed without index
 * entries — the undercount the {@code maxBatchCells} headroom absorbs.
 */
@Internal
final class InformationSchemaCellWeights {

    private InformationSchemaCellWeights() {}

    /**
     * Returns the query listing (table, column, index) for every secondary index of the default
     * schema.
     *
     * @param dialect the database's dialect
     * @return the SQL to run
     */
    static String queryFor(Dialect dialect) {
        switch (dialect) {
            case POSTGRESQL:
                return "SELECT table_name, column_name, index_name"
                        + " FROM information_schema.index_columns"
                        + " WHERE table_schema = 'public' AND index_name != 'PRIMARY_KEY'";
            case GOOGLE_STANDARD_SQL:
                return "SELECT TABLE_NAME, COLUMN_NAME, INDEX_NAME"
                        + " FROM INFORMATION_SCHEMA.INDEX_COLUMNS"
                        + " WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = ''"
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
     * Reads the weights from the result of {@link #queryFor}. Does not close the result set.
     *
     * @param resultSet the query result, positioned before the first row
     * @return the weights
     */
    static CellWeights read(ResultSet resultSet) {
        CellWeights.Builder weights = CellWeights.builder();
        while (resultSet.next()) {
            // Read positionally: the two dialects' queries select the same three values under
            // different spellings.
            if (resultSet.isNull(0) || resultSet.isNull(1) || resultSet.isNull(2)) {
                continue;
            }
            weights.indexColumn(
                    resultSet.getString(0), resultSet.getString(1), resultSet.getString(2));
        }
        return weights.build();
    }
}
