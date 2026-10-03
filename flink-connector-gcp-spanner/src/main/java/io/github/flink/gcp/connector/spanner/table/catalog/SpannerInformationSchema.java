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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.annotation.Internal;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;

/**
 * The {@code INFORMATION_SCHEMA} statements the catalog runs, one per dialect behind a {@code
 * switch}, as {@code SpannerTableReadResolver.metadataQuery} does: the two dialects differ in
 * column-name case, parameter syntax and name comparison, so a shared template would hide which
 * spelling each one sends.
 *
 * <p>Both list base tables only: views, which the table source cannot read through its read API,
 * and the system schemas are left out. A GoogleSQL name is compared case-insensitively and a
 * PostgreSQL one exactly, the rule {@code SpannerTableName.catalogKey} also applies.
 */
@Internal
final class SpannerInformationSchema {

    private SpannerInformationSchema() {}

    /** Lists the base tables outside the system schemas, as schema and table name. */
    static Statement listTables(Dialect dialect) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return Statement.of(
                        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
                                + " WHERE TABLE_CATALOG = '' AND TABLE_TYPE = 'BASE TABLE'"
                                + " AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
                                + " ORDER BY TABLE_SCHEMA, TABLE_NAME");
            case POSTGRESQL:
                return Statement.of(
                        "SELECT table_schema, table_name FROM information_schema.tables"
                                + " WHERE table_type = 'BASE TABLE'"
                                + " AND table_schema NOT IN"
                                + " ('information_schema', 'pg_catalog', 'spanner_sys')"
                                + " ORDER BY table_schema, table_name");
            default:
                throw unsupported(dialect);
        }
    }

    /**
     * Reads one base table's columns in ordinal order: the table's own schema and name spelling,
     * each column's name, {@code SPANNER_TYPE}, nullability, whether Spanner generates it, whether
     * it is hidden, its position in the primary key, and whether a generated column is stored.
     */
    static Statement columnsOf(Dialect dialect, String schema, String table) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return Statement.newBuilder(
                                "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, c.COLUMN_NAME,"
                                        + " c.SPANNER_TYPE, c.IS_NULLABLE, c.IS_GENERATED,"
                                        + " c.IS_HIDDEN, k.ORDINAL_POSITION, c.IS_STORED"
                                        + " FROM INFORMATION_SCHEMA.TABLES AS t"
                                        + " JOIN INFORMATION_SCHEMA.COLUMNS AS c"
                                        + " ON c.TABLE_CATALOG = t.TABLE_CATALOG"
                                        + " AND c.TABLE_SCHEMA = t.TABLE_SCHEMA"
                                        + " AND c.TABLE_NAME = t.TABLE_NAME"
                                        + " LEFT JOIN INFORMATION_SCHEMA.INDEX_COLUMNS AS k"
                                        + " ON k.TABLE_CATALOG = c.TABLE_CATALOG"
                                        + " AND k.TABLE_SCHEMA = c.TABLE_SCHEMA"
                                        + " AND k.TABLE_NAME = c.TABLE_NAME"
                                        + " AND k.COLUMN_NAME = c.COLUMN_NAME"
                                        + " AND k.INDEX_NAME = 'PRIMARY_KEY'"
                                        + " AND k.INDEX_TYPE = 'PRIMARY_KEY'"
                                        + " WHERE t.TABLE_CATALOG = ''"
                                        + " AND t.TABLE_TYPE = 'BASE TABLE'"
                                        + " AND LOWER(t.TABLE_SCHEMA) = LOWER(@schema_name)"
                                        + " AND LOWER(t.TABLE_NAME) = LOWER(@table_name)"
                                        + " ORDER BY c.ORDINAL_POSITION")
                        .bind("schema_name")
                        .to(schema)
                        .bind("table_name")
                        .to(table)
                        .build();
            case POSTGRESQL:
                return Statement.newBuilder(
                                "SELECT t.table_schema, t.table_name, c.column_name,"
                                        + " c.spanner_type, c.is_nullable, c.is_generated,"
                                        + " c.is_hidden, k.ordinal_position, c.is_stored"
                                        + " FROM information_schema.tables AS t"
                                        + " JOIN information_schema.columns AS c"
                                        + " ON c.table_catalog = t.table_catalog"
                                        + " AND c.table_schema = t.table_schema"
                                        + " AND c.table_name = t.table_name"
                                        + " LEFT JOIN information_schema.index_columns AS k"
                                        + " ON k.table_catalog = c.table_catalog"
                                        + " AND k.table_schema = c.table_schema"
                                        + " AND k.table_name = c.table_name"
                                        + " AND k.column_name = c.column_name"
                                        + " AND k.index_name = 'PRIMARY_KEY'"
                                        + " AND k.index_type = 'PRIMARY_KEY'"
                                        + " WHERE t.table_type = 'BASE TABLE'"
                                        + " AND t.table_schema = $1 AND t.table_name = $2"
                                        + " ORDER BY c.ordinal_position")
                        .bind("p1")
                        .to(schema)
                        .bind("p2")
                        .to(table)
                        .build();
            default:
                throw unsupported(dialect);
        }
    }

    private static IllegalStateException unsupported(Dialect dialect) {
        return new IllegalStateException(
                "Unsupported Spanner dialect "
                        + dialect
                        + "; the catalog reads GOOGLE_STANDARD_SQL and POSTGRESQL databases only.");
    }
}
