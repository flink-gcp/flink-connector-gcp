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
import io.github.flink.gcp.connector.spanner.SpannerObjectName;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The metadata requests the {@code spanner} catalog makes: one instance's databases, and one
 * database's tables, columns and proto types. A seam, so the catalog's naming and mapping rules are
 * tested against a hand-written fake rather than an emulator.
 *
 * <p>Every method may throw a {@code SpannerException}; the catalog turns it into a {@code
 * CatalogException} naming what it asked for.
 */
@Internal
interface SpannerCatalogClient extends AutoCloseable {

    /** The instance's database ids. */
    List<String> listDatabases();

    /** The database's dialect, or {@code null} when the instance has no such database. */
    @Nullable
    Dialect dialect(String database);

    /** The database's base tables outside the system schemas, as native schema and table names. */
    List<SpannerObjectName> listTables(String database, Dialect dialect);

    /**
     * One base table's columns in ordinal order, or {@code null} when no base table matches. A
     * GoogleSQL name matches case-insensitively, as GoogleSQL compares names; a PostgreSQL name
     * exactly.
     */
    @Nullable
    TableMetadata table(String database, Dialect dialect, String schema, String table);

    /**
     * The kind of each PROTO message and ENUM type the database's proto bundle declares, by fully
     * qualified name. Asked only for a table that has such a column.
     */
    Map<String, NamedTypeKind> protoBundleTypes(String database);

    @Override
    void close();

    /** Whether a fully qualified name a column's type gives is a message or an enum. */
    enum NamedTypeKind {
        PROTO,
        ENUM
    }

    /** One base table: its native names and its columns in ordinal order. */
    final class TableMetadata {
        private final SpannerObjectName name;
        private final List<ColumnMetadata> columns;

        TableMetadata(SpannerObjectName name, List<ColumnMetadata> columns) {
            this.name = name;
            this.columns = Collections.unmodifiableList(columns);
        }

        SpannerObjectName name() {
            return name;
        }

        List<ColumnMetadata> columns() {
            return columns;
        }
    }

    /** One {@code INFORMATION_SCHEMA.COLUMNS} row, joined to its primary-key position. */
    final class ColumnMetadata {
        private final String name;
        private final String spannerType;
        private final boolean nullable;
        private final boolean generated;
        private final boolean stored;
        private final boolean hidden;
        @Nullable private final Long keyPosition;

        ColumnMetadata(
                String name,
                String spannerType,
                boolean nullable,
                boolean generated,
                boolean stored,
                boolean hidden,
                @Nullable Long keyPosition) {
            this.name = name;
            this.spannerType = spannerType;
            this.nullable = nullable;
            this.generated = generated;
            this.stored = stored;
            this.hidden = hidden;
            this.keyPosition = keyPosition;
        }

        String name() {
            return name;
        }

        String spannerType() {
            return spannerType;
        }

        boolean nullable() {
            return nullable;
        }

        boolean generated() {
            return generated;
        }

        /**
         * Whether a generated column is stored; meaningless for a column Spanner does not generate.
         */
        boolean stored() {
            return stored;
        }

        boolean hidden() {
            return hidden;
        }

        /** The column's 1-based position in the primary key, or {@code null} for a non-key one. */
        @Nullable
        Long keyPosition() {
            return keyPosition;
        }
    }
}
