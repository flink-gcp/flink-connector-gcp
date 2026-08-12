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

package io.github.flink.gcp.connector.spanner.table.source;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolver;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Resolves a filtered table read from the access path's live key metadata. */
final class SpannerTableReadResolver implements SpannerReadOperationResolver {
    private static final long serialVersionUID = 1L;

    private static final String PRIMARY_KEY = "PRIMARY_KEY";

    private final SpannerTableSchemaConverter schema;
    private final String table;
    @Nullable private final String index;
    private final List<String> projectedColumns;
    private final boolean zeroColumnProjection;
    private final Dialect dialect;
    private final SpannerFilterPushDown.RuntimeState filters;

    SpannerTableReadResolver(
            SpannerTableSchemaConverter schema,
            String table,
            @Nullable String index,
            List<String> projectedColumns,
            boolean zeroColumnProjection,
            Dialect dialect,
            SpannerFilterPushDown.RuntimeState filters) {
        this.schema = schema;
        this.table = table;
        this.index = index;
        this.projectedColumns = Collections.unmodifiableList(new ArrayList<>(projectedColumns));
        this.zeroColumnProjection = zeroColumnProjection;
        this.dialect = dialect;
        this.filters = filters;
    }

    @Override
    public SpannerReadOperation resolve(DatabaseClient client, Timestamp readTimestamp)
            throws IOException {
        Statement statement = metadataQuery(dialect, table, index == null ? PRIMARY_KEY : index);
        try (ReadContext context = client.singleUse(TimestampBound.ofReadTimestamp(readTimestamp));
                ResultSet rows = context.executeQuery(statement)) {
            return resolve(IndexMetadata.read(rows, dialect));
        } catch (SpannerException e) {
            throw new IOException(
                    "Failed to read Spanner index metadata for "
                            + accessPath()
                            + " on table '"
                            + table
                            + "'.",
                    e);
        }
    }

    SpannerReadOperation resolve(IndexMetadata metadata) throws IOException {
        if (!metadata.exists()) {
            throw invalid("was not found in the default schema or is not visible to this job");
        }
        if (index != null && !"READ_WRITE".equals(metadata.indexState)) {
            throw invalid(
                    "is not ready for reads; INFORMATION_SCHEMA reported state "
                            + metadata.indexState);
        }

        List<SpannerFilterPushDown.KeyPart> keyParts = new ArrayList<>();
        for (IndexColumn column : metadata.keyColumns) {
            int physicalIndex = physicalIndex(column.name);
            keyParts.add(
                    new SpannerFilterPushDown.KeyPart(
                            column.name,
                            physicalIndex,
                            "DESC".equals(column.ordering),
                            column.nullable));
        }
        if (index == null) {
            validateDeclaredPrimaryKey(keyParts);
        }
        if (metadata.nullFiltered) {
            for (SpannerFilterPushDown.KeyPart keyPart : keyParts) {
                if (keyPart.isNullable()
                        && (keyPart.physicalIndex() < 0
                                || !filters.provesNonNull(keyPart.physicalIndex()))) {
                    throw invalid(
                            "omits rows where nullable key column '"
                                    + keyPart.name()
                                    + "' is NULL, but the pushed filters do not exclude them");
                }
            }
        }

        List<String> columns = projectedColumns;
        if (index != null) {
            Set<String> readableColumns = new java.util.LinkedHashSet<>(metadata.readableColumns);
            for (int primaryKeyIndex : schema.getPrimaryKeyIndexes()) {
                readableColumns.add(schema.getColumns().get(primaryKeyIndex).getName());
            }
            Set<String> unreadable = new HashSet<>(columns);
            unreadable.removeAll(readableColumns);
            if (!unreadable.isEmpty()) {
                throw invalid(
                        "cannot return columns "
                                + unreadable
                                + "; the read API can use only index keys, base primary-key"
                                + " columns, and STORING/INCLUDE columns");
            }
            if (zeroColumnProjection) {
                if (readableColumns.isEmpty()) {
                    throw invalid("exposes no readable carrier column");
                }
                columns = Collections.singletonList(readableColumns.iterator().next());
            }
        } else if (zeroColumnProjection) {
            columns = Collections.singletonList(schema.getColumns().get(0).getName());
        }

        KeySet keys = filters.keySet(keyParts);
        if (keys == null) {
            keys = KeySet.all();
        }
        return index == null
                ? SpannerReadOperation.read(table, keys, columns)
                : SpannerReadOperation.readUsingIndex(table, index, keys, columns);
    }

    private void validateDeclaredPrimaryKey(List<SpannerFilterPushDown.KeyPart> live)
            throws IOException {
        int[] declaredIndexes = schema.getPrimaryKeyIndexes();
        if (live.size() != declaredIndexes.length) {
            throw invalid(
                    "does not match the declared PRIMARY KEY; the live key has "
                            + live.size()
                            + " columns while the DDL declares "
                            + declaredIndexes.length);
        }
        for (int position = 0; position < declaredIndexes.length; position++) {
            String declared = schema.getColumns().get(declaredIndexes[position]).getName();
            if (!declared.equals(live.get(position).name())) {
                throw invalid(
                        "does not match the declared PRIMARY KEY at position "
                                + (position + 1)
                                + ": the live column is '"
                                + live.get(position).name()
                                + "' but the DDL declares '"
                                + declared
                                + "'");
            }
        }
    }

    private int physicalIndex(String name) {
        for (SpannerTableSchemaConverter.Column column : schema.getColumns()) {
            if (column.getName().equals(name)) {
                return column.getIndex();
            }
        }
        return -1;
    }

    private IOException invalid(String reason) {
        return new IOException(
                "Spanner access path "
                        + accessPath()
                        + " on table '"
                        + table
                        + "' "
                        + reason
                        + ".");
    }

    private String accessPath() {
        return index == null ? "PRIMARY_KEY" : "index '" + index + "'";
    }

    static Statement metadataQuery(Dialect dialect, String table, String index) {
        String sql;
        String tableParameter;
        String indexParameter;
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                sql =
                        "SELECT i.INDEX_STATE, i.IS_NULL_FILTERED, c.COLUMN_NAME,"
                                + " c.ORDINAL_POSITION, c.COLUMN_ORDERING, c.IS_NULLABLE"
                                + " FROM INFORMATION_SCHEMA.INDEXES AS i"
                                + " JOIN INFORMATION_SCHEMA.INDEX_COLUMNS AS c"
                                + " USING (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, INDEX_NAME)"
                                + " WHERE i.TABLE_CATALOG = '' AND i.TABLE_SCHEMA = ''"
                                + " AND i.TABLE_NAME = @table_name"
                                + " AND i.INDEX_NAME = @index_name"
                                + " ORDER BY c.ORDINAL_POSITION, c.COLUMN_NAME";
                tableParameter = "table_name";
                indexParameter = "index_name";
                break;
            case POSTGRESQL:
                sql =
                        "SELECT i.index_state, i.is_null_filtered, c.column_name,"
                                + " c.ordinal_position, c.column_ordering, c.is_nullable"
                                + " FROM information_schema.indexes AS i"
                                + " JOIN information_schema.index_columns AS c"
                                + " USING (table_catalog, table_schema, table_name, index_name)"
                                + " WHERE i.table_schema = 'public'"
                                + " AND i.table_name = $1 AND i.index_name = $2"
                                + " ORDER BY c.ordinal_position, c.column_name";
                tableParameter = "p1";
                indexParameter = "p2";
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported Spanner dialect "
                                + dialect
                                + "; index metadata supports GOOGLE_STANDARD_SQL and POSTGRESQL"
                                + " only.");
        }
        return Statement.newBuilder(sql)
                .bind(tableParameter)
                .to(table)
                .bind(indexParameter)
                .to(index)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpannerTableReadResolver that = (SpannerTableReadResolver) o;
        return zeroColumnProjection == that.zeroColumnProjection
                && schema.equals(that.schema)
                && table.equals(that.table)
                && Objects.equals(index, that.index)
                && projectedColumns.equals(that.projectedColumns)
                && dialect == that.dialect
                && filters.equals(that.filters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema, table, index, projectedColumns, zeroColumnProjection, dialect, filters);
    }

    @Override
    public String toString() {
        return accessPath() + " of table " + table;
    }

    static final class IndexMetadata {
        @Nullable private final String indexState;
        private final boolean nullFiltered;
        private final List<IndexColumn> keyColumns;
        private final Set<String> readableColumns;

        private IndexMetadata(
                @Nullable String indexState,
                boolean nullFiltered,
                List<IndexColumn> keyColumns,
                Set<String> readableColumns) {
            this.indexState = indexState;
            this.nullFiltered = nullFiltered;
            this.keyColumns = Collections.unmodifiableList(new ArrayList<>(keyColumns));
            this.readableColumns =
                    Collections.unmodifiableSet(new java.util.LinkedHashSet<>(readableColumns));
        }

        static IndexMetadata read(ResultSet rows, Dialect dialect) {
            @Nullable String state = null;
            boolean nullFiltered = false;
            List<IndexColumn> keys = new ArrayList<>();
            Set<String> readable = new java.util.LinkedHashSet<>();
            while (rows.next()) {
                if (!rows.isNull(0)) {
                    state = rows.getString(0);
                }
                if (!rows.isNull(1)) {
                    nullFiltered =
                            dialect == Dialect.GOOGLE_STANDARD_SQL
                                    ? rows.getBoolean(1)
                                    : "YES".equals(rows.getString(1));
                }
                if (rows.isNull(2)) {
                    continue;
                }
                String name = rows.getString(2);
                readable.add(name);
                if (!rows.isNull(3) && !rows.isNull(4)) {
                    keys.add(
                            new IndexColumn(
                                    name,
                                    rows.getLong(3),
                                    rows.getString(4),
                                    !rows.isNull(5) && "YES".equals(rows.getString(5))));
                }
            }
            keys.sort(Comparator.comparingLong(column -> column.ordinal));
            return new IndexMetadata(state, nullFiltered, keys, readable);
        }

        static IndexMetadata of(
                @Nullable String indexState,
                boolean nullFiltered,
                List<IndexColumn> keyColumns,
                Set<String> readableColumns) {
            return new IndexMetadata(indexState, nullFiltered, keyColumns, readableColumns);
        }

        private boolean exists() {
            return !keyColumns.isEmpty() || !readableColumns.isEmpty();
        }
    }

    static final class IndexColumn {
        private final String name;
        private final long ordinal;
        private final String ordering;
        private final boolean nullable;

        IndexColumn(String name, long ordinal, String ordering, boolean nullable) {
            this.name = name;
            this.ordinal = ordinal;
            this.ordering = ordering;
            this.nullable = nullable;
        }
    }
}
