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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.TableSchema;

/**
 * A point-in-time read of a destination table's schema, as returned by {@link
 * TableAdmin#getSchema}.
 *
 * <p>Besides the schema in Storage API form, the snapshot carries the REST {@code Table} it was
 * read from ({@link #getTable()}, {@code null} in test fakes), which {@link
 * TableAdmin#updateSchema} uses both to condition the update on the table not having changed since
 * this read (the table's etag makes the update optimistic-concurrency safe) and to preserve
 * REST-only column attributes the Storage API form cannot represent.
 */
@Internal
public final class TableSchemaSnapshot {

    private final TableSchema schema;
    private final Table table;

    private TableSchemaSnapshot(TableSchema schema, Table table) {
        this.schema = schema;
        this.table = table;
    }

    /**
     * Creates a snapshot.
     *
     * @param schema the schema in Storage API form
     * @param table the REST table the schema was read from, or {@code null}
     * @return the snapshot
     */
    public static TableSchemaSnapshot of(TableSchema schema, Table table) {
        return new TableSchemaSnapshot(
                Preconditions.checkNotNull(schema, "schema must not be null"), table);
    }

    /** Returns the schema in Storage API form. */
    public TableSchema getSchema() {
        return schema;
    }

    /** Returns the REST table the schema was read from, or {@code null}. */
    public Table getTable() {
        return table;
    }
}
