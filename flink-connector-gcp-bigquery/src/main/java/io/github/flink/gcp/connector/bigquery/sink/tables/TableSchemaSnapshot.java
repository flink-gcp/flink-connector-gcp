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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import javax.annotation.Nullable;

/**
 * A point-in-time read of a destination table's schema, as returned by {@link
 * TableAdmin#getSchema}.
 *
 * <p>Besides the schema in Storage API form, the snapshot carries the REST {@code Table} it was
 * read from ({@link #getTable()}, {@code null} in test fakes), which {@link
 * TableAdmin#updateSchema} uses both to condition the update on the table not having changed since
 * this read (the table's etag, sent as an {@code If-Match} precondition, makes the update
 * optimistic-concurrency safe) and to preserve REST-only column attributes the Storage API form
 * cannot represent. Only the schema is written back; the table's other attributes are read here but
 * never submitted.
 *
 * <p>It also carries the table's {@link TableLayout}, read from the same table, which {@code
 * FILE_LOADS} loads its temporary tables with so they can be copied into it, and the Cloud KMS key
 * that encrypts the table, which {@code FILE_LOADS} encrypts its temporary tables and every job
 * that replaces a table with.
 */
@Internal
public final class TableSchemaSnapshot {

    private final TableSchema schema;
    private final Table table;
    private final TableLayout layout;
    @Nullable private final String kmsKeyName;

    private TableSchemaSnapshot(
            TableSchema schema, Table table, TableLayout layout, @Nullable String kmsKeyName) {
        this.schema = schema;
        this.table = table;
        this.layout = layout;
        this.kmsKeyName = kmsKeyName;
    }

    /**
     * Creates a snapshot whose layout and key are read from {@code table}, or are {@link
     * TableLayout#NONE} and none without one.
     *
     * @param schema the schema in Storage API form
     * @param table the REST table the schema was read from, or {@code null}
     * @return the snapshot
     */
    public static TableSchemaSnapshot of(TableSchema schema, Table table) {
        return table == null
                ? of(schema, null, TableLayout.NONE, null)
                : of(schema, table, TableLayout.of(table.getDefinition()), kmsKeyNameOf(table));
    }

    /**
     * Creates a snapshot with an explicit layout and key, for a test double that has no REST table
     * to read them from, or that sets them apart from the table it carries.
     *
     * @param schema the schema in Storage API form
     * @param table the REST table the schema was read from, or {@code null}
     * @param layout the table's layout
     * @param kmsKeyName the Cloud KMS key that encrypts the table, or {@code null} for none
     * @return the snapshot
     */
    @VisibleForTesting
    public static TableSchemaSnapshot of(
            TableSchema schema, Table table, TableLayout layout, @Nullable String kmsKeyName) {
        return new TableSchemaSnapshot(
                Preconditions.checkNotNull(schema, "schema must not be null"),
                table,
                Preconditions.checkNotNull(layout, "layout must not be null"),
                kmsKeyName);
    }

    @Nullable
    private static String kmsKeyNameOf(Table table) {
        EncryptionConfiguration encryption = table.getEncryptionConfiguration();
        return encryption == null ? null : encryption.getKmsKeyName();
    }

    /** Returns the schema in Storage API form. */
    public TableSchema getSchema() {
        return schema;
    }

    /** Returns the REST table the schema was read from, or {@code null}. */
    public Table getTable() {
        return table;
    }

    /** Returns the table's partitioning and clustering. */
    public TableLayout getLayout() {
        return layout;
    }

    /**
     * Returns the resource name of the Cloud KMS key that encrypts the table, or {@code null} when
     * the table is encrypted with a Google-owned key.
     */
    @Nullable
    public String getKmsKeyName() {
        return kmsKeyName;
    }
}
