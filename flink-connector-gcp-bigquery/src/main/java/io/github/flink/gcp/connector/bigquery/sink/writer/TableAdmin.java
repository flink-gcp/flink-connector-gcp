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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * Administers destination tables: creation for {@link
 * io.github.flink.gcp.connector.bigquery.sink.CreateDisposition#CREATE_IF_NEEDED} and schema
 * reads/updates for schema evolution.
 *
 * <p>Abstracts the BigQuery REST client so writer logic is unit-testable.
 */
@Internal
public interface TableAdmin {

    /**
     * Creates the given table. Idempotent: creating a table that already exists (for example
     * because a parallel subtask created it first) succeeds silently.
     *
     * @param destination the table to create
     * @param schema the table schema, in Storage API form
     * @param options partitioning and clustering options
     * @throws IOException if the table cannot be created
     */
    void create(TableDestination destination, TableSchema schema, TableCreateOptions options)
            throws IOException;

    /**
     * Reads the live schema of the given table, bypassing any caches.
     *
     * @param destination the table to read
     * @return the schema snapshot, or {@code null} when the table does not exist
     * @throws IOException if the schema cannot be read
     */
    TableSchemaSnapshot getSchema(TableDestination destination) throws IOException;

    /**
     * Replaces the given table's schema with the proposed one, conditioned on the table not having
     * changed since the {@code base} snapshot was taken.
     *
     * <p>Losing a race — the table changed concurrently (for example a parallel subtask updated it
     * first) or the per-table metadata-update quota was momentarily exceeded — returns {@code
     * false} so the caller can re-read and re-derive the proposal; unions of concurrent unions
     * converge. Everything else (permissions, invalid schema changes, ...) throws.
     *
     * @param destination the table to update
     * @param base the snapshot the proposal was derived from
     * @param proposed the proposed schema, in Storage API form
     * @return whether the update was applied
     * @throws IOException if the update fails for a reason retrying cannot fix
     */
    boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed)
            throws IOException;
}
