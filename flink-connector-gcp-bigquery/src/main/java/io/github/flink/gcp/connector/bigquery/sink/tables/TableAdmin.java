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

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
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
     * <p>Losing that race the other way — the service rate-limiting the creation rather than
     * reporting the table as already there — is reported as a {@link RetriableTableAdminException}.
     * Every other {@link IOException} is terminal. An implementation that cannot tell the two apart
     * must report the terminal one: a repeat costs a budget, and repeating what will never succeed
     * spends it before failing.
     *
     * <p>Nothing that <em>calls</em> this method repeats it. {@link RetryingTableAdmin} does, and
     * every construction site wraps in one, so a caller sees a creation that either succeeded or
     * exhausted a budget it never had to name.
     *
     * @param destination the table to create
     * @param schema the table schema, in Storage API form
     * @param options partitioning and clustering options
     * @throws RetriableTableAdminException if the creation failed in a way repeating it can fix
     * @throws IOException if the table cannot be created
     */
    void create(TableDestination destination, TableSchema schema, TableCreateOptions options)
            throws IOException;

    /**
     * Ensures a CDC destination satisfies its configured table contract.
     *
     * <p>The operation is idempotent across retries and parallel subtasks. It creates a missing
     * table through the Tables API only when the create disposition permits it, completes any
     * matching partial attempt, and verifies or reconciles an existing table according to policy.
     *
     * @param destination the CDC destination
     * @param schema the physical table schema, without CDC pseudocolumns
     * @param createOptionsProvider options resolved only when a missing table is created
     * @param cdcOptions desired primary key and maximum-staleness contract
     * @param createDisposition whether a missing table may be created
     * @param reconciliationPolicy how an existing table is handled
     * @return whether this call found the table absent and requested its creation
     * @throws RetriableTableAdminException if repeating the operation can complete it
     * @throws TableAdminException if provisioning fails after creation was requested, or retry
     *     processing fails; {@link TableAdminException#wasCreationRequested()} retains the outcome
     * @throws IOException if the destination cannot satisfy the CDC table contract
     */
    boolean ensureCdcTable(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider createOptionsProvider,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy reconciliationPolicy)
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
