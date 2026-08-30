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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.tables.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.sink.tables.StorageSchemaConverter;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The live-table half of one {@code FILE_LOADS} commit: reconciles each destination table and
 * returns the schema every load of that commit carries.
 *
 * <p>The {@code FILE_LOADS} counterpart of {@code StorageWriteSchemaReconciler}, which the two
 * Storage Write API writers share. {@code SchemaUnifier} is the common policy across all three;
 * loads need their own reconciler because a load job has to be handed the reconciled schema, which
 * means returning it for the destination plan and varying it by write disposition ({@code
 * docs/adr/0022}).
 *
 * <p><b>Every load consults the live table first</b> ({@code docs/adr/0021}). A missing table is
 * created through the {@link TableAdmin} with the configured partitioning and clustering, and —
 * gated by the sink's schema update options — the live schema is unioned with the serializer's,
 * which demotes a new {@code REQUIRED} column to {@code NULLABLE} because BigQuery cannot add
 * {@code REQUIRED} columns to an existing table. A load job supplying an unreconciled schema would
 * be rejected at submission for exactly that case, and whether a run fits one partition must not
 * decide whether its records load.
 *
 * <p>{@link LoadJobOrchestrator} constructs one for each destination in the commit's reconciliation
 * phase and stores the returned schema in that destination's plan. One commit therefore reconciles
 * each destination once, however many temporary-table loads that destination needs.
 *
 * <p>Each instance is thread-confined. Distinct destinations reconcile concurrently, but calls to
 * the user-provided serialization schema and table-create-options provider are serialized through a
 * commit-scoped lock because those callback APIs do not require thread safety.
 */
@Internal
final class FileLoadsSchemaReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsSchemaReconciler.class);

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final TableAdmin tableAdmin;
    private final Object userCallbackLock;
    private final RetrySchedule schemaReconcileSchedule;

    FileLoadsSchemaReconciler(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            TableAdmin tableAdmin,
            Object userCallbackLock) {
        this.config = config;
        this.options = options;
        this.tableAdmin = tableAdmin;
        this.userCallbackLock = userCallbackLock;
        this.schemaReconcileSchedule = options.toSchemaReconcileSchedule();
    }

    /** Reconciles one destination and returns the schema stored in its commit plan. */
    Schema finalTableSchema(TableDestination destination) throws IOException {
        return ensureFinalTable(destination);
    }

    /**
     * Reconciles the destination table and returns the schema every load of this commit carries —
     * the one decision shared by direct loads and the temp-table path (through {@link
     * #finalTableSchema}), so the same records cannot succeed or fail depending on partition count.
     * Creates a missing table under {@code CREATE_IF_NEEDED} with the configured
     * partitioning/clustering (fails under {@code CREATE_NEVER}), then re-reads it — creation
     * swallows a lost race, so what exists may not be what was asked for. Under {@code
     * WRITE_TRUNCATE} the load or copy replaces the table's schema wholesale, so the serializer's
     * schema is used as-is. Otherwise — appending, replacing only data, or writing into an empty
     * table — the live schema wins: it is returned untouched when schema updates are disabled, and
     * unioned with the serializer's when they are enabled (new {@code REQUIRED} columns arrive
     * {@code NULLABLE}, since BigQuery cannot add {@code REQUIRED} columns), retrying lost update
     * races.
     */
    private Schema ensureFinalTable(TableDestination destination) throws IOException {
        TableSchema desired;
        synchronized (userCallbackLock) {
            desired = config.getTableSchema(destination);
        }
        TableSchemaSnapshot snapshot = tableAdmin.getSchema(destination);
        if (snapshot == null) {
            if (config.getCreateDisposition() == CreateDisposition.CREATE_NEVER) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " does not exist and createDisposition is CREATE_NEVER.");
            }
            TableCreateOptions createOptions;
            synchronized (userCallbackLock) {
                createOptions = config.getTableCreateOptionsProvider().optionsFor(destination);
            }
            tableAdmin.create(destination, desired, createOptions);
            // Creation swallows a lost race (HTTP 409 = someone else created it first), so the
            // table's actual schema may be a concurrent creator's rather than the desired one —
            // re-read and reconcile against what is really there instead of trusting the
            // argument.
            snapshot = tableAdmin.getSchema(destination);
            if (snapshot == null) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " disappeared right after it was created.");
            }
        }
        if (options.getWriteDisposition() == WriteDisposition.WRITE_TRUNCATE) {
            return StorageSchemaConverter.toBigQuerySchema(desired);
        }
        if (!config.getSchemaUpdateOptions().isEnabled()) {
            try {
                SchemaUnifier.union(snapshot.getSchema(), desired, config.getSchemaUpdateOptions());
            } catch (SchemaUnifier.SchemaUnionException e) {
                // The union's message names the difference; the outcome depends on which kind it
                // is. A serializer column the table lacks is silently ignored by the load
                // (measured) — dropped data, not an error — while a type disagreement surfaces
                // when the load runs. Either way, say what wins, once per destination per commit.
                LOG.warn(
                        "Schema updates are disabled, so the live schema of {} wins over the"
                                + " serializer's: {}",
                        destination,
                        e.getMessage());
            }
            return StorageSchemaConverter.toBigQuerySchema(snapshot.getSchema());
        }
        for (int attempt = 1; attempt <= schemaReconcileSchedule.maxAttempts(); attempt++) {
            SchemaUnifier.UnionResult union =
                    SchemaUnifier.union(
                            snapshot.getSchema(), desired, config.getSchemaUpdateOptions());
            if (!union.isChanged()
                    || tableAdmin.updateSchema(destination, snapshot, union.getSchema())) {
                return StorageSchemaConverter.toBigQuerySchema(union.getSchema());
            }
            Retries.sleep(
                    schemaReconcileSchedule.backoffMs(attempt),
                    "Interrupted while reconciling the schema of " + destination);
            snapshot = tableAdmin.getSchema(destination);
            if (snapshot == null) {
                throw new IOException(
                        "Destination table "
                                + destination
                                + " disappeared while reconciling its"
                                + " schema.");
            }
        }
        throw new IOException(
                "Failed to reconcile the schema of "
                        + destination
                        + " after "
                        + schemaReconcileSchedule.maxAttempts()
                        + " attempts (concurrent updates kept winning).");
    }
}
