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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.SchemaUnifier;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Coordinates table-schema reconciliation for the two Storage Write API writers.
 *
 * <p>The schema union policy itself lives in {@link SchemaUnifier} and is shared with {@code
 * FILE_LOADS}. Load jobs retain their own orchestration because they must carry the reconciled
 * schema and apply behavior specific to each write disposition.
 */
@Internal
final class StorageWriteSchemaReconciler<T> {

    private static final Logger LOG = LoggerFactory.getLogger(StorageWriteSchemaReconciler.class);

    /**
     * Retry schedule for appends while a table schema update propagates to the Storage Write API
     * backend. Flat 30-second waits, jittered by 25%, allow roughly fifteen minutes.
     */
    static final RetrySchedule DEFAULT_SCHEMA_WAIT_SCHEDULE =
            new RetrySchedule(30_000, 30_000, 30, RetrySchedule.DEFAULT_JITTER_RATIO);

    /** Fresh read/union/update attempts after losing an etag-conditioned update race. */
    static final int MAX_UPDATE_ATTEMPTS = 5;

    /** Maximum jitter before retrying a lost concurrent update race. */
    private static final long MAX_UPDATE_JITTER_MS = 500;

    enum Outcome {
        UNCHANGED,
        UPDATED,
        CREATED
    }

    private final BigQuerySinkConfig<T> config;
    private final TableAdmin tableAdmin;

    StorageWriteSchemaReconciler(BigQuerySinkConfig<T> config, TableAdmin tableAdmin) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.tableAdmin = Preconditions.checkNotNull(tableAdmin, "tableAdmin must not be null");
    }

    /**
     * Reconciles one live table against the serializer schema using an additive, convergent union.
     * Each lost update race re-reads the live schema before trying again.
     */
    Outcome reconcile(TableDestination destination) throws IOException {
        TableSchema desired = config.getSerializer().getTableSchema(destination);
        for (int attempt = 1; attempt <= MAX_UPDATE_ATTEMPTS; attempt++) {
            TableSchemaSnapshot live = tableAdmin.getSchema(destination);
            if (live == null) {
                if (config.getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED) {
                    throw new IOException(
                            "Cannot update the schema of BigQuery table "
                                    + destination
                                    + " because the table does not exist and createDisposition"
                                    + " is CREATE_NEVER");
                }
                LOG.info(
                        "The table behind {} does not exist, creating it instead of updating its"
                                + " schema (CREATE_IF_NEEDED)",
                        destination);
                tableAdmin.create(
                        destination,
                        desired,
                        config.getTableCreateOptionsProvider().optionsFor(destination));
                return Outcome.CREATED;
            }
            SchemaUnifier.UnionResult union =
                    SchemaUnifier.union(live.getSchema(), desired, config.getSchemaUpdateOptions());
            if (!union.isChanged()) {
                return Outcome.UNCHANGED;
            }
            if (tableAdmin.updateSchema(destination, live, union.getSchema())) {
                LOG.info("Updated the schema of {} to cover the serializer schema", destination);
                return Outcome.UPDATED;
            }
            sleepJitter();
        }
        throw new IOException(
                "Failed to update the schema of BigQuery table "
                        + destination
                        + ": lost a concurrent-update race "
                        + MAX_UPDATE_ATTEMPTS
                        + " times");
    }

    private static void sleepJitter() throws IOException {
        Retries.sleep(
                ThreadLocalRandom.current().nextLong(MAX_UPDATE_JITTER_MS + 1),
                "Interrupted while waiting to retry a BigQuery table schema update");
    }
}
