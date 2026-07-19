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
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default {@link TableAdmin} backed by the BigQuery REST client.
 *
 * <p>The client is created lazily on the first use, so jobs whose destination tables all exist and
 * never evolve their schemas never construct it. HTTP conflicts on creation (409, the table was
 * created concurrently — for example by a parallel subtask) are treated as success.
 *
 * <p>Schema updates are etag-conditioned: {@link #getSchema} snapshots the REST {@code Table}
 * (which carries the etag), and {@link #updateSchema} submits the modified table so BigQuery
 * rejects the update when the table changed since the snapshot. Lost races — the etag precondition
 * failing, a concurrent-modification conflict, or the per-table metadata-update quota (about five
 * updates per ten seconds) being momentarily exceeded — are reported as {@code false} for the
 * caller to re-read and retry.
 */
@Internal
public class BigQueryTableAdmin implements TableAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableAdmin.class);

    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_PRECONDITION_FAILED = 412;

    /** Error reason of a failed etag precondition. */
    private static final String REASON_CONDITION_NOT_MET = "conditionNotMet";

    /** Error reason of the per-table metadata-update quota. */
    private static final String REASON_RATE_LIMIT_EXCEEDED = "rateLimitExceeded";

    private BigQuery client;

    /** Creates an admin using application-default credentials. */
    public BigQueryTableAdmin() {}

    /**
     * Creates an admin using the given client.
     *
     * @param client the BigQuery REST client
     */
    public BigQueryTableAdmin(BigQuery client) {
        this.client = client;
    }

    @Override
    public void create(TableDestination destination, TableSchema schema, TableCreateOptions options)
            throws IOException {
        TableInfo tableInfo = buildTableInfo(destination, schema, options);
        try {
            client().create(tableInfo);
            LOG.info("Created BigQuery table {} with options {}", destination, options);
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_CONFLICT) {
                LOG.info("BigQuery table {} already exists, not creating it", destination);
                return;
            }
            throw new IOException("Failed to create BigQuery table " + destination, e);
        }
    }

    @Override
    public TableSchemaSnapshot getSchema(TableDestination destination) throws IOException {
        Table table;
        try {
            table = client().getTable(toTableId(destination));
        } catch (BigQueryException e) {
            throw new IOException("Failed to read the schema of BigQuery table " + destination, e);
        }
        if (table == null) {
            return null;
        }
        Schema schema = table.<TableDefinition>getDefinition().getSchema();
        if (schema == null) {
            throw new IOException("BigQuery table " + destination + " has no schema");
        }
        return TableSchemaSnapshot.of(BigQuerySchemaConverter.toStorageSchema(schema), table);
    }

    @Override
    public boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed)
            throws IOException {
        Table baseTable = (Table) base.getRaw();
        StandardTableDefinition definition =
                baseTable.<StandardTableDefinition>getDefinition().toBuilder()
                        .setSchema(StorageSchemaConverter.toBigQuerySchema(proposed))
                        .build();
        try {
            // The table carries the snapshot's etag, so BigQuery rejects the update when the
            // table changed since the snapshot was taken.
            client().update(baseTable.toBuilder().setDefinition(definition).build());
            LOG.info("Updated the schema of BigQuery table {}", destination);
            return true;
        } catch (BigQueryException e) {
            if (isLostRace(e)) {
                LOG.info(
                        "A schema update of BigQuery table {} lost a race and will be retried"
                                + " from a fresh read (cause: {})",
                        destination,
                        e.toString());
                return false;
            }
            throw new IOException(
                    "Failed to update the schema of BigQuery table " + destination, e);
        }
    }

    /**
     * Whether a schema-update failure means the update lost a race (concurrent change or metadata
     * quota) rather than being invalid: an etag-precondition failure, a conflict, or the per-table
     * metadata-update rate limit.
     */
    @VisibleForTesting
    static boolean isLostRace(BigQueryException e) {
        if (e.getCode() == HTTP_CONFLICT || e.getCode() == HTTP_PRECONDITION_FAILED) {
            return true;
        }
        String reason = e.getError() != null ? e.getError().getReason() : e.getReason();
        return REASON_CONDITION_NOT_MET.equals(reason) || REASON_RATE_LIMIT_EXCEEDED.equals(reason);
    }

    private static TableId toTableId(TableDestination destination) {
        return TableId.of(
                destination.getProject(), destination.getDataset(), destination.getTable());
    }

    @VisibleForTesting
    static TableInfo buildTableInfo(
            TableDestination destination, TableSchema schema, TableCreateOptions options) {
        StandardTableDefinition.Builder definition =
                StandardTableDefinition.newBuilder()
                        .setSchema(StorageSchemaConverter.toBigQuerySchema(schema));
        if (options.getTimePartitioningType() != null) {
            TimePartitioning.Builder partitioning =
                    TimePartitioning.newBuilder(
                            TimePartitioning.Type.valueOf(
                                    options.getTimePartitioningType().name()));
            if (options.getTimePartitioningField() != null) {
                partitioning.setField(options.getTimePartitioningField());
            }
            if (options.getTimePartitioningExpirationMs() != null) {
                partitioning.setExpirationMs(options.getTimePartitioningExpirationMs());
            }
            definition.setTimePartitioning(partitioning.build());
        }
        if (!options.getClusteredFields().isEmpty()) {
            definition.setClustering(
                    Clustering.newBuilder().setFields(options.getClusteredFields()).build());
        }
        return TableInfo.newBuilder(toTableId(destination), definition.build()).build();
    }

    private BigQuery client() {
        if (client == null) {
            client = BigQueryOptions.getDefaultInstance().getService();
        }
        return client;
    }
}
