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
import com.google.cloud.bigquery.StandardTableDefinition;
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
 * Default {@link TableCreator} backed by the BigQuery REST client.
 *
 * <p>The client is created lazily on the first table creation, so jobs whose destination tables all
 * exist never construct it. HTTP conflicts (409, the table was created concurrently — for example
 * by a parallel subtask) are treated as success.
 */
@Internal
public class BigQueryTableCreator implements TableCreator {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableCreator.class);

    private static final int HTTP_CONFLICT = 409;

    private BigQuery client;

    /** Creates a creator using application-default credentials. */
    public BigQueryTableCreator() {}

    /**
     * Creates a creator using the given client.
     *
     * @param client the BigQuery REST client
     */
    public BigQueryTableCreator(BigQuery client) {
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
        return TableInfo.newBuilder(
                        TableId.of(
                                destination.getProject(),
                                destination.getDataset(),
                                destination.getTable()),
                        definition.build())
                .build();
    }

    private BigQuery client() {
        if (client == null) {
            client = BigQueryOptions.getDefaultInstance().getService();
        }
        return client;
    }
}
