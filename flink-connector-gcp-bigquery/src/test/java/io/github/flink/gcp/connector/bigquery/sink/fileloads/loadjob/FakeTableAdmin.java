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

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableSchemaSnapshot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/** Recording in-memory {@link TableAdmin} fake with scriptable lost update races. */
public final class FakeTableAdmin implements TableAdmin {

    public final Map<TableDestination, TableSchema> tables = new HashMap<>();

    /**
     * Layouts of the tables above; a table without an entry is neither partitioned nor clustered.
     */
    public final Map<TableDestination, TableLayout> layouts = new HashMap<>();

    public final List<TableDestination> created = new ArrayList<>();
    public final Map<TableDestination, TableCreateOptions> createOptions = new HashMap<>();
    public final List<TableDestination> schemaUpdates = new ArrayList<>();
    public int schemaReads;
    public int updateRacesToLose;
    public CyclicBarrier firstSchemaReadBarrier;
    private boolean firstSchemaReadObserved;

    @Override
    public void create(
            TableDestination destination, TableSchema schema, TableCreateOptions options) {
        if (tables.putIfAbsent(destination, schema) == null) {
            layouts.put(destination, layoutOf(options));
        }
        created.add(destination);
        createOptions.put(destination, options);
    }

    @Override
    public boolean ensureCdcTable(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider optionsProvider,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy reconciliationPolicy) {
        boolean creationRequested = !tables.containsKey(destination);
        create(destination, schema, optionsProvider.optionsFor(destination));
        return creationRequested;
    }

    /** Live temporary tables, mapped to the creation time of their current incarnation. */
    public final Map<TableDestination, Long> temporaryTables = new LinkedHashMap<>();

    /** Every temporary-table preparation, in order, with the layout and expiration it carried. */
    public final List<TableDestination> preparedTemporaryTables = new ArrayList<>();

    public final List<TableLayout> preparedLayouts = new ArrayList<>();
    public final List<Schema> preparedSchemas = new ArrayList<>();
    public final List<Duration> preparedExpirations = new ArrayList<>();

    /** The creation time the next created temporary table takes, less one. */
    public long clock = 1_000;

    @Override
    public long prepareTemporaryTable(
            TableDestination table, Schema schema, TableLayout layout, Duration expiration) {
        preparedTemporaryTables.add(table);
        preparedLayouts.add(layout);
        preparedSchemas.add(schema);
        preparedExpirations.add(expiration);
        return temporaryTables.computeIfAbsent(table, unused -> ++clock);
    }

    @Override
    public TableSchemaSnapshot getSchema(TableDestination destination) {
        if (firstSchemaReadBarrier != null && !firstSchemaReadObserved) {
            firstSchemaReadObserved = true;
            try {
                firstSchemaReadBarrier.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Table schema read was interrupted", failure);
            } catch (Exception failure) {
                throw new AssertionError("Timed out waiting for table schema reads", failure);
            }
        }
        schemaReads++;
        TableSchema schema = tables.get(destination);
        return schema == null
                ? null
                : TableSchemaSnapshot.of(
                        schema, null, layouts.getOrDefault(destination, TableLayout.NONE));
    }

    /** The layout {@code BigQueryTableAdmin} would create a table with from these options. */
    private static TableLayout layoutOf(TableCreateOptions options) {
        TimePartitioning partitioning = null;
        if (options.getTimePartitioningType() != null) {
            TimePartitioning.Builder builder =
                    TimePartitioning.newBuilder(
                            TimePartitioning.Type.valueOf(
                                    options.getTimePartitioningType().name()));
            if (options.getTimePartitioningField() != null) {
                builder.setField(options.getTimePartitioningField());
            }
            partitioning = builder.build();
        }
        Clustering clustering =
                options.getClusteredFields().isEmpty()
                        ? null
                        : Clustering.newBuilder().setFields(options.getClusteredFields()).build();
        return TableLayout.of(partitioning, null, clustering);
    }

    @Override
    public boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
        if (updateRacesToLose > 0) {
            updateRacesToLose--;
            return false;
        }
        tables.put(destination, proposed);
        schemaUpdates.add(destination);
        return true;
    }
}
