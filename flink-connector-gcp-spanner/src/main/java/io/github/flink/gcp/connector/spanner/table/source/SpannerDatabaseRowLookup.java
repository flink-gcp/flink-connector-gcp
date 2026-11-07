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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;

import com.google.api.core.ApiFuture;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerClientMetrics;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.table.SpannerClientMetricsConfig;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Point and range reads through a client owned by one lookup function instance. */
@Internal
final class SpannerDatabaseRowLookup implements SpannerRowLookup {
    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final String table;
    private final List<String> columns;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    private final SpannerClientMetricsConfig clientMetricsConfig;

    /** The table whose live primary key orders {@code keyOrderColumn}, or null for none. */
    @Nullable private final SpannerTableName keyOrderTable;

    @Nullable private final String keyOrderColumn;
    private transient boolean keyColumnDescending;
    @Nullable private transient Spanner spanner;
    @Nullable private transient ClientMetrics clientMetrics;
    @Nullable private transient DatabaseClient client;

    SpannerDatabaseRowLookup(
            DatabaseDestination database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint) {
        this(database, table, columns, emulatorEndpoint, null);
    }

    SpannerDatabaseRowLookup(
            DatabaseDestination database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this(
                database,
                table,
                columns,
                emulatorEndpoint,
                serviceAccountKeyFile,
                SpannerClientMetricsConfig.unset());
    }

    SpannerDatabaseRowLookup(
            DatabaseDestination database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            SpannerClientMetricsConfig clientMetricsConfig) {
        this(
                database,
                table,
                columns,
                emulatorEndpoint,
                serviceAccountKeyFile,
                clientMetricsConfig,
                null,
                null);
    }

    /**
     * Creates a lookup that also resolves, when it opens, whether {@code keyOrderColumn} sorts
     * descending in {@code keyOrderTable}'s live primary key.
     */
    SpannerDatabaseRowLookup(
            DatabaseDestination database,
            String table,
            List<String> columns,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            SpannerClientMetricsConfig clientMetricsConfig,
            @Nullable SpannerTableName keyOrderTable,
            @Nullable String keyOrderColumn) {
        this.database = database;
        this.table = table;
        this.columns = columns;
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.clientMetricsConfig = clientMetricsConfig;
        this.keyOrderTable = keyOrderTable;
        this.keyOrderColumn = keyOrderColumn;
    }

    /**
     * Creates the lookup a lookup function reads {@code table} through. When the declared key has a
     * coarse {@code TIMESTAMP_LTZ} part, the lookup also resolves that column's live order when it
     * opens, so a range read can be ordered.
     */
    static SpannerDatabaseRowLookup forTable(
            DatabaseDestination database,
            SpannerTableName table,
            List<String> columns,
            SpannerTableSchemaConverter schema,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            SpannerClientMetricsConfig clientMetrics) {
        @Nullable String coarseColumn = SpannerLookupKeyEncoder.coarseColumn(schema);
        return new SpannerDatabaseRowLookup(
                database,
                table.apiName(),
                columns,
                emulatorEndpoint,
                serviceAccountKeyFile,
                clientMetrics,
                coarseColumn == null ? null : table,
                coarseColumn);
    }

    /** Returns the exact table passed to the Spanner point- and range-read APIs. */
    @VisibleForTesting
    String table() {
        return table;
    }

    /** Returns the serialized credential path without reading it. */
    @VisibleForTesting
    @Nullable
    String serviceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public void open(MetricGroup metricGroup) throws Exception {
        // Before the client exists. A lookup's client creates a session for its database handle
        // and reads each key with a streaming read, synchronous or not; resolving a key column's
        // order adds one query.
        if (clientMetricsConfig.exportToFlink()) {
            clientMetrics =
                    keyOrderColumn == null
                            ? SpannerClientMetrics.register(
                                    metricGroup,
                                    SpannerClientMetrics.CREATE_SESSION,
                                    SpannerClientMetrics.STREAMING_READ)
                            : SpannerClientMetrics.register(
                                    metricGroup,
                                    SpannerClientMetrics.CREATE_SESSION,
                                    SpannerClientMetrics.STREAMING_READ,
                                    SpannerClientMetrics.EXECUTE_STREAMING_SQL);
        }
        spanner = SpannerClients.open(database, settings());
        client =
                spanner.getDatabaseClient(
                        DatabaseId.of(
                                database.getProject(),
                                database.getInstance(),
                                database.getDatabase()));
        if (keyOrderColumn != null && keyOrderTable != null) {
            keyColumnDescending =
                    SpannerTableReadResolver.isPrimaryKeyColumnDescending(
                            client, keyOrderTable, keyOrderColumn);
        }
    }

    /**
     * Builds the settings the one client is opened on.
     *
     * <p>Reads the key here rather than caching it, because {@link #open()} is this method's only
     * production caller and is where this lookup's runtime component starts: the load already
     * happens exactly once. Tests call it directly to inspect what a component would be built with.
     */
    @VisibleForTesting
    SpannerOptions settings() throws Exception {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint, SpannerConnectorOptions.EMULATOR_ENDPOINT.key());
        return SpannerClients.settings(
                database,
                endpoint,
                SpannerCredentials.load(serviceAccountKeyFile),
                clientMetrics,
                clientMetricsConfig.exportToCloudMonitoring());
    }

    @Override
    public Struct read(Key key) {
        return client().singleUse().readRow(table, key, columns);
    }

    @Override
    public ApiFuture<Struct> readAsync(Key key) {
        return client().singleUse().readRowAsync(table, key, columns);
    }

    @Override
    public List<Struct> read(KeySet keys) {
        List<Struct> rows = new ArrayList<>();
        try (ResultSet resultSet = client().singleUse().read(table, keys, columns)) {
            while (resultSet.next()) {
                rows.add(resultSet.getCurrentRowAsStruct());
            }
        }
        return rows;
    }

    @Override
    public ApiFuture<List<Struct>> readAsync(KeySet keys) {
        // As the client's own readRowAsync does: the callback is set before the result set is
        // closed, and copying a row out blocks nothing, so it runs on the thread delivering it.
        try (AsyncResultSet resultSet = client().singleUse().readAsync(table, keys, columns)) {
            return resultSet.toListAsync(
                    row -> ((AsyncResultSet) row).getCurrentRowAsStruct(), Runnable::run);
        }
    }

    @Override
    public boolean isKeyColumnDescending() {
        return keyColumnDescending;
    }

    @Override
    public void close() {
        try {
            if (spanner != null) {
                spanner.close();
                spanner = null;
                client = null;
            }
        } finally {
            // After the client, whatever its close did: closed client metrics drop what they are
            // given, and a client still closing would lose whatever it recorded meanwhile.
            if (clientMetrics != null) {
                clientMetrics.close();
                clientMetrics = null;
            }
        }
    }

    private DatabaseClient client() {
        if (client == null) {
            throw new IllegalStateException("The Spanner row lookup has not been opened.");
        }
        return client;
    }
}
