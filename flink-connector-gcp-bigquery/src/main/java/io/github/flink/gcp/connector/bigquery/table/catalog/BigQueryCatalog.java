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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * A read-only Flink catalog over one BigQuery project: its datasets are the databases, and each
 * table's schema is resolved from BigQuery when the planner asks for it.
 *
 * <p>The shape is the one docs/adr/0168 records for every connector catalog, and the BigQuery
 * mapping is docs/adr/0169's:
 *
 * <ul>
 *   <li>{@link #getTable} returns a {@link CatalogTable} whose options select the {@code bigquery}
 *       connector and name the table, plus the catalog's credential and emulator options. A logical
 *       or materialized view also carries {@code scan.materialize-views}, so reading it runs a
 *       query job: naming the view is the opt-in. {@link #listViews} is empty, because a BigQuery
 *       view's GoogleSQL is not a Flink view, and {@link #listTables} names both.
 *   <li>Nothing touches the network before the first metadata call: {@link #open} builds no client
 *       and loads no credentials, so {@code CREATE CATALOG} and {@code USE CATALOG} work offline.
 *   <li>Statistics are {@code UNKNOWN}. The planner reads only a row count, asks on every lookup,
 *       and BigQuery's count excludes the streaming buffer.
 *   <li>Every mutating method throws {@link UnsupportedOperationException}; tables are created by
 *       the sink.
 * </ul>
 *
 * <p>Methods Flink declares on one supported major only — {@code listMaterializedTables} on 2.x —
 * are declared here without {@code @Override}, so the one source compiles against both.
 */
@Internal
final class BigQueryCatalog implements Catalog {

    /** Opens the REST client on first use; a seam so tests can hand over a stub. */
    @FunctionalInterface
    interface ClientOpener {
        BigQuery open() throws IOException;
    }

    /**
     * A dataset id's grammar: letters, digits and underscores. A name outside it names no dataset,
     * and is answered without a request — Calcite asks every catalog on its path whether a
     * qualified name's first part is one of its databases, including another catalog's name.
     */
    private static final Pattern DATASET_ID = Pattern.compile("[A-Za-z0-9_]+");

    private final String name;
    private final String project;
    private final String defaultDatabase;
    private final Map<String, String> carriedOptions;
    private final ClientOpener clientOpener;

    @Nullable private BigQuery client;

    /**
     * Creates a catalog from options the factory has validated.
     *
     * @param name the catalog name
     * @param project the project whose datasets are listed
     * @param defaultDatabase the dataset unqualified names resolve against
     * @param carriedOptions the options every resolved table carries besides its identity
     * @param clientOpener opens the REST client on the first metadata call
     */
    BigQueryCatalog(
            String name,
            String project,
            String defaultDatabase,
            Map<String, String> carriedOptions,
            ClientOpener clientOpener) {
        this.name = name;
        this.project = project;
        this.defaultDatabase = defaultDatabase;
        this.carriedOptions = Collections.unmodifiableMap(new LinkedHashMap<>(carriedOptions));
        this.clientOpener = clientOpener;
    }

    /**
     * The options every resolved table carries besides its identity, in the connector's keys.
     *
     * @param serviceAccountKeyFile the key-file path, or {@code null}
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null}
     * @param emulatorRestEndpoint the emulator's REST endpoint, or {@code null}
     * @param parentProject the Storage Read billing project, or {@code null}
     * @return the options, without absent ones
     */
    static Map<String, String> carriedOptions(
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable String emulatorRestEndpoint,
            @Nullable String parentProject) {
        Map<String, String> options = new LinkedHashMap<>();
        putIfPresent(
                options,
                BigQueryConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(),
                serviceAccountKeyFile);
        putIfPresent(options, BigQueryConnectorOptions.EMULATOR_ENDPOINT.key(), emulatorEndpoint);
        putIfPresent(
                options,
                BigQueryConnectorOptions.EMULATOR_REST_ENDPOINT.key(),
                emulatorRestEndpoint);
        putIfPresent(options, BigQueryConnectorOptions.SCAN_PARENT_PROJECT.key(), parentProject);
        return options;
    }

    private static void putIfPresent(
            Map<String, String> options, String key, @Nullable String value) {
        if (value != null) {
            options.put(key, value);
        }
    }

    // ------------------------------------------------------------------------
    //  Lifecycle
    // ------------------------------------------------------------------------

    /** Opens nothing: the client is built on the first metadata call. */
    @Override
    public void open() {}

    @Override
    public synchronized void close() {
        // The REST client holds no resource to release; dropping it lets a reopened catalog
        // build a fresh one.
        client = null;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new BigQueryDynamicTableFactory());
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    // ------------------------------------------------------------------------
    //  Databases
    // ------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() {
        List<String> databases = new ArrayList<>();
        try {
            for (Dataset dataset : client().listDatasets(project).iterateAll()) {
                databases.add(dataset.getDatasetId().getDataset());
            }
        } catch (BigQueryException e) {
            throw new CatalogException(
                    "Failed to list the datasets of BigQuery project '" + project + "'.", e);
        }
        return databases;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        Dataset dataset = dataset(databaseName);
        if (dataset == null) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return new BigQueryCatalogDatabase(Collections.emptyMap(), dataset.getDescription());
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return dataset(databaseName) != null;
    }

    @Nullable
    private Dataset dataset(String databaseName) {
        if (!DATASET_ID.matcher(databaseName).matches()) {
            return null;
        }
        try {
            return client().getDataset(DatasetId.of(project, databaseName));
        } catch (BigQueryException e) {
            throw new CatalogException(
                    "Failed to read BigQuery dataset '" + project + "." + databaseName + "'.", e);
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw readOnly();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------
    //  Tables and views
    // ------------------------------------------------------------------------

    /** Names every table in the dataset, views included, as {@link Catalog#listTables} asks. */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        List<String> tables = new ArrayList<>();
        try {
            for (Table table :
                    client().listTables(DatasetId.of(project, databaseName)).iterateAll()) {
                tables.add(table.getTableId().getTable());
            }
        } catch (BigQueryException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new DatabaseNotExistException(name, databaseName, e);
            }
            throw new CatalogException(
                    "Failed to list the tables of BigQuery dataset '"
                            + project
                            + "."
                            + databaseName
                            + "'.",
                    e);
        }
        return tables;
    }

    /**
     * Returns no names: {@link #getTable} answers a BigQuery view as a table, since its GoogleSQL
     * definition cannot be expanded as a Flink view, and {@link #listTables} already names it.
     */
    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    /**
     * Returns no names; Flink 2.x asks, and 1.x has no such method, hence no {@code @Override}.
     *
     * @param databaseName the dataset
     * @return an empty list
     * @throws DatabaseNotExistException if the dataset does not exist
     */
    public List<String> listMaterializedTables(String databaseName)
            throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        Table table = table(tablePath);
        if (table == null) {
            throw new TableNotExistException(name, tablePath);
        }
        TableDefinition definition = table.getDefinition();
        Schema schema = definition == null ? null : definition.getSchema();
        if (schema == null) {
            throw new CatalogException(
                    "BigQuery table '" + qualified(tablePath) + "' reports no schema.");
        }
        org.apache.flink.table.api.Schema flinkSchema;
        try {
            flinkSchema =
                    BigQuerySchemaToFlinkConverter.toSchema(
                            schema.getFields(), primaryKey(table.getTableConstraints()));
        } catch (BigQuerySchemaToFlinkConverter.UnsupportedColumnException e) {
            throw new CatalogException(
                    "BigQuery table '"
                            + qualified(tablePath)
                            + "' cannot be resolved: "
                            + e.getMessage(),
                    e);
        }
        return CatalogTable.newBuilder()
                .schema(flinkSchema)
                .comment(table.getDescription())
                .options(tableOptions(tablePath, isView(definition)))
                .build();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return table(tablePath) != null;
    }

    @Nullable
    private Table table(ObjectPath tablePath) {
        try {
            return client().getTable(
                            TableId.of(
                                    project,
                                    tablePath.getDatabaseName(),
                                    tablePath.getObjectName()));
        } catch (BigQueryException e) {
            throw new CatalogException(
                    "Failed to read BigQuery table '" + qualified(tablePath) + "'.", e);
        }
    }

    private static List<String> primaryKey(@Nullable TableConstraints constraints) {
        if (constraints == null || constraints.getPrimaryKey() == null) {
            return Collections.emptyList();
        }
        List<String> columns = constraints.getPrimaryKey().getColumns();
        return columns == null ? Collections.emptyList() : columns;
    }

    private static boolean isView(TableDefinition definition) {
        return definition.getType() == TableDefinition.Type.VIEW
                || definition.getType() == TableDefinition.Type.MATERIALIZED_VIEW;
    }

    private Map<String, String> tableOptions(ObjectPath tablePath, boolean view) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigQueryDynamicTableFactory.IDENTIFIER);
        options.put(BigQueryConnectorOptions.PROJECT.key(), project);
        options.put(BigQueryConnectorOptions.DATASET.key(), tablePath.getDatabaseName());
        options.put(BigQueryConnectorOptions.TABLE.key(), tablePath.getObjectName());
        options.putAll(carriedOptions);
        if (view) {
            options.put(BigQueryConnectorOptions.SCAN_MATERIALIZE_VIEWS.key(), "true");
        }
        return options;
    }

    private String qualified(ObjectPath tablePath) {
        return project + "." + tablePath.getDatabaseName() + "." + tablePath.getObjectName();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------
    //  Partitions: BigQuery partitioning is not a Flink partition key
    // ------------------------------------------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        throw new PartitionNotExistException(name, tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------
    //  Functions and procedures: none, and answered without the network
    // ------------------------------------------------------------------------

    @Override
    public List<String> listFunctions(String dbName) {
        return Collections.emptyList();
    }

    /**
     * Throws: the catalog holds no functions. Answered without the network, because after {@code
     * USE CATALOG} Flink asks here for every function name that is not built in.
     */
    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
        throw new FunctionNotExistException(name, functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public List<String> listProcedures(String dbName) {
        return Collections.emptyList();
    }

    // ------------------------------------------------------------------------
    //  Statistics
    // ------------------------------------------------------------------------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------

    private synchronized BigQuery client() {
        if (client == null) {
            try {
                client = clientOpener.open();
            } catch (IOException | IllegalArgumentException e) {
                // BigQueryCredentials keeps the key-file path out of its message; so does this.
                throw new CatalogException(
                        "Failed to open the BigQuery client of catalog '" + name + "'.", e);
            }
        }
        return client;
    }

    private static UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException(
                "The BigQuery catalog is read-only; create BigQuery tables through the sink or"
                        + " BigQuery itself.");
    }
}
