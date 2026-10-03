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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
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
import io.github.flink.gcp.connector.base.catalog.AbstractReadOnlyCatalog;
import io.github.flink.gcp.connector.base.catalog.ReadOnlyCatalogDatabase;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory;

import javax.annotation.Nullable;

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
 * <p>The shape is the one docs/adr/0168 records for every connector catalog, held by {@link
 * AbstractReadOnlyCatalog}, and the BigQuery mapping is docs/adr/0169's:
 *
 * <ul>
 *   <li>{@link #getTable(ObjectPath)} returns a {@link CatalogTable} whose options select the
 *       {@code bigquery} connector and name the table, plus the catalog's credential and emulator
 *       options. A logical or materialized view also carries {@code scan.materialize-views}, so
 *       reading it runs a query job: naming the view is the opt-in. {@link #listTables(String)}
 *       names both tables and views.
 *   <li>Statistics are {@code UNKNOWN}. The planner reads only a row count, asks on every lookup,
 *       and BigQuery's count excludes the streaming buffer.
 *   <li>Every mutating method throws {@link UnsupportedOperationException}; tables are created by
 *       the sink.
 * </ul>
 */
@Internal
final class BigQueryCatalog extends AbstractReadOnlyCatalog<BigQuery> {

    /**
     * A dataset id's grammar: letters, digits and underscores. A name outside it names no dataset,
     * and is answered without a request — Calcite asks every catalog on its path whether a
     * qualified name's first part is one of its databases, including another catalog's name.
     */
    private static final Pattern DATASET_ID = Pattern.compile("[A-Za-z0-9_]+");

    private final String project;
    private final Map<String, String> carriedOptions;

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
            ClientOpener<BigQuery> clientOpener) {
        super(
                name,
                defaultDatabase,
                "BigQuery",
                "The BigQuery catalog is read-only; create BigQuery tables through the sink or"
                        + " BigQuery itself.",
                clientOpener,
                // The REST client holds no resource to release; dropping it lets a reopened
                // catalog build a fresh one.
                client -> {});
        this.project = project;
        this.carriedOptions = Collections.unmodifiableMap(new LinkedHashMap<>(carriedOptions));
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

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new BigQueryDynamicTableFactory());
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
            throw new DatabaseNotExistException(name(), databaseName);
        }
        return new ReadOnlyCatalogDatabase(Collections.emptyMap(), dataset.getDescription());
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

    // ------------------------------------------------------------------------
    //  Tables and views
    // ------------------------------------------------------------------------

    /**
     * Names every table in the dataset, views included, as {@link Catalog#listTables(String)} asks.
     */
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
                throw new DatabaseNotExistException(name(), databaseName, e);
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

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        Table table = table(tablePath);
        if (table == null) {
            throw new TableNotExistException(name(), tablePath);
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
}
