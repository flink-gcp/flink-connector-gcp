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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;

import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.base.catalog.AbstractReadOnlyCatalog;
import io.github.flink.gcp.connector.base.catalog.ReadOnlyCatalogDatabase;
import io.github.flink.gcp.connector.spanner.SpannerObjectName;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerDynamicTableFactory;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.TableMetadata;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * A read-only Flink catalog over one Spanner instance: its databases are the Flink databases, and
 * each base table's schema is resolved from the database's {@code INFORMATION_SCHEMA} when the
 * planner asks for it.
 *
 * <p>The shape is the one docs/adr/0168 records for every connector catalog, held by {@link
 * AbstractReadOnlyCatalog}, and the Spanner mapping is docs/adr/0176's:
 *
 * <ul>
 *   <li>A table in a database's default schema is named alone, and one in a named schema as {@code
 *       schema.table}, each part in the dialect's canonical quoting ({@link SpannerObjectName}), so
 *       every name {@link #listTables(String)} returns resolves through {@link
 *       #getTable(ObjectPath)}.
 *   <li>{@link #getTable(ObjectPath)} returns a {@link CatalogTable} whose options select the
 *       {@code spanner} connector, name the table and its dialect, carry the catalog's credential
 *       and emulator options, and mark the columns whose Flink type alone does not name their
 *       Spanner type.
 *   <li>Views are not listed: the table source reads through Spanner's read API, which takes a
 *       table. Change streams are not listed either; an {@code OPTIONS} hint turns a catalog table
 *       into its change-stream source.
 * </ul>
 */
@Internal
final class SpannerCatalog extends AbstractReadOnlyCatalog<SpannerCatalogClient> {

    /**
     * A database id's grammar, as Spanner enforces it. A name outside it names no database and is
     * answered without a request: Calcite asks every catalog on its path whether a qualified name's
     * first part is one of its databases, including another catalog's name.
     */
    static final Pattern DATABASE_ID = Pattern.compile("[a-z][a-z0-9_-]{0,28}[a-z0-9]");

    private final String project;
    private final String instance;
    private final Map<String, String> carriedOptions;

    /**
     * A database's dialect is fixed at creation, so one answer serves until the database is found
     * dropped; a missing database is never cached, so one created later is found.
     */
    private final Map<String, Dialect> dialects = new ConcurrentHashMap<>();

    /**
     * Creates a catalog from options the factory has validated.
     *
     * @param name the catalog name
     * @param project the project that owns the instance
     * @param instance the instance whose databases are listed
     * @param defaultDatabase the database unqualified names resolve against
     * @param carriedOptions the options every resolved table carries besides its identity
     * @param clientOpener opens the metadata client on the first metadata call
     */
    SpannerCatalog(
            String name,
            String project,
            String instance,
            String defaultDatabase,
            Map<String, String> carriedOptions,
            ClientOpener<SpannerCatalogClient> clientOpener) {
        super(
                name,
                defaultDatabase,
                "Spanner",
                "The Spanner catalog is read-only; create Spanner tables and databases through"
                        + " Spanner itself.",
                clientOpener,
                SpannerCatalogClient::close);
        this.project = project;
        this.instance = instance;
        this.carriedOptions = Collections.unmodifiableMap(new LinkedHashMap<>(carriedOptions));
    }

    /**
     * The options every resolved table carries besides its identity, in the connector's keys.
     *
     * @param serviceAccountKeyFile the key-file path, or {@code null}
     * @param emulatorEndpoint the emulator's endpoint, or {@code null}
     * @return the options, without absent ones
     */
    static Map<String, String> carriedOptions(
            @Nullable String serviceAccountKeyFile, @Nullable String emulatorEndpoint) {
        Map<String, String> options = new LinkedHashMap<>();
        if (serviceAccountKeyFile != null) {
            options.put(
                    SpannerConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(), serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            options.put(SpannerConnectorOptions.EMULATOR_ENDPOINT.key(), emulatorEndpoint);
        }
        return options;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new SpannerDynamicTableFactory());
    }

    // ------------------------------------------------------------------------
    //  Databases
    // ------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() {
        return ask(
                () -> client().listDatabases(),
                () -> "list the databases of Spanner instance '" + instanceName() + "'");
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        if (dialect(databaseName) == null) {
            throw new DatabaseNotExistException(name(), databaseName);
        }
        return new ReadOnlyCatalogDatabase(Collections.emptyMap(), null);
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return dialect(databaseName) != null;
    }

    @Nullable
    private Dialect dialect(String databaseName) {
        if (!DATABASE_ID.matcher(databaseName).matches()) {
            return null;
        }
        Dialect cached = dialects.get(databaseName);
        if (cached != null) {
            return cached;
        }
        Dialect dialect =
                ask(
                        () -> client().dialect(databaseName),
                        () -> "read Spanner database '" + qualified(databaseName) + "'");
        if (dialect != null) {
            dialects.put(databaseName, dialect);
        }
        return dialect;
    }

    // ------------------------------------------------------------------------
    //  Tables
    // ------------------------------------------------------------------------

    /**
     * Names every base table outside the system schemas, as {@link SpannerObjectName} renders it.
     */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        Dialect dialect = dialect(databaseName);
        if (dialect == null) {
            throw new DatabaseNotExistException(name(), databaseName);
        }
        List<SpannerObjectName> tables;
        try {
            tables =
                    ask(
                            () -> client().listTables(databaseName, dialect),
                            () ->
                                    "list the tables of Spanner database '"
                                            + qualified(databaseName)
                                            + "'");
        } catch (DatabaseNotFoundException e) {
            dialects.remove(databaseName);
            throw new DatabaseNotExistException(name(), databaseName, e);
        }
        List<String> names = new ArrayList<>(tables.size());
        for (SpannerObjectName table : tables) {
            names.add(SpannerObjectName.format(table.schema(), table.table(), dialect));
        }
        return names;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        Dialect dialect = dialect(tablePath.getDatabaseName());
        TableMetadata table = dialect == null ? null : table(tablePath, dialect);
        if (table == null) {
            throw new TableNotExistException(name(), tablePath);
        }
        String database = tablePath.getDatabaseName();
        Supplier<Map<String, SpannerCatalogClient.NamedTypeKind>> protoBundleTypes =
                () ->
                        ask(
                                () -> client().protoBundleTypes(database),
                                () ->
                                        "read the proto bundle of Spanner database '"
                                                + qualified(database)
                                                + "'");
        SpannerSchemaToFlinkConverter.Converted converted;
        try {
            converted =
                    SpannerSchemaToFlinkConverter.convert(
                            table.columns(), dialect, protoBundleTypes);
        } catch (DatabaseNotFoundException e) {
            dialects.remove(database);
            throw new TableNotExistException(name(), tablePath, e);
        } catch (SpannerSchemaToFlinkConverter.UnsupportedColumnException e) {
            throw new CatalogException(
                    "Spanner table '"
                            + qualified(tablePath)
                            + "' cannot be resolved: "
                            + e.getMessage(),
                    e);
        }
        return CatalogTable.newBuilder()
                .schema(converted.schema())
                .options(tableOptions(database, dialect, table.name(), converted.markerOptions()))
                .build();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        Dialect dialect = dialect(tablePath.getDatabaseName());
        return dialect != null && table(tablePath, dialect) != null;
    }

    @Nullable
    private TableMetadata table(ObjectPath tablePath, Dialect dialect) {
        Optional<SpannerObjectName> parsed =
                SpannerObjectName.parse(tablePath.getObjectName(), dialect);
        if (!parsed.isPresent()) {
            return null;
        }
        try {
            return ask(
                    () ->
                            client().table(
                                            tablePath.getDatabaseName(),
                                            dialect,
                                            parsed.get().schema(),
                                            parsed.get().table()),
                    () -> "read Spanner table '" + qualified(tablePath) + "'");
        } catch (DatabaseNotFoundException e) {
            // Dropped since its dialect was cached: the table is gone with it.
            dialects.remove(tablePath.getDatabaseName());
            return null;
        }
    }

    /**
     * Runs one metadata request. A database dropped while the catalog was open passes through for
     * the caller to report as missing; the catalog's own exceptions pass through as they are; any
     * other failure, the service's or the client's, becomes a {@link CatalogException} naming what
     * was asked.
     */
    private static <T> T ask(Supplier<T> request, Supplier<String> asked) {
        try {
            return request.get();
        } catch (DatabaseNotFoundException | CatalogException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CatalogException("Failed to " + asked.get() + ".", e);
        }
    }

    /**
     * The table's options. A table in the default schema names itself through {@code table} alone,
     * which the connector passes to Spanner as given; one in a named schema sets {@code schema} and
     * {@code table} in canonical quoting, which the connector decodes back to the native names.
     */
    private Map<String, String> tableOptions(
            String database,
            Dialect dialect,
            SpannerObjectName table,
            Map<String, String> markers) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", SpannerDynamicTableFactory.IDENTIFIER);
        options.put(SpannerConnectorOptions.PROJECT.key(), project);
        options.put(SpannerConnectorOptions.INSTANCE.key(), instance);
        options.put(SpannerConnectorOptions.DATABASE.key(), database);
        options.put(SpannerConnectorOptions.DIALECT.key(), dialect.name());
        if (table.schema().equals(dialect.getDefaultSchema())) {
            options.put(SpannerConnectorOptions.TABLE.key(), table.table());
        } else {
            options.put(
                    SpannerConnectorOptions.SCHEMA.key(),
                    SpannerObjectName.encodePart(table.schema(), dialect));
            options.put(
                    SpannerConnectorOptions.TABLE.key(),
                    SpannerObjectName.encodePart(table.table(), dialect));
        }
        options.putAll(carriedOptions);
        options.putAll(markers);
        return options;
    }

    private String instanceName() {
        return project + "/" + instance;
    }

    private String qualified(String database) {
        return instanceName() + "/" + database;
    }

    private String qualified(ObjectPath tablePath) {
        return qualified(tablePath.getDatabaseName()) + "/" + tablePath.getObjectName();
    }
}
