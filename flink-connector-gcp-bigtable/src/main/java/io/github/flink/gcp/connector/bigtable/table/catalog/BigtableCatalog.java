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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;

import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.base.catalog.AbstractReadOnlyCatalog;
import io.github.flink.gcp.connector.base.catalog.ReadOnlyCatalogDatabase;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableDynamicTableFactory;
import io.github.flink.gcp.connector.bigtable.table.CatalogKeyType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * A read-only Flink catalog over one Bigtable instance: the instance is the catalog's one database,
 * its tables are the catalog's tables, and each table's schema is derived from its column families
 * when the planner asks for it.
 *
 * <p>The shape is the one docs/adr/0168 records for every connector catalog, held by {@link
 * AbstractReadOnlyCatalog}, and the Bigtable mapping is docs/adr/0178's:
 *
 * <ul>
 *   <li>The database is named after the instance, so {@code bt.my-instance.orders} names table
 *       {@code orders}; no other name is a database, and none is asked about.
 *   <li>{@link #getTable(ObjectPath)} returns a {@link CatalogTable} whose schema is {@link
 *       BigtableCatalogSchema}'s and whose options select the {@code bigtable} connector, name the
 *       table, and carry the catalog's credential and emulator options.
 *   <li>Change streams are not listed: a Change Streams table has a fixed envelope schema that an
 *       {@code OPTIONS} hint cannot give a catalog table, so it is declared by hand.
 * </ul>
 */
@Internal
final class BigtableCatalog extends AbstractReadOnlyCatalog<BigtableCatalogClient> {

    /**
     * A table id's grammar, as the admin API documents it. A name outside it names no table and is
     * answered without a request, which also keeps a {@code /} out of the resource name.
     */
    private static final Pattern TABLE_ID = Pattern.compile("[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,49}");

    private final String project;
    private final String instance;
    private final CatalogKeyType keyType;
    private final Map<String, String> carriedOptions;

    /**
     * Creates a catalog from options the factory has validated.
     *
     * @param name the catalog name
     * @param project the project that owns the instance
     * @param instance the instance whose tables are listed, and the name of its one database
     * @param keyType the type of each table's row key and map keys
     * @param carriedOptions the options every resolved table carries besides its identity
     * @param clientOpener opens the metadata client on the first metadata call
     */
    BigtableCatalog(
            String name,
            String project,
            String instance,
            CatalogKeyType keyType,
            Map<String, String> carriedOptions,
            ClientOpener<BigtableCatalogClient> clientOpener) {
        super(
                name,
                instance,
                "Bigtable",
                "The Bigtable catalog is read-only; create Bigtable tables through Bigtable"
                        + " itself or the sink's table creation.",
                clientOpener,
                BigtableCatalogClient::close);
        this.project = project;
        this.instance = instance;
        this.keyType = keyType;
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
                    BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(), serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            options.put(BigtableConnectorOptions.EMULATOR_ENDPOINT.key(), emulatorEndpoint);
        }
        return options;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new BigtableDynamicTableFactory());
    }

    // ------------------------------------------------------------------------
    //  Databases
    // ------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() {
        return Collections.singletonList(instance);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name(), databaseName);
        }
        return new ReadOnlyCatalogDatabase(Collections.emptyMap(), null);
    }

    /**
     * Whether the name is the instance's. Answered without a request: a missing instance shows on
     * the first listing, whose failure names it, and a query that names a table reaches one, since
     * the planner lists the tables after the lookup finds none; {@code DESCRIBE} does not list, so
     * it reports the table missing.
     */
    @Override
    public boolean databaseExists(String databaseName) {
        return instance.equals(databaseName);
    }

    // ------------------------------------------------------------------------
    //  Tables
    // ------------------------------------------------------------------------

    /** Names every table of the instance by its id, in the order the service lists them. */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name(), databaseName);
        }
        return new ArrayList<>(
                ask(
                        BigtableCatalogClient::listTables,
                        () -> "list the tables of Bigtable instance '" + instanceName() + "'"));
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        Map<String, Type> families = families(tablePath);
        if (families == null) {
            throw new TableNotExistException(name(), tablePath);
        }
        Schema schema;
        try {
            schema = BigtableCatalogSchema.of(families, keyType);
        } catch (IllegalArgumentException e) {
            throw new CatalogException(
                    "Bigtable table '"
                            + qualified(tablePath)
                            + "' cannot be resolved: "
                            + e.getMessage(),
                    e);
        }
        return CatalogTable.newBuilder()
                .schema(schema)
                .options(tableOptions(tablePath.getObjectName()))
                .build();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return families(tablePath) != null;
    }

    @Nullable
    private Map<String, Type> families(ObjectPath tablePath) {
        if (!databaseExists(tablePath.getDatabaseName())
                || !TABLE_ID.matcher(tablePath.getObjectName()).matches()) {
            return null;
        }
        return ask(
                client -> client.columnFamilies(tablePath.getObjectName()),
                () -> "read Bigtable table '" + qualified(tablePath) + "'");
    }

    /**
     * Runs one metadata request against the client. The catalog's own exceptions pass through as
     * they are; any other failure, the service's or the client's, becomes a {@link
     * CatalogException} naming what was asked.
     */
    private <T> T ask(
            Function<? super BigtableCatalogClient, ? extends T> request, Supplier<String> asked) {
        try {
            return withClient(request);
        } catch (CatalogException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CatalogException("Failed to " + asked.get() + ".", e);
        }
    }

    private Map<String, String> tableOptions(String table) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put(BigtableConnectorOptions.PROJECT.key(), project);
        options.put(BigtableConnectorOptions.INSTANCE.key(), instance);
        options.put(BigtableConnectorOptions.TABLE.key(), table);
        options.putAll(carriedOptions);
        return options;
    }

    private String instanceName() {
        return project + "/" + instance;
    }

    private String qualified(ObjectPath tablePath) {
        return instanceName() + "/" + tablePath.getObjectName();
    }
}
