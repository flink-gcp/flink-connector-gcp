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

package io.github.flink.gcp.connector.base.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * The part of a read-only connector catalog that does not depend on the service: the lifecycle, the
 * lazily opened client, and every answer docs/adr/0168 fixes for all connector catalogs.
 *
 * <ul>
 *   <li>Nothing touches the network before the first metadata call: {@link #open()} builds no
 *       client and loads no credentials, so {@code CREATE CATALOG} and {@code USE CATALOG} work
 *       offline. A metadata request runs through {@link #withClient(Function)}, which opens the
 *       client on first use, and {@link #close()} releases it only after every request in flight
 *       has returned, so no request sees its client closed under it.
 *   <li>Every mutating method declared here throws {@link UnsupportedOperationException} with the
 *       message the subclass supplies. Flink 2.x's {@code createModel} and {@code alterModel} are
 *       the exception: they take a {@code CatalogModel}, a type 1.20 lacks, so a source that
 *       compiles against both cannot declare them, and they throw Flink's own message.
 *   <li>{@link #listViews(String)} is empty: a service view's SQL is not a Flink view, so a catalog
 *       that reads views answers them through {@link #listTables(String)} and {@link
 *       #getTable(ObjectPath)} as tables.
 *   <li>Partitions, functions, procedures and statistics are answered without a request: after
 *       {@code USE CATALOG} Flink asks here for every function name that is not built in.
 *       Statistics are {@code UNKNOWN}; a connector that meets docs/adr/0168's reopen condition may
 *       override the two table-statistics getters, which are the only answers left open.
 *   <li>Only {@link DatabaseNotExistException} and the not-exist exceptions of tables, partitions,
 *       functions and procedures report a missing object; any other failure, including one opening
 *       the client, is a {@link CatalogException}.
 * </ul>
 *
 * <p>A subclass resolves databases and tables, names its table factory, and takes its service's
 * name grammar into {@link #databaseExists(String)}. Everything else here is {@code final} except
 * the table-statistics getters, so the contract holds the same way in every connector. The
 * service-specific inputs are constructor arguments rather than overridable hooks, so the
 * constructor calls nothing a subclass could override.
 *
 * <p>Methods Flink declares on one supported major only, {@code listMaterializedTables}, {@code
 * dropModel} and {@code renameModel} on 2.x, are declared here without {@code @Override}, so the
 * one source compiles against both.
 *
 * @param <C> the service client the catalog's metadata calls go through
 */
@Internal
public abstract class AbstractReadOnlyCatalog<C> implements Catalog {

    /**
     * Opens the service client on the first metadata call. A connector's factory passes one that
     * builds the real client from the catalog's options; a test passes one that returns a stub.
     *
     * @param <C> the service client
     */
    @FunctionalInterface
    public interface ClientOpener<C> {

        /**
         * Opens the client.
         *
         * @return the client
         * @throws IOException if the client or its credentials cannot be created; an unchecked
         *     failure is reported the same way, as a {@link CatalogException}
         */
        C open() throws IOException;
    }

    /**
     * Releases a client {@link #close()} drops.
     *
     * @param <C> the service client
     */
    @FunctionalInterface
    public interface ClientCloser<C> {

        /**
         * Releases the client's resources.
         *
         * @param client the client
         * @throws Exception if releasing fails
         */
        void close(C client) throws Exception;
    }

    private final String name;
    private final String defaultDatabase;
    private final String serviceName;
    private final String readOnlyMessage;
    private final ClientOpener<? extends C> clientOpener;
    private final ClientCloser<? super C> clientCloser;

    /**
     * Requests hold the read side for as long as they use the client, and {@link #close()} takes
     * the write side, so it waits for them rather than closing the client they are using.
     */
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

    /** Guarded by this catalog's monitor; cleared only under the write side of the lifecycle. */
    @Nullable private C client;

    /**
     * Creates a catalog from options its factory has validated.
     *
     * @param name the catalog name
     * @param defaultDatabase the database unqualified names resolve against
     * @param serviceName the service's name, as the client-opening failure names it
     * @param readOnlyMessage the message every mutating method throws with
     * @param clientOpener opens the client on the first metadata call
     * @param clientCloser releases the client when the catalog closes
     */
    protected AbstractReadOnlyCatalog(
            String name,
            String defaultDatabase,
            String serviceName,
            String readOnlyMessage,
            ClientOpener<? extends C> clientOpener,
            ClientCloser<? super C> clientCloser) {
        this.name = Preconditions.checkNotNull(name);
        this.defaultDatabase = Preconditions.checkNotNull(defaultDatabase);
        this.serviceName = Preconditions.checkNotNull(serviceName);
        this.readOnlyMessage = Preconditions.checkNotNull(readOnlyMessage);
        this.clientOpener = Preconditions.checkNotNull(clientOpener);
        this.clientCloser = Preconditions.checkNotNull(clientCloser);
    }

    /**
     * Returns the catalog name, which the not-exist exceptions carry.
     *
     * @return the catalog name
     */
    protected final String name() {
        return name;
    }

    /**
     * Runs one metadata request against the client, opening the client on the first request after
     * construction or {@link #close()}. The request must finish with the client inside it, a lazily
     * paged listing included: {@link #close()} waits for it, and a client kept past it may be
     * closed. Requests run concurrently with each other, and a request that calls {@link #close()}
     * deadlocks.
     *
     * @param request the request
     * @param <T> the request's result
     * @return what the request returned
     * @throws CatalogException if the client cannot be opened
     */
    protected final <T> T withClient(Function<? super C, ? extends T> request) {
        Lock lock = lifecycle.readLock();
        lock.lock();
        try {
            return request.apply(openedClient());
        } finally {
            lock.unlock();
        }
    }

    private synchronized C openedClient() {
        if (client == null) {
            try {
                client =
                        Preconditions.checkNotNull(
                                clientOpener.open(), "The client opener returned null.");
            } catch (IOException | RuntimeException e) {
                // Each connector's credential loader keeps the key-file path out of its message;
                // so does this.
                throw new CatalogException(
                        "Failed to open the " + serviceName + " client of catalog '" + name + "'.",
                        e);
            }
        }
        return client;
    }

    // ------------------------------------------------------------------------
    //  Lifecycle
    // ------------------------------------------------------------------------

    /** Opens nothing: the client is built on the first metadata call. */
    @Override
    public final void open() {}

    /**
     * Releases the client, if one was opened, once every request in flight has returned; a reopened
     * catalog builds a fresh one.
     *
     * @throws CatalogException if releasing the client fails
     */
    @Override
    public final void close() {
        Lock lock = lifecycle.writeLock();
        lock.lock();
        try {
            closeClient();
        } finally {
            lock.unlock();
        }
    }

    private void closeClient() {
        C opened;
        synchronized (this) {
            opened = client;
            client = null;
        }
        if (opened == null) {
            return;
        }
        try {
            clientCloser.close(opened);
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new CatalogException(
                    "Failed to close the " + serviceName + " client of catalog '" + name + "'.", e);
        }
    }

    /**
     * Returns the connector's table factory, which the planner uses for every table the catalog
     * resolves.
     *
     * @return the table factory
     */
    @Override
    public abstract Optional<Factory> getFactory();

    @Override
    public final String getDefaultDatabase() {
        return defaultDatabase;
    }

    // ------------------------------------------------------------------------
    //  Databases
    // ------------------------------------------------------------------------

    @Override
    public final void createDatabase(
            String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public final void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw readOnly();
    }

    @Override
    public final void alterDatabase(
            String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------
    //  Tables and views
    // ------------------------------------------------------------------------

    /**
     * Returns no names: a service view the connector reads is answered by {@link
     * #getTable(ObjectPath)} as a table, since its SQL cannot be expanded as a Flink view, and
     * {@link #listTables(String)} names it.
     */
    @Override
    public final List<String> listViews(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    /**
     * Returns no names; Flink 2.x asks, and 1.x has no such method, hence no {@code @Override}.
     *
     * @param databaseName the database
     * @return an empty list
     * @throws DatabaseNotExistException if the database does not exist
     */
    public final List<String> listMaterializedTables(String databaseName)
            throws DatabaseNotExistException {
        return listViews(databaseName);
    }

    @Override
    public final void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void renameTable(
            ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void createTable(
            ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public final void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    /**
     * Throws; Flink 2.x declares this, and 1.x has no such method, hence no {@code @Override}.
     *
     * @param modelPath the model
     * @param ignoreIfNotExists ignored
     */
    public final void dropModel(ObjectPath modelPath, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    /**
     * Throws; Flink 2.x declares this, and 1.x has no such method, hence no {@code @Override}.
     *
     * @param modelPath the model
     * @param newModelName ignored
     * @param ignoreIfNotExists ignored
     */
    public final void renameModel(
            ObjectPath modelPath, String newModelName, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------
    //  Partitions: a service's partitioning is not a Flink partition key
    // ------------------------------------------------------------------------

    @Override
    public final List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        return Collections.emptyList();
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return Collections.emptyList();
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        return Collections.emptyList();
    }

    @Override
    public final CatalogPartition getPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        throw new PartitionNotExistException(name, tablePath, partitionSpec);
    }

    @Override
    public final boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public final void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public final void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void alterPartition(
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
    public final List<String> listFunctions(String dbName) {
        return Collections.emptyList();
    }

    /**
     * Throws: the catalog holds no functions. Answered without the network, because after {@code
     * USE CATALOG} Flink asks here for every function name that is not built in.
     */
    @Override
    public final CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException {
        throw new FunctionNotExistException(name, functionPath);
    }

    @Override
    public final boolean functionExists(ObjectPath functionPath) {
        return false;
    }

    @Override
    public final void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw readOnly();
    }

    @Override
    public final void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final List<String> listProcedures(String dbName) {
        return Collections.emptyList();
    }

    /**
     * Throws: the catalog holds no procedures. Flink's own default throws {@link
     * UnsupportedOperationException}, which {@code CALL} does not treat as a missing procedure.
     */
    @Override
    public final Procedure getProcedure(ObjectPath procedurePath)
            throws ProcedureNotExistException {
        throw new ProcedureNotExistException(name, procedurePath);
    }

    // ------------------------------------------------------------------------
    //  Statistics
    // ------------------------------------------------------------------------

    /**
     * Returns {@code UNKNOWN}. Not {@code final}: docs/adr/0168 lets a connector fill table
     * statistics from metadata its {@link #getTable(ObjectPath)} request already returned.
     */
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        return CatalogTableStatistics.UNKNOWN;
    }

    /**
     * Returns {@code UNKNOWN}. Not {@code final}, for the reason {@link
     * #getTableStatistics(ObjectPath)} gives.
     */
    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public final CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public final CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public final void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    @Override
    public final void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw readOnly();
    }

    // ------------------------------------------------------------------------

    private UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException(readOnlyMessage);
    }
}
