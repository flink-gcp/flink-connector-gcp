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

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
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
import org.apache.flink.table.factories.Factory;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link AbstractReadOnlyCatalog} through a subclass whose client is a counted token. */
class AbstractReadOnlyCatalogTest {

    private static final String READ_ONLY = "The stub catalog is read-only.";
    private static final Pattern MUTATOR = Pattern.compile("create|alter|drop|rename");

    /**
     * Mutators whose parameter types 1.20 lacks ({@code CatalogModel}, and {@code
     * CatalogConnection} on 2.4), so a source compiled against both lines cannot declare them; they
     * keep Flink's own refusal. {@code dropConnection} takes no such type, but 2.4 is not a
     * supported line yet.
     */
    private static final List<String> UNDECLARABLE =
            Arrays.asList(
                    "createModel",
                    "alterModel",
                    "createConnection",
                    "alterConnection",
                    "dropConnection");

    private static final ObjectPath EVENTS = new ObjectPath("analytics", "events");
    private static final CatalogPartitionSpec SPEC =
            new CatalogPartitionSpec(Collections.singletonMap("p", "1"));

    private final AtomicInteger opened = new AtomicInteger();
    private final List<String> closed = new ArrayList<>();

    private StubCatalog catalog() {
        return new StubCatalog(
                () -> "client-" + opened.incrementAndGet(), client -> closed.add(client));
    }

    @Test
    void opensNoClientBeforeTheFirstMetadataCallAndOneAfterIt() {
        StubCatalog catalog = catalog();

        // What CREATE CATALOG and USE CATALOG call.
        catalog.open();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("analytics");
        assertThat(opened).hasValue(0);

        assertThat(catalog.listDatabases()).containsExactly("analytics");
        catalog.listDatabases();
        assertThat(catalog.clients).containsExactly("client-1", "client-1");
        assertThat(opened).hasValue(1);
    }

    /**
     * Four threads ask for the client at once. The opener is held until one thread is inside it and
     * the other three are blocked on the catalog's monitor, so the race this guards is forced
     * rather than left to the scheduler: without the monitor, a second thread enters the opener and
     * the wait ends early.
     */
    @Test
    void concurrentFirstCallsOpenOneClient() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        StubCatalog catalog =
                new StubCatalog(
                        () -> {
                            entered.incrementAndGet();
                            try {
                                release.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException(e);
                            }
                            return "client-" + opened.incrementAndGet();
                        },
                        client -> {});
        List<Thread> threads = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool =
                Executors.newFixedThreadPool(
                        4,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            threads.add(thread);
                            return thread;
                        });
        try {
            List<Future<List<String>>> calls = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                calls.add(pool.submit(catalog::listDatabases));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (entered.get() < 2
                    && !(entered.get() == 1 && blockedCount(threads) == 3)
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(entered).as("threads inside the opener before it was released").hasValue(1);
            release.countDown();
            for (Future<List<String>> call : calls) {
                call.get(10, TimeUnit.SECONDS);
            }
        } finally {
            release.countDown();
            pool.shutdownNow();
        }

        assertThat(opened).hasValue(1);
        assertThat(catalog.clients).hasSize(4).containsOnly("client-1");
    }

    private static long blockedCount(List<Thread> threads) {
        synchronized (threads) {
            return threads.stream()
                    .filter(thread -> thread.getState() == Thread.State.BLOCKED)
                    .count();
        }
    }

    @Test
    void anOpeningFailureNamesTheServiceAndCatalogAndKeepsItsCause() {
        IOException cause = new IOException("Failed to load the configured key file.");
        StubCatalog catalog =
                new StubCatalog(
                        () -> {
                            throw cause;
                        },
                        client -> {});

        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to open the Stub client of catalog 'stub'.")
                .hasCause(cause);
    }

    @Test
    void anUncheckedOpeningFailureIsACatalogExceptionToo() {
        StubCatalog catalog =
                new StubCatalog(
                        () -> {
                            throw new IllegalStateException("bad endpoint");
                        },
                        client -> {});

        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to open the Stub client of catalog 'stub'.")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void anOpenerReturningNothingIsAnOpeningFailure() {
        StubCatalog catalog = new StubCatalog(() -> null, client -> {});

        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(CatalogException.class)
                .hasCauseInstanceOf(NullPointerException.class);
    }

    @Test
    void closeReleasesTheOpenedClientAndAReopenedCatalogOpensAFreshOne() {
        StubCatalog catalog = catalog();

        catalog.close();
        assertThat(closed).isEmpty();

        catalog.listDatabases();
        catalog.close();
        catalog.close();
        assertThat(closed).containsExactly("client-1");

        catalog.open();
        catalog.listDatabases();
        assertThat(catalog.clients).containsExactly("client-1", "client-2");
    }

    @Test
    void aClosingFailureIsACatalogExceptionAndStillDropsTheClient() {
        StubCatalog catalog =
                new StubCatalog(
                        () -> "client-" + opened.incrementAndGet(),
                        client -> {
                            throw new IOException("channel stuck");
                        });
        catalog.listDatabases();

        assertThatThrownBy(catalog::close)
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to close the Stub client of catalog 'stub'.")
                .hasCauseInstanceOf(IOException.class);

        catalog.listDatabases();
        assertThat(opened).hasValue(2);
    }

    @Test
    void aCloserThrowingACatalogExceptionIsNotWrappedAgain() {
        CatalogException failure = new CatalogException("already explained");
        StubCatalog catalog =
                new StubCatalog(
                        () -> "client",
                        client -> {
                            throw failure;
                        });
        catalog.listDatabases();

        assertThatThrownBy(catalog::close).isSameAs(failure);
    }

    @Test
    void anInterruptedCloseKeepsTheInterrupt() {
        StubCatalog catalog =
                new StubCatalog(
                        () -> "client",
                        client -> {
                            throw new InterruptedException();
                        });
        catalog.listDatabases();

        try {
            assertThatThrownBy(catalog::close)
                    .isInstanceOf(CatalogException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * Every mutator {@link Catalog} declares on the Flink line under test, default methods
     * included, is refused, and every one the skeleton declares carries the catalog's message. A
     * mutator the skeleton leaves to Flink's default must be on {@link #UNDECLARABLE}: one whose
     * parameter type does not exist on 1.20. A mutator a later Flink adds fails here until it is
     * either declared or listed.
     */
    @Test
    void everyMutationIsRefusedWithTheCatalogsMessage() throws Exception {
        StubCatalog catalog = catalog();
        List<String> declared = new ArrayList<>();

        for (Method method : Catalog.class.getMethods()) {
            if (!MUTATOR.matcher(method.getName()).lookingAt()) {
                continue;
            }
            Object[] arguments =
                    Arrays.stream(method.getParameterTypes())
                            .map(type -> type == boolean.class ? (Object) false : null)
                            .toArray();
            boolean ownMessage = declaresOwnAnswer(method);
            if (ownMessage) {
                declared.add(method.getName());
            } else {
                assertThat(UNDECLARABLE)
                        .as("a mutator left to Flink's default: %s", method.toGenericString())
                        .contains(method.getName());
            }
            assertThatThrownBy(() -> invoke(method, catalog, arguments))
                    .as(method.toGenericString())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .satisfies(
                            refusal -> {
                                if (ownMessage) {
                                    assertThat(refusal).hasMessage(READ_ONLY);
                                }
                            });
        }

        assertThat(declared)
                .as("the mutators Flink declares on both lines")
                .contains(
                        "createDatabase",
                        "dropDatabase",
                        "alterDatabase",
                        "createTable",
                        "dropTable",
                        "alterTable",
                        "renameTable",
                        "createPartition",
                        "dropPartition",
                        "alterPartition",
                        "createFunction",
                        "alterFunction",
                        "dropFunction",
                        "alterTableStatistics",
                        "alterTableColumnStatistics",
                        "alterPartitionStatistics",
                        "alterPartitionColumnStatistics");
        assertThat(opened).hasValue(0);
    }

    /**
     * Whether the skeleton answers this method itself, directly or through a default that delegates
     * to one it declares (the two-argument {@code dropDatabase}, the {@code TableChange} {@code
     * alterTable}).
     */
    private static boolean declaresOwnAnswer(Method method) {
        try {
            AbstractReadOnlyCatalog.class.getDeclaredMethod(
                    method.getName(), method.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return Arrays.stream(AbstractReadOnlyCatalog.class.getDeclaredMethods())
                    .anyMatch(declared -> declared.getName().equals(method.getName()));
        }
    }

    private static void invoke(Method method, Catalog catalog, Object[] arguments)
            throws Throwable {
        try {
            method.invoke(catalog, arguments);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * The answers docs/adr/0168 fixes stay fixed: a subclass can override only what it must supply,
     * plus the two table-statistics getters the ADR's reopen condition leaves open.
     */
    @Test
    void everyAnswerTheSkeletonGivesIsFinal() {
        List<String> overridable = new ArrayList<>();
        for (Method method : AbstractReadOnlyCatalog.class.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            if (method.isSynthetic()
                    || Modifier.isPrivate(modifiers)
                    || Modifier.isStatic(modifiers)
                    || Modifier.isAbstract(modifiers)
                    || Modifier.isFinal(modifiers)) {
                continue;
            }
            overridable.add(method.getName());
        }

        assertThat(overridable)
                .containsExactlyInAnyOrder("getTableStatistics", "getTableColumnStatistics");
    }

    @Test
    void partitionsFunctionsProceduresAndStatisticsAreAnsweredWithoutTheClient() throws Exception {
        StubCatalog catalog = catalog();

        assertThat(catalog.listPartitions(EVENTS)).isEmpty();
        assertThat(catalog.listPartitions(EVENTS, SPEC)).isEmpty();
        assertThat(catalog.listPartitionsByFilter(EVENTS, Collections.emptyList())).isEmpty();
        assertThat(catalog.partitionExists(EVENTS, SPEC)).isFalse();
        assertThatThrownBy(() -> catalog.getPartition(EVENTS, SPEC))
                .isInstanceOf(PartitionNotExistException.class);
        assertThat(catalog.listFunctions("analytics")).isEmpty();
        assertThat(catalog.functionExists(EVENTS)).isFalse();
        assertThatThrownBy(() -> catalog.getFunction(EVENTS))
                .isInstanceOf(FunctionNotExistException.class);
        assertThat(catalog.listProcedures("analytics")).isEmpty();
        assertThatThrownBy(() -> catalog.getProcedure(EVENTS))
                .isInstanceOf(ProcedureNotExistException.class);
        assertThat(catalog.getTableStatistics(EVENTS)).isSameAs(CatalogTableStatistics.UNKNOWN);
        assertThat(catalog.getTableColumnStatistics(EVENTS))
                .isSameAs(CatalogColumnStatistics.UNKNOWN);
        assertThat(catalog.getPartitionStatistics(EVENTS, SPEC))
                .isSameAs(CatalogTableStatistics.UNKNOWN);
        assertThat(catalog.getPartitionColumnStatistics(EVENTS, SPEC))
                .isSameAs(CatalogColumnStatistics.UNKNOWN);
        assertThat(opened).hasValue(0);
    }

    @Test
    void viewsAndMaterializedTablesAreEmptyForADatabaseThatExists() throws Exception {
        StubCatalog catalog = catalog();

        assertThat(catalog.listViews("analytics")).isEmpty();
        assertThat(catalog.listMaterializedTables("analytics")).isEmpty();
        assertThatThrownBy(() -> catalog.listViews("missing"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.listMaterializedTables("missing"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    /** One database, {@code analytics}, and no tables; records the client each call received. */
    private static final class StubCatalog extends AbstractReadOnlyCatalog<String> {

        private final List<String> clients = Collections.synchronizedList(new ArrayList<>());

        StubCatalog(ClientOpener<String> opener, ClientCloser<String> closer) {
            super("stub", "analytics", "Stub", READ_ONLY, opener, closer);
        }

        @Override
        public Optional<Factory> getFactory() {
            return Optional.empty();
        }

        @Override
        public List<String> listDatabases() {
            clients.add(client());
            return Collections.singletonList("analytics");
        }

        @Override
        public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
            if (!databaseExists(databaseName)) {
                throw new DatabaseNotExistException(name(), databaseName);
            }
            return new ReadOnlyCatalogDatabase(Collections.emptyMap(), null);
        }

        @Override
        public boolean databaseExists(String databaseName) {
            return "analytics".equals(databaseName);
        }

        @Override
        public List<String> listTables(String databaseName) {
            return Collections.emptyList();
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
            throw new TableNotExistException(name(), tablePath);
        }

        @Override
        public boolean tableExists(ObjectPath tablePath) {
            return false;
        }
    }
}
