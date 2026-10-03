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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.bigtable.table.BigtableDynamicTableFactory;
import io.github.flink.gcp.connector.bigtable.table.CatalogKeyType;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link BigtableCatalog} against an in-memory instance. */
class BigtableCatalogTest {

    private static final Type RAW = Type.getDefaultInstance();

    private final FakeBigtableCatalogClient client = new FakeBigtableCatalogClient();

    private BigtableCatalog catalog() {
        return catalog(Collections.emptyMap());
    }

    private BigtableCatalog catalog(Map<String, String> carried) {
        return new BigtableCatalog(
                "bt", "proj", "inst", CatalogKeyType.BYTES, carried, () -> client);
    }

    @Test
    void opensNoClientBeforeTheFirstTableRequest() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        BigtableCatalog catalog =
                new BigtableCatalog(
                        "bt",
                        "proj",
                        "inst",
                        CatalogKeyType.BYTES,
                        Collections.emptyMap(),
                        () -> {
                            opened.incrementAndGet();
                            return client;
                        });

        catalog.open();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("inst");
        assertThat(catalog.listDatabases()).containsExactly("inst");
        assertThat(catalog.databaseExists("inst")).isTrue();
        catalog.getDatabase("inst");
        assertThat(opened).hasValue(0);

        catalog.listTables("inst");
        assertThat(opened).hasValue(1);
        catalog.close();
        assertThat(client.closed).isTrue();
    }

    /** Calcite asks the current catalog about other catalogs' names; none of them is a database. */
    @Test
    void theInstanceIsTheOnlyDatabaseAndOtherNamesAskNothing() {
        BigtableCatalog catalog = catalog();

        assertThat(catalog.databaseExists("default_catalog")).isFalse();
        assertThat(catalog.databaseExists("INST")).isFalse();
        assertThatThrownBy(() -> catalog.getDatabase("other"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.listTables("other"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("other", "orders")))
                .isInstanceOf(TableNotExistException.class);
        assertThat(catalog.tableExists(new ObjectPath("other", "orders"))).isFalse();
        assertThat(client.requests).isEmpty();
    }

    @Test
    void listsTheInstancesTables() throws Exception {
        client.table("orders", "cf", RAW).table("events");

        assertThat(catalog().listTables("inst")).containsExactly("orders", "events");
    }

    @Test
    void resolvesATableWithTheConnectorIdentityAndTheCarriedOptions() throws Exception {
        client.table("orders", "cf", RAW);
        Map<String, String> carried = new LinkedHashMap<>();
        carried.put("emulator-endpoint", "localhost:8086");

        CatalogTable table =
                (CatalogTable) catalog(carried).getTable(new ObjectPath("inst", "orders"));

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        expected.put("project", "proj");
        expected.put("instance", "inst");
        expected.put("table", "orders");
        expected.put("emulator-endpoint", "localhost:8086");
        assertThat(table.getOptions()).containsExactlyEntriesOf(expected);
        assertThat(table.getUnresolvedSchema().getColumns()).hasSize(2);
        assertThat(table.getUnresolvedSchema().getPrimaryKey()).isPresent();
    }

    /**
     * The key file reaches every resolved table under the connector's key, so its runtime uses it.
     */
    @Test
    void theCarriedOptionsAreTheCredentialAndEmulatorOptionsUnderTheConnectorsKeys() {
        Map<String, String> keyFile = new LinkedHashMap<>();
        keyFile.put("service-account-key-file", "/key.json");
        Map<String, String> emulator = new LinkedHashMap<>();
        emulator.put("emulator-endpoint", "localhost:8086");

        assertThat(BigtableCatalog.carriedOptions("/key.json", null))
                .containsExactlyEntriesOf(keyFile);
        assertThat(BigtableCatalog.carriedOptions(null, "localhost:8086"))
                .containsExactlyEntriesOf(emulator);
        assertThat(BigtableCatalog.carriedOptions(null, null)).isEmpty();
    }

    /** A client that cannot open fails with the open failure, not wrapped in the request's. */
    @Test
    void aClientThatCannotOpenFailsNamingTheOpenNotTheRequest() {
        BigtableCatalog catalog =
                new BigtableCatalog(
                        "bt",
                        "proj",
                        "inst",
                        CatalogKeyType.BYTES,
                        Collections.emptyMap(),
                        () -> {
                            throw new java.io.IOException("no key");
                        });

        assertThatThrownBy(() -> catalog.listTables("inst"))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to open the Bigtable client of catalog 'bt'.");
    }

    /** The grammar's accepted edges reach the service; a name it rejects asks nothing. */
    @Test
    void everyNameInsideTheTableIdGrammarIsAskedFor() {
        BigtableCatalog catalog = catalog();

        for (String name : new String[] {"t", "_t", "9", "a.b", "a-b", "a_b"}) {
            assertThat(catalog.tableExists(new ObjectPath("inst", name))).as(name).isFalse();
        }
        assertThat(client.requests)
                .containsExactly(
                        "columnFamilies t",
                        "columnFamilies _t",
                        "columnFamilies 9",
                        "columnFamilies a.b",
                        "columnFamilies a-b",
                        "columnFamilies a_b");
    }

    @Test
    void aMissingTableIsTableNotExist() throws Exception {
        BigtableCatalog catalog = catalog();

        assertThat(catalog.tableExists(new ObjectPath("inst", "missing"))).isFalse();
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("inst", "missing")))
                .isInstanceOf(TableNotExistException.class);
        // The catalog asks only for the table; any listing that follows is the planner's.
        assertThat(client.requests)
                .containsExactly("columnFamilies missing", "columnFamilies missing");
    }

    /**
     * A name outside the table-id grammar, such as one with a '/', never reaches a resource name.
     */
    @Test
    void aNameOutsideTheTableIdGrammarIsNoTableAndAsksNothing() {
        BigtableCatalog catalog = catalog();

        for (String name : new String[] {"a/b", "-orders", "a b", repeat('t', 51)}) {
            assertThat(catalog.tableExists(new ObjectPath("inst", name))).as(name).isFalse();
        }
        assertThat(catalog.tableExists(new ObjectPath("inst", repeat('t', 50)))).isFalse();
        assertThat(client.requests).containsExactly("columnFamilies " + repeat('t', 50));
    }

    @Test
    void aFamilyNamedLikeTheRowKeyFailsTheLookupNamingTheTable() {
        client.table("clash", "_key", RAW);

        assertThatThrownBy(() -> catalog().getTable(new ObjectPath("inst", "clash")))
                .isInstanceOf(CatalogException.class)
                .hasMessageStartingWith("Bigtable table 'proj/inst/clash' cannot be resolved:")
                .hasMessageContaining("column family '_key'");
    }

    @Test
    void aServiceFailureIsACatalogExceptionNamingWhatWasAsked() {
        client.failure =
                ApiExceptionFactory.createException(
                        "denied", null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false);
        BigtableCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.listTables("inst"))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to list the tables of Bigtable instance 'proj/inst'.")
                .hasCauseReference(client.failure);
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("inst", "orders")))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to read Bigtable table 'proj/inst/orders'.");
    }

    private static org.apache.flink.table.api.TableEnvironment environment(
            BigtableCatalog catalog) {
        org.apache.flink.table.api.TableEnvironment table =
                org.apache.flink.table.api.TableEnvironment.create(
                        org.apache.flink.table.api.EnvironmentSettings.inBatchMode());
        table.registerCatalog("bt", catalog);
        table.useCatalog("bt");
        return table;
    }

    /**
     * A query that names a missing table makes the planner list the database's tables after {@code
     * getTable} reports it missing, and the listing's failure is what the query reports: on a
     * missing instance, whose table lookup answers {@code NOT_FOUND}, that names the instance
     * (measured against the service by {@code BigtableCatalogMissingInstanceRealGcpITCase}).
     */
    @Test
    void aQueryOnAMissingTableReportsAFailingListing() {
        client.listFailure =
                ApiExceptionFactory.createException(
                        "instance missing", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
        org.apache.flink.table.api.TableEnvironment table = environment(catalog());

        assertThatThrownBy(() -> table.executeSql("SELECT * FROM orders"))
                .hasStackTraceContaining(
                        "Failed to list the tables of Bigtable instance 'proj/inst'.");
        assertThat(client.requests).startsWith("columnFamilies orders").contains("listTables");
    }

    /**
     * DESCRIBE resolves the table through Flink's catalog manager, which reports a missing table
     * without listing, so on a missing instance it names the table, not the instance.
     */
    @Test
    void aDescribeOfAMissingTableReportsTheTableWithoutListing() {
        client.listFailure =
                ApiExceptionFactory.createException(
                        "instance missing", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
        org.apache.flink.table.api.TableEnvironment table = environment(catalog());

        assertThatThrownBy(() -> table.executeSql("DESCRIBE orders"))
                .hasMessageContaining("Tables or views with the identifier 'bt.inst.orders'")
                .hasMessageContaining("doesn't exist");
        assertThat(client.requests).containsExactly("columnFamilies orders");
    }

    @Test
    void mutationsAreRefusedWithTheBigtableMessage() {
        assertThatThrownBy(() -> catalog().dropTable(new ObjectPath("inst", "orders"), true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("The Bigtable catalog is read-only");
    }

    private static String repeat(char c, int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }
}
