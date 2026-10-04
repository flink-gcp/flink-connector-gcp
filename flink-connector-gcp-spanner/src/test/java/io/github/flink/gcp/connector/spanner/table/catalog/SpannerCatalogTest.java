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

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.table.SpannerDynamicTableFactory;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.NamedTypeKind;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.flink.gcp.connector.spanner.table.catalog.FakeSpannerCatalogClient.column;
import static io.github.flink.gcp.connector.spanner.table.catalog.FakeSpannerCatalogClient.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link SpannerCatalog} against an in-memory instance. */
class SpannerCatalogTest {

    private final FakeSpannerCatalogClient client =
            new FakeSpannerCatalogClient()
                    .database("gsql", Dialect.GOOGLE_STANDARD_SQL)
                    .database("pg", Dialect.POSTGRESQL);

    private SpannerCatalog catalog() {
        return catalog(Collections.emptyMap());
    }

    private SpannerCatalog catalog(Map<String, String> carried) {
        return new SpannerCatalog("sp", "proj", "inst", "gsql", carried, () -> client);
    }

    @Test
    void opensNoClientBeforeTheFirstMetadataCall() {
        AtomicInteger opened = new AtomicInteger();
        SpannerCatalog catalog =
                new SpannerCatalog(
                        "sp",
                        "proj",
                        "inst",
                        "gsql",
                        Collections.emptyMap(),
                        () -> {
                            opened.incrementAndGet();
                            return client;
                        });

        catalog.open();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("gsql");
        assertThat(opened).hasValue(0);

        assertThat(catalog.listDatabases()).containsExactly("gsql", "pg");
        catalog.close();
        assertThat(client.closed).isTrue();
    }

    @Test
    void aNameOutsideTheDatabaseIdGrammarIsNoDatabaseAndAsksNothing() {
        SpannerCatalog catalog = catalog();

        // Calcite asks the current catalog about another catalog's name, and about names a
        // database id cannot have.
        assertThat(catalog.databaseExists("default_catalog")).isFalse();
        assertThat(catalog.databaseExists("Orders")).isFalse();
        assertThat(client.requests).containsExactly("dialect default_catalog");
    }

    @Test
    void aDatabasesDialectIsAskedOnce() throws Exception {
        SpannerCatalog catalog = catalog();

        catalog.databaseExists("gsql");
        catalog.getDatabase("gsql");
        catalog.listTables("gsql");

        assertThat(client.requests).containsExactly("dialect gsql", "listTables gsql");
    }

    @Test
    void aMissingDatabaseIsDatabaseNotExist() {
        SpannerCatalog catalog = catalog();

        assertThat(catalog.databaseExists("missing")).isFalse();
        assertThatThrownBy(() -> catalog.getDatabase("missing"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.listTables("missing"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("missing", "t")))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void listsDefaultSchemaTablesAloneAndNamedSchemaTablesQualifiedInCanonicalQuoting()
            throws Exception {
        client.table("gsql", "", "Orders", key("Id", "INT64", 1))
                .table("gsql", "sales", "Order Items", key("Id", "INT64", 1))
                .table("pg", "public", "Orders", key("id", "bigint", 1))
                .table("pg", "public", "orders", key("id", "bigint", 1))
                .table("pg", "Sales", "items", key("id", "bigint", 1));
        SpannerCatalog catalog = catalog();

        assertThat(catalog.listTables("gsql")).containsExactly("Orders", "sales.`Order Items`");
        assertThat(catalog.listTables("pg"))
                .containsExactly("\"Orders\"", "orders", "\"Sales\".items");
    }

    @Test
    void everyListedNameResolvesToTheTableItCameFrom() throws Exception {
        client.table("pg", "public", "Orders", key("id", "bigint", 1), column("a", "bigint"))
                .table("pg", "public", "orders", key("id", "bigint", 1), column("b", "bigint"))
                .table("pg", "Sales", "items", key("id", "bigint", 1), column("c", "bigint"));
        SpannerCatalog catalog = catalog();

        for (String name : catalog.listTables("pg")) {
            CatalogBaseTable table = catalog.getTable(new ObjectPath("pg", name));
            assertThat(table.getUnresolvedSchema().getColumns()).as(name).hasSize(2);
        }
        assertThat(
                        catalog.getTable(new ObjectPath("pg", "\"Orders\""))
                                .getUnresolvedSchema()
                                .getColumns()
                                .get(1)
                                .getName())
                .isEqualTo("a");
        assertThat(
                        catalog.getTable(new ObjectPath("pg", "orders"))
                                .getUnresolvedSchema()
                                .getColumns()
                                .get(1)
                                .getName())
                .isEqualTo("b");
    }

    @Test
    void googleSqlComparesNamesCaseInsensitivelyAndPostgreSqlExactly() throws Exception {
        client.table("gsql", "", "Orders", key("Id", "INT64", 1))
                .table("pg", "public", "Orders", key("id", "bigint", 1));
        SpannerCatalog catalog = catalog();

        assertThat(catalog.tableExists(new ObjectPath("gsql", "orders"))).isTrue();
        assertThat(catalog.getTable(new ObjectPath("gsql", "ORDERS")).getOptions())
                .containsEntry("table", "Orders");
        // An unquoted PostgreSQL name folds to lower case, which this table is not.
        assertThat(catalog.tableExists(new ObjectPath("pg", "Orders"))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath("pg", "\"Orders\""))).isTrue();
    }

    @Test
    void aDefaultSchemaTableCarriesItsNativeNameAndANamedOneItsCanonicalParts() throws Exception {
        client.table("gsql", "", "Orders", key("Id", "INT64", 1))
                .table("pg", "Sales", "Items", key("id", "bigint", 1));
        Map<String, String> carried = SpannerCatalog.carriedOptions("/key.json", null);
        SpannerCatalog catalog = catalog(carried);

        assertThat(catalog.getTable(new ObjectPath("gsql", "Orders")).getOptions())
                .containsOnly(
                        Map.entry("connector", SpannerDynamicTableFactory.IDENTIFIER),
                        Map.entry("project", "proj"),
                        Map.entry("instance", "inst"),
                        Map.entry("database", "gsql"),
                        Map.entry("dialect", "GOOGLE_STANDARD_SQL"),
                        Map.entry("table", "Orders"),
                        Map.entry("service-account-key-file", "/key.json"));
        assertThat(catalog.getTable(new ObjectPath("pg", "\"Sales\".\"Items\"")).getOptions())
                .containsEntry("dialect", "POSTGRESQL")
                .containsEntry("named-schema", "\"Sales\"")
                .doesNotContainKey("schema")
                .containsEntry("table", "\"Items\"")
                .containsEntry("service-account-key-file", "/key.json");
    }

    @Test
    void aNameThatIsNotACanonicalOneOrTwoPartNameIsTableNotExist() {
        SpannerCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("gsql", "a.b.c")))
                .isInstanceOf(TableNotExistException.class);
        assertThat(catalog.tableExists(new ObjectPath("pg", "\"unterminated"))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath("gsql", "`unterminated"))).isFalse();
        assertThat(client.requests).noneMatch(request -> request.startsWith("table "));
    }

    @Test
    void protoAndEnumColumnsAreClassifiedFromTheProtoBundleOnlyWhenATableHasThem()
            throws Exception {
        Map<String, NamedTypeKind> kinds = new HashMap<>();
        kinds.put("example.Event", NamedTypeKind.PROTO);
        kinds.put("example.Status", NamedTypeKind.ENUM);
        client.protoBundleTypes.put("gsql", kinds);
        client.table("gsql", "", "Plain", key("Id", "INT64", 1))
                .table(
                        "gsql",
                        "",
                        "Events",
                        key("Id", "INT64", 1),
                        column("Ev", "`example.Event`"),
                        column("St", "`example.Status`"));
        SpannerCatalog catalog = catalog();

        catalog.getTable(new ObjectPath("gsql", "Plain"));
        assertThat(client.requests).doesNotContain("protoBundleTypes gsql");

        assertThat(catalog.getTable(new ObjectPath("gsql", "Events")).getOptions())
                .containsEntry("proto-type-names", "Ev:example.Event")
                .containsEntry("enum-type-names", "St:example.Status");
    }

    @Test
    void anUnsupportedColumnFailsTheLookupNamingTheTableAndColumn() {
        client.table("gsql", "", "Events", key("Id", "INT64", 1), column("Iv", "INTERVAL"));
        SpannerCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("gsql", "Events")))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Spanner table 'proj/inst/gsql/Events' cannot be resolved")
                .hasMessageContaining("Column 'Iv' has Spanner type INTERVAL");
    }

    @Test
    void aServiceFailureIsACatalogExceptionNamingWhatWasAsked() {
        client.failure = FakeSpannerCatalogClient.unavailable();
        SpannerCatalog catalog = catalog();

        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to list the databases of Spanner instance 'proj/inst'.");
        assertThatThrownBy(() -> catalog.databaseExists("gsql"))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to read Spanner database 'proj/inst/gsql'.");
    }

    @Test
    void everyFailureOfARequestIsACatalogExceptionNamingWhatWasAsked() throws Exception {
        client.protoBundleTypes.put("gsql", new HashMap<>());
        client.table("gsql", "", "Events", key("Id", "INT64", 1), column("Ev", "`x.E`"));
        SpannerCatalog catalog = catalog();
        catalog.databaseExists("gsql");

        client.failure = FakeSpannerCatalogClient.unavailable();
        assertThatThrownBy(() -> catalog.listTables("gsql"))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to list the tables of Spanner database 'proj/inst/gsql'.");
        assertThatThrownBy(() -> catalog.tableExists(new ObjectPath("gsql", "Events")))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to read Spanner table 'proj/inst/gsql/Events'.");

        // A failure that is not the service's, such as a proto bundle that does not parse, too.
        client.failure = null;
        client.protoBundleTypes.remove("gsql");
        FakeSpannerCatalogClient failingBundle =
                new FakeSpannerCatalogClient() {
                    @Override
                    public Map<String, NamedTypeKind> protoBundleTypes(String database) {
                        throw new IllegalStateException("descriptors do not parse");
                    }
                }.database("gsql", Dialect.GOOGLE_STANDARD_SQL)
                        .table("gsql", "", "Events", key("Id", "INT64", 1), column("Ev", "`x.E`"));
        SpannerCatalog bundleCatalog =
                new SpannerCatalog(
                        "sp", "proj", "inst", "gsql", Collections.emptyMap(), () -> failingBundle);
        assertThatThrownBy(() -> bundleCatalog.getTable(new ObjectPath("gsql", "Events")))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Failed to read the proto bundle of Spanner database 'proj/inst/gsql'.")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void aDatabaseDroppedWhileTheCatalogIsOpenIsReportedMissingAndForgotten() throws Exception {
        client.table("gsql", "", "Orders", key("Id", "INT64", 1));
        SpannerCatalog catalog = catalog();
        assertThat(catalog.databaseExists("gsql")).isTrue();

        client.drop("gsql");

        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("gsql", "Orders")))
                .isInstanceOf(TableNotExistException.class);
        assertThat(catalog.databaseExists("gsql")).isFalse();
        assertThatThrownBy(() -> catalog.listTables("gsql"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void aMissingDatabaseIsNotRememberedSoOneCreatedLaterIsFound() {
        SpannerCatalog catalog = catalog();
        assertThat(catalog.databaseExists("later")).isFalse();

        client.database("later", Dialect.POSTGRESQL);

        assertThat(catalog.databaseExists("later")).isTrue();
    }

    @Test
    void everyMutationIsRefusedNamingSpanner() {
        assertThatThrownBy(() -> catalog().createDatabase("d", null, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The Spanner catalog is read-only; create Spanner tables and databases"
                                + " through Spanner itself.");
    }

    @Test
    void theFactoryIsTheConnectors() {
        assertThat(catalog().getFactory()).containsInstanceOf(SpannerDynamicTableFactory.class);
    }
}
