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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.ViewDefinition;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.TableAnswer;
import io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link BigQueryCatalog} against a stubbed REST client. */
class BigQueryCatalogTest {

    private static final String PROJECT = "stub-project";
    private static final ObjectPath EVENTS = new ObjectPath("analytics", "events");

    private final StubBigQuery bigquery = new StubBigQuery();

    private BigQueryCatalog catalog() {
        return catalog(Collections.emptyMap());
    }

    private BigQueryCatalog catalog(Map<String, String> carried) {
        return new BigQueryCatalog("bq", PROJECT, "analytics", carried, () -> bigquery);
    }

    @Test
    void createsNoClientBeforeTheFirstMetadataCall() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        BigQueryCatalog catalog =
                new BigQueryCatalog(
                        "bq",
                        PROJECT,
                        "analytics",
                        Collections.emptyMap(),
                        () -> {
                            opened.incrementAndGet();
                            return bigquery;
                        });

        // What CREATE CATALOG and USE CATALOG call, plus the calls answered without the service.
        catalog.open();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("analytics");
        assertThat(catalog.functionExists(new ObjectPath("analytics", "f"))).isFalse();
        assertThat(catalog.getTableStatistics(EVENTS)).isSameAs(CatalogTableStatistics.UNKNOWN);
        assertThat(opened).hasValue(0);

        bigquery.listedDatasets.add("analytics");
        catalog.listDatabases();
        catalog.listDatabases();
        assertThat(opened).hasValue(1);
    }

    @Test
    void listsTheProjectsDatasetsAsDatabases() {
        bigquery.listedDatasets.addAll(Arrays.asList("analytics", "staging"));

        assertThat(catalog().listDatabases()).containsExactly("analytics", "staging");
        assertThat(bigquery.listDatasetsCalls).containsExactly(PROJECT);
    }

    @Test
    void aListingFailureIsACatalogExceptionNamingTheProject() {
        bigquery.listDatasetsFailure = new BigQueryException(403, "denied");

        assertThatThrownBy(() -> catalog().listDatabases())
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("'stub-project'")
                .hasCauseInstanceOf(BigQueryException.class);
    }

    @Test
    void aDatabaseExistsWhenItsDatasetDoes() throws Exception {
        bigquery.locatedDataset(DatasetId.of(PROJECT, "analytics"), "US");
        BigQueryCatalog catalog = catalog();

        assertThat(catalog.databaseExists("analytics")).isTrue();
        assertThat(catalog.listMaterializedTables("analytics")).isEmpty();
        assertThat(catalog.databaseExists("absent")).isFalse();
        assertThat(catalog.getDatabase("analytics").getProperties()).isEmpty();
        assertThatThrownBy(() -> catalog.getDatabase("absent"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void aNameOutsideTheDatasetGrammarIsNoDatabaseAndAsksNothing() {
        // Calcite asks the current catalog about a qualified name's first part, which may be
        // another catalog's name; a hyphen cannot be in a dataset id.
        assertThat(catalog().databaseExists("hive-prod")).isFalse();
        assertThat(bigquery.getDatasetCalls).isEmpty();
    }

    @Test
    void listingMaterializedTablesOfAMissingDatasetIsDatabaseNotExist() {
        assertThatThrownBy(() -> catalog().listMaterializedTables("absent"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void aClientThatCannotBeBuiltIsACatalogException() {
        BigQueryCatalog catalog =
                new BigQueryCatalog(
                        "bq",
                        PROJECT,
                        "analytics",
                        Collections.emptyMap(),
                        () -> {
                            throw new IllegalArgumentException("A project ID is required");
                        });

        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("catalog 'bq'")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void listsTablesAndViewsTogetherAndNoViewsSeparately() throws Exception {
        bigquery.tablesListed(DatasetId.of(PROJECT, "analytics"), "events", "daily_view");
        bigquery.locatedDataset(DatasetId.of(PROJECT, "analytics"), "US");
        BigQueryCatalog catalog = catalog();

        assertThat(catalog.listTables("analytics")).containsExactly("events", "daily_view");
        assertThat(catalog.listViews("analytics")).isEmpty();
    }

    @Test
    void listingAMissingDatasetIsDatabaseNotExist() {
        BigQueryCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.listTables("absent"))
                .isInstanceOf(DatabaseNotExistException.class);
        assertThatThrownBy(() -> catalog.listViews("absent"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void resolvesATableToTheConnectorOptionsItsSchemaAndItsComment() throws Exception {
        bigquery.tablesAnswering(
                TableAnswer.described(
                        StandardTableDefinition.of(
                                com.google.cloud.bigquery.Schema.of(
                                        Field.of("id", StandardSQLTypeName.STRING),
                                        Field.of("amount", StandardSQLTypeName.INT64))),
                        "Raw events",
                        null));
        Map<String, String> carried = new LinkedHashMap<>();
        carried.put("emulator-endpoint", "localhost:9060");
        carried.put("emulator-rest-endpoint", "localhost:9050");

        CatalogBaseTable table = catalog(carried).getTable(EVENTS);

        assertThat(table).isInstanceOf(CatalogTable.class);
        assertThat(table.getComment()).isEqualTo("Raw events");
        assertThat(table.getOptions())
                .containsExactly(
                        Map.entry("connector", BigQueryDynamicTableFactory.IDENTIFIER),
                        Map.entry("project", PROJECT),
                        Map.entry("dataset", "analytics"),
                        Map.entry("table", "events"),
                        Map.entry("emulator-endpoint", "localhost:9060"),
                        Map.entry("emulator-rest-endpoint", "localhost:9050"));
        assertThat(table.getUnresolvedSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING())
                                .column("amount", DataTypes.BIGINT())
                                .build());
        assertThat(bigquery.getTableCalls)
                .containsExactly(TableId.of(PROJECT, "analytics", "events"));
    }

    @Test
    void aViewOrMaterializedViewResolvesAsATableThatMaterializesIt() throws Exception {
        com.google.cloud.bigquery.Schema schema =
                com.google.cloud.bigquery.Schema.of(Field.of("day", StandardSQLTypeName.DATE));
        bigquery.tablesAnswering(
                TableAnswer.described(
                        ViewDefinition.newBuilder("SELECT day FROM t").setSchema(schema).build(),
                        null,
                        null),
                TableAnswer.described(
                        MaterializedViewDefinition.newBuilder("SELECT day FROM t")
                                .setSchema(schema)
                                .build(),
                        null,
                        null));
        BigQueryCatalog catalog = catalog();

        assertThat(catalog.getTable(EVENTS).getOptions())
                .containsEntry("scan.materialize-views", "true");
        assertThat(catalog.getTable(EVENTS).getOptions())
                .containsEntry("scan.materialize-views", "true");
    }

    @Test
    void aStandardTableDoesNotMaterialize() throws Exception {
        bigquery.tablesAnswering(
                TableAnswer.described(
                        StandardTableDefinition.of(
                                com.google.cloud.bigquery.Schema.of(
                                        Field.of("id", StandardSQLTypeName.STRING))),
                        null,
                        null));

        assertThat(catalog().getTable(EVENTS).getOptions())
                .doesNotContainKey("scan.materialize-views");
    }

    @Test
    void aPrimaryKeyOnNullableColumnsResolvesWithThoseColumnsNotNull() throws Exception {
        // The connector's own CDC tables carry NULLABLE key columns unless
        // sink.derive-required-columns is set; Flink rejects a nullable primary-key column.
        bigquery.tablesAnswering(
                TableAnswer.described(
                        StandardTableDefinition.of(
                                com.google.cloud.bigquery.Schema.of(
                                        Field.of("id", StandardSQLTypeName.STRING),
                                        Field.of("amount", StandardSQLTypeName.INT64))),
                        null,
                        Arrays.asList("id")));

        Schema schema = catalog().getTable(EVENTS).getUnresolvedSchema();

        assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build());
    }

    @Test
    void anUnsupportedColumnFailsTheTableNamingTheTableAndTheColumn() {
        bigquery.tablesAnswering(
                TableAnswer.described(
                        StandardTableDefinition.of(
                                com.google.cloud.bigquery.Schema.of(
                                        Field.of("id", StandardSQLTypeName.STRING),
                                        Field.of("wait", StandardSQLTypeName.INTERVAL))),
                        null,
                        null));

        assertThatThrownBy(() -> catalog().getTable(EVENTS))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("'stub-project.analytics.events'")
                .hasMessageContaining("Column 'wait' has BigQuery type INTERVAL");
    }

    @Test
    void anExistingTableExists() {
        bigquery.tablesAnswering(TableAnswer.existing());

        assertThat(catalog().tableExists(EVENTS)).isTrue();
    }

    @Test
    void aMissingTableIsTableNotExist() {
        bigquery.tablesAnswering(TableAnswer.absent());
        BigQueryCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.getTable(EVENTS))
                .isInstanceOf(TableNotExistException.class);
        assertThat(catalog.tableExists(EVENTS)).isFalse();
    }

    @Test
    void aTableLookupFailureIsACatalogException() {
        bigquery.tablesAnswering(TableAnswer.failing(new BigQueryException(500, "boom")));

        assertThatThrownBy(() -> catalog().getTable(EVENTS))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("'stub-project.analytics.events'");
    }

    @Test
    void statisticsAreUnknownAndFunctionsAbsentWithoutAskingTheService() throws Exception {
        BigQueryCatalog catalog = catalog();

        assertThat(catalog.getTableStatistics(EVENTS)).isSameAs(CatalogTableStatistics.UNKNOWN);
        assertThat(catalog.getTableColumnStatistics(EVENTS))
                .isSameAs(CatalogColumnStatistics.UNKNOWN);
        assertThat(catalog.listFunctions("analytics")).isEmpty();
        assertThat(catalog.listProcedures("analytics")).isEmpty();
        assertThat(catalog.listPartitions(EVENTS)).isEmpty();
        assertThatThrownBy(() -> catalog.getFunction(new ObjectPath("analytics", "f")))
                .isInstanceOf(FunctionNotExistException.class);
        assertThat(bigquery.getTableCalls).isEmpty();
        assertThat(bigquery.getDatasetCalls).isEmpty();
    }

    @Test
    void everyMutationIsRefused() {
        BigQueryCatalog catalog = catalog();

        assertThatThrownBy(() -> catalog.createTable(EVENTS, null, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("read-only");
        assertThatThrownBy(() -> catalog.dropTable(EVENTS, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.alterTable(EVENTS, null, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.renameTable(EVENTS, "other", false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.createDatabase("d", null, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.dropDatabase("d", false, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.alterDatabase("d", null, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.createFunction(EVENTS, null, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> catalog.alterTableStatistics(EVENTS, null, false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void theFactoryIsTheConnectors() {
        assertThat(catalog().getFactory()).containsInstanceOf(BigQueryDynamicTableFactory.class);
    }

    @Test
    void carriesOnlyTheOptionsThatWereSet() {
        assertThat(BigQueryCatalog.carriedOptions(null, null, null, null)).isEmpty();
        assertThat(BigQueryCatalog.carriedOptions("/key.json", null, null, "billing"))
                .containsExactly(
                        Map.entry("service-account-key-file", "/key.json"),
                        Map.entry("scan.parent-project", "billing"));
    }
}
