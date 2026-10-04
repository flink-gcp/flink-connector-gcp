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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code bigtable} catalog against the emulator: listing and describing an instance's tables,
 * reading them as the equivalent {@code ROW} DDL reads them, writing through them, and the {@code
 * string} key type that lets GoogleSQL for Bigtable's own example query run unchanged.
 *
 * <p>The emulator has no aggregate families, so their mapping is held by {@link
 * BigtableCatalogSchemaTest}.
 */
class BigtableCatalogITCase extends AbstractBigtableEmulatorITCase {

    /** Every environment a test opened, so each catalog's admin client is closed after it. */
    private static final List<TableEnvironment> CATALOGS = new ArrayList<>();

    /** Opens the catalog in batch mode; a {@code null} key type leaves the option unset. */
    private static TableEnvironment catalog(@Nullable String keyType) {
        return catalog(EnvironmentSettings.inBatchMode(), keyType);
    }

    private static TableEnvironment catalog(
            EnvironmentSettings settings, @Nullable String keyType) {
        TableEnvironment table = TableEnvironment.create(settings);
        table.executeSql(
                "CREATE CATALOG bt WITH ('type' = 'bigtable', 'project' = '"
                        + PROJECT
                        + "', 'instance' = '"
                        + INSTANCE
                        + (keyType == null ? "" : "', 'key-type' = '" + keyType)
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        table.executeSql("USE CATALOG bt");
        CATALOGS.add(table);
        return table;
    }

    @AfterEach
    void closeCatalogs() throws Exception {
        List<AutoCloseable> catalogs = new ArrayList<>();
        for (TableEnvironment table : CATALOGS) {
            table.getCatalog("bt").ifPresent(catalog -> catalogs.add(catalog::close));
        }
        try {
            // Every catalog is closed even if one fails; the first failure is rethrown.
            Closers.closeAll(catalogs);
        } finally {
            CATALOGS.clear();
        }
    }

    private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    /** Every cell as {@code key/family:qualifier=value}, key and qualifier and value UTF-8. */
    private static List<String> cells(TableDestination destination) {
        List<String> cells = new ArrayList<>();
        for (com.google.cloud.bigtable.data.v2.models.Row row : readRows(destination)) {
            for (RowCell cell : row.getCells()) {
                cells.add(
                        row.getKey().toStringUtf8()
                                + '/'
                                + cell.getFamily()
                                + ':'
                                + cell.getQualifier().toStringUtf8()
                                + '='
                                + cell.getValue().toStringUtf8());
            }
        }
        return cells;
    }

    @Test
    void showsTheInstanceAsTheDatabaseAndListsAndDescribesItsTables() throws Exception {
        createTable("described", "profile", "address");
        // No key-type: the default is what DESCRIBE shows.
        TableEnvironment table = catalog(null);

        assertThat(collect(table, "SHOW DATABASES")).containsExactly(Row.of(INSTANCE));
        assertThat(collect(table, "SHOW TABLES")).contains(Row.of("described"));
        List<String> described =
                collect(table, "DESCRIBE described").stream()
                        .map(
                                row ->
                                        row.getField(0)
                                                + " "
                                                + row.getField(1)
                                                + " nullable="
                                                + row.getField(2)
                                                + " "
                                                + row.getField(3))
                        .collect(Collectors.toList());
        assertThat(described)
                .containsExactly(
                        "_key BYTES nullable=false PRI(_key)",
                        "address MAP<BYTES, BYTES> nullable=true null",
                        "profile MAP<BYTES, BYTES> nullable=true null");
    }

    /** The issue's acceptance: a map value read with CAST is what the equivalent ROW DDL reads. */
    @Test
    void aCastMapValueReadsWhatTheEquivalentRowDdlReads() throws Exception {
        TableDestination destination = createTable("compared", "cf");
        writeCell(destination, "k1", "cf", "name", "alice");
        writeCell(destination, "k1", "cf", "city", "Tokyo");
        writeCell(destination, "k2", "cf", "name", "bob");
        TableEnvironment table = catalog("bytes");
        table.executeSql(
                "CREATE TEMPORARY TABLE default_catalog.default_database.declared ("
                        + " rowkey STRING, cf ROW<name STRING, city STRING>"
                        + ") WITH ('connector' = 'bigtable', 'project' = '"
                        + PROJECT
                        + "', 'instance' = '"
                        + INSTANCE
                        + "', 'table' = 'compared', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");

        List<Row> throughCatalog =
                collect(
                        table,
                        "SELECT CAST(_key AS STRING), CAST(cf[CAST('name' AS BYTES)] AS STRING),"
                                + " CAST(cf[x'63697479'] AS STRING) FROM compared");
        List<Row> throughDeclared =
                collect(
                        table,
                        "SELECT rowkey, cf.name, cf.city FROM"
                                + " default_catalog.default_database.declared");

        assertThat(throughCatalog)
                .containsExactlyInAnyOrderElementsOf(throughDeclared)
                .containsExactlyInAnyOrder(
                        Row.of("k1", "alice", "Tokyo"), Row.of("k2", "bob", null));
    }

    @Test
    void writesThroughACatalogTable() throws Exception {
        TableDestination destination = createTable("written", "cf", "other");
        TableEnvironment table = catalog("bytes");

        table.executeSql(
                        "INSERT INTO written"
                                + " VALUES (CAST('k1' AS BYTES),"
                                + " MAP[CAST('name' AS BYTES), CAST('alice' AS BYTES)],"
                                + " CAST(NULL AS MAP<BYTES, BYTES>))")
                .await();

        assertThat(cells(destination)).containsExactly("k1/cf:name=alice");
    }

    /** GoogleSQL for Bigtable's overview example, unchanged, under the string key type. */
    @Test
    void theStringKeyTypeRunsGoogleSqlsExampleQueryUnchanged() throws Exception {
        TableDestination destination = createTable("myTable", "address");
        writeCell(destination, "user1", "address", "street", "1 Main St");
        writeCell(destination, "user1", "address", "city", "Tokyo");
        writeCell(destination, "user2", "address", "city", "Osaka");
        TableEnvironment table = catalog("string");

        List<Row> rows =
                collect(
                        table,
                        "SELECT address['street'], address['city'] FROM myTable"
                                + " WHERE _key = 'user1'");

        assertThat(rows)
                .containsExactly(
                        Row.of(
                                "1 Main St".getBytes(StandardCharsets.UTF_8),
                                "Tokyo".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * A row key and a qualifier that are not UTF-8 pass through a STRING column with their bytes
     * intact, so copying a table through the string key type loses nothing.
     */
    @Test
    void theStringKeyTypeKeepsBytesThatAreNotUtf8() throws Exception {
        ByteString key = ByteString.copyFrom(new byte[] {(byte) 0xFF, 0x00, (byte) 0x80, 'k'});
        ByteString qualifier = ByteString.copyFrom(new byte[] {(byte) 0xC3, 0x28, 'q'});
        TableDestination source = createTable("binary_source", "cf");
        TableDestination copy = createTable("binary_copy", "cf");
        writeCell(source, key, "cf", qualifier, "v");
        TableEnvironment table = catalog("string");

        table.executeSql("INSERT INTO binary_copy SELECT * FROM binary_source").await();

        List<com.google.cloud.bigtable.data.v2.models.Row> copied = readRows(copy);
        assertThat(copied).hasSize(1);
        assertThat(copied.get(0).getKey()).isEqualTo(key);
        assertThat(copied.get(0).getCells()).hasSize(1);
        assertThat(copied.get(0).getCells().get(0).getQualifier()).isEqualTo(qualifier);
        assertThat(cells(source)).hasSize(1);
    }

    /**
     * A BYTES map value follows the connector's null convention: an empty cell reads as NULL, as an
     * empty BYTES qualifier of a ROW family does.
     */
    @Test
    void anEmptyCellReadsAsANullMapValue() throws Exception {
        TableDestination destination = createTable("empty_cell", "cf");
        writeCell(destination, "k", "cf", "q", "");
        TableEnvironment table = catalog("string");

        assertThat(collect(table, "SELECT cf['q'] IS NULL, CARDINALITY(cf) FROM empty_cell"))
                .containsExactly(Row.of(true, 1));
    }

    /** A string literal cast to BYTES folds to a literal, so the row key predicate is pushed. */
    @Test
    void aCastStringLiteralOnABytesKeyNarrowsTheScan() {
        createTable("pushed", "cf");
        TableEnvironment table = catalog("bytes");

        assertThat(table.explainSql("SELECT * FROM pushed WHERE _key = CAST('user1' AS BYTES)"))
                .contains("filter=[=(_key, X'7573657231':VARBINARY(2147483647))]");
    }

    /** Flink coerces no string literal to BYTES, which is why the string key type exists. */
    @Test
    void aStringLiteralIsRefusedAgainstABytesKeyAndItsQualifiers() {
        createTable("refused", "cf");
        TableEnvironment table = catalog("bytes");

        assertThatThrownBy(() -> table.explainSql("SELECT * FROM refused WHERE _key = 'user1'"))
                .hasStackTraceContaining("Cannot apply '=' to arguments of type");
        assertThatThrownBy(() -> table.explainSql("SELECT cf['name'] FROM refused"))
                .hasStackTraceContaining("Cannot apply 'ITEM' to arguments of type");
    }

    /**
     * Under the string key type, equality and a range are pushed, and LIKE is left to Flink after a
     * full scan, which is why the docs offer the range instead.
     */
    @Test
    void underStringKeysEqualityAndRangesArePushedAndLikeIsNot() {
        createTable("string_keys", "cf");
        TableEnvironment table = catalog("string");

        assertThat(table.explainSql("SELECT * FROM string_keys WHERE _key = 'user1'"))
                .contains("filter=[=(_key, _UTF-16LE'user1'");
        assertThat(
                        table.explainSql(
                                "SELECT * FROM string_keys WHERE _key >= 'user' AND _key < 'uses'"))
                .contains("filter=[and(>=(_key, _UTF-16LE'user'), <(_key, _UTF-16LE'uses'))]");
        assertThat(table.explainSql("SELECT * FROM string_keys WHERE _key LIKE 'user%'"))
                .contains("filter=[]")
                .contains("where=[LIKE(_key, 'user%')]");
    }

    /** The declared primary key is what lets a catalog table serve a lookup join unannotated. */
    @Test
    void aCatalogTableServesALookupJoin() {
        createTable("dimension", "cf");
        TableEnvironment table = catalog(EnvironmentSettings.inStreamingMode(), "string");
        table.executeSql(
                "CREATE TEMPORARY TABLE default_catalog.default_database.probe ("
                        + " k STRING, pt AS PROCTIME()) WITH ('connector' = 'datagen')");

        assertThat(
                        table.explainSql(
                                "SELECT p.k, d.cf FROM default_catalog.default_database.probe AS p"
                                        + " JOIN dimension FOR SYSTEM_TIME AS OF p.pt AS d"
                                        + " ON p.k = d._key"))
                .contains("LookupJoin");
    }

    /** The emulator accepts a family named '_key'; the catalog refuses to resolve its table. */
    @Test
    void aFamilyNamedLikeTheRowKeyColumnFailsTheLookupNamingTheTable() {
        createTable("clash", "_key", "cf");
        TableEnvironment table = catalog("bytes");

        assertThatThrownBy(() -> table.executeSql("SELECT * FROM clash"))
                .hasStackTraceContaining(
                        "Bigtable table '"
                                + PROJECT
                                + "/"
                                + INSTANCE
                                + "/clash' cannot be resolved: its column family '_key'");
    }

    @Test
    void aMissingTableIsNotFound() {
        TableEnvironment table = catalog("bytes");

        assertThatThrownBy(() -> table.executeSql("SELECT * FROM missing"))
                .hasStackTraceContaining("Object 'missing' not found");
    }
}
