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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.testutils.spanner.SpannerTestClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code spanner} catalog against the emulator, in both dialects: listing, the type mapping,
 * reads and writes through catalog tables, a named schema, and the hints that turn a catalog table
 * into a lookup, index or change-stream source.
 *
 * <p>The emulator is not the service (docs/adr/0154); what it measured here, and where the two
 * might differ, is recorded in docs/adr/0176.
 */
class SpannerCatalogITCase extends AbstractSpannerEmulatorITCase {

    /**
     * Every catalog a test registered: each opens a Spanner client of its own on its first metadata
     * call, which nothing else closes, and the integration-test forks are reused.
     */
    private static final List<TableEnvironment> CATALOGS = new ArrayList<>();

    private static final String UUID_VALUE = "5f0c8a52-91a5-4d3d-9b0c-0f1e2d3c4b5a";
    private static final Duration ROW_DEADLINE = Duration.ofSeconds(90);

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void listsTheInstancesDatabasesAndEachDatabasesBaseTables(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        TableEnvironment table = batch(database);

        assertThat(strings(table, "SHOW DATABASES")).contains(database.getDatabase());
        // Views and change streams are not listed, and neither are the system schemas.
        assertThat(strings(table, "SHOW TABLES"))
                .containsExactlyInAnyOrderElementsOf(
                        dialect == Dialect.POSTGRESQL
                                ? Arrays.asList(
                                        "orders",
                                        "\"Customers\"",
                                        "pairs",
                                        "scalars",
                                        "sales.items")
                                : Arrays.asList("Orders", "Pairs", "Scalars", "sales.Items"));
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void everyListedTableResolves(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        TableEnvironment table = batch(database);

        for (String name : strings(table, "SHOW TABLES")) {
            assertThat(table.from("`" + name.replace("`", "``") + "`").getResolvedSchema())
                    .as(name)
                    .isNotNull();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void resolvesEachColumnToTheTypeTheConnectorPageDocuments(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        TableEnvironment table = batch(database);

        // The hidden TOKENLIST column and the virtual Label, which the read API refuses, are left
        // out; the stored generated Total is kept and nullable.
        assertThat(table.from(orders(dialect)).getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.STRING(),
                        DataTypes.DECIMAL(38, 9),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.TIMESTAMP_LTZ(9),
                        DataTypes.DECIMAL(38, 9));
        // Key order is Spanner's, which differs from the column order here.
        assertThat(
                        table.from(dialect == Dialect.POSTGRESQL ? "pairs" : "Pairs")
                                .getResolvedSchema())
                .satisfies(
                        schema ->
                                assertThat(schema.getPrimaryKey().get().getColumns())
                                        .containsExactlyElementsOf(
                                                dialect == Dialect.POSTGRESQL
                                                        ? Arrays.asList("b", "a")
                                                        : Arrays.asList("B", "A")));
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void writesThroughACatalogTableAndSpannerComputesItsGeneratedColumn(Dialect dialect)
            throws Exception {
        DatabaseDestination database = database(dialect);
        TableEnvironment streaming = streaming(database);
        String columns =
                dialect == Dialect.POSTGRESQL
                        ? "id, customer, amount, doc, `ref`, tags, created_at"
                        : "Id, Customer, Amount, Doc, `Ref`, Tags, CreatedAt";

        streaming
                .executeSql(
                        "INSERT INTO "
                                + orders(dialect)
                                + " ("
                                + columns
                                + ") VALUES (1, 'Ada', 12.5, '{\"a\":1}', '"
                                + UUID_VALUE
                                + "', ARRAY['x', 'y'], TO_TIMESTAMP_LTZ(1700000000123, 3))")
                .await();

        Struct written =
                query(
                                database,
                                dialect == Dialect.POSTGRESQL
                                        ? "SELECT total FROM orders WHERE id = 1"
                                        : "SELECT Total FROM Orders WHERE Id = 1")
                        .get(0);
        BigDecimal total =
                dialect == Dialect.POSTGRESQL
                        ? new BigDecimal(written.getString(0))
                        : written.getBigDecimal(0);
        assertThat(total).isEqualByComparingTo("25");

        List<Row> read = rows(batch(database), "SELECT * FROM " + orders(dialect));
        assertThat(read).hasSize(1);
        Row row = read.get(0);
        assertThat(row.getField(0)).isEqualTo(1L);
        assertThat(row.getField(1)).isEqualTo("Ada");
        assertThat((BigDecimal) row.getField(2)).isEqualByComparingTo("12.5");
        assertThat(row.getField(3).toString()).contains("\"a\"");
        assertThat(row.getField(4)).isEqualTo(UUID_VALUE);
        assertThat((String[]) row.getField(5)).containsExactly("x", "y");
        assertThat(row.getField(6)).isEqualTo(Instant.ofEpochMilli(1700000000123L));
        assertThat((BigDecimal) row.getField(7)).isEqualByComparingTo("25");
        assertThat(row.getArity()).as("the virtual Label is not part of the table").isEqualTo(8);
    }

    /**
     * A generated key column is {@code NOT NULL}, so an {@code INSERT INTO} supplies it, and the
     * upsert leaves the supplied value out: Spanner keys the row by the value it computes, which is
     * why the page asks for that value and not another.
     */
    @Test
    void anInsertSuppliesAGeneratedKeyAndSpannerKeysTheRowByTheValueItComputes() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE Keyed (A INT64 NOT NULL,"
                                + " Id INT64 NOT NULL AS (A * 10) STORED) PRIMARY KEY (Id)");

        streaming(database).executeSql("INSERT INTO Keyed VALUES (5, 50), (7, 1)").await();

        List<Long> keys = new ArrayList<>();
        for (Struct row : query(database, "SELECT Id FROM Keyed ORDER BY Id")) {
            keys.add(row.getLong(0));
        }
        assertThat(keys).containsExactly(50L, 70L);
    }

    /** The rows of the type mapping the other tables leave out, read back as the page says. */
    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void theRemainingScalarTypesResolveAndReadBackAsDocumented(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        String scalars = dialect == Dialect.POSTGRESQL ? "scalars" : "Scalars";
        streaming(database)
                .executeSql(
                        "INSERT INTO "
                                + scalars
                                + " VALUES (1, TRUE, CAST(1.5 AS FLOAT), 2.25, X'0102',"
                                + " DATE '2026-10-03', ARRAY[CAST(0.5 AS DOUBLE)])")
                .await();
        TableEnvironment table = batch(database);

        assertThat(table.from(scalars).getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.BOOLEAN(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.BYTES(),
                        DataTypes.DATE(),
                        DataTypes.ARRAY(DataTypes.DOUBLE()));
        Row row = rows(table, "SELECT * FROM " + scalars).get(0);
        assertThat(row.getField(1)).isEqualTo(true);
        assertThat(row.getField(2)).isEqualTo(1.5f);
        assertThat(row.getField(3)).isEqualTo(2.25d);
        assertThat((byte[]) row.getField(4)).containsExactly(1, 2);
        assertThat(row.getField(5)).isEqualTo(LocalDate.of(2026, 10, 3));
        assertThat((Double[]) row.getField(6)).containsExactly(0.5d);
    }

    /**
     * The production statements' name comparison, not a fake's: GoogleSQL finds a table whatever
     * the case of the name asked, PostgreSQL only by its exact decoded name.
     */
    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void namesAreComparedAsTheDialectComparesThem(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        items(database, dialect);
        TableEnvironment table = batch(database);

        if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
            assertThat(rows(table, "SELECT Id FROM `SALES.items` ORDER BY Id"))
                    .containsExactly(Row.of(1L), Row.of(2L));
            assertThat(table.from("orders").getResolvedSchema().getColumnNames())
                    .startsWith("Id", "Customer");
            return;
        }
        // An unquoted PostgreSQL name folds to lower case, which "Customers" is not.
        assertThatThrownBy(() -> table.from("Customers"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("not found");
        streaming(database).executeSql("INSERT INTO `\"Customers\"` VALUES (7, 'Grace')").await();
        assertThat(rows(table, "SELECT id, name FROM `\"Customers\"`"))
                .containsExactly(Row.of(7L, "Grace"));
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void aNamedSchemaTableServesALookupJoinAndAnIndexHint(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        items(database, dialect);
        TableEnvironment table = batch(database);
        table.executeSql(
                "CREATE TEMPORARY TABLE facts (id BIGINT, event_time AS PROCTIME()) WITH ("
                        + "'connector'='datagen', 'number-of-rows'='2', "
                        + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                        + "'fields.id.end'='2')");
        String items = items(dialect);
        String id = dialect == Dialect.POSTGRESQL ? "id" : "Id";
        String name = dialect == Dialect.POSTGRESQL ? "name" : "Name";
        String index = dialect == Dialect.POSTGRESQL ? "items_by_name" : "ItemsByName";

        assertThat(
                        rows(
                                table,
                                "SELECT f.id, s."
                                        + name
                                        + " FROM facts AS f JOIN "
                                        + items
                                        + " FOR SYSTEM_TIME AS OF f.event_time AS s ON f.id = s."
                                        + id
                                        + " ORDER BY f.id"))
                .extracting(row -> row.getField(1))
                .containsExactly("Ada", "Grace");
        // The index resolves in the table's own schema, as for a hand-written table.
        assertThat(
                        rows(
                                table,
                                "SELECT "
                                        + id
                                        + " FROM "
                                        + items
                                        + " /*+ OPTIONS('scan.index' = '"
                                        + index
                                        + "') */ WHERE "
                                        + name
                                        + " = 'Grace'"))
                .containsExactly(Row.of(2L));
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void aHintTurnsACatalogTableIntoItsChangeStreamSource(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        Timestamp firstCommit = items(database, dialect);
        TableEnvironment streaming = streaming(database);
        streaming.getConfig().set("parallelism.default", "1");

        List<Row> changes =
                firstRows(
                        streaming,
                        "SELECT * FROM "
                                + items(dialect)
                                + " /*+ OPTIONS('scan.mode' = 'change-stream',"
                                + " 'scan.change-stream.name' = 'changes',"
                                + " 'scan.change-stream.changelog-mode' = 'full',"
                                + " 'scan.startup.mode' = 'timestamp',"
                                + " 'scan.startup.timestamp-millis' = '"
                                + firstCommit.toSqlTimestamp().getTime()
                                + "', 'scan.change-stream.heartbeat-interval' = '1 s') */",
                        2);

        // Both inserts are one commit, so one record carries them; asking for no row a later
        // commit would have to deliver keeps the read from waiting on the emulator's timing.
        assertThat(changes)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1L, "Ada"),
                        Row.ofKind(RowKind.INSERT, 2L, "Grace"));
    }

    /**
     * Spanner change streams do not watch a generated column outside the primary key, so the hint
     * is refused at planning for such a table, while a generated key column is watched and read.
     */
    @Test
    void aChangeStreamHintRefusesANonKeyGeneratedColumnAndReadsAGeneratedKey() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE Totals (Id INT64 NOT NULL, A INT64,"
                                + " Doubled INT64 AS (A * 2) STORED) PRIMARY KEY (Id)",
                        "CREATE TABLE Keyed (A INT64 NOT NULL,"
                                + " Id INT64 NOT NULL AS (A * 10) STORED) PRIMARY KEY (Id)",
                        "CREATE CHANGE STREAM changes FOR Totals, Keyed");
        updateDdl(
                database,
                "ALTER CHANGE STREAM changes SET OPTIONS"
                        + " (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
        Timestamp firstCommit =
                client(database)
                        .write(List.of(Mutation.newInsertBuilder("Keyed").set("A").to(5L).build()));
        TableEnvironment streaming = streaming(database);
        String hint =
                " /*+ OPTIONS('scan.mode' = 'change-stream',"
                        + " 'scan.change-stream.name' = 'changes',"
                        + " 'scan.change-stream.changelog-mode' = 'full',"
                        + " 'scan.startup.mode' = 'timestamp',"
                        + " 'scan.startup.timestamp-millis' = '"
                        + firstCommit.toSqlTimestamp().getTime()
                        + "', 'scan.change-stream.heartbeat-interval' = '1 s') */";

        assertThatThrownBy(() -> streaming.executeSql("SELECT * FROM Totals" + hint))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "scan.mode=change-stream cannot read the generated columns [Doubled]");
        assertThat(firstRows(streaming, "SELECT * FROM Keyed" + hint, 1))
                .containsExactly(Row.ofKind(RowKind.INSERT, 5L, 50L));
    }

    @Test
    void protoAndEnumColumnsReadAndWriteThroughTheirMarkers() throws Exception {
        DatabaseDestination database = database(Dialect.GOOGLE_STANDARD_SQL);
        try (Spanner spanner = SpannerTestClients.forEmulator(emulatorEndpoint(), PROJECT)) {
            DatabaseAdminClient admin = spanner.getDatabaseAdminClient();
            Database withProtos =
                    admin.newDatabaseBuilder(
                                    DatabaseId.of(PROJECT, INSTANCE, database.getDatabase()))
                            .setProtoDescriptors(protoDescriptors())
                            .build();
            admin.updateDatabaseDdl(
                            withProtos,
                            Arrays.asList(
                                    "CREATE PROTO BUNDLE (example.events.Event,"
                                            + " example.events.Status)",
                                    "CREATE TABLE Events (Id INT64 NOT NULL,"
                                            + " Ev example.events.Event,"
                                            + " St example.events.Status,"
                                            + " Sts ARRAY<example.events.Status>)"
                                            + " PRIMARY KEY (Id)"),
                            null)
                    .get();
        }
        TableEnvironment streaming = streaming(database);
        byte[] event = new byte[] {0x08, 0x2a};

        assertThat(streaming.from("Events").getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.BYTES(),
                        DataTypes.BIGINT(),
                        DataTypes.ARRAY(DataTypes.BIGINT()));
        streaming
                .executeSql(
                        "INSERT INTO Events VALUES (1, X'082a', 1, ARRAY[CAST(1 AS BIGINT), 0])")
                .await();

        List<Row> read = rows(batch(database), "SELECT * FROM Events");
        assertThat(read).hasSize(1);
        assertThat((byte[]) read.get(0).getField(1)).isEqualTo(event);
        assertThat(read.get(0).getField(2)).isEqualTo(1L);
        assertThat((Long[]) read.get(0).getField(3)).containsExactly(1L, 0L);
    }

    @Test
    void aMissingDatabaseOrTableFailsAsNotFound() throws Exception {
        DatabaseDestination database = database(Dialect.GOOGLE_STANDARD_SQL);
        TableEnvironment table = batch(database);

        assertThatThrownBy(() -> table.from("sp.`missing-db`.Orders"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("not found");
        assertThatThrownBy(() -> table.from("Missing"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("not found");
    }

    @AfterEach
    void closeCatalogs() throws Exception {
        List<AutoCloseable> catalogs = new ArrayList<>();
        for (TableEnvironment table : CATALOGS) {
            table.getCatalog("sp").ifPresent(catalog -> catalogs.add(catalog::close));
        }
        try {
            // Every catalog is closed even if one fails; the first failure is rethrown.
            Closers.closeAll(catalogs);
        } finally {
            CATALOGS.clear();
        }
    }

    // ------------------------------------------------------------------------

    private static DatabaseDestination database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            DatabaseDestination database =
                    createDatabase(
                            dialect,
                            "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY,"
                                    + " customer varchar, amount numeric, doc jsonb, ref uuid,"
                                    + " tags varchar[], created_at timestamptz,"
                                    + " total numeric GENERATED ALWAYS AS (amount * 2) STORED,"
                                    + " label varchar GENERATED ALWAYS AS (customer || '!') VIRTUAL,"
                                    + " tok spanner.tokenlist GENERATED ALWAYS AS"
                                    + " (spanner.tokenize_fulltext(customer)) VIRTUAL HIDDEN)",
                            "CREATE TABLE \"Customers\" (id bigint NOT NULL PRIMARY KEY, name varchar)",
                            "CREATE TABLE pairs (a bigint NOT NULL, b varchar NOT NULL, v bigint,"
                                    + " PRIMARY KEY (b, a))",
                            "CREATE TABLE scalars (id bigint NOT NULL PRIMARY KEY, b boolean,"
                                    + " f32 real, f64 double precision, bytes bytea, d date,"
                                    + " f64s double precision[])",
                            "CREATE SCHEMA sales",
                            "CREATE TABLE sales.items (id bigint NOT NULL PRIMARY KEY,"
                                    + " name varchar)",
                            "CREATE INDEX items_by_name ON sales.items (name)",
                            "CREATE VIEW sales.item_names SQL SECURITY INVOKER AS"
                                    + " SELECT i.name FROM sales.items AS i",
                            "CREATE CHANGE STREAM changes FOR sales.items");
            updateDdl(
                    database,
                    "ALTER CHANGE STREAM changes SET"
                            + " (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
            return database;
        }
        DatabaseDestination database =
                createDatabase(
                        dialect,
                        "CREATE TABLE Orders (Id INT64 NOT NULL, Customer STRING(MAX),"
                                + " Amount NUMERIC, Doc JSON, Ref UUID, Tags ARRAY<STRING(MAX)>,"
                                + " CreatedAt TIMESTAMP,"
                                + " Total NUMERIC AS (Amount * 2) STORED,"
                                + " Label STRING(MAX) AS (CONCAT(Customer, '!')),"
                                + " Tok TOKENLIST AS (TOKENIZE_FULLTEXT(Customer)) HIDDEN)"
                                + " PRIMARY KEY (Id)",
                        "CREATE TABLE Pairs (A INT64 NOT NULL, B STRING(MAX) NOT NULL, V INT64)"
                                + " PRIMARY KEY (B, A)",
                        "CREATE TABLE Scalars (Id INT64 NOT NULL, B BOOL, F32 FLOAT32,"
                                + " F64 FLOAT64, Bytes BYTES(MAX), D DATE, F64s ARRAY<FLOAT64>)"
                                + " PRIMARY KEY (Id)",
                        "CREATE SCHEMA sales",
                        "CREATE TABLE sales.Items (Id INT64 NOT NULL, Name STRING(MAX))"
                                + " PRIMARY KEY (Id)",
                        "CREATE INDEX sales.ItemsByName ON sales.Items (Name)",
                        "CREATE VIEW sales.ItemNames SQL SECURITY INVOKER AS"
                                + " SELECT i.Name FROM sales.Items AS i",
                        "CREATE CHANGE STREAM changes FOR sales.Items");
        updateDdl(
                database,
                "ALTER CHANGE STREAM changes SET OPTIONS"
                        + " (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
        return database;
    }

    /** Writes two items and returns the commit timestamp of that write. */
    private static Timestamp items(DatabaseDestination database, Dialect dialect) {
        String table = dialect == Dialect.POSTGRESQL ? "sales.items" : "sales.Items";
        String id = dialect == Dialect.POSTGRESQL ? "id" : "Id";
        String name = dialect == Dialect.POSTGRESQL ? "name" : "Name";
        return client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder(table)
                                        .set(id)
                                        .to(1L)
                                        .set(name)
                                        .to("Ada")
                                        .build(),
                                Mutation.newInsertBuilder(table)
                                        .set(id)
                                        .to(2L)
                                        .set(name)
                                        .to("Grace")
                                        .build()));
    }

    private static String orders(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? "orders" : "Orders";
    }

    private static String items(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? "`sales.items`" : "`sales.Items`";
    }

    private static TableEnvironment batch(DatabaseDestination database) {
        return withCatalog(
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build()),
                database);
    }

    private static TableEnvironment streaming(DatabaseDestination database) {
        return withCatalog(
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build()),
                database);
    }

    private static TableEnvironment withCatalog(
            TableEnvironment table, DatabaseDestination database) {
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                "CREATE CATALOG sp WITH ("
                        + "'type' = 'spanner', "
                        + "'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'default-database' = '"
                        + database.getDatabase()
                        + "', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        table.executeSql("USE CATALOG sp");
        CATALOGS.add(table);
        return table;
    }

    private static List<String> strings(TableEnvironment table, String sql) throws Exception {
        List<String> values = new ArrayList<>();
        for (Row row : rows(table, sql)) {
            values.add(String.valueOf(row.getField(0)));
        }
        return values;
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    /**
     * The first rows of an unbounded query, read within a deadline. {@code hasNext()} blocks until
     * a row arrives and ignores interruption, so a row that never comes would hang the fork rather
     * than fail the test; past the deadline the iterator is closed, which cancels the job, and the
     * rows read so far are what the failure shows.
     */
    private static List<Row> firstRows(TableEnvironment table, String sql, int count)
            throws Exception {
        List<Row> rows = new CopyOnWriteArrayList<>();
        CloseableIterator<Row> iterator = table.executeSql(sql).collect();
        ExecutorService reader =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "catalog-it-first-rows");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> reading =
                    reader.submit(
                            () -> {
                                while (rows.size() < count && iterator.hasNext()) {
                                    rows.add(iterator.next());
                                }
                            });
            try {
                reading.get(ROW_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                        "Read "
                                + rows
                                + " within "
                                + ROW_DEADLINE
                                + " where "
                                + count
                                + " rows were expected from: "
                                + sql,
                        e);
            }
            return new ArrayList<>(rows);
        } finally {
            iterator.close();
            reader.shutdownNow();
        }
    }

    private static byte[] protoDescriptors() {
        FileDescriptorProto file =
                FileDescriptorProto.newBuilder()
                        .setName("example/events.proto")
                        .setPackage("example.events")
                        .setSyntax("proto3")
                        .addMessageType(
                                DescriptorProto.newBuilder()
                                        .setName("Event")
                                        .addField(
                                                FieldDescriptorProto.newBuilder()
                                                        .setName("id")
                                                        .setNumber(1)
                                                        .setType(
                                                                FieldDescriptorProto.Type
                                                                        .TYPE_INT64)))
                        .addEnumType(
                                EnumDescriptorProto.newBuilder()
                                        .setName("Status")
                                        .addValue(
                                                EnumValueDescriptorProto.newBuilder()
                                                        .setName("UNKNOWN")
                                                        .setNumber(0))
                                        .addValue(
                                                EnumValueDescriptorProto.newBuilder()
                                                        .setName("ACTIVE")
                                                        .setNumber(1)))
                        .build();
        return FileDescriptorSet.newBuilder().addFile(file).build().toByteArray();
    }
}
