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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.AbstractSpannerRealGcpITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code spanner} catalog against real Spanner, in both dialects: what the emulator cannot
 * settle for docs/adr/0176. The service's own {@code SPANNER_TYPE} spellings, {@code IS_HIDDEN},
 * the system schemas' names, how a missing database is reported, and a catalog whose metadata and
 * rows both go through application-default credentials rather than an emulator.
 *
 * <p>One ephemeral instance serves the whole class, as for every gated Spanner suite; the class is
 * kept to one so a run costs one instance's lifetime of a few minutes.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "SPANNER_IT_PROJECT", matches = ".+")
class SpannerCatalogRealGcpITCase extends AbstractSpannerRealGcpITCase {

    /**
     * Every catalog a test registered: each opens a Spanner client of its own on its first metadata
     * call, which nothing else closes, and the integration-test forks are reused.
     */
    private static final List<TableEnvironment> CATALOGS = new ArrayList<>();

    private static final Logger LOG = LoggerFactory.getLogger(SpannerCatalogRealGcpITCase.class);

    private static DatabaseDestination googleSql;
    private static DatabaseDestination postgreSql;

    @BeforeAll
    static void createDatabases() throws Exception {
        googleSql =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE AllTypes (Id INT64 NOT NULL, B BOOL, F32 FLOAT32,"
                                + " F64 FLOAT64, N NUMERIC, S STRING(MAX), S10 STRING(10),"
                                + " Bytes BYTES(MAX), D DATE, Ts TIMESTAMP, J JSON, U UUID,"
                                + " Tags ARRAY<STRING(MAX)>, Vec ARRAY<FLOAT32>(vector_length=>3),"
                                + " Total INT64 AS (Id * 2) STORED, Label STRING(MAX) AS (S),"
                                + " Tok TOKENLIST AS (TOKENIZE_FULLTEXT(S)) HIDDEN)"
                                + " PRIMARY KEY (Id)",
                        "CREATE SCHEMA sales",
                        "CREATE TABLE sales.Items (Id INT64 NOT NULL, Name STRING(MAX))"
                                + " PRIMARY KEY (Id)",
                        "CREATE VIEW sales.ItemNames SQL SECURITY INVOKER AS"
                                + " SELECT i.Name FROM sales.Items AS i",
                        "CREATE CHANGE STREAM changes FOR sales.Items");
        DatabaseAdminClient admin = spanner().getDatabaseAdminClient();
        Database withProtos =
                admin.newDatabaseBuilder(
                                DatabaseId.of(
                                        PROJECT, googleSql.getInstance(), googleSql.getDatabase()))
                        .setProtoDescriptors(protoDescriptors())
                        .build();
        admin.updateDatabaseDdl(
                        withProtos,
                        Arrays.asList(
                                "CREATE PROTO BUNDLE (example.events.Event,"
                                        + " example.events.Event.Kind, example.events.Status)",
                                "CREATE TABLE Events (Id INT64 NOT NULL, Ev example.events.Event,"
                                        + " Kind example.events.Event.Kind,"
                                        + " Sts ARRAY<example.events.Status>) PRIMARY KEY (Id)"),
                        null)
                .get();
        postgreSql =
                createDatabase(
                        Dialect.POSTGRESQL,
                        "CREATE TABLE alltypes (id bigint NOT NULL PRIMARY KEY, b boolean,"
                                + " f32 real, f64 double precision, n numeric, s varchar,"
                                + " s10 character varying(10), txt text, bytes bytea, d date,"
                                + " ts timestamptz, ct spanner.commit_timestamp, j jsonb,"
                                + " u uuid, tags varchar[], vec real[] vector length 3,"
                                + " total bigint GENERATED ALWAYS AS (id * 2) STORED,"
                                + " label varchar GENERATED ALWAYS AS (s) VIRTUAL,"
                                + " tok spanner.tokenlist GENERATED ALWAYS AS"
                                + " (spanner.tokenize_fulltext(s)) VIRTUAL HIDDEN)",
                        "CREATE SCHEMA sales",
                        "CREATE TABLE sales.items (id bigint NOT NULL PRIMARY KEY, name varchar)",
                        "CREATE VIEW sales.item_names SQL SECURITY INVOKER AS"
                                + " SELECT i.name FROM sales.items AS i",
                        "CREATE CHANGE STREAM changes FOR sales.items");
        for (DatabaseDestination database : Arrays.asList(googleSql, postgreSql)) {
            for (Struct row :
                    query(
                            database,
                            database == postgreSql
                                    ? "SELECT table_schema, table_name, column_name, spanner_type,"
                                            + " is_hidden, is_generated FROM"
                                            + " information_schema.columns WHERE table_schema"
                                            + " NOT IN ('information_schema', 'pg_catalog',"
                                            + " 'spanner_sys') ORDER BY 1, 2, 3"
                                    : "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE,"
                                            + " CAST(IS_HIDDEN AS STRING), IS_GENERATED FROM"
                                            + " INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG ="
                                            + " '' AND TABLE_SCHEMA NOT IN"
                                            + " ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
                                            + " ORDER BY 1, 2, 3")) {
                LOG.info("Measured column {}", row);
            }
        }
    }

    @Test
    void listsBaseTablesOutsideTheSystemSchemasInBothDialects() throws Exception {
        assertThat(strings(catalog(googleSql), "SHOW TABLES"))
                .containsExactlyInAnyOrder("AllTypes", "Events", "sales.Items");
        assertThat(strings(catalog(postgreSql), "SHOW TABLES"))
                .containsExactlyInAnyOrder("alltypes", "sales.items");
    }

    @Test
    void resolvesEveryColumnFromTheServicesOwnSpellings() {
        assertThat(catalog(googleSql).from("AllTypes").getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.BOOLEAN(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(38, 9),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.BYTES(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP_LTZ(9),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        DataTypes.BIGINT());
        assertThat(catalog(googleSql).from("Events").getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.BYTES(),
                        DataTypes.BIGINT(),
                        DataTypes.ARRAY(DataTypes.BIGINT()));
        assertThat(catalog(postgreSql).from("alltypes").getResolvedSchema().getColumnDataTypes())
                .containsExactly(
                        DataTypes.BIGINT().notNull(),
                        DataTypes.BOOLEAN(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(38, 9),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.BYTES(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP_LTZ(9),
                        DataTypes.TIMESTAMP_LTZ(9),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        DataTypes.BIGINT());
    }

    @Test
    void aMissingDatabaseIsNoDatabaseRatherThanAFailure() throws Exception {
        Catalog catalog = catalog(googleSql).getCatalog("sp").orElseThrow();

        assertThat(catalog.databaseExists("no-such-db")).isFalse();
        assertThat(catalog.databaseExists(googleSql.getDatabase())).isTrue();
    }

    /** The spelling {@code InformationSchemaCellWeights} and the catalog exclude by. */
    @Test
    void aPostgreSqlDatabasesSystemSchemasAreLowerCase() {
        List<String> schemas = new ArrayList<>();
        for (Struct row :
                query(postgreSql, "SELECT schema_name FROM information_schema.schemata")) {
            schemas.add(row.getString(0));
        }
        LOG.info("Measured PostgreSQL schemata {}", schemas);
        List<String> indexed = new ArrayList<>();
        for (Struct row :
                query(
                        postgreSql,
                        "SELECT DISTINCT table_schema FROM information_schema.index_columns")) {
            indexed.add(row.getString(0));
        }
        LOG.info("Measured PostgreSQL index_columns schemas {}", indexed);

        assertThat(schemas).contains("spanner_sys").doesNotContain("SPANNER_SYS");
    }

    @Test
    void writesAndReadsThroughACatalogTableWithGeneratedColumns() throws Exception {
        TableEnvironment streaming =
                withCatalog(
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance().inStreamingMode().build()),
                        postgreSql);
        streaming.executeSql("INSERT INTO `sales.items` VALUES (1, 'Ada'), (2, 'Grace')").await();
        streaming.executeSql("INSERT INTO alltypes (id, s) VALUES (7, 'Lin')").await();

        TableEnvironment batch = catalog(postgreSql);
        assertThat(rows(batch, "SELECT id, name FROM `sales.items` ORDER BY id"))
                .containsExactly(Row.of(1L, "Ada"), Row.of(2L, "Grace"));
        // The stored generated column reads back; the virtual one, which the read API refuses
        // ("Cannot read generated column without STORED attribute"), is not part of the table.
        assertThat(rows(batch, "SELECT id, total FROM alltypes")).containsExactly(Row.of(7L, 14L));
        assertThat(batch.from("alltypes").getResolvedSchema().getColumnNames())
                .doesNotContain("label", "tok");
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

    private static TableEnvironment catalog(DatabaseDestination database) {
        return withCatalog(
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build()),
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
                                                                        .TYPE_INT64))
                                        .addEnumType(
                                                EnumDescriptorProto.newBuilder()
                                                        .setName("Kind")
                                                        .addValue(
                                                                EnumValueDescriptorProto
                                                                        .newBuilder()
                                                                        .setName("KIND_UNSPECIFIED")
                                                                        .setNumber(0))))
                        .addEnumType(
                                EnumDescriptorProto.newBuilder()
                                        .setName("Status")
                                        .addValue(
                                                EnumValueDescriptorProto.newBuilder()
                                                        .setName("UNKNOWN")
                                                        .setNumber(0)))
                        .build();
        return FileDescriptorSet.newBuilder().addFile(file).build().toByteArray();
    }
}
