/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end bounded SQL scan coverage against both emulator dialects. */
class SpannerTableSourceITCase extends AbstractSpannerEmulatorITCase {

    @ParameterizedTest
    @MethodSource("changeStreamMetadataCases")
    void readsChangeStreamMetadataAndUsesSourceWatermarks(Dialect dialect, String changelogMode)
            throws Exception {
        String tableDdl =
                dialect == Dialect.POSTGRESQL
                        ? "CREATE TABLE metadata_records (id bigint NOT NULL PRIMARY KEY, name varchar(64))"
                        : "CREATE TABLE metadata_records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)";
        String alterStream =
                dialect == Dialect.POSTGRESQL
                        ? "ALTER CHANGE STREAM metadata_changes SET (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')"
                        : "ALTER CHANGE STREAM metadata_changes SET OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')";
        SpannerDatabase database =
                createDatabase(
                        dialect,
                        tableDdl,
                        "CREATE CHANGE STREAM metadata_changes FOR metadata_records",
                        alterStream);
        Timestamp commit =
                client(database)
                        .write(
                                List.of(
                                        Mutation.newInsertBuilder("metadata_records")
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada")
                                                .build(),
                                        Mutation.newInsertBuilder("metadata_records")
                                                .set("id")
                                                .to(2L)
                                                .set("name")
                                                .to("Grace")
                                                .build()));
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                changeStreamMetadataTableDdl(
                        database,
                        dialect,
                        changelogMode,
                        commit.toSqlTimestamp().toInstant().toEpochMilli()));
        java.time.Instant commitAtWatermarkPrecision =
                java.time.Instant.ofEpochMilli(commit.toSqlTimestamp().toInstant().toEpochMilli());

        List<Row> rows = firstRows(table, "SELECT * FROM metadata_cdc", 2);

        assertThat(rows).extracting(Row::getKind).containsOnly(RowKind.INSERT);
        assertThat(rows).extracting(row -> row.getFieldAs(0)).containsExactlyInAnyOrder(1L, 2L);
        assertThat(rows)
                .allSatisfy(
                        row -> {
                            assertThat((Object) row.getField(2))
                                    .isEqualTo(commitAtWatermarkPrecision);
                            assertThat(row.getFieldAs(3).toString()).isNotBlank();
                            assertThat(row.getFieldAs(4).toString()).isNotBlank();
                            assertThat((Object) row.getField(5)).isEqualTo(true);
                            assertThat(row.getFieldAs(6).toString()).isEqualTo("metadata_records");
                            assertThat(row.getFieldAs(7).toString()).isEqualTo("INSERT");
                            assertThat(row.getFieldAs(8).toString())
                                    .isEqualTo("NEW_ROW_AND_OLD_VALUES");
                            assertThat((Object) row.getField(9)).isEqualTo(1L);
                            assertThat((Object) row.getField(10)).isEqualTo(1L);
                            assertThat(row.getFieldAs(11).toString()).isEmpty();
                            assertThat((Object) row.getField(12)).isEqualTo(false);
                        });
        assertThat(rows).extracting(row -> row.getFieldAs(13)).containsExactlyInAnyOrder(0, 1);
    }

    private static Stream<Arguments> changeStreamMetadataCases() {
        return Stream.of(Dialect.values())
                .flatMap(
                        dialect ->
                                Stream.of(
                                        Arguments.of(dialect, "full"),
                                        Arguments.of(dialect, "upsert")));
    }

    @ParameterizedTest
    @MethodSource("changeStreamTableCases")
    void readsTableChangelogForDefaultNamedAndQuotedTables(Dialect dialect, String schemaKind)
            throws Exception {
        boolean named = !"default".equals(schemaKind);
        boolean quoted = "quoted".equals(schemaKind);
        String schema = namedIdentifier(dialect, quoted, "analytics", "QuotedAnalytics");
        String tableName = namedIdentifier(dialect, quoted, "records", "QuotedRecords");
        String qualified = named ? schema + "." + tableName : tableName;
        String tableDdl =
                dialect == Dialect.POSTGRESQL
                        ? "CREATE TABLE "
                                + qualified
                                + " (id bigint NOT NULL PRIMARY KEY, name varchar(64))"
                        : "CREATE TABLE "
                                + qualified
                                + " (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)";
        List<String> ddl = new ArrayList<>();
        if (named) {
            ddl.add("CREATE SCHEMA " + schema);
        }
        ddl.add(tableDdl);
        ddl.add("CREATE CHANGE STREAM changes FOR " + qualified);
        ddl.add(
                dialect == Dialect.POSTGRESQL
                        ? "ALTER CHANGE STREAM changes SET (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')"
                        : "ALTER CHANGE STREAM changes SET OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
        SpannerDatabase database = createDatabase(dialect, ddl.toArray(new String[0]));
        String apiTable =
                named
                        ? SpannerTableName.of(schema, tableName, dialect).apiName()
                        : SpannerTableName.of(null, tableName, dialect).apiName();
        Timestamp firstCommit =
                client(database)
                        .write(
                                List.of(
                                        Mutation.newInsertBuilder(apiTable)
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada")
                                                .build()));
        client(database)
                .write(
                        List.of(
                                Mutation.newUpdateBuilder(apiTable)
                                        .set("id")
                                        .to(1L)
                                        .set("name")
                                        .to("Grace")
                                        .build()));
        client(database)
                .write(List.of(Mutation.delete(apiTable, com.google.cloud.spanner.Key.of(1L))));

        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                changeStreamTableDdl(
                        database,
                        dialect,
                        named ? schema : null,
                        tableName,
                        firstCommit.toSqlTimestamp().toInstant().toEpochMilli()));

        assertThat(firstRows(table, "SELECT id, name FROM cdc", 4))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1L, "Ada"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1L, "Ada"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1L, "Grace"),
                        Row.ofKind(RowKind.DELETE, 1L, "Grace"));
    }

    private static Stream<Arguments> changeStreamTableCases() {
        return Stream.of(Dialect.values())
                .flatMap(
                        dialect ->
                                Stream.of(
                                        Arguments.of(dialect, "default"),
                                        Arguments.of(dialect, "named"),
                                        Arguments.of(dialect, "quoted")));
    }

    @ParameterizedTest
    @MethodSource("namedSchemaCases")
    void writesScansAndLooksUpANamedSchema(Dialect dialect, boolean async, boolean quoted)
            throws Exception {
        SpannerDatabase database = namedSchemaDatabase(dialect, quoted);
        TableEnvironment sink =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        sink.executeSql(namedSchemaTableDdl("named_sink", database, dialect, async, quoted));
        sink.executeSql("INSERT INTO named_sink VALUES (1, 'Ada'), (2, 'Grace')").await();

        TableEnvironment source =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        source.getConfig().set("parallelism.default", "1");
        source.executeSql(namedSchemaTableDdl("named_source", database, dialect, async, quoted));

        assertThat(rows(source, "SELECT id FROM named_source WHERE name = 'Grace'"))
                .containsExactly(Row.of(2L));

        source.executeSql(
                "CREATE TABLE named_facts (id BIGINT, event_time AS PROCTIME()) WITH ("
                        + "'connector'='datagen', 'number-of-rows'='2', "
                        + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                        + "'fields.id.end'='2')");
        assertThat(
                        rows(
                                source,
                                "SELECT f.id, s.name FROM named_facts AS f LEFT JOIN named_source "
                                        + "FOR SYSTEM_TIME AS OF f.event_time AS s "
                                        + "ON f.id = s.id ORDER BY f.id"))
                .extracting(row -> row.getField(1))
                .containsExactly("Ada", "Grace");
    }

    @ParameterizedTest
    @MethodSource("lookupCases")
    void looksUpCompositeHitsAndMissesInBothDialects(Dialect dialect, boolean async)
            throws Exception {
        SpannerDatabase database = lookupDatabase(dialect);
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("lookup_records")
                                        .set("id")
                                        .to(1L)
                                        .set("tenant")
                                        .to(1L)
                                        .set("name")
                                        .to("Ada")
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(lookupTableDdl(database, dialect, async));
        table.executeSql(
                "CREATE TABLE facts (id BIGINT, tenant BIGINT, event_time AS PROCTIME()) WITH ("
                        + "'connector'='datagen', 'number-of-rows'='2', "
                        + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                        + "'fields.id.end'='2', 'fields.tenant.kind'='sequence', "
                        + "'fields.tenant.start'='1', 'fields.tenant.end'='2')");

        assertThat(
                        rows(
                                table,
                                "SELECT f.id, s.name FROM facts AS f LEFT JOIN lookup_source "
                                        + "FOR SYSTEM_TIME AS OF f.event_time AS s "
                                        + "ON f.tenant = s.tenant AND f.id = s.id ORDER BY f.id"))
                .extracting(row -> row.getField(1))
                .containsExactly("Ada", null);
    }

    @ParameterizedTest
    @MethodSource("lookupCases")
    void looksUpUuidCompositeKeysInBothDialects(Dialect dialect, boolean async) throws Exception {
        SpannerDatabase database = uuidLookupDatabase(dialect);
        UUID id = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("uuid_lookup_records")
                                        .set("external_id")
                                        .to(Value.uuid(id))
                                        .set("tenant")
                                        .to(1L)
                                        .set("name")
                                        .to("Ada")
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(uuidLookupTableDdl(database, dialect, async));
        table.executeSql(
                "CREATE TABLE uuid_facts (id BIGINT, tenant BIGINT, event_time AS PROCTIME()) "
                        + "WITH ('connector'='datagen', 'number-of-rows'='2', "
                        + "'fields.id.kind'='sequence', 'fields.id.start'='1', "
                        + "'fields.id.end'='2', 'fields.tenant.kind'='sequence', "
                        + "'fields.tenant.start'='1', 'fields.tenant.end'='2')");

        assertThat(
                        rows(
                                table,
                                "SELECT f.id, s.name FROM uuid_facts AS f "
                                        + "LEFT JOIN uuid_lookup_source "
                                        + "FOR SYSTEM_TIME AS OF f.event_time AS s "
                                        + "ON f.tenant = s.tenant AND "
                                        + "CASE WHEN f.id = 1 THEN "
                                        + "'F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6' ELSE "
                                        + "'00000000-0000-0000-0000-000000000000' END "
                                        + "= s.external_id ORDER BY f.id"))
                .extracting(row -> row.getField(1))
                .containsExactly("Ada", null);
    }

    private static Stream<Arguments> lookupCases() {
        return Stream.of(Dialect.values())
                .flatMap(
                        dialect ->
                                Stream.of(
                                        Arguments.of(dialect, false), Arguments.of(dialect, true)));
    }

    private static Stream<Arguments> namedSchemaCases() {
        return lookupCases()
                .flatMap(
                        arguments ->
                                Stream.of(
                                        Arguments.of(arguments.get()[0], arguments.get()[1], false),
                                        Arguments.of(
                                                arguments.get()[0], arguments.get()[1], true)));
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void scansProjectedColumnsAndAggregates(Dialect dialect) throws Exception {
        SpannerDatabase database = database(dialect);
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(1L)
                                        .set("name")
                                        .to("Ada")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":1}"))
                                        .build(),
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(2L)
                                        .set("name")
                                        .to("Grace")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":2}"))
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(tableDdl(database, dialect));

        assertThat(rows(table, "SELECT name FROM source ORDER BY name"))
                .extracting(row -> row.getFieldAs(0).toString())
                .containsExactly("Ada", "Grace");
        assertThat(rows(table, "SELECT metadata FROM source WHERE id = 1"))
                .singleElement()
                .satisfies(
                        row ->
                                assertThat(row.getField(0).toString())
                                        .matches("\\{\\\"rank\\\":\\s*1}"));
        assertThat(rows(table, "SELECT name FROM source WHERE id = 2"))
                .isEqualTo(rows(table, "SELECT name FROM source WHERE id + 0 = 2"));
        assertThat(rows(table, "SELECT COUNT(*) FROM source"))
                .singleElement()
                .satisfies(row -> assertThat((Object) row.getField(0)).isEqualTo(2L));
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void scansASecondaryIndexWithAResidualFilter(Dialect dialect) throws Exception {
        SpannerDatabase database = indexedDatabase(dialect);
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(1L)
                                        .set("name")
                                        .to("Ada")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":1}"))
                                        .build(),
                                Mutation.newInsertBuilder("records")
                                        .set("id")
                                        .to(2L)
                                        .set("name")
                                        .to("Grace")
                                        .set("metadata")
                                        .to(json(dialect, "{\"rank\":2}"))
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(tableDdl(database, dialect, "records_by_name"));

        assertThat(rows(table, "SELECT id, metadata FROM source WHERE name = 'Grace'"))
                .singleElement()
                .satisfies(
                        row -> {
                            assertThat((Object) row.getField(0)).isEqualTo(2L);
                            assertThat(row.getField(1).toString())
                                    .matches("\\{\\\"rank\\\":\\s*2}");
                        });
        assertThat(rows(table, "SELECT id FROM source WHERE name = 'Grace'"))
                .isEqualTo(rows(table, "SELECT id FROM source WHERE UPPER(name) = 'GRACE'"));
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static List<Row> firstRows(TableEnvironment table, String sql, int count)
            throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            while (rows.size() < count && iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        return rows;
    }

    private static SpannerDatabase database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64), metadata jsonb)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64), metadata JSON) PRIMARY KEY (id)");
    }

    private static SpannerDatabase lookupDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE lookup_records (id bigint NOT NULL, tenant bigint NOT NULL, name varchar(64), PRIMARY KEY (id, tenant))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE lookup_records (id INT64 NOT NULL, tenant INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id, tenant)");
    }

    private static SpannerDatabase uuidLookupDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE uuid_lookup_records (external_id uuid NOT NULL, "
                            + "tenant bigint NOT NULL, name varchar(64), "
                            + "PRIMARY KEY (external_id, tenant))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE uuid_lookup_records (external_id UUID NOT NULL, "
                        + "tenant INT64 NOT NULL, name STRING(64)) "
                        + "PRIMARY KEY (external_id, tenant)");
    }

    private static SpannerDatabase indexedDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64), metadata jsonb)",
                    "CREATE INDEX records_by_name ON records (name) INCLUDE (metadata)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64), metadata JSON) PRIMARY KEY (id)",
                "CREATE INDEX records_by_name ON records (name) STORING (metadata)");
    }

    private static SpannerDatabase namedSchemaDatabase(Dialect dialect, boolean quoted)
            throws Exception {
        String schema = namedIdentifier(dialect, quoted, "analytics", "QuotedAnalytics");
        String table = namedIdentifier(dialect, quoted, "records", "QuotedRecords");
        String index = namedIdentifier(dialect, quoted, "records_by_name", "QuotedRecordsByName");
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE SCHEMA " + schema,
                    "CREATE TABLE "
                            + schema
                            + "."
                            + table
                            + " ("
                            + "id bigint NOT NULL PRIMARY KEY, name varchar(64))",
                    "CREATE INDEX " + index + " ON " + schema + "." + table + " (name)");
        }
        return createDatabase(
                dialect,
                "CREATE SCHEMA " + schema,
                "CREATE TABLE "
                        + schema
                        + "."
                        + table
                        + " ("
                        + "id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                "CREATE INDEX " + schema + "." + index + " ON " + schema + "." + table + " (name)");
    }

    private static Value json(Dialect dialect, String value) {
        return dialect == Dialect.POSTGRESQL ? Value.pgJsonb(value) : Value.json(value);
    }

    private static String tableDdl(SpannerDatabase database, Dialect dialect) {
        return tableDdl(database, dialect, null);
    }

    private static String tableDdl(SpannerDatabase database, Dialect dialect, String scanIndex) {
        return "CREATE TABLE source (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  metadata STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'schema.json-field-paths' = 'metadata',\n"
                + (scanIndex == null ? "" : "  'scan.index' = '" + scanIndex + "',\n")
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String lookupTableDdl(SpannerDatabase database, Dialect dialect, boolean async) {
        return "CREATE TABLE lookup_source (\n"
                + "  id BIGINT,\n"
                + "  tenant BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id, tenant) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'lookup_records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'lookup.async' = '"
                + async
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String namedSchemaTableDdl(
            String tableName,
            SpannerDatabase database,
            Dialect dialect,
            boolean async,
            boolean quoted) {
        String schema = namedIdentifier(dialect, quoted, "analytics", "QuotedAnalytics");
        String table = namedIdentifier(dialect, quoted, "records", "QuotedRecords");
        String index = namedIdentifier(dialect, quoted, "records_by_name", "QuotedRecordsByName");
        return "CREATE TABLE "
                + tableName
                + " (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'schema' = '"
                + schema
                + "',\n"
                + "  'table' = '"
                + table
                + "',\n"
                + "  'scan.index' = '"
                + index
                + "',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'lookup.async' = '"
                + async
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String namedIdentifier(
            Dialect dialect, boolean quoted, String unquoted, String quotedName) {
        if (!quoted) {
            return unquoted;
        }
        char quote = dialect == Dialect.POSTGRESQL ? '"' : '`';
        return quote + quotedName + quote;
    }

    private static String uuidLookupTableDdl(
            SpannerDatabase database, Dialect dialect, boolean async) {
        return "CREATE TABLE uuid_lookup_source (\n"
                + "  external_id STRING,\n"
                + "  tenant BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (external_id, tenant) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'uuid_lookup_records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'schema.uuid-field-paths' = 'external_id',\n"
                + "  'lookup.async' = '"
                + async
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String changeStreamTableDdl(
            SpannerDatabase database,
            Dialect dialect,
            String schema,
            String tableName,
            long startupTimestampMillis) {
        return "CREATE TABLE cdc (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + (schema == null ? "" : "  'schema' = '" + schema + "',\n")
                + "  'table' = '"
                + tableName
                + "',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'scan.mode' = 'change-stream',\n"
                + "  'scan.change-stream.name' = 'changes',\n"
                + "  'scan.change-stream.changelog-mode' = 'full',\n"
                + "  'scan.startup.mode' = 'timestamp',\n"
                + "  'scan.startup.timestamp-millis' = '"
                + startupTimestampMillis
                + "',\n"
                + "  'scan.change-stream.heartbeat-interval' = '1 s',\n"
                + "  'scan.max-concurrent-queries-per-subtask' = '2',\n"
                + "  'scan.parallelism' = '1',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String changeStreamMetadataTableDdl(
            SpannerDatabase database,
            Dialect dialect,
            String changelogMode,
            long startupTimestampMillis) {
        return "CREATE TABLE metadata_cdc (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  commit_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'commit-timestamp',\n"
                + "  record_sequence STRING METADATA FROM 'sequence',\n"
                + "  server_transaction_id STRING METADATA FROM 'server-transaction-id',\n"
                + "  is_last_record BOOLEAN METADATA FROM 'is-last-record-in-transaction-in-partition',\n"
                + "  source_table STRING METADATA FROM 'table',\n"
                + "  mod_type STRING METADATA FROM 'mod-type',\n"
                + "  value_capture_type STRING METADATA FROM 'value-capture-type',\n"
                + "  records_in_transaction BIGINT METADATA FROM 'number-of-records-in-transaction',\n"
                + "  partitions_in_transaction BIGINT METADATA FROM 'number-of-partitions-in-transaction',\n"
                + "  transaction_tag STRING METADATA FROM 'transaction-tag',\n"
                + "  system_transaction BOOLEAN METADATA FROM 'system-transaction',\n"
                + "  mod_number INT METADATA FROM 'mod-number',\n"
                + "  WATERMARK FOR commit_timestamp AS SOURCE_WATERMARK(),\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'metadata_records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'scan.mode' = 'change-stream',\n"
                + "  'scan.change-stream.name' = 'metadata_changes',\n"
                + "  'scan.change-stream.changelog-mode' = '"
                + changelogMode
                + "',\n"
                + "  'scan.startup.mode' = 'timestamp',\n"
                + "  'scan.startup.timestamp-millis' = '"
                + startupTimestampMillis
                + "',\n"
                + "  'scan.change-stream.heartbeat-interval' = '1 s',\n"
                + "  'scan.parallelism' = '1',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }
}
