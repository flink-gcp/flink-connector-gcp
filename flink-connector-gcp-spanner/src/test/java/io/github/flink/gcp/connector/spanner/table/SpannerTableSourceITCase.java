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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.spanner.table.UnboundedQueryRows.firstRows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end bounded SQL scan coverage against both emulator dialects. */
class SpannerTableSourceITCase extends AbstractSpannerEmulatorITCase {

    private static final Duration ROW_DEADLINE = Duration.ofSeconds(90);

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
        DatabaseDestination database =
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

        List<Row> rows = firstRows(table, "SELECT * FROM metadata_cdc", 2, ROW_DEADLINE);

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
        DatabaseDestination database = createDatabase(dialect, ddl.toArray(new String[0]));
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

        assertThat(firstRows(table, "SELECT id, name FROM cdc", 4, ROW_DEADLINE))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1L, "Ada"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1L, "Ada"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1L, "Grace"),
                        Row.ofKind(RowKind.DELETE, 1L, "Grace"));
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void boundedChangeStreamTableFinishesWithEveryChangeUpToTheBound(Dialect dialect)
            throws Exception {
        DatabaseDestination database =
                createDatabase(
                        dialect,
                        dialect == Dialect.POSTGRESQL
                                ? "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64))"
                                : "CREATE TABLE records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                        "CREATE CHANGE STREAM changes FOR records",
                        dialect == Dialect.POSTGRESQL
                                ? "ALTER CHANGE STREAM changes SET (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')"
                                : "ALTER CHANGE STREAM changes SET OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
        Instant first = insert(database, 1L, "Ada");
        Instant last = insert(database, 2L, "Grace");
        // The option is in milliseconds; rounding up keeps the last commit inside the bound.
        Instant bound =
                Instant.ofEpochMilli(
                        Math.addExact(
                                last.toEpochMilli(), last.getNano() % 1_000_000 == 0 ? 0 : 1));
        List<Row> expected = new ArrayList<>();
        expected.add(Row.ofKind(RowKind.INSERT, 1L, "Ada"));
        expected.add(Row.ofKind(RowKind.INSERT, 2L, "Grace"));
        // Write until a commit lies past the bound; one that still lands inside the bound's
        // millisecond is read like the others.
        long id = 3L;
        Instant committed;
        do {
            committed = insert(database, id, "Hedy");
            if (!committed.isAfter(bound)) {
                expected.add(Row.ofKind(RowKind.INSERT, id, "Hedy"));
            }
            id++;
        } while (!committed.isAfter(bound));

        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                changeStreamTableDdl(database, dialect, null, "records", first.toEpochMilli())
                        .replace(
                                "  'scan.parallelism' = '1',\n",
                                "  'scan.parallelism' = '1',\n"
                                        + "  'scan.bounded.timestamp-millis' = '"
                                        + bound.toEpochMilli()
                                        + "',\n"));

        // Asking for one row more than the bound admits returns early with it if the bound is
        // ignored, and the deadline fails rather than hangs a source that never finishes.
        assertThat(firstRows(table, "SELECT id, name FROM cdc", expected.size() + 1, ROW_DEADLINE))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * The coordinate columns end to end (#1960): the job serializes them to the reader, and a
     * key-only delete leaves the source carrying its own commit timestamp. Whether it still does at
     * the sink is the planner's choice, which differs by Flink version, so the delete's expected
     * timestamp branches on it as {@code SpannerChangeStreamCoordinatePlanTest} does.
     */
    @Test
    void anUpsertDeleteCarriesItsOwnCoordinatesThroughPhysicalColumns() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                        "CREATE CHANGE STREAM changes FOR records",
                        "ALTER CHANGE STREAM changes SET OPTIONS (value_capture_type = 'NEW_ROW')");
        Instant inserted = insert(database, 1L, "Ada");
        Instant updated =
                commit(
                        database,
                        Mutation.newUpdateBuilder("records")
                                .set("id")
                                .to(1L)
                                .set("name")
                                .to("Grace")
                                .build());
        Instant deleted = commit(database, Mutation.delete("records", Key.of(1L)));
        Instant bound =
                Instant.ofEpochMilli(
                        Math.addExact(
                                deleted.toEpochMilli(),
                                deleted.getNano() % 1_000_000 == 0 ? 0 : 1));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.executeSql(
                changeStreamTableDdl(
                                database,
                                Dialect.GOOGLE_STANDARD_SQL,
                                null,
                                "records",
                                inserted.toEpochMilli())
                        .replace(
                                "  name STRING,\n",
                                "  name STRING,\n"
                                        + "  commit_ts TIMESTAMP_LTZ(9) NOT NULL,\n"
                                        + "  record_sequence STRING NOT NULL,\n"
                                        + "  mod_number INT NOT NULL,\n")
                        .replace(
                                "'scan.change-stream.changelog-mode' = 'full'",
                                "'scan.change-stream.changelog-mode' = 'upsert'")
                        .replace(
                                "  'scan.parallelism' = '1',\n",
                                "  'scan.parallelism' = '1',\n"
                                        + "  'scan.bounded.timestamp-millis' = '"
                                        + bound.toEpochMilli()
                                        + "',\n"
                                        + "  'scan.change-stream.commit-timestamp-column' ="
                                        + " 'commit_ts',\n"
                                        + "  'scan.change-stream.sequence-column' ="
                                        + " 'record_sequence',\n"
                                        + "  'scan.change-stream.mod-number-column' ="
                                        + " 'mod_number',\n"));

        List<Row> changes = new ArrayList<>();
        // An upsert stream rather than collect(): SQL result collection would turn the changelog
        // into retractions and hide the delete the source emitted.
        try (CloseableIterator<Row> rows =
                table.toChangelogStream(
                                table.sqlQuery(
                                        "SELECT id, name, commit_ts, record_sequence, mod_number"
                                                + " FROM cdc"),
                                Schema.newBuilder().primaryKey("id").build(),
                                ChangelogMode.upsert())
                        .executeAndCollect()) {
            rows.forEachRemaining(changes::add);
        }

        String version = DataTypes.class.getPackage().getImplementationVersion();
        assertThat(version).as("the Flink version on the classpath").isNotNull();
        // Flink 1.20 completes the delete from the row it holds, the update.
        Instant deleteCommit = version.startsWith("1.") ? updated : deleted;
        assertThat(changes)
                .extracting(row -> Row.ofKind(row.getKind(), row.getField(0), row.getField(2)))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1L, inserted),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1L, updated),
                        Row.ofKind(RowKind.DELETE, 1L, deleteCommit));
        assertThat(changes)
                .allSatisfy(
                        row -> {
                            assertThat(row.getFieldAs(3).toString()).isNotBlank();
                            assertThat((Object) row.getField(4)).isEqualTo(0);
                        });
    }

    private static Instant commit(DatabaseDestination database, Mutation mutation) {
        Timestamp commit = client(database).write(List.of(mutation));
        return Instant.ofEpochSecond(commit.getSeconds(), commit.getNanos());
    }

    private static Instant insert(DatabaseDestination database, long id, String name) {
        Timestamp commit =
                client(database)
                        .write(
                                List.of(
                                        Mutation.newInsertBuilder("records")
                                                .set("id")
                                                .to(id)
                                                .set("name")
                                                .to(name)
                                                .build()));
        return Instant.ofEpochSecond(commit.getSeconds(), commit.getNanos());
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
        DatabaseDestination database = namedSchemaDatabase(dialect, quoted);
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
    @MethodSource("coarseKeyLookupCases")
    void aCoarserTimestampKeyLooksUpEveryStoredValueInItsBucket(
            Dialect dialect, boolean descending, boolean async, String cache) throws Exception {
        DatabaseDestination database =
                createDatabase(
                        dialect,
                        dialect == Dialect.POSTGRESQL
                                ? "CREATE TABLE events (tenant bigint NOT NULL, seen_at timestamptz"
                                        + " NOT NULL, seq bigint NOT NULL, name varchar(64),"
                                        + " PRIMARY KEY (tenant, seen_at, seq))"
                                : "CREATE TABLE events (tenant INT64 NOT NULL, seen_at TIMESTAMP"
                                        + " NOT NULL, seq INT64 NOT NULL, name STRING(64))"
                                        + " PRIMARY KEY (tenant,"
                                        + (descending ? " seen_at DESC," : " seen_at,")
                                        + " seq)");
        client(database)
                .write(
                        List.of(
                                event(7L, "2026-08-11T01:02:03.123456789Z", 2L, "Ada"),
                                // A second stored key in Ada's bucket, which the same probe joins.
                                event(7L, "2026-08-11T01:02:03.1231Z", 2L, "Ann"),
                                event(7L, "2026-08-11T01:02:03.123999Z", 3L, "Bob"),
                                event(7L, "2026-08-11T01:02:03.124Z", 2L, "Zed"),
                                event(8L, "2026-08-11T01:02:03.123456Z", 2L, "Other")));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                "CREATE TABLE event_source (tenant BIGINT, seen_at TIMESTAMP_LTZ(3), seq BIGINT,"
                        + " name STRING, PRIMARY KEY (tenant, seen_at, seq) NOT ENFORCED) WITH ("
                        + "'connector' = 'spanner', 'dialect' = '"
                        + dialect.name()
                        + "', 'lookup.async' = '"
                        + async
                        + "', 'lookup.cache' = '"
                        + cache
                        + "',"
                        + ("PARTIAL".equals(cache)
                                ? " 'lookup.partial-cache.max-rows' = '10',"
                                : "")
                        + " 'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'database' = '"
                        + database.getDatabase()
                        + "', 'table' = 'events', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        long millis = Instant.parse("2026-08-11T01:02:03.123Z").toEpochMilli();
        table.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT tenant, TO_TIMESTAMP_LTZ(millis, 3) AS"
                        + " probe_at, seq, PROCTIME() AS t FROM (VALUES"
                        + " (CAST(7 AS BIGINT), CAST("
                        + millis
                        + " AS BIGINT), CAST(2 AS BIGINT)),"
                        + " (CAST(7 AS BIGINT), CAST("
                        + millis
                        + " AS BIGINT), CAST(3 AS BIGINT)),"
                        // The first probe again, which a PARTIAL cache may answer from the
                        // multi-row entry the first lookup stored.
                        + " (CAST(7 AS BIGINT), CAST("
                        + millis
                        + " AS BIGINT), CAST(2 AS BIGINT)),"
                        + " (CAST(7 AS BIGINT), CAST("
                        + (millis + 2)
                        + " AS BIGINT), CAST(2 AS BIGINT))) AS v(tenant, millis, seq)");

        assertThat(
                        rows(
                                table,
                                "SELECT p.seq, p.probe_at, s.name FROM probes AS p LEFT JOIN"
                                        + " event_source FOR SYSTEM_TIME AS OF p.t AS s"
                                        + " ON p.tenant = s.tenant AND p.probe_at = s.seen_at"
                                        + " AND p.seq = s.seq"))
                .extracting(row -> row.getField(0) + " " + row.getField(2))
                .containsExactlyInAnyOrder("2 Ada", "2 Ann", "3 Bob", "2 Ada", "2 Ann", "2 null");
    }

    @Test
    void aCoarserTimestampKeyLooksUpALegacyQualifiedTable() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE SCHEMA sales",
                        "CREATE TABLE sales.events (tenant INT64 NOT NULL, seen_at TIMESTAMP"
                                + " NOT NULL, seq INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (tenant, seen_at DESC, seq)");
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertOrUpdateBuilder("sales.events")
                                        .set("tenant")
                                        .to(7L)
                                        .set("seen_at")
                                        .to(Timestamp.parseTimestamp("2026-08-11T01:02:03.123456Z"))
                                        .set("seq")
                                        .to(2L)
                                        .set("name")
                                        .to("Ada")
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        // The schema rides in 'table' with no 'named-schema', as the data API accepts it.
        table.executeSql(
                "CREATE TABLE event_source (tenant BIGINT, seen_at TIMESTAMP_LTZ(3), seq BIGINT,"
                        + " name STRING, PRIMARY KEY (tenant, seen_at, seq) NOT ENFORCED) WITH ("
                        + "'connector' = 'spanner', 'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'database' = '"
                        + database.getDatabase()
                        + "', 'table' = 'sales.events', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        table.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT tenant, TO_TIMESTAMP_LTZ(millis, 3) AS"
                        + " probe_at, seq, PROCTIME() AS t FROM (VALUES (CAST(7 AS BIGINT), CAST("
                        + Instant.parse("2026-08-11T01:02:03.123Z").toEpochMilli()
                        + " AS BIGINT), CAST(2 AS BIGINT))) AS v(tenant, millis, seq)");

        assertThat(
                        rows(
                                table,
                                "SELECT s.name FROM probes AS p JOIN event_source FOR SYSTEM_TIME"
                                        + " AS OF p.t AS s ON p.tenant = s.tenant"
                                        + " AND p.probe_at = s.seen_at AND p.seq = s.seq"))
                .containsExactly(Row.of("Ada"));
    }

    private static Stream<Arguments> coarseKeyLookupCases() {
        return Stream.of(
                Arguments.of(Dialect.GOOGLE_STANDARD_SQL, false, false, "NONE"),
                Arguments.of(Dialect.GOOGLE_STANDARD_SQL, false, true, "PARTIAL"),
                Arguments.of(Dialect.GOOGLE_STANDARD_SQL, true, false, "PARTIAL"),
                Arguments.of(Dialect.GOOGLE_STANDARD_SQL, true, true, "NONE"),
                Arguments.of(Dialect.POSTGRESQL, false, false, "PARTIAL"));
    }

    private static Mutation event(long tenant, String seenAt, long seq, String name) {
        return Mutation.newInsertOrUpdateBuilder("events")
                .set("tenant")
                .to(tenant)
                .set("seen_at")
                .to(Timestamp.parseTimestamp(seenAt))
                .set("seq")
                .to(seq)
                .set("name")
                .to(name)
                .build();
    }

    @ParameterizedTest
    @MethodSource("lookupCases")
    void looksUpCompositeHitsAndMissesInBothDialects(Dialect dialect, boolean async)
            throws Exception {
        DatabaseDestination database = lookupDatabase(dialect);
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
        DatabaseDestination database = uuidLookupDatabase(dialect);
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

    @ParameterizedTest
    @MethodSource("lookupCases")
    void additionalEqualityKeysFilterOnAndWhereWithBothCaches(Dialect dialect, boolean async)
            throws Exception {
        DatabaseDestination database = lookupDatabase(dialect);
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertBuilder("lookup_records")
                                        .set("id")
                                        .to(1L)
                                        .set("tenant")
                                        .to(7L)
                                        .set("name")
                                        .to("Ada")
                                        .build(),
                                Mutation.newInsertBuilder("lookup_records")
                                        .set("id")
                                        .to(2L)
                                        .set("tenant")
                                        .to(8L)
                                        .set("name")
                                        .to("Bob")
                                        .build()));
        for (boolean partial : new boolean[] {false, true}) {
            TableEnvironment table =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance().inBatchMode().build());
            table.getConfig().set("parallelism.default", "1");
            String ddl = lookupTableDdl(database, dialect, async);
            if (partial) {
                ddl =
                        ddl.replace(
                                "'connector' = 'spanner',",
                                "'connector' = 'spanner', 'lookup.cache' = 'PARTIAL', 'lookup.partial-cache.max-rows' = '10',");
            }
            table.executeSql(ddl);
            table.executeSql(
                    "CREATE TEMPORARY VIEW facts AS SELECT id, tenant, PROCTIME() AS t FROM (VALUES (CAST(1 AS BIGINT), CAST(7 AS BIGINT)), (CAST(2 AS BIGINT), CAST(8 AS BIGINT)), (CAST(3 AS BIGINT), CAST(9 AS BIGINT))) AS v(id,tenant)");
            assertThat(
                            rows(
                                    table,
                                    "SELECT f.id, s.name FROM facts AS f LEFT JOIN lookup_source FOR SYSTEM_TIME AS OF f.t AS s ON f.tenant = s.tenant AND f.id = s.id AND s.name = 'Ada'"))
                    .containsExactlyInAnyOrder(
                            Row.of(1L, "Ada"), Row.of(2L, null), Row.of(3L, null));
            assertThat(
                            rows(
                                    table,
                                    "SELECT f.id, s.name FROM facts AS f JOIN lookup_source FOR SYSTEM_TIME AS OF f.t AS s ON f.tenant = s.tenant AND f.id = s.id WHERE s.name = 'Ada'"))
                    .containsExactly(Row.of(1L, "Ada"));
            table.executeSql(
                    "CREATE TEMPORARY VIEW expected_names AS SELECT id, tenant, expected, PROCTIME() AS t FROM (VALUES (CAST(1 AS BIGINT), CAST(7 AS BIGINT), 'Ada'), (CAST(1 AS BIGINT), CAST(7 AS BIGINT), 'Grace'), (CAST(1 AS BIGINT), CAST(7 AS BIGINT), 'Ada')) AS v(id,tenant,expected)");
            assertThat(
                            rows(
                                    table,
                                    "SELECT f.id, s.name FROM expected_names AS f JOIN lookup_source FOR SYSTEM_TIME AS OF f.t AS s ON f.tenant = s.tenant AND f.id = s.id AND f.expected = s.name"))
                    .containsExactlyInAnyOrder(Row.of(1L, "Ada"), Row.of(1L, "Ada"));
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void aFullCacheJoinsOnPrimaryAndNonPrimaryKeysInBothDialects(Dialect dialect) throws Exception {
        DatabaseDestination database = lookupDatabase(dialect);
        client(database)
                .write(
                        List.of(
                                lookupRecord(1L, 7L, "Ada"),
                                lookupRecord(2L, 8L, "Bob"),
                                lookupRecord(3L, 9L, "Ada")));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(fullCacheLookupTableDdl(database, dialect, false, "1 h"));
        table.executeSql(
                "CREATE TEMPORARY VIEW facts AS SELECT id, tenant, PROCTIME() AS t FROM (VALUES (CAST(1 AS BIGINT), CAST(7 AS BIGINT)), (CAST(2 AS BIGINT), CAST(8 AS BIGINT)), (CAST(2 AS BIGINT), CAST(9 AS BIGINT))) AS v(id,tenant)");
        table.executeSql(
                "CREATE TEMPORARY VIEW names AS SELECT name, PROCTIME() AS t FROM (VALUES ('Ada'), ('Zed')) AS v(name)");

        assertThat(
                        rows(
                                table,
                                "SELECT f.id, f.tenant, s.name FROM facts AS f LEFT JOIN lookup_source FOR SYSTEM_TIME AS OF f.t AS s ON f.tenant = s.tenant AND f.id = s.id"))
                .containsExactlyInAnyOrder(
                        Row.of(1L, 7L, "Ada"), Row.of(2L, 8L, "Bob"), Row.of(2L, 9L, null));
        // No primary-key column in the join: the cache indexes every loaded row by name and
        // keeps both rows that share one.
        assertThat(
                        rows(
                                table,
                                "SELECT n.name, s.id FROM names AS n LEFT JOIN lookup_source FOR SYSTEM_TIME AS OF n.t AS s ON n.name = s.name"))
                .containsExactlyInAnyOrder(
                        Row.of("Ada", 1L), Row.of("Ada", 3L), Row.of("Zed", null));
        assertThat(
                        rows(
                                table,
                                "SELECT n.name, s.id FROM names AS n JOIN lookup_source FOR SYSTEM_TIME AS OF n.t AS s ON n.name = s.name AND s.tenant = 9"))
                .containsExactly(Row.of("Ada", 3L));
    }

    @Test
    void aFullCacheAndAScanTruncateTimestampsToTheDeclaredPrecision() throws Exception {
        Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;
        DatabaseDestination database =
                createDatabase(
                        dialect,
                        "CREATE TABLE instants (id INT64 NOT NULL, seen_at TIMESTAMP, seen_at_micros TIMESTAMP)"
                                + " PRIMARY KEY (id)");
        Timestamp stored = Timestamp.parseTimestamp("2026-08-11T01:02:03.123456789Z");
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertOrUpdateBuilder("instants")
                                        .set("id")
                                        .to(1L)
                                        .set("seen_at")
                                        .to(stored)
                                        .set("seen_at_micros")
                                        .to(stored)
                                        .build()));
        Instant millis = Instant.parse("2026-08-11T01:02:03.123Z");
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                "CREATE TABLE instant_source (id BIGINT, seen_at TIMESTAMP_LTZ(3), seen_at_micros TIMESTAMP_LTZ(6),"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector' = 'spanner', 'lookup.cache' = 'FULL',"
                        + " 'lookup.full-cache.periodic-reload.interval' = '1 h',"
                        + " 'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'database' = '"
                        + database.getDatabase()
                        + "', 'table' = 'instants', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        table.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT TO_TIMESTAMP_LTZ(millis, 3) AS probe_at,"
                        + " PROCTIME() AS t FROM (VALUES (CAST("
                        + millis.toEpochMilli()
                        + " AS BIGINT))) AS v(millis)");

        // A collected TIMESTAMP_LTZ(3) is serialized as milliseconds whatever it held, so only a
        // finer precision shows what the scan produced.
        assertThat(rows(table, "SELECT id, seen_at_micros FROM instant_source"))
                .containsExactly(Row.of(1L, Instant.parse("2026-08-11T01:02:03.123456Z")));
        // The cache indexes loaded rows by value equality, so a stored value that kept its
        // sub-millisecond digits would never equal a TIMESTAMP_LTZ(3) probe.
        assertThat(
                        rows(
                                table,
                                "SELECT p.probe_at, s.id FROM probes AS p LEFT JOIN instant_source FOR SYSTEM_TIME AS OF p.t AS s ON p.probe_at = s.seen_at"))
                .containsExactly(Row.of(millis, 1L));
    }

    @Test
    void pushedFiltersOnACoarserTimestampKeyCompareTheTruncatedValues() throws Exception {
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE moments (seen_at TIMESTAMP NOT NULL, id INT64)"
                                + " PRIMARY KEY (seen_at)");
        client(database)
                .write(
                        List.of(
                                Mutation.newInsertOrUpdateBuilder("moments")
                                        .set("seen_at")
                                        .to(
                                                Timestamp.parseTimestamp(
                                                        "2026-08-11T01:02:03.123456789Z"))
                                        .set("id")
                                        .to(1L)
                                        .build(),
                                Mutation.newInsertOrUpdateBuilder("moments")
                                        .set("seen_at")
                                        .to(Timestamp.parseTimestamp("2026-08-11T01:02:03.124Z"))
                                        .set("id")
                                        .to(2L)
                                        .build()));
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                "CREATE TABLE moment_source (seen_at TIMESTAMP_LTZ(3), id BIGINT,"
                        + " PRIMARY KEY (seen_at) NOT ENFORCED) WITH ('connector' = 'spanner',"
                        + " 'project' = '"
                        + database.getProject()
                        + "', 'instance' = '"
                        + database.getInstance()
                        + "', 'database' = '"
                        + database.getDatabase()
                        + "', 'table' = 'moments', 'emulator-endpoint' = '"
                        + emulatorEndpoint()
                        + "')");
        String millis =
                "TO_TIMESTAMP_LTZ("
                        + Instant.parse("2026-08-11T01:02:03.123Z").toEpochMilli()
                        + ", 3)";

        assertThat(table.explainSql("SELECT id FROM moment_source WHERE seen_at > " + millis))
                .as("the comparison reaches the source and also stays for Flink")
                .contains("filter=[")
                .contains("where=[");
        // The first row reads as .123: it equals the literal and is not greater than it.
        assertThat(rows(table, "SELECT id FROM moment_source WHERE seen_at = " + millis))
                .containsExactly(Row.of(1L));
        assertThat(rows(table, "SELECT id FROM moment_source WHERE seen_at <= " + millis))
                .containsExactly(Row.of(1L));
        assertThat(rows(table, "SELECT id FROM moment_source WHERE seen_at > " + millis))
                .containsExactly(Row.of(2L));
    }

    @Test
    void aPeriodicFullCacheReloadObservesAChangedRow() throws Exception {
        Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;
        DatabaseDestination database = lookupDatabase(dialect);
        client(database).write(List.of(lookupRecord(1L, 7L, "Ada")));
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(fullCacheLookupTableDdl(database, dialect, false, "1 s"));
        table.executeSql(
                "CREATE TABLE ticks (id BIGINT, tenant AS CAST(7 AS BIGINT), t AS PROCTIME())"
                        + " WITH ('connector'='datagen', 'rows-per-second'='20',"
                        + " 'fields.id.min'='1', 'fields.id.max'='2')");
        List<Row> seen = Collections.synchronizedList(new ArrayList<>());

        try (CloseableIterator<Row> rows =
                table.executeSql(
                                "SELECT k.id, s.name FROM ticks AS k JOIN lookup_source FOR SYSTEM_TIME AS OF k.t AS s ON k.id = s.id AND k.tenant = s.tenant")
                        .collect()) {
            awaitRow(rows, row -> "Ada".equals(row.getField(1)), seen);
            client(database).write(List.of(lookupRecord(1L, 7L, "Grace")));
            awaitRow(rows, row -> "Grace".equals(row.getField(1)), seen);
        }
    }

    @Test
    void aFullCacheRefusesAsynchronousLookupsWhileTheJoinIsPlanned() throws Exception {
        Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;
        DatabaseDestination database = lookupDatabase(dialect);
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(fullCacheLookupTableDdl(database, dialect, true, "1 h"));
        table.executeSql(
                "CREATE TEMPORARY VIEW facts AS SELECT id, tenant, PROCTIME() AS t FROM (VALUES (CAST(1 AS BIGINT), CAST(7 AS BIGINT))) AS v(id,tenant)");

        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "SELECT f.id, s.name FROM facts AS f JOIN lookup_source FOR SYSTEM_TIME AS OF f.t AS s ON f.tenant = s.tenant AND f.id = s.id"))
                .hasStackTraceContaining(
                        "'lookup.async' cannot be true when 'lookup.cache' is FULL");
    }

    private static Mutation lookupRecord(long id, long tenant, String name) {
        return Mutation.newInsertOrUpdateBuilder("lookup_records")
                .set("id")
                .to(id)
                .set("tenant")
                .to(tenant)
                .set("name")
                .to(name)
                .build();
    }

    private static String fullCacheLookupTableDdl(
            DatabaseDestination database, Dialect dialect, boolean async, String reloadInterval) {
        String ddl = lookupTableDdl(database, dialect, async);
        String full =
                ddl.replace(
                        "'connector' = 'spanner',",
                        "'connector' = 'spanner', 'lookup.cache' = 'FULL',"
                                + " 'lookup.full-cache.periodic-reload.interval' = '"
                                + reloadInterval
                                + "',");
        // Without FULL the reload test would pass on uncached point reads.
        assertThat(full).as("the FULL options reached the DDL").isNotEqualTo(ddl);
        return full;
    }

    /**
     * Reads an unbounded query's rows until one matches, within {@link #ROW_DEADLINE}. The iterator
     * stays open for the caller's next wait; the caller closes it, which cancels the job and ends a
     * read still blocked past the deadline.
     */
    private static void awaitRow(CloseableIterator<Row> rows, Predicate<Row> match, List<Row> seen)
            throws Exception {
        ExecutorService reader =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "it-await-row");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> reading =
                    reader.submit(
                            () -> {
                                while (rows.hasNext()) {
                                    Row row = rows.next();
                                    seen.add(row);
                                    if (match.test(row)) {
                                        return;
                                    }
                                }
                                throw new AssertionError(
                                        "The query ended after "
                                                + seen.size()
                                                + " rows without a matching row");
                            });
            try {
                reading.get(ROW_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                        "No matching row within "
                                + ROW_DEADLINE
                                + " after "
                                + seen.size()
                                + " rows; the last was "
                                + (seen.isEmpty() ? "none" : seen.get(seen.size() - 1)),
                        e);
            }
        } finally {
            reader.shutdownNow();
        }
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
        DatabaseDestination database = database(dialect);
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
        DatabaseDestination database = indexedDatabase(dialect);
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

    private static DatabaseDestination database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64), metadata jsonb)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64), metadata JSON) PRIMARY KEY (id)");
    }

    private static DatabaseDestination lookupDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE lookup_records (id bigint NOT NULL, tenant bigint NOT NULL, name varchar(64), PRIMARY KEY (id, tenant))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE lookup_records (id INT64 NOT NULL, tenant INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id, tenant)");
    }

    private static DatabaseDestination uuidLookupDatabase(Dialect dialect) throws Exception {
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

    private static DatabaseDestination indexedDatabase(Dialect dialect) throws Exception {
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

    private static DatabaseDestination namedSchemaDatabase(Dialect dialect, boolean quoted)
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

    private static String tableDdl(DatabaseDestination database, Dialect dialect) {
        return tableDdl(database, dialect, null);
    }

    private static String tableDdl(
            DatabaseDestination database, Dialect dialect, String scanIndex) {
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
                + "  'json-field-paths' = 'metadata',\n"
                + (scanIndex == null ? "" : "  'scan.index' = '" + scanIndex + "',\n")
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String lookupTableDdl(
            DatabaseDestination database, Dialect dialect, boolean async) {
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
            DatabaseDestination database,
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
                + "  'named-schema' = '"
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
            DatabaseDestination database, Dialect dialect, boolean async) {
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
                + "  'uuid-field-paths' = 'external_id',\n"
                + "  'lookup.async' = '"
                + async
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String changeStreamTableDdl(
            DatabaseDestination database,
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
                + (schema == null ? "" : "  'named-schema' = '" + schema + "',\n")
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
            DatabaseDestination database,
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
