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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;

import static io.github.flink.gcp.connector.spanner.table.UnboundedQueryRows.firstRows;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end SQL sink coverage against both dialects of the Spanner emulator. */
class SpannerTableSinkITCase extends AbstractSpannerEmulatorITCase {

    private static final Duration ROW_DEADLINE = Duration.ofSeconds(90);

    /**
     * The sink's change-stream exclusion, read back through the connector's own change-stream
     * source. One stream allows transaction exclusion and one does not, so the test also shows that
     * the exclusion is the stream's to honour: the stream without the option still records the
     * excluded write.
     *
     * <p>The transaction tag is set but not asserted. The emulator records no tag in a change
     * stream, not even a {@code Commit}'s (measured 2026-11-07); the gated suite asserts it.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void excludesTaggedWritesOnlyFromAStreamThatAllowsIt(Dialect dialect) throws Exception {
        DatabaseDestination database =
                dialect == Dialect.POSTGRESQL
                        ? createDatabase(
                                dialect,
                                "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64))",
                                "CREATE CHANGE STREAM excludable FOR records WITH"
                                        + " (allow_txn_exclusion = true,"
                                        + " value_capture_type = 'NEW_ROW_AND_OLD_VALUES')",
                                "CREATE CHANGE STREAM recording FOR records WITH"
                                        + " (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')")
                        : createDatabase(
                                dialect,
                                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64))"
                                        + " PRIMARY KEY (id)",
                                "CREATE CHANGE STREAM excludable FOR records OPTIONS"
                                        + " (allow_txn_exclusion = true,"
                                        + " value_capture_type = 'NEW_ROW_AND_OLD_VALUES')",
                                "CREATE CHANGE STREAM recording FOR records OPTIONS"
                                        + " (value_capture_type = 'NEW_ROW_AND_OLD_VALUES')");
        // A commit's own timestamp rather than the host clock: the emulator runs in a container
        // whose clock may drift from the JVM's. The row is filtered out of both reads.
        long start =
                client(database)
                        .write(
                                List.of(
                                        Mutation.newInsertBuilder("records")
                                                .set("id")
                                                .to(4L)
                                                .set("name")
                                                .to("start")
                                                .build()))
                        .toSqlTimestamp()
                        .getTime();
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.executeSql(
                sinkDdl(
                        "tagged",
                        database,
                        dialect,
                        "  'sink.transaction-tag' = 'flink-tagged',\n"));
        table.executeSql(
                sinkDdl(
                        "excluded",
                        database,
                        dialect,
                        "  'sink.exclude-txn-from-change-streams' = 'true',\n"
                                + "  'sink.transaction-tag' = 'flink-excluded',\n"));
        table.executeSql("INSERT INTO tagged VALUES (1, 'tagged')").await();
        table.executeSql("INSERT INTO excluded VALUES (2, 'excluded')").await();
        // A write outside the sink closes the window: the bound covers it, so a read that stops at
        // the bound has seen everything the two sink writes could have produced.
        Timestamp last =
                client(database)
                        .write(
                                List.of(
                                        Mutation.newInsertBuilder("records")
                                                .set("id")
                                                .to(3L)
                                                .set("name")
                                                .to("plain")
                                                .build()));
        long bound = Math.addExact(last.toSqlTimestamp().getTime(), 1);

        table.executeSql(cdcDdl("excludable_cdc", "excludable", database, dialect, start, bound));
        table.executeSql(cdcDdl("recording_cdc", "recording", database, dialect, start, bound));

        // One row more than expected: a bounded read returns early only if an extra row arrives.
        assertThat(
                        firstRows(
                                table,
                                "SELECT id FROM excludable_cdc WHERE id IN (1, 2, 3)",
                                3,
                                ROW_DEADLINE))
                .containsExactlyInAnyOrder(Row.of(1L), Row.of(3L));
        assertThat(
                        firstRows(
                                table,
                                "SELECT id FROM recording_cdc WHERE id IN (1, 2, 3)",
                                4,
                                ROW_DEADLINE))
                .containsExactlyInAnyOrder(Row.of(1L), Row.of(2L), Row.of(3L));
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void insertsAndUpsertsThroughTheProductionFactory(Dialect dialect) throws Exception {
        DatabaseDestination database = database(dialect);
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.executeSql(tableDdl(database, dialect));

        table.executeSql("INSERT INTO target VALUES (1, 'first'), (2, 'keep')").await();
        // Separate jobs give writes for the same key a defined order even though BatchWrite may
        // apply mutations within one request in any order.
        table.executeSql("INSERT INTO target VALUES (1, 'second')").await();

        List<Struct> rows = query(database, "SELECT id, name FROM records ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(0).getString("name")).isEqualTo("second");
        assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
        assertThat(rows.get(1).getString("name")).isEqualTo("keep");
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void roundTripsNumericThroughTheProductionSinkAndBoundedSource(Dialect dialect)
            throws Exception {
        DatabaseDestination database = numericDatabase(dialect);
        TableEnvironment sink =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        sink.executeSql(numericTableDdl("numeric_sink", database, dialect));
        if (dialect == Dialect.POSTGRESQL) {
            sink.executeSql(
                            "INSERT INTO numeric_sink VALUES "
                                    + "(1, CAST(12.34 AS DECIMAL(10, 2)), "
                                    + "CAST(0.123456789012345678 AS DECIMAL(18, 18))), "
                                    + "(2, CAST(NULL AS DECIMAL(10, 2)), "
                                    + "CAST(NULL AS DECIMAL(18, 18)))")
                    .await();
        } else {
            sink.executeSql(
                            "INSERT INTO numeric_sink VALUES "
                                    + "(1, CAST(12.340000000 AS DECIMAL(38, 9))), "
                                    + "(2, CAST(NULL AS DECIMAL(38, 9)))")
                    .await();
        }

        TableEnvironment source =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        source.executeSql(numericTableDdl("numeric_source", database, dialect));
        if (dialect == Dialect.POSTGRESQL) {
            try (CloseableIterator<Row> rows =
                    source.executeSql("SELECT id, amount, fraction FROM numeric_source ORDER BY id")
                            .collect()) {
                assertThat(rows)
                        .toIterable()
                        .containsExactly(
                                Row.of(
                                        1L,
                                        new java.math.BigDecimal("12.34"),
                                        new java.math.BigDecimal("0.123456789012345678")),
                                Row.of(2L, null, null));
            }
        } else {
            try (CloseableIterator<Row> rows =
                    source.executeSql("SELECT id, amount FROM numeric_source ORDER BY id")
                            .collect()) {
                assertThat(rows)
                        .toIterable()
                        .containsExactly(
                                Row.of(1L, new java.math.BigDecimal("12.340000000")),
                                Row.of(2L, null));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void roundTripsUuidScalarsArraysAndNulls(Dialect dialect) throws Exception {
        DatabaseDestination database = uuidDatabase(dialect);
        TableEnvironment sink =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        sink.executeSql(uuidTableDdl("uuid_sink", database, dialect));
        sink.executeSql(
                        "INSERT INTO uuid_sink VALUES "
                                + "('F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6', "
                                + "ARRAY['00000000-0000-0000-0000-000000000001', "
                                + "CAST(NULL AS STRING), "
                                + "'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF']), "
                                + "('00000000-0000-0000-0000-000000000000', "
                                + "CAST(NULL AS ARRAY<STRING>))")
                .await();

        TableEnvironment source =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        source.executeSql(uuidTableDdl("uuid_source", database, dialect));
        try (CloseableIterator<Row> rows =
                source.executeSql("SELECT id, related FROM uuid_source ORDER BY id").collect()) {
            assertThat(rows)
                    .toIterable()
                    .containsExactly(
                            Row.of("00000000-0000-0000-0000-000000000000", null),
                            Row.of(
                                    "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
                                    new String[] {
                                        "00000000-0000-0000-0000-000000000001",
                                        null,
                                        "ffffffff-ffff-ffff-ffff-ffffffffffff"
                                    }));
        }
    }

    private static DatabaseDestination database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)");
    }

    private static DatabaseDestination numericDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE numeric_records ("
                            + "id bigint NOT NULL PRIMARY KEY, amount numeric, fraction numeric)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE numeric_records (id INT64 NOT NULL, amount NUMERIC) PRIMARY KEY (id)");
    }

    private static DatabaseDestination uuidDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE uuid_records (id uuid NOT NULL PRIMARY KEY, related uuid[])");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE uuid_records (id UUID NOT NULL, related ARRAY<UUID>) PRIMARY KEY (id)");
    }

    private static String tableDdl(DatabaseDestination database, Dialect dialect) {
        return "CREATE TABLE target (\n"
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
                + "  'table' = 'records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String sinkDdl(
            String name, DatabaseDestination database, Dialect dialect, String sinkOptions) {
        return tableDdl(database, dialect)
                .replace("CREATE TABLE target (", "CREATE TABLE " + name + " (")
                .replace("  'emulator-endpoint'", sinkOptions + "  'emulator-endpoint'");
    }

    private static String cdcDdl(
            String name,
            String changeStream,
            DatabaseDestination database,
            Dialect dialect,
            long startupTimestampMillis,
            long boundedTimestampMillis) {
        return "CREATE TABLE "
                + name
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
                + "  'table' = 'records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'scan.mode' = 'change-stream',\n"
                + "  'scan.change-stream.name' = '"
                + changeStream
                + "',\n"
                + "  'scan.change-stream.changelog-mode' = 'full',\n"
                + "  'scan.startup.mode' = 'timestamp',\n"
                + "  'scan.startup.timestamp-millis' = '"
                + startupTimestampMillis
                + "',\n"
                + "  'scan.bounded.timestamp-millis' = '"
                + boundedTimestampMillis
                + "',\n"
                + "  'scan.change-stream.heartbeat-interval' = '1 s',\n"
                + "  'scan.parallelism' = '1',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String numericTableDdl(
            String tableName, DatabaseDestination database, Dialect dialect) {
        return "CREATE TABLE "
                + tableName
                + " (\n"
                + "  id BIGINT,\n"
                + (dialect == Dialect.POSTGRESQL
                        ? "  amount DECIMAL(10, 2),\n  fraction DECIMAL(18, 18),\n"
                        : "  amount DECIMAL(38, 9),\n")
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
                + "  'table' = 'numeric_records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }

    private static String uuidTableDdl(
            String tableName, DatabaseDestination database, Dialect dialect) {
        return "CREATE TABLE "
                + tableName
                + " (\n"
                + "  id STRING,\n"
                + "  related ARRAY<STRING>,\n"
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
                + "  'table' = 'uuid_records',\n"
                + "  'dialect' = '"
                + dialect.name()
                + "',\n"
                + "  'uuid-field-paths' = 'id;related',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }
}
