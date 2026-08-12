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
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end SQL sink coverage against both dialects of the Spanner emulator. */
class SpannerTableSinkITCase extends AbstractSpannerEmulatorITCase {

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void insertsAndUpsertsThroughTheProductionFactory(Dialect dialect) throws Exception {
        SpannerDatabase database = database(dialect);
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
        SpannerDatabase database = numericDatabase(dialect);
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
        SpannerDatabase database = uuidDatabase(dialect);
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

    private static SpannerDatabase database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)");
    }

    private static SpannerDatabase numericDatabase(Dialect dialect) throws Exception {
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

    private static SpannerDatabase uuidDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE uuid_records (id uuid NOT NULL PRIMARY KEY, related uuid[])");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE uuid_records (id UUID NOT NULL, related ARRAY<UUID>) PRIMARY KEY (id)");
    }

    private static String tableDdl(SpannerDatabase database, Dialect dialect) {
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

    private static String numericTableDdl(
            String tableName, SpannerDatabase database, Dialect dialect) {
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
            String tableName, SpannerDatabase database, Dialect dialect) {
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
                + "  'schema.uuid-field-paths' = 'id;related',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }
}
