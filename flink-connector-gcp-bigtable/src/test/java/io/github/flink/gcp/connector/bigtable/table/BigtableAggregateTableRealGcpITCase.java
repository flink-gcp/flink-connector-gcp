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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Type;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real-service evidence for aggregate provisioning, SQL contributions and replay behavior. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableAggregateTableRealGcpITCase extends AbstractBigtableRealGcpITCase {
    private static final long TIMESTAMP = 1_000_000L;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void createsAllTypesAndReappliesIntegerContributions(boolean primaryKey) throws Exception {
        String id = primaryKey ? "aggregate-sql-key" : "aggregate-sql-no-key";
        TableDestination destination = tableDestination(id);
        TableEnvironment env = environment();
        env.executeSql(ddl(id, true, true, primaryKey));
        String insert =
                "INSERT INTO bt VALUES "
                        + "('r', ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), TO_TIMESTAMP_LTZ(1000, 3)),"
                        + "('r', ROW(CAST(5 AS BIGINT)), ROW(CAST(5 AS BIGINT)), ROW(CAST(5 AS BIGINT)), ROW(CAST(5 AS BIGINT)), TO_TIMESTAMP_LTZ(1000, 3)),"
                        + "('r', ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), ROW(CAST(3 AS BIGINT)), TO_TIMESTAMP_LTZ(1000, 3))";
        for (int application = 1; application <= 2; application++) {
            if (application == 2) {
                env.executeSql("DROP TABLE bt");
                env.executeSql(ddl(id, false, true, primaryKey));
            }
            env.executeSql(insert).await();
            Row row = readRows(destination).get(0);
            assertThat(value(row, "totals")).isEqualTo(application * 11L);
            assertThat(value(row, "minimums")).isEqualTo(3);
            assertThat(value(row, "maximums")).isEqualTo(5);
            assertThat(hllCount(destination)).isEqualTo(2);
            for (String family : new String[] {"totals", "minimums", "maximums", "users"}) {
                assertThat(row.getCells(family, "q"))
                        .singleElement()
                        .satisfies(cell -> assertThat(cell.getTimestamp()).isEqualTo(TIMESTAMP));
            }
        }
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, destination.getInstance())) {
            var families = admin.getTable(id).getColumnFamilies();
            assertThat(families).hasSize(4);
            assertThat(families).allSatisfy(family -> assertThat(family.hasGCRule()).isTrue());
            assertThat(families)
                    .extracting(family -> family.getValueType())
                    .containsExactlyInAnyOrder(
                            Type.int64Sum(), Type.int64Min(), Type.int64Max(), Type.int64Hll());
        }
    }

    @Test
    void writerClockCreatesNewVersionsWhileNullCellsContributeNothing() throws Exception {
        String id = "aggregate-writer-clock";
        TableDestination destination = tableDestination(id);
        TableEnvironment env = environment();
        env.executeSql(ddl(id, true, false, false));
        String insert =
                "INSERT INTO bt VALUES ('r', ROW(CAST(7 AS BIGINT)), "
                        + "ROW(CAST(NULL AS BIGINT)), CAST(NULL AS ROW<q BIGINT>), ROW(CAST(42 AS BIGINT)))";
        env.executeSql(insert).await();
        Row first = readRows(destination).get(0);
        long initial = first.getCells("totals", "q").get(0).getTimestamp();
        assertThat(first.getCells("minimums", "q")).isEmpty();
        assertThat(first.getCells("maximums", "q")).isEmpty();
        env.executeSql(insert).await();
        Row replay = readRows(destination).get(0);
        assertThat(replay.getCells("totals", "q"))
                .hasSize(2)
                .allSatisfy(
                        cell ->
                                assertThat(ByteBuffer.wrap(cell.getValue().toByteArray()).getLong())
                                        .isEqualTo(7));
        assertThat(replay.getCells("totals", "q").get(0).getTimestamp()).isGreaterThan(initial);
        assertThat(hllCount(destination)).isEqualTo(1);
    }

    @Test
    void addsTypedFamiliesWithoutChangingExistingGcAndRejectsMismatches() throws Exception {
        TableDestination destination = tableDestination("aggregate-reconcile");
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, destination.getInstance())) {
            admin.createTable(
                    CreateTableRequest.of(destination.getTable())
                            .addFamily("raw")
                            .addFamily("totals", Type.int64Sum()));
        }
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily("raw")
                        .columnFamily("totals", ColumnFamilyType.INT64_SUM, GcRule.maxVersions(2))
                        .columnFamily("minimums", ColumnFamilyType.INT64_MIN, GcRule.maxVersions(2))
                        .columnFamily("maximums", ColumnFamilyType.INT64_MAX, null)
                        .columnFamily("users", ColumnFamilyType.INT64_HLL, null)
                        .build();
        try (BigtableTableAdmin admin = new BigtableTableAdmin()) {
            assertThat(admin.ensureTable(destination, options).columnFamiliesAdded()).isEqualTo(3);
            assertThat(admin.ensureTable(destination, options).columnFamiliesAdded()).isZero();
            assertThatThrownBy(
                            () ->
                                    admin.ensureTable(
                                            destination,
                                            TableCreateOptions.builder()
                                                    .columnFamily(
                                                            "raw", ColumnFamilyType.INT64_SUM, null)
                                                    .build()))
                    .hasMessageContaining(
                            "column family 'raw' has value type raw; expected int64-sum");
        }
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, destination.getInstance())) {
            assertThat(admin.getTable(destination.getTable()).getColumnFamilies())
                    .filteredOn(f -> f.getId().equals("totals"))
                    .singleElement()
                    .satisfies(family -> assertThat(family.hasGCRule()).isFalse());
        }
        TableEnvironment env = environment();
        env.executeSql(
                ddl(destination.getTable(), false, true, false)
                        .replace("totals:int64-sum", "totals:int64-max"));
        assertThatThrownBy(
                        () ->
                                env.executeSql(
                                                "INSERT INTO bt VALUES ('bad', ROW(CAST(1 AS BIGINT)), "
                                                        + "ROW(CAST(1 AS BIGINT)), ROW(CAST(1 AS BIGINT)), ROW(CAST(1 AS BIGINT)), TO_TIMESTAMP_LTZ(1000, 3))")
                                        .await())
                .hasStackTraceContaining(
                        "column family 'totals' has value type int64-sum; expected int64-max");
        assertThat(readRows(destination)).isEmpty();
    }

    private static TableEnvironment environment() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.getConfig().getConfiguration().setString("restart-strategy.type", "none");
        env.getConfig().getConfiguration().setString("parallelism.default", "1");
        return env;
    }

    private static String ddl(String id, boolean create, boolean timestamp, boolean primaryKey) {
        return "CREATE TABLE bt (rowkey STRING, totals ROW<q BIGINT>, minimums ROW<q BIGINT>, "
                + "maximums ROW<q BIGINT>, users ROW<q BIGINT>"
                + (timestamp ? ", ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp'" : "")
                + (primaryKey ? ", PRIMARY KEY (rowkey) NOT ENFORCED" : "")
                + ") WITH ('connector'='bigtable', 'project'='"
                + PROJECT
                + "', 'instance'='"
                + tableDestination(id).getInstance()
                + "', 'table'='"
                + id
                + "', 'sink.write-mode'='aggregate', "
                + "'sink.aggregate.column-family-types'='totals:int64-sum,minimums:int64-min,maximums:int64-max,users:int64-hll'"
                + (create
                        ? ", 'sink.create-disposition'='create-if-needed', 'sink.table-create.gc-rule.max-versions'='2'"
                        : "")
                + ")";
    }

    private static long value(Row row, String family) {
        return ByteBuffer.wrap(row.getCells(family, "q").get(0).getValue().toByteArray()).getLong();
    }

    private static long hllCount(TableDestination destination) throws Exception {
        try (BigtableDataClient client =
                        BigtableDataClient.create(PROJECT, destination.getInstance());
                ResultSet result =
                        client.executeQuery(
                                client.prepareStatement(
                                                "SELECT HLL_COUNT.EXTRACT(users['q']) AS n FROM `"
                                                        + destination.getTable()
                                                        + "`",
                                                Map.of())
                                        .bind()
                                        .build())) {
            assertThat(result.next()).isTrue();
            long count = result.getLong("n");
            assertThat(result.next()).isFalse();
            return count;
        }
    }
}
