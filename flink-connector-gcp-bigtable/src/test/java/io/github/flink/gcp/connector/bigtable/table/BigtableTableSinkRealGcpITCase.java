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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The table sink against real Cloud Bigtable, driven through SQL with <b>no</b> {@code
 * emulator-endpoint} option.
 *
 * <p>SQL jobs reach application-default credential authentication, which emulator-endpoint DDL
 * bypasses. They also exercise {@code sink.app-profile-id}, which the emulator ignores entirely
 * (ADR-0080), so only the service can establish that the option reached the wire.
 *
 * <p>Keep-latest tests inspect all cell versions immediately after completed writes, without a
 * latest-version filter or GC retention rule. The emulator cannot establish these service
 * behaviors.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableTableSinkRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String APP_PROFILE = "flink-table-sink-it";

    private static String ddl(String tableId, String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put("project", PROJECT);
        options.put("instance", tableDestination(tableId).getInstance());
        options.put("table", tableId);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return "CREATE TABLE bt (\n"
                + "  rowkey STRING,\n"
                + "  "
                + FAMILY
                + " ROW<name STRING, amount BIGINT>,\n"
                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                + ") "
                + options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepLatestReplacesAllVersionsWithoutTouchingOmittedCells(boolean explicitTimestamp)
            throws Exception {
        String id = explicitTimestamp ? "table-keep-latest-explicit" : "table-keep-latest-clock";
        TableDestination table = createTable(id);
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, table.getInstance())) {
            admin.modifyFamilies(ModifyColumnFamiliesRequest.of(id).addFamily("untouched"));
        }
        mutateRow(
                table,
                ByteString.copyFromUtf8("r1"),
                seed -> {
                    for (long timestamp : new long[] {1_000L, 2_000L}) {
                        seed.setCell(FAMILY, "name", timestamp, "old")
                                .setCell(FAMILY, "amount", timestamp, 1L)
                                .setCell(FAMILY, "undeclared", timestamp, "keep")
                                .setCell("untouched", "flag", timestamp, "keep");
                    }
                });
        Row before = readRows(table).get(0);
        assertThat(before.getCells(FAMILY, "name")).hasSize(2);
        assertThat(before.getCells(FAMILY, "amount")).hasSize(2);
        List<RowCell> undeclared = before.getCells(FAMILY, "undeclared");
        List<RowCell> omittedFamily = before.getCells("untouched", "flag");
        TableEnvironment env = tableEnvironment();
        String sqlDdl =
                ddl(
                                id,
                                "sink.write-mode",
                                "keep-latest",
                                "sink.insert-only-input-mode",
                                "insert-only")
                        .replace("  PRIMARY KEY", "  untouched ROW<flag STRING>,\n  PRIMARY KEY");
        if (explicitTimestamp) {
            sqlDdl =
                    sqlDdl.replace(
                            "  PRIMARY KEY",
                            "  ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp',\n  PRIMARY KEY");
        }
        env.executeSql(sqlDdl);
        String insert =
                explicitTimestamp
                        ? "INSERT INTO bt (rowkey, cf, ts) VALUES ('r1', ROW(CAST(NULL AS STRING), CAST(7 AS BIGINT)), TO_TIMESTAMP_LTZ(0, 3))"
                        : "INSERT INTO bt (rowkey, cf) VALUES ('r1', ROW(CAST(NULL AS STRING), CAST(7 AS BIGINT)))";
        long previousTimestamp = -1;
        for (int replay = 0; replay < 2; replay++) {
            // Each completed job serializes anew. The explicit zero also proves the delete
            // removes versions newer than the replacement; the clock must advance on replay.
            env.executeSql(insert).await();
            Row result = readRows(table).get(0);
            assertThat(result.getCells(FAMILY, "name")).hasSize(1);
            assertThat(result.getCells(FAMILY, "name").get(0).getValue().toStringUtf8())
                    .isEqualTo("null");
            assertThat(result.getCells(FAMILY, "amount")).hasSize(1);
            assertThat(result.getCells(FAMILY, "amount").get(0).getValue().toByteArray())
                    .isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 7});
            assertThat(result.getCells(FAMILY, "undeclared")).isEqualTo(undeclared);
            assertThat(result.getCells("untouched", "flag")).isEqualTo(omittedFamily);
            long actualTimestamp = result.getCells(FAMILY, "name").get(0).getTimestamp();
            if (explicitTimestamp) {
                assertThat(actualTimestamp).isZero();
            } else {
                assertThat(actualTimestamp).isGreaterThan(previousTimestamp);
            }
            previousTimestamp = actualTimestamp;
        }
    }

    @Test
    void keepLatestReplacementAlsoSurvivesServerTimestampReplayThroughDataStream()
            throws Exception {
        TableDestination table = createTable("table-keep-latest-server");
        mutateRow(
                table,
                ByteString.copyFromUtf8("r1"),
                seed ->
                        seed.setCell(FAMILY, "name", 1_000L, "old")
                                .setCell(FAMILY, "name", 2_000L, "older"));
        assertThat(readRows(table).get(0).getCells(FAMILY, "name")).hasSize(2);
        long previousTimestamp = -1;
        for (int replay = 0; replay < 2; replay++) {
            Configuration config = new Configuration();
            config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(config);
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(1);
            env.fromData("r1")
                    .sinkTo(
                            BigtableSink.<String>builder()
                                    .table(table)
                                    .serializer(
                                            (key, context) ->
                                                    RowMutationEntry.createUnsafe(
                                                                    ByteString.copyFromUtf8(key))
                                                            .deleteCells(FAMILY, "name")
                                                            .setCell(
                                                                    FAMILY,
                                                                    "name",
                                                                    -1L,
                                                                    "replacement"))
                                    .build());
            env.execute("keep-latest-server-timestamp-replay");
            List<RowCell> cells = readRows(table).get(0).getCells(FAMILY, "name");
            assertThat(cells).hasSize(1);
            assertThat(cells.get(0).getValue().toStringUtf8()).isEqualTo("replacement");
            assertThat(cells.get(0).getTimestamp()).isGreaterThan(previousTimestamp);
            previousTimestamp = cells.get(0).getTimestamp();
        }
    }

    @Test
    void writesThroughTheProductionClientPath() throws Exception {
        TableDestination table = createTable("table-sink");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(ddl("table-sink"));

        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('alice', CAST(7 AS BIGINT)))").await();

        assertThat(readRows(table))
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactly("r1");
        assertThat(readRows(table).get(0).getCells(FAMILY, "name").get(0).getValue().toStringUtf8())
                .isEqualTo("alice");
    }

    @Test
    void writesThroughTheConfiguredApplicationProfile() throws Exception {
        TableDestination table = createTable("table-sink-app-profile");
        createSingleClusterAppProfile(APP_PROFILE);
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(ddl("table-sink-app-profile", "sink.app-profile-id", APP_PROFILE));

        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('alice', CAST(7 AS BIGINT)))").await();

        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void failsWhenTheApplicationProfileDoesNotExist() {
        // The load-bearing half: a factory that dropped the option would pass the test above by
        // writing through the instance's default profile, and fail only here.
        createTable("table-sink-app-profile-missing");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl("table-sink-app-profile-missing", "sink.app-profile-id", "no-such-profile"));

        // On the profile's name, not merely on "something threw": assertThatThrownBy already
        // fails when nothing does, so isNotNull() would accept a DDL parse error or an auth
        // failure and read as proof of something it never checked.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO bt VALUES ('r1', ROW('alice',"
                                                        + " CAST(7 AS BIGINT)))")
                                        .await())
                .hasStackTraceContaining("no-such-profile");
    }
}
