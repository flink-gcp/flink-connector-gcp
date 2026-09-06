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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.ByteBuffer;
import java.util.List;

import static io.github.flink.gcp.connector.bigtable.table.function.BigtableAsyncSqlITCase.collect;
import static io.github.flink.gcp.connector.bigtable.table.function.BigtableAsyncSqlPlanTest.set;
import static org.assertj.core.api.Assertions.assertThat;

/** SQL-only service acceptance on one ephemeral instance removed by the inherited teardown. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableAsyncSqlRealGcpITCase extends AbstractBigtableRealGcpITCase {
    private static final String PROFILE = "async-sql";

    @BeforeAll
    static void enableSingleRowTransactions() throws Exception {
        String instance = tableDestination("unused").getInstance();
        try (BigtableInstanceAdminClient admin = BigtableInstanceAdminClient.create(PROJECT)) {
            String cluster = admin.listClusters(instance).get(0).getId();
            admin.createAppProfile(
                    CreateAppProfileRequest.of(instance, PROFILE)
                            .setRoutingPolicy(
                                    AppProfile.SingleClusterRoutingPolicy.of(cluster, true)));
        }
    }

    private static TableEnvironment environment(TableDestination table) {
        TableEnvironment env = BigtableAsyncSqlPlanTest.environment();
        set(env, "project", PROJECT);
        set(env, "instance", table.getInstance());
        set(env, "table", table.getTable());
        set(env, "app-profile-id", PROFILE);
        env.getConfig().set("parallelism.default", "1");
        return env;
    }

    @Test
    void conditionalBooleanMatchesTheSelectedBranchAndDirectRead() throws Exception {
        TableDestination table = createTable("sql-conditional");
        mutateRow(
                table,
                ByteString.copyFromUtf8("matched"),
                mutation -> mutation.setCell("cf", "status", 1000, "before"));
        mutateRow(
                table,
                ByteString.copyFromUtf8("unmatched"),
                mutation -> mutation.setCell("cf", "status", 1000, "other"));
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.conditional(env);
        assertThat(
                        collect(
                                env,
                                "SELECT k, BT_CHECK_AND_MUTATE('test', k, expected, replacement) "
                                        + "FROM (VALUES ('matched', 'before', 'after'), ('unmatched', 'before', 'after')) "
                                        + "AS v(k, expected, replacement)"))
                .containsExactlyInAnyOrder(Row.of("matched", true), Row.of("unmatched", false));
        var stored = readRows(table);
        assertThat(stored.get(0).getCells("cf", "status").get(0).getValue().toStringUtf8())
                .isEqualTo("after");
        assertThat(stored.get(1).getCells("cf", "status").get(0).getValue().toStringUtf8())
                .isEqualTo("other");
    }

    @Test
    void mixedRulesReturnRawAndNumericChangedCellsMatchingTheService() throws Exception {
        TableDestination table = createTable("sql-rmw");
        mutateRow(
                table,
                ByteString.copyFromUtf8("row"),
                mutation ->
                        mutation.setCell("cf", "text", 1000, "before")
                                .setCell("cf", "untouched", 1000, "keep"));
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.increment(env);
        set(env, "rules.1.operation", "append");
        set(env, "rules.1.family", "cf");
        set(env, "rules.1.qualifier", "text");
        set(env, "rules.1.value-argument", "1");
        set(env, "rules.2.operation", "increment");
        set(env, "rules.2.family", "cf");
        set(env, "rules.2.qualifier", "count");
        set(env, "rules.2.value-int64", "-2");
        List<Row> output =
                collect(
                        env,
                        "SELECT BT_READ_MODIFY_WRITE('test', k, delta, suffix) "
                                + "FROM (VALUES ('row', CAST(7 AS BIGINT), '-after')) AS v(k, delta, suffix)");
        assertThat(output).hasSize(1);
        Row changed = (Row) output.get(0).getField(0);
        assertThat(ByteString.copyFrom((byte[]) changed.getField(0)).toStringUtf8())
                .isEqualTo("row");
        Row[] cells = (Row[]) changed.getField(1);
        assertThat(cells).hasSize(2);
        var stored = readRows(table).get(0);
        for (Row cell : cells) {
            var direct =
                    stored.getCells().stream()
                            .filter(
                                    c ->
                                            c.getFamily().equals(cell.getField(0))
                                                    && c.getQualifier()
                                                            .equals(
                                                                    ByteString.copyFrom(
                                                                            (byte[])
                                                                                    cell.getField(
                                                                                            1))))
                            .findFirst()
                            .orElseThrow();
            assertThat((byte[]) cell.getField(2)).isEqualTo(direct.getValue().toByteArray());
            assertThat(cell.getField(3)).isEqualTo(direct.getTimestamp());
            if (direct.getQualifier().equals(ByteString.copyFromUtf8("count"))) {
                assertThat(cell.getField(4)).isEqualTo(5L);
                assertThat(ByteBuffer.wrap(direct.getValue().toByteArray()).getLong())
                        .isEqualTo(5L);
            } else {
                assertThat(cell.getField(4)).isNull();
                assertThat(direct.getValue().toStringUtf8()).isEqualTo("before-after");
            }
        }
        assertThat(stored.getCells("cf", "untouched").get(0).getValue().toStringUtf8())
                .isEqualTo("keep");
    }
}
