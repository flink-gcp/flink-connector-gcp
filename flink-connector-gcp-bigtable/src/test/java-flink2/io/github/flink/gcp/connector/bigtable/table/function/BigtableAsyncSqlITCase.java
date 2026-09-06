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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static io.github.flink.gcp.connector.bigtable.table.function.BigtableAsyncSqlPlanTest.set;
import static org.assertj.core.api.Assertions.assertThat;

/** Executes SQL registration, specialization and both asynchronous write paths. */
class BigtableAsyncSqlITCase extends AbstractBigtableEmulatorITCase {
    private static TableEnvironment environment(TableDestination table) {
        TableEnvironment env = BigtableAsyncSqlPlanTest.environment();
        set(env, "project", PROJECT);
        set(env, "instance", INSTANCE);
        set(env, "table", table.getTable());
        set(env, "emulator-endpoint", emulatorEndpoint());
        env.getConfig().set("parallelism.default", "1");
        return env;
    }

    static List<Row> collect(TableEnvironment env, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> results = env.executeSql(sql).collect()) {
            results.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void sqlInternalConversionsRetainTheExistingScalarCellEncodings() throws Exception {
        TableDestination table = createTable("async-codecs");
        TableEnvironment env = environment(table);
        env.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        set(env, "predicate.type", "row-exists");
        String[] expressions = {
            "TRUE",
            "CAST(2 AS TINYINT)",
            "CAST(258 AS SMALLINT)",
            "16909060",
            "CAST(72623859790382856 AS BIGINT)",
            "CAST(1.25 AS DECIMAL(4, 2))",
            "CAST(1 AS FLOAT)",
            "CAST(1 AS DOUBLE)",
            "DATE '1970-01-02'",
            "CAST('00:00:01' AS TIME(3))",
            "TIMESTAMP '1970-01-01 00:00:01'",
            "CAST(TIMESTAMP '1970-01-01 00:00:01' AS TIMESTAMP_LTZ(3))",
            "INTERVAL '2' MONTH",
            "INTERVAL '1' SECOND"
        };
        byte[][] expected = {
            new byte[] {-1},
            new byte[] {2},
            new byte[] {1, 2},
            new byte[] {1, 2, 3, 4},
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
            new byte[] {0, 0, 0, 2, 125},
            ByteBuffer.allocate(4).putFloat(1).array(),
            ByteBuffer.allocate(8).putDouble(1).array(),
            ByteBuffer.allocate(4).putInt(1).array(),
            ByteBuffer.allocate(4).putInt(1000).array(),
            ByteBuffer.allocate(8).putLong(1000).array(),
            ByteBuffer.allocate(8).putLong(1000).array(),
            ByteBuffer.allocate(4).putInt(2).array(),
            ByteBuffer.allocate(8).putLong(1000).array()
        };
        for (int i = 0; i < expressions.length; i++) {
            String prefix = "otherwise." + i + ".";
            set(env, prefix + "operation", "set-cell");
            set(env, prefix + "family", "cf");
            set(env, prefix + "qualifier", "q" + i);
            set(env, prefix + "value-argument", Integer.toString(i));
        }
        assertThat(
                        collect(
                                env,
                                "SELECT BT_CHECK_AND_MUTATE('test', 16909060, "
                                        + String.join(", ", expressions)
                                        + ")"))
                .containsExactly(Row.of(false));
        var stored = readRows(table).get(0);
        assertThat(stored.getKey().toByteArray()).containsExactly(1, 2, 3, 4);
        for (int i = 0; i < expressions.length; i++) {
            assertThat(stored.getCells("cf", "q" + i).get(0).getValue().toByteArray())
                    .as(expressions[i])
                    .isEqualTo(expected[i]);
        }
    }

    @Test
    void declaredNotNullColumnsReachTheConditionalWrite() throws Exception {
        TableDestination table = createTable("async-not-null");
        writeCell(table, "typed", "cf", "status", "before");
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.conditional(env);
        env.createTemporaryView(
                "typed_input",
                env.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.STRING().notNull()),
                                DataTypes.FIELD("expected", DataTypes.STRING().notNull()),
                                DataTypes.FIELD("replacement", DataTypes.STRING().notNull())),
                        Row.of("typed", "before", "after")));
        assertThat(
                        collect(
                                env,
                                "SELECT BT_CHECK_AND_MUTATE('test', k, expected, replacement) FROM typed_input"))
                .containsExactly(Row.of(true));
        assertThat(readRows(table).get(0).getCells("cf", "status").get(0).getValue().toStringUtf8())
                .isEqualTo("after");
    }

    @Test
    void explicitCoalesceMakesNullableIncrementsExecutable() throws Exception {
        TableDestination table = createTable("async-coalesce");
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.increment(env);
        List<Row> output =
                collect(
                        env,
                        "SELECT k, BT_READ_MODIFY_WRITE('test', k, COALESCE(delta, CAST(0 AS BIGINT))) "
                                + "FROM (VALUES ('supplied', CAST(2 AS BIGINT)), ('defaulted', CAST(NULL AS BIGINT))) AS v(k, delta)");
        assertThat(output)
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder("supplied", "defaulted");
        for (Row result : output) {
            long expected = result.getField(0).equals("supplied") ? 2L : 0L;
            Row changed = (Row) result.getField(1);
            assertThat((Row[]) changed.getField(1))
                    .singleElement()
                    .satisfies(cell -> assertThat(cell.getField(4)).isEqualTo(expected));
        }
        var stored = readRows(table);
        assertThat(stored)
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactlyInAnyOrder("supplied", "defaulted");
        assertThat(stored)
                .allSatisfy(
                        row -> {
                            long expected =
                                    row.getKey().toStringUtf8().equals("supplied") ? 2L : 0L;
                            assertThat(
                                            ByteBuffer.wrap(
                                                            row.getCells("cf", "count")
                                                                    .get(0)
                                                                    .getValue()
                                                                    .toByteArray())
                                                    .getLong())
                                    .isEqualTo(expected);
                        });
    }

    @Test
    void cellExistsSelectsOnlyRowsContainingTheConfiguredCell() throws Exception {
        TableDestination table = createTable("async-cell-exists");
        writeCell(table, "target", "cf", "wanted", "value");
        writeCell(table, "other", "cf", "different", "value");
        TableEnvironment env = environment(table);
        set(env, "predicate.type", "cell-exists");
        set(env, "predicate.family", "cf");
        set(env, "predicate.qualifier", "wanted");
        set(env, "then.0.operation", "delete-row");
        assertThat(
                        collect(
                                env,
                                "SELECT k, BT_CHECK_AND_MUTATE('test', k) "
                                        + "FROM (VALUES ('target'), ('other'), ('missing')) AS v(k)"))
                .containsExactlyInAnyOrder(
                        Row.of("target", true), Row.of("other", false), Row.of("missing", false));
        assertThat(readRows(table))
                .singleElement()
                .satisfies(row -> assertThat(row.getKey().toStringUtf8()).isEqualTo("other"));
    }

    @Test
    void returnsBothConditionalOutcomesIncludingAnEmptySelectedBranch() throws Exception {
        TableDestination table = createTable("async-conditional");
        writeCell(table, "matched", "cf", "status", "before");
        writeCell(table, "unmatched", "cf", "status", "other");
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.conditional(env);
        assertThat(
                        collect(
                                env,
                                "SELECT k, BT_CHECK_AND_MUTATE('test', k, expected, replacement) "
                                        + "FROM (VALUES ('matched', 'before', 'after'), ('unmatched', 'before', 'after')) "
                                        + "AS v(k, expected, replacement)"))
                .containsExactlyInAnyOrder(Row.of("matched", true), Row.of("unmatched", false));
        assertThat(readRows(table).get(0).getCells("cf", "status").get(0).getValue())
                .isEqualTo(ByteString.copyFromUtf8("after"));
        assertThat(readRows(table).get(1).getCells("cf", "status").get(0).getValue())
                .isEqualTo(ByteString.copyFromUtf8("other"));
    }

    @Test
    void returnsMixedChangedCellsAndPreservesRepeatedRuleOrder() throws Exception {
        TableDestination table = createTable("async-mixed");
        writeCell(table, "row", "cf", "text", "before");
        writeCell(table, "row", "cf", "untouched", "keep");
        TableEnvironment env = environment(table);
        BigtableAsyncSqlPlanTest.increment(env);
        set(env, "rules.1.operation", "append");
        set(env, "rules.1.family", "cf");
        set(env, "rules.1.qualifier-base64", "AP8=");
        set(env, "rules.1.value-argument", "1");
        set(env, "rules.2.operation", "append");
        set(env, "rules.2.family", "cf");
        set(env, "rules.2.qualifier", "text");
        set(env, "rules.2.value-utf8", "-first");
        set(env, "rules.3.operation", "append");
        set(env, "rules.3.family", "cf");
        set(env, "rules.3.qualifier", "text");
        set(env, "rules.3.value-utf8", "-second");
        List<Row> output =
                collect(
                        env,
                        "SELECT BT_READ_MODIFY_WRITE('test', k, delta, suffix) "
                                + "FROM (VALUES ('row', CAST(-2 AS BIGINT), X'FF00')) AS v(k, delta, suffix)");
        assertThat(output).hasSize(1);
        Row changed = (Row) output.get(0).getField(0);
        assertThat((byte[]) changed.getField(0)).containsExactly('r', 'o', 'w');
        Row[] cells = (Row[]) changed.getField(1);
        assertThat(cells).hasSize(3);
        for (Row cell : cells) {
            assertThat(cell.getField(0)).isEqualTo("cf");
            assertThat((Long) cell.getField(3)).isPositive();
            byte[] qualifier = (byte[]) cell.getField(1);
            if (ByteString.copyFrom(qualifier).equals(ByteString.copyFromUtf8("count"))) {
                assertThat(cell.getField(4)).isEqualTo(-2L);
                assertThat(ByteBuffer.wrap((byte[]) cell.getField(2)).getLong()).isEqualTo(-2L);
            } else if (ByteString.copyFrom(qualifier).equals(ByteString.copyFromUtf8("text"))) {
                assertThat(ByteString.copyFrom((byte[]) cell.getField(2)).toStringUtf8())
                        .isEqualTo("before-first-second");
                assertThat(cell.getField(4)).isNull();
            } else {
                assertThat(qualifier).containsExactly(0, -1);
                assertThat((byte[]) cell.getField(2)).containsExactly(-1, 0);
                assertThat(cell.getField(4)).isNull();
            }
        }
        var stored = readRows(table).get(0);
        assertThat(stored.getCells("cf", "untouched").get(0).getValue().toStringUtf8())
                .isEqualTo("keep");
        assertThat(
                        ByteBuffer.wrap(
                                        stored.getCells("cf", "count")
                                                .get(0)
                                                .getValue()
                                                .toByteArray())
                                .getLong())
                .isEqualTo(-2L);
    }
}
