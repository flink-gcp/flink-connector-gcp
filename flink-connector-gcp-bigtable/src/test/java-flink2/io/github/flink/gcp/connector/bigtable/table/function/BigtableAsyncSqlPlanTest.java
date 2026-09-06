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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Registration and executable translation require neither credentials nor a service. */
class BigtableAsyncSqlPlanTest {
    static TableEnvironment environment() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TEMPORARY SYSTEM FUNCTION BT_CHECK_AND_MUTATE AS '"
                        + BigtableCheckAndMutateFunction.class.getName()
                        + "'");
        env.executeSql(
                "CREATE TEMPORARY SYSTEM FUNCTION BT_READ_MODIFY_WRITE AS '"
                        + BigtableReadModifyWriteFunction.class.getName()
                        + "'");
        env.getConfig().set("table.exec.async-scalar.max-attempts", "1");
        set(env, "project", "project");
        set(env, "instance", "instance");
        set(env, "table", "table");
        return env;
    }

    static void set(TableEnvironment env, String property, String value) {
        env.getConfig().set("bigtable.functions.test." + property, value);
    }

    static void conditional(TableEnvironment env) {
        set(env, "predicate.type", "latest-cell-value-equals");
        set(env, "predicate.family", "cf");
        set(env, "predicate.qualifier", "status");
        set(env, "predicate.value-argument", "0");
        set(env, "then.0.operation", "set-cell");
        set(env, "then.0.family", "cf");
        set(env, "then.0.qualifier", "status");
        set(env, "then.0.value-argument", "1");
    }

    static void increment(TableEnvironment env) {
        set(env, "rules.0.operation", "increment");
        set(env, "rules.0.family", "cf");
        set(env, "rules.0.qualifier", "count");
        set(env, "rules.0.value-argument", "0");
    }

    @Test
    void conditionalRegistrationReachesExecutableTranslation() {
        TableEnvironment env = environment();
        conditional(env);
        assertThat(
                        env.explainSql(
                                "SELECT BT_CHECK_AND_MUTATE('test', k, expected, replacement) "
                                        + "FROM (VALUES ('row', 'before', 'after')) AS v(k, expected, replacement)"))
                .containsIgnoringCase("AsyncCalc");
    }

    @Test
    void readModifyWritePublishesSqlNativeResultFields() {
        TableEnvironment env = environment();
        increment(env);
        var table =
                env.sqlQuery(
                        "SELECT BT_READ_MODIFY_WRITE('test', k, delta) AS changed "
                                + "FROM (VALUES ('row', CAST(1 AS BIGINT))) AS v(k, delta)");
        assertThat(table.getResolvedSchema().getColumnDataTypes().get(0).toString())
                .contains("row_key", "cells", "value_int64", "timestamp_micros");
        assertThat(table.explain()).containsIgnoringCase("AsyncCalc");
    }

    @Test
    void implicitRetriesAreRejectedBeforeOpeningTheMissingCredentialFile() {
        TableEnvironment env = environment();
        increment(env);
        env.getConfig().set("table.exec.async-scalar.max-attempts", "3");
        set(env, "service-account-key-file", "/path-that-must-not-be-opened");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "SELECT BT_READ_MODIFY_WRITE('test', 'row', CAST(1 AS BIGINT))"))
                .hasStackTraceContaining("SET 'table.exec.async-scalar.max-attempts' = '1'");
    }

    @Test
    void wrongIncrementTypeAndUnknownSettingsFailAtPlanning() {
        TableEnvironment env = environment();
        increment(env);
        assertThatThrownBy(() -> env.explainSql("SELECT BT_READ_MODIFY_WRITE('test', 'row', 1)"))
                .hasStackTraceContaining("bigtable.functions.test.rules.0.value-argument")
                .hasStackTraceContaining("requires BIGINT");
        set(env, "rules.0.unknown", "x");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "SELECT BT_READ_MODIFY_WRITE('test', 'row', CAST(1 AS BIGINT))"))
                .hasStackTraceContaining("bigtable.functions.test.rules.0.unknown");
    }

    @Test
    void nullAndNullableArgumentsFailAtPlanningForBothFunctions() {
        TableEnvironment conditionalEnv = environment();
        conditional(conditionalEnv);
        TableEnvironment rmwEnv = environment();
        increment(rmwEnv);
        for (TableEnvironment env : new TableEnvironment[] {conditionalEnv, rmwEnv}) {
            env.executeSql(
                    "CREATE TEMPORARY TABLE nullable_input "
                            + "(k STRING, payload STRING, delta BIGINT) WITH ('connector' = 'datagen')");
        }
        String[][] rejected = {
            {"BT_CHECK_AND_MUTATE('test', CAST(NULL AS STRING), 'before', 'after')", "row key"},
            {
                "BT_CHECK_AND_MUTATE('test', 'row', CAST(NULL AS STRING), 'after')",
                "value argument 0"
            },
            {
                "BT_CHECK_AND_MUTATE('test', 'row', 'before', CAST(NULL AS STRING))",
                "value argument 1"
            },
            {"BT_CHECK_AND_MUTATE('test', k, 'before', 'after') FROM nullable_input", "row key"},
            {
                "BT_CHECK_AND_MUTATE('test', 'row', payload, 'after') FROM nullable_input",
                "value argument 0"
            },
            {"BT_READ_MODIFY_WRITE('test', CAST(NULL AS STRING), CAST(1 AS BIGINT))", "row key"},
            {"BT_READ_MODIFY_WRITE('test', 'row', CAST(NULL AS BIGINT))", "value argument 0"},
            {"BT_READ_MODIFY_WRITE('test', k, CAST(1 AS BIGINT)) FROM nullable_input", "row key"},
            {"BT_READ_MODIFY_WRITE('test', 'row', delta) FROM nullable_input", "value argument 0"},
            {
                "BT_READ_MODIFY_WRITE('test', CAST(k AS STRING), CAST(1 AS BIGINT)) FROM nullable_input",
                "row key"
            },
            {
                "BT_READ_MODIFY_WRITE('test', k, CAST(1 AS BIGINT)) FROM nullable_input WHERE k IS NOT NULL",
                "row key"
            }
        };
        for (String[] rejectedCall : rejected) {
            TableEnvironment env =
                    rejectedCall[0].startsWith("BT_CHECK_AND_MUTATE") ? conditionalEnv : rmwEnv;
            assertThatThrownBy(() -> env.explainSql("SELECT " + rejectedCall[0]), rejectedCall[0])
                    .hasStackTraceContaining(
                            "Bigtable SQL " + rejectedCall[1] + " must have a NOT NULL type");
        }
    }

    @Test
    void unusedNullableOperandsAreRejectedBecauseFlinkGuardsEveryArgument() {
        for (String function : new String[] {"BT_CHECK_AND_MUTATE", "BT_READ_MODIFY_WRITE"}) {
            TableEnvironment env = environment();
            if (function.equals("BT_CHECK_AND_MUTATE")) {
                set(env, "predicate.type", "row-exists");
                set(env, "then.0.operation", "delete-row");
            } else {
                set(env, "rules.0.operation", "increment");
                set(env, "rules.0.family", "cf");
                set(env, "rules.0.qualifier", "count");
                set(env, "rules.0.value-int64", "1");
            }
            assertThat(env.explainSql("SELECT " + function + "('test', 'row', 'ignored')"))
                    .containsIgnoringCase("AsyncCalc");
            assertThatThrownBy(
                            () ->
                                    env.explainSql(
                                            "SELECT "
                                                    + function
                                                    + "('test', 'row', CAST(NULL AS STRING))"))
                    .hasStackTraceContaining("value argument 0 must have a NOT NULL type");
        }
    }

    @Test
    void functionDefinitionsCannotFoldConstantWrites() {
        var conditional = new BigtableCheckAndMutateFunction();
        var rmw = new BigtableReadModifyWriteFunction();
        assertThat(conditional.isDeterministic()).isFalse();
        assertThat(conditional.supportsConstantFolding()).isFalse();
        assertThat(rmw.isDeterministic()).isFalse();
        assertThat(rmw.supportsConstantFolding()).isFalse();
    }
}
