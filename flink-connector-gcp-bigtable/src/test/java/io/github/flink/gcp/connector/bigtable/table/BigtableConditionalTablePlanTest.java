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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableConditionalTablePlanTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void plansPlainInsertValuesAndSelectWithOrWithoutAPrimaryKey(boolean primaryKey) {
        TableEnvironment env = table(primaryKey, "'sink.write-mode' = 'insert-if-absent'");
        assertThat(
                        env.explainSql(
                                "INSERT INTO bt VALUES ('r', ROW('first')), ('r', ROW('second'))"))
                .containsIgnoringCase("sink");
        String plan =
                env.explainSql(
                        "INSERT INTO bt SELECT k, ROW(v) FROM (VALUES ('r', 'first'), ('r', 'second')) AS input(k, v)");
        assertThat(plan).doesNotContain("UpsertMaterialize", "ChangelogNormalize");
        assertThat(env.explainSql("SELECT * FROM bt")).containsIgnoringCase("source");
    }

    @Test
    void updatingInputsAreRejectedAsConditionalCommands() {
        TableEnvironment env = table(false, "'sink.write-mode' = 'insert-if-absent'");
        env.executeSql(
                "CREATE TABLE src (k STRING, v STRING) WITH ('connector'='datagen', 'rows-per-second'='1')");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "INSERT INTO bt SELECT k, ROW(CAST(COUNT(*) AS STRING)) FROM src GROUP BY k"))
                .hasStackTraceContaining("INSERT-only");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "'sink.batching.element-count-threshold' = '1'",
                "'sink.in-flight.max-entries' = '1'",
                "'sink.in-flight.max-bytes' = '1mb'",
                "'sink.insert-only-input-mode' = 'upsert'",
                "'sink.create-disposition' = 'create-never'",
                "'sink.recovery.max-attempts' = '1'"
            })
    void refusesExplicitInertOptionsEvenWhenTheySelectADefault(String extra) {
        TableEnvironment env = table(false, "'sink.write-mode'='insert-if-absent', " + extra);
        assertThatThrownBy(() -> env.explainSql("INSERT INTO bt VALUES ('r', ROW('v'))"))
                .hasStackTraceContaining(
                        "cannot be used with 'sink.write-mode' = 'insert-if-absent'");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "'sink.conditional.empty-branch-policy' = 'ignore'",
                "'sink.request-timeout' = '3s'",
                "'sink.in-flight.max-requests' = '4'"
            })
    void refusesConditionalOptionsInOrdinaryUpsert(String extra) {
        TableEnvironment env = table(false, extra);
        assertThatThrownBy(() -> env.explainSql("INSERT INTO bt VALUES ('r', ROW('v'))"))
                .hasStackTraceContaining("cannot be used with 'sink.write-mode' = 'upsert'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepLatestPlansPortableInsertsAndUpdatingQueries(boolean primaryKey) {
        TableEnvironment env =
                table(
                        primaryKey,
                        "'sink.write-mode'='keep-latest', 'sink.insert-only-input-mode'='insert-only'");
        assertThat(
                        env.explainSql(
                                "INSERT INTO bt VALUES ('r', ROW('first')), ('r', ROW('second'))"))
                .containsIgnoringCase("sink");
        assertThat(
                        env.explainSql(
                                "INSERT INTO bt SELECT k, ROW(v) FROM (VALUES ('r', 'first'), ('r', 'second')) AS input(k, v)"))
                .containsIgnoringCase("sink");
        env.executeSql(
                "CREATE TABLE src (k STRING, v STRING) WITH ('connector'='datagen', 'rows-per-second'='1')");
        assertThat(
                        env.explainSql(
                                "INSERT INTO bt SELECT k, ROW(CAST(COUNT(*) AS STRING)) FROM src GROUP BY k"))
                .containsIgnoringCase("sink");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "'sink.conditional.empty-branch-policy'='ignore'",
                "'sink.request-timeout'='3s'",
                "'sink.in-flight.max-requests'='4'"
            })
    void keepLatestRejectsConditionalOptions(String extra) {
        TableEnvironment env = table(false, "'sink.write-mode'='keep-latest', " + extra);
        assertThatThrownBy(() -> env.explainSql("INSERT INTO bt VALUES ('r', ROW('v'))"))
                .hasStackTraceContaining("cannot be used with 'sink.write-mode' = 'keep-latest'");
    }

    @Test
    void keepLatestValidatesWriterOptionsUsingTheSqlKey() {
        TableEnvironment env =
                table(
                        false,
                        "'sink.write-mode'='keep-latest', 'sink.batching.element-count-threshold'='0'");
        assertThatThrownBy(() -> env.explainSql("INSERT INTO bt VALUES ('r', ROW('v'))"))
                .hasStackTraceContaining(
                        "Option 'sink.batching.element-count-threshold' is invalid:");
    }

    private static TableEnvironment table(boolean primaryKey, String extra) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<v STRING>"
                        + (primaryKey ? ", PRIMARY KEY(k) NOT ENFORCED" : "")
                        + ") WITH ('connector'='bigtable', 'project'='p', 'instance'='i', 'table'='t', 'emulator-endpoint'='localhost:1', "
                        + extra
                        + ")");
        return env;
    }
}
