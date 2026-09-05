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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableReadModifyWriteTablePlanTest {
    @ParameterizedTest
    @CsvSource(
            value = {
                "append, STRING, 'v', false",
                "append, STRING, 'v', true",
                "append, BYTES, X'01', false",
                "increment, BIGINT, 1, false",
                "increment, BIGINT, 1, true"
            },
            quoteCharacter = '"')
    void plansInsertCommandsAndPreservesRepeatedInputs(
            String mode, String type, String value, boolean primaryKey) {
        TableEnvironment env = table(mode, "cf ROW<v " + type + ">", primaryKey, "");
        String plan =
                env.explainSql(
                        "INSERT INTO bt VALUES ('r', ROW("
                                + value
                                + ")), ('r', ROW("
                                + value
                                + "))");
        assertThat(plan)
                .containsIgnoringCase("sink")
                .doesNotContain("UpsertMaterialize", "ChangelogNormalize");
        assertThat(env.explainSql("SELECT * FROM bt")).containsIgnoringCase("source");
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "append, BIGINT, 1",
                "append, BOOLEAN, TRUE",
                "increment, INT, 1",
                "increment, DOUBLE, 1.0",
                "increment, STRING, 'v'",
                "increment, BYTES, X'01'"
            },
            quoteCharacter = '"')
    void rejectsIncompatibleOrMixedCellsAtPlanning(String mode, String type, String value) {
        String validType = mode.equals("append") ? "STRING" : "BIGINT";
        String validValue = mode.equals("append") ? "'ok'" : "1";
        TableEnvironment env =
                table(mode, "cf ROW<v " + validType + ", bad " + type + ">", false, "");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "INSERT INTO bt VALUES ('r', ROW("
                                                + validValue
                                                + ", "
                                                + value
                                                + "))"))
                .hasStackTraceContaining("sink.write-mode")
                .hasStackTraceContaining("cf.bad")
                .hasStackTraceContaining("requires");
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "increment"})
    void updatingQueriesCannotBecomeReadModifyWriteCommands(String mode) {
        String type = mode.equals("append") ? "STRING" : "BIGINT";
        TableEnvironment env = table(mode, "cf ROW<v " + type + ">", false, "");
        env.executeSql("CREATE TABLE src (k STRING) WITH ('connector'='datagen')");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "INSERT INTO bt SELECT k, ROW(CAST(COUNT(*) AS "
                                                + type
                                                + ")) FROM src GROUP BY k"))
                .hasStackTraceContaining("INSERT-only");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "'sink.batching.element-count-threshold'='1'",
                "'sink.in-flight.max-entries'='1'",
                "'sink.in-flight.max-bytes'='1mb'",
                "'sink.insert-only-input-mode'='upsert'",
                "'sink.create-disposition'='create-never'",
                "'sink.recovery.max-attempts'='1'",
                "'sink.conditional.empty-branch-policy'='ignore'",
                "'sink.cell-timestamp.truncate-to-millis'='false'"
            })
    void rejectsExplicitOptionsTheSelectedModeCannotApply(String option) {
        for (String mode : new String[] {"append", "increment"}) {
            String type = mode.equals("append") ? "STRING" : "BIGINT";
            String value = mode.equals("append") ? "'v'" : "1";
            TableEnvironment env = table(mode, "cf ROW<v " + type + ">", false, ", " + option);
            assertThatThrownBy(
                            () -> env.explainSql("INSERT INTO bt VALUES ('r', ROW(" + value + "))"))
                    .hasStackTraceContaining(option.substring(0, option.indexOf('=')))
                    .hasStackTraceContaining(
                            "cannot be used with 'sink.write-mode' = '" + mode + "'");
        }
    }

    @Test
    void invalidRequestLimitsNameTheSqlOptionAndTheBuilderConstraint() {
        TableEnvironment env =
                table(
                        "increment",
                        "cf ROW<v BIGINT>",
                        false,
                        ", 'sink.in-flight.max-requests'='0'");
        assertThatThrownBy(() -> env.explainSql("INSERT INTO bt VALUES ('r', ROW(1))"))
                .hasStackTraceContaining("sink.in-flight.max-requests")
                .hasStackTraceContaining("maxInFlightRequests must be positive");
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "increment"})
    void writableTimestampMetadataIsRejected(String mode) {
        String type = mode.equals("append") ? "STRING" : "BIGINT";
        String value = mode.equals("append") ? "'v'" : "1";
        TableEnvironment env =
                table(
                        mode,
                        "cf ROW<v " + type + ">, ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp'",
                        false,
                        "");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "INSERT INTO bt VALUES ('r', ROW("
                                                + value
                                                + "), TO_TIMESTAMP_LTZ(1000, 3))"))
                .hasStackTraceContaining("timestamp");
    }

    private static TableEnvironment table(
            String mode, String cells, boolean primaryKey, String extra) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TABLE bt (k STRING, "
                        + cells
                        + (primaryKey ? ", PRIMARY KEY(k) NOT ENFORCED" : "")
                        + ") WITH ('connector'='bigtable', 'project'='p', 'instance'='i', 'table'='t', 'emulator-endpoint'='localhost:1', 'sink.write-mode'='"
                        + mode
                        + "'"
                        + extra
                        + ")");
        return env;
    }
}
