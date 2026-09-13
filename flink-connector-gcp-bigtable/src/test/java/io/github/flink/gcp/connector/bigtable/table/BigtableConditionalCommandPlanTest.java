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

class BigtableConditionalCommandPlanTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void plainValuesAndSelectNeedNoPlannerConflictHandling(boolean batch) {
        TableEnvironment env = table(batch);
        String values = env.explainSql("INSERT INTO commands VALUES ('r', 1), ('r', 1)");
        String select =
                env.explainSql(
                        "INSERT INTO commands SELECT k, v FROM (VALUES ('r', 1), ('r', 1)) AS src(k, v)");
        assertThat(values)
                .containsIgnoringCase("sink")
                .doesNotContain("UpsertMaterialize", "ChangelogNormalize");
        assertThat(select)
                .containsIgnoringCase("sink")
                .doesNotContain("UpsertMaterialize", "ChangelogNormalize");
    }

    @Test
    void updatingInputAndLookupUseAreRejectedThroughThePlanner() {
        TableEnvironment env = table(false);
        env.executeSql(
                "CREATE TABLE source_table (k STRING, v BIGINT, pt AS PROCTIME()) WITH ('connector'='datagen', 'rows-per-second'='1')");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "INSERT INTO commands SELECT k, COUNT(*) FROM source_table GROUP BY k"))
                .hasStackTraceContaining("requires INSERT-only input");
        assertThatThrownBy(() -> env.explainSql("SELECT * FROM commands"))
                .hasStackTraceContaining("command tables are write-only");
        assertThatThrownBy(
                        () ->
                                env.explainSql(
                                        "SELECT s.k, c.cell_value FROM source_table s LEFT JOIN commands FOR SYSTEM_TIME AS OF s.pt c ON s.k=c.k"))
                .hasStackTraceContaining("command tables are write-only");
    }

    private static TableEnvironment table(boolean batch) {
        TableEnvironment env =
                TableEnvironment.create(
                        batch
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TABLE commands (k STRING, cell_value BIGINT) WITH ("
                        + "'connector'='bigtable', 'project'='p', 'instance'='i', 'table'='t',"
                        + "'emulator-endpoint'='localhost:1', 'sink.write-mode'='conditional',"
                        + "'sink.conditional.row-key-column'='k', 'sink.conditional.predicate'='row-exists',"
                        + "'sink.conditional.then.0.operation'='set-cell', 'sink.conditional.then.0.family'='cf',"
                        + "'sink.conditional.then.0.qualifier'='q', 'sink.conditional.then.0.value-column'='cell_value')");
        return env;
    }
}
