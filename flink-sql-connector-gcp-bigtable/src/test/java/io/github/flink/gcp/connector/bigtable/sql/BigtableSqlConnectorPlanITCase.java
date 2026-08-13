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

package io.github.flink.gcp.connector.bigtable.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorSmokeITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Plans the Change Streams SQL surface using the shaded connector jar. */
class BigtableSqlConnectorPlanITCase extends AbstractSqlConnectorSmokeITCase {

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.FACTORY_CLASS;
    }

    @Test
    void packagedConnectorPlansMetadataAndOrderedEntryExpansion() {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE mutations (\n"
                        + "  row_key BYTES,\n"
                        + "  entries ARRAY<ROW<\n"
                        + "    entry_index INT,\n"
                        + "    kind STRING,\n"
                        + "    family STRING,\n"
                        + "    qualifier ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    `value` ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    delete_range ROW<start_bound STRING, start_micros BIGINT,"
                        + " end_bound STRING, end_micros BIGINT>\n"
                        + "  >>,\n"
                        + "  mutation_type STRING NOT NULL METADATA FROM 'mutation-type'"
                        + " VIRTUAL,\n"
                        + "  committed_at TIMESTAMP_LTZ(9) NOT NULL METADATA FROM"
                        + " 'commit-timestamp' VIRTUAL\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bigtable',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'instance' = 'my-instance',\n"
                        + "  'table' = 'my-table',\n"
                        + "  'scan.mode' = 'change-stream',\n"
                        + "  'scan.change-stream.changelog-mode' = 'envelope',\n"
                        + "  'scan.app-profile-id' = 'single-cluster-profile'\n"
                        + ")");

        String plan =
                tEnv.explainSql(
                        "SELECT row_key, entry_index, kind, mutation_type, committed_at "
                                + "FROM mutations CROSS JOIN UNNEST(entries) AS entry_table("
                                + "entry_index, kind, family, qualifier, entry_timestamp,"
                                + " entry_value, delete_range)");

        assertThat(plan)
                .contains("mutations", "entry_index", "kind", "mutation_type", "committed_at")
                .contains("Uncollect");
    }

    @Test
    void packagedConnectorPlansSelectedCellUpsertsWithASeparateFormatJar() {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TABLE profiles (\n"
                        + "  name STRING,\n"
                        + "  profile_id STRING NOT NULL,\n"
                        + "  score INT,\n"
                        + "  source_cluster STRING METADATA FROM 'source-cluster-id' VIRTUAL,\n"
                        + "  PRIMARY KEY (profile_id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'bigtable',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'instance' = 'my-instance',\n"
                        + "  'table' = 'my-table',\n"
                        + "  'scan.mode' = 'change-stream',\n"
                        + "  'scan.change-stream.changelog-mode' = 'selected-cell',\n"
                        + "  'scan.app-profile-id' = 'single-cluster-profile',\n"
                        + "  'scan.change-stream.selected-cell.family' = 'state',\n"
                        + "  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',\n"
                        + "  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-1',\n"
                        + "  'value.format' = 'json'\n"
                        + ")");

        String plan = tEnv.explainSql("SELECT profile_id, name, score FROM profiles");

        assertThat(plan).contains("profiles", "profile_id", "name", "score");
    }
}
