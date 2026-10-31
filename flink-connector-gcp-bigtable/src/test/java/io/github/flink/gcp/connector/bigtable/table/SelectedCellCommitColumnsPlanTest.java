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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether the planner puts a {@code ChangelogNormalize} between a selected-cell source and a sink
 * that accepts key-only deletes, which decides whose commit timestamp a delete carries (#1960).
 *
 * <p>That operator completes a key-only delete from the last row it holds for the key, commit
 * columns included, and drops the delete of a key it holds no row for. Flink 2.x removes it when
 * the query reads no metadata column, filters nothing and needs no update-before rows; Flink 1.20
 * always keeps it after an upsert source. The commit-timestamp and tie-breaker columns are physical
 * for exactly this reason, and the documentation's version split rests on these two plans, so a
 * Flink upgrade that changes either one fails here first.
 *
 * <p>Not an ITCase: the plan is built before any client is opened.
 */
class SelectedCellCommitColumnsPlanTest {

    private static final String SOURCE =
            "CREATE TABLE profiles (\n"
                    + "  profile_id STRING NOT NULL,\n"
                    + "  name STRING,\n"
                    + "  commit_ts TIMESTAMP_LTZ(9) NOT NULL,\n"
                    + "  tie_breaker INT NOT NULL,\n"
                    + "  metadata_commit_ts TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp'"
                    + " VIRTUAL,\n"
                    + "  PRIMARY KEY (profile_id) NOT ENFORCED\n"
                    + ") WITH (\n"
                    + "  'connector' = 'bigtable',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'instance' = 'my-instance',\n"
                    + "  'table' = 'profiles',\n"
                    + "  'scan.mode' = 'change-stream',\n"
                    + "  'scan.change-stream.changelog-mode' = 'selected-cell',\n"
                    + "  'scan.app-profile-id' = 'single-cluster-profile',\n"
                    + "  'scan.change-stream.selected-cell.family' = 'state',\n"
                    + "  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',\n"
                    + "  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',\n"
                    + "  'scan.change-stream.commit-timestamp-column' = 'commit_ts',\n"
                    + "  'scan.change-stream.tie-breaker-column' = 'tie_breaker',\n"
                    + "  'value.format' = 'json'\n"
                    + ")";

    /** A sink that declares its primary key, so it accepts a key-only delete. */
    private static final String SINK =
            "CREATE TABLE replica (\n"
                    + "  rowkey STRING,\n"
                    + "  cf ROW<name STRING, committed STRING>,\n"
                    + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                    + ") WITH (\n"
                    + "  'connector' = 'bigtable',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'instance' = 'my-instance',\n"
                    + "  'table' = 'replica',\n"
                    + "  'emulator-endpoint' = 'localhost:1'\n"
                    + ")";

    @Test
    void physicalCommitColumnsLetFlink2HandADeleteOnAsItIs() {
        String plan =
                plan(
                        "INSERT INTO replica SELECT profile_id, ROW(name, CONCAT(CAST(commit_ts"
                                + " AS STRING), '/', CAST(tie_breaker AS STRING))) FROM profiles");

        String version = DataTypes.class.getPackage().getImplementationVersion();
        assertThat(version).as("the Flink version on the classpath").isNotNull();
        if (version.startsWith("1.")) {
            assertThat(plan).contains("ChangelogNormalize");
        } else {
            assertThat(plan).doesNotContain("ChangelogNormalize");
        }
    }

    @Test
    void theCommitTimestampMetadataColumnKeepsTheNormalizeOnEveryVersion() {
        // The control: the same query over the metadata column is what the physical columns
        // replace, and it is normalized whatever the version.
        String plan =
                plan(
                        "INSERT INTO replica SELECT profile_id, ROW(name,"
                                + " CAST(metadata_commit_ts AS STRING)) FROM profiles");

        assertThat(plan).contains("ChangelogNormalize");
    }

    private static String plan(String insert) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(SOURCE);
        env.executeSql(SINK);
        return env.explainSql(insert);
    }
}
