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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether the planner puts a {@code ChangelogNormalize} between an upsert change-stream source and
 * a sink that accepts key-only deletes, which decides whose coordinates a delete carries (#1960).
 *
 * <p>That operator completes a key-only delete from the last row it holds for the key, coordinate
 * columns included. Flink 2.x removes it when the query reads no metadata column, filters nothing
 * and needs no update-before rows; Flink 1.20 always keeps it after an upsert source. The
 * documentation's version split rests on these two plans, so a Flink upgrade that changes either
 * one fails here first.
 */
class SpannerChangeStreamCoordinatePlanTest {

    private static final String SOURCE =
            "CREATE TABLE order_changes (\n"
                    + "  OrderId BIGINT NOT NULL,\n"
                    + "  Status STRING,\n"
                    + "  commit_ts TIMESTAMP_LTZ(9) NOT NULL,\n"
                    + "  record_sequence STRING NOT NULL,\n"
                    + "  mod_number INT NOT NULL,\n"
                    + "  metadata_commit_ts TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp'"
                    + " VIRTUAL,\n"
                    + "  PRIMARY KEY (OrderId) NOT ENFORCED\n"
                    + ") WITH (\n"
                    + "  'connector' = 'spanner',\n"
                    + "  'project' = 'p',\n"
                    + "  'instance' = 'i',\n"
                    + "  'database' = 'd',\n"
                    + "  'table' = 'Orders',\n"
                    + "  'scan.mode' = 'change-stream',\n"
                    + "  'scan.change-stream.name' = 'order_changes',\n"
                    + "  'scan.change-stream.changelog-mode' = 'upsert',\n"
                    + "  'scan.change-stream.commit-timestamp-column' = 'commit_ts',\n"
                    + "  'scan.change-stream.sequence-column' = 'record_sequence',\n"
                    + "  'scan.change-stream.mod-number-column' = 'mod_number'\n"
                    + ")";

    /** A sink that declares its primary key, so it accepts a key-only delete. */
    private static final String SINK =
            "CREATE TABLE replica (\n"
                    + "  OrderId BIGINT NOT NULL,\n"
                    + "  Status STRING,\n"
                    + "  Committed STRING,\n"
                    + "  PRIMARY KEY (OrderId) NOT ENFORCED\n"
                    + ") WITH (\n"
                    + "  'connector' = 'spanner',\n"
                    + "  'project' = 'p',\n"
                    + "  'instance' = 'i',\n"
                    + "  'database' = 'd',\n"
                    + "  'table' = 'Replica',\n"
                    + "  'emulator-endpoint' = 'localhost:1'\n"
                    + ")";

    @Test
    void physicalCoordinateColumnsLetFlink2HandADeleteOnAsItIs() {
        String plan =
                plan(
                        "INSERT INTO replica SELECT OrderId, Status, CONCAT(CAST(commit_ts AS"
                                + " STRING), '/', record_sequence, '/', CAST(mod_number AS"
                                + " STRING)) FROM order_changes");

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
        String plan =
                plan(
                        "INSERT INTO replica SELECT OrderId, Status, CAST(metadata_commit_ts AS"
                                + " STRING) FROM order_changes");

        assertThat(plan).contains("ChangelogNormalize");
    }

    private static String plan(String insert) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(SOURCE);
        env.executeSql(SINK);
        return env.explainSql(insert);
    }
}
