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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When the connector's DDL-shape rejections actually reach a user.
 *
 * <p>Deliberately not an ITCase: every case here throws while the job graph is being built, before
 * a client is opened, so the emulator endpoint below is a string that only has to parse.
 *
 * <p>The question it answers is one {@code FactoryMocks} cannot, because that harness calls the
 * factory directly: a {@code CREATE TABLE} does not call the factory at all — it registers a
 * catalog entry — so a table this connector could never write is <em>accepted</em> by the DDL and
 * refused when an {@code INSERT} over it is planned. Any documentation saying such a table is
 * "rejected when the table is created" is wrong, and this is what keeps that from being written
 * again.
 */
class BigtableTablePlanTest {

    private static final String WITH_CLAUSE =
            "WITH (\n"
                    + "  'connector' = 'bigtable',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'instance' = 'my-instance',\n"
                    + "  'table' = 'my-table',\n"
                    + "  'emulator-endpoint' = 'localhost:1'\n"
                    + ")";

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void aColumnWithNoCellEncodingIsAcceptedByCreateTableAndRefusedWhenAnInsertIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE bt (\n"
                                                + "  rowkey STRING,\n"
                                                + "  cf1 ROW<tags ARRAY<STRING>>,\n"
                                                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                                                + ") "
                                                + WITH_CLAUSE))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW(ARRAY['a']))"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no Bigtable cell encoding");
    }

    @Test
    void theDocumentationsOwnExampleParses() {
        // Copied from docs/content/docs/connectors/table/bigtable.md, which is the first code
        // block a reader meets. Pinned because a column family is a *column name* here, so the
        // example is one reserved word away from not parsing — 'identity' was, and the page shipped
        // it until this test was written.
        TableEnvironment tEnv = tableEnvironment();

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE profiles (\n"
                                                + "  rowkey STRING,\n"
                                                + "  profile ROW<name STRING, email STRING>,\n"
                                                + "  usage ROW<requests BIGINT, last_seen"
                                                + " TIMESTAMP_LTZ(3)>,\n"
                                                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                                                + ") "
                                                + WITH_CLAUSE))
                .doesNotThrowAnyException();
    }

    @Test
    void aTableWithNoColumnFamilyIsRefusedWhenAnInsertIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE keys_only (\n"
                        + "  rowkey STRING,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO keys_only VALUES ('r1')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs at least one column family");
    }
}
