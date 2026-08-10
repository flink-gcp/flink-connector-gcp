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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When the connector's DDL-shape rejections actually reach a user, and what the planner puts
 * between a changelog and this sink.
 *
 * <p>Deliberately not an ITCase: every case here throws, or is explained, while the job graph is
 * being built, before a client is opened, so the emulator endpoint below is a string that only has
 * to parse.
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
    void theRowIsCompletedForADeleteAndForNothingElse() {
        // #470. Without a declared key the planner keys its upserts on whatever the query is
        // unique by — here 'id', a column the Bigtable table does not even have — so a key-only
        // delete would otherwise reach the sink with the row-key column null. Asserting the plan
        // rather than the changelog mode is what makes this portable: ChangelogMode.keyOnlyDeletes
        // does not exist on the 1.20 LTS build, and naming it here would break that build and not
        // this one.
        //
        // The two halves are one test on purpose. Alone, the second is an assertion that cannot
        // fail: an append-only VALUES query gets no ChangelogNormalize whatever the sink answers —
        // measured, it passes under insertOnly(), upsert(), upsert(false) and all(). Its meaning
        // comes entirely from being the *same* string, through the *same* environment, that the
        // first half has just found present. That also makes a rename of the operator loud in both
        // directions rather than silently vacuous in the negative one.
        //
        // The key is pinned as well as the node: a normalize appearing for some unrelated reason
        // is not the one this test is about.
        StreamTableEnvironment tEnv = streamTableEnvironmentWithAnUpsertSourceKeyedOnId();

        assertThat(
                        planOfInsertInto(
                                tEnv,
                                "no_pk",
                                "SELECT rowkey, CAST(ROW(v) AS ROW<v STRING>)" + " FROM src"))
                .contains("ChangelogNormalize(key=[id])");
        assertThat(
                        planOfInsertInto(
                                tEnv, "no_pk", "VALUES ('r1', CAST(ROW('x') AS ROW<v STRING>))"))
                .doesNotContain("ChangelogNormalize");
    }

    /**
     * A streaming environment holding a {@code no_pk} Bigtable table with no {@code PRIMARY KEY}
     * and an upsert view {@code src} keyed on {@code id}, which is not the row-key column.
     */
    private static StreamTableEnvironment streamTableEnvironmentWithAnUpsertSourceKeyedOnId() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE no_pk (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);

        DataStream<Row> changelog =
                env.fromData(
                        Collections.singletonList(
                                Row.ofKind(RowKind.DELETE, "id1", "rk1", "hello")),
                        Types.ROW_NAMED(
                                new String[] {"id", "rowkey", "v"},
                                Types.STRING,
                                Types.STRING,
                                Types.STRING));
        tEnv.createTemporaryView(
                "src",
                tEnv.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("rowkey", DataTypes.STRING())
                                .column("v", DataTypes.STRING())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return tEnv;
    }

    private static String planOfInsertInto(
            StreamTableEnvironment tEnv, String table, String query) {
        return tEnv.explainSql("INSERT INTO " + table + " " + query);
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
