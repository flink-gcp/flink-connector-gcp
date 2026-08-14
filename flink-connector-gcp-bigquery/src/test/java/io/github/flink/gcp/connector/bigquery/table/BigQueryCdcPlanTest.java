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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Planner-level coverage for the BigQuery CDC sink changelog and writable metadata. */
class BigQueryCdcPlanTest {

    @Test
    void acceptsAnUpsertChangelogWithoutDroppingDeletesAndProjectsWritableMetadata() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        DataStream<Row> changelog =
                env.fromData(
                        Arrays.asList(
                                Row.ofKind(RowKind.DELETE, "old-id", null, "0001/0000000000000001"),
                                Row.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        "new-id",
                                        1L,
                                        "0001/0000000000000002")),
                        Types.ROW_NAMED(
                                new String[] {"id", "amount", "sequence"},
                                Types.STRING,
                                Types.LONG,
                                Types.STRING));
        table.createTemporaryView(
                "changes",
                table.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .column("sequence", DataTypes.STRING())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        table.executeSql(
                "CREATE TABLE current_rows ("
                        + "id STRING NOT NULL, amount BIGINT, "
                        + "sequence STRING METADATA FROM 'change-sequence-number', "
                        + "PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector'='bigquery', 'project'='p', 'dataset'='d', "
                        + "'table'='current_rows', 'sink.cdc.enabled'='true', "
                        + "'sink.cdc.max-staleness'='10 min', "
                        + "'emulator-endpoint'='localhost:1')");

        String plan =
                table.explainSql(
                        "INSERT INTO current_rows SELECT id, amount, sequence FROM changes");

        assertThat(plan)
                .contains("change-sequence-number")
                .contains("current_rows")
                .contains("sequence")
                .doesNotContain("DropUpdateBefore");
    }
}
