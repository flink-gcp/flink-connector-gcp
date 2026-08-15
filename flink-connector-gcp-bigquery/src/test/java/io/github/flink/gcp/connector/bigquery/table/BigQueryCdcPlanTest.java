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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /**
     * The SQL route documented for TiCDC reads a {@code debezium-json} source, whose changelog
     * carries UPDATE_BEFORE. The sink rejects that row kind at runtime, so the plan has to drop it
     * before the sink rather than leaving the job to fail on its first update.
     */
    @Test
    void dropsUpdateBeforeFromAnAllChangesSourceCarryingDebeziumSourceProperties() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Map<String, String> sourceProperties = new LinkedHashMap<>();
        sourceProperties.put("connector", "TiCDC");
        sourceProperties.put("commit_ts", "449574614268182531");
        sourceProperties.put("cluster_id", "test_cluster");
        DataStream<Row> changelog =
                env.fromData(
                        Arrays.asList(
                                Row.ofKind(RowKind.UPDATE_BEFORE, "id", 1L, sourceProperties),
                                Row.ofKind(RowKind.UPDATE_AFTER, "id", 2L, sourceProperties)),
                        Types.ROW_NAMED(
                                new String[] {"id", "amount", "source_properties"},
                                Types.STRING,
                                Types.LONG,
                                Types.MAP(Types.STRING, Types.STRING)));
        table.createTemporaryView(
                "ticdc_changes",
                table.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .column(
                                        "source_properties",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.all()));
        table.executeSql(
                "CREATE TABLE current_ticdc_rows ("
                        + "id STRING NOT NULL, amount BIGINT, "
                        + "source_properties MAP<STRING, STRING> "
                        + "METADATA FROM 'debezium-source-properties', "
                        + "PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector'='bigquery', 'project'='p', 'dataset'='d', "
                        + "'table'='current_ticdc_rows', 'sink.cdc.enabled'='true', "
                        + "'sink.cdc.ticdc.cluster-id'='test_cluster', "
                        + "'emulator-endpoint'='localhost:1')");

        String plan =
                table.explainSql(
                        "INSERT INTO current_ticdc_rows"
                                + " SELECT id, amount, source_properties FROM ticdc_changes");

        assertThat(plan).contains("debezium-source-properties").contains("DropUpdateBefore");
    }

    /**
     * The native Spanner change-stream source exposes typed ordering metadata, so its sequence
     * column is a row rather than a string. The planner has to accept that row against the declared
     * metadata type without the query routing it through a Debezium-shaped map.
     */
    @Test
    void projectsTheTypedSpannerChangeSequenceRowOntoItsMetadataColumn() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        DataStream<Row> changelog =
                env.fromData(
                        Collections.singletonList(
                                Row.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        "id",
                                        1L,
                                        Instant.ofEpochSecond(1670955531L, 785000000),
                                        "00000001",
                                        0)),
                        Types.ROW_NAMED(
                                new String[] {
                                    "id",
                                    "amount",
                                    "commit_timestamp",
                                    "record_sequence",
                                    "mod_number"
                                },
                                Types.STRING,
                                Types.LONG,
                                Types.INSTANT,
                                Types.STRING,
                                Types.INT));
        table.createTemporaryView(
                "spanner_changes",
                table.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .column("commit_timestamp", DataTypes.TIMESTAMP_LTZ(9))
                                .column("record_sequence", DataTypes.STRING())
                                .column("mod_number", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        table.executeSql(
                "CREATE TABLE current_spanner_rows ("
                        + "id STRING NOT NULL, amount BIGINT, "
                        + "change_sequence ROW<commit_timestamp TIMESTAMP_LTZ(9),"
                        + " record_sequence STRING, mod_number INT> "
                        + "METADATA FROM 'spanner-change-sequence', "
                        + "PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector'='bigquery', 'project'='p', 'dataset'='d', "
                        + "'table'='current_spanner_rows', 'sink.cdc.enabled'='true', "
                        + "'emulator-endpoint'='localhost:1')");

        String plan =
                table.explainSql(
                        "INSERT INTO current_spanner_rows SELECT id, amount,"
                                + " ROW(commit_timestamp, record_sequence, mod_number)"
                                + " FROM spanner_changes");

        assertThat(plan)
                .contains("ROW(commit_timestamp, record_sequence, mod_number)")
                .contains("AS spanner-change-sequence");
    }

    /**
     * The three fields have three distinct types, so a declaration that renames them into another
     * order cannot be assigned positionally. That is what keeps the resolver's index-based reads
     * from silently attributing one coordinate to another.
     */
    @Test
    void rejectsASpannerChangeSequenceRowDeclaredInAnotherFieldOrder() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        DataStream<Row> changelog =
                env.fromData(
                        Collections.singletonList(
                                Row.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        "id",
                                        Instant.ofEpochSecond(1670955531L, 785000000),
                                        "00000007",
                                        0)),
                        Types.ROW_NAMED(
                                new String[] {
                                    "id", "commit_timestamp", "record_sequence", "mod_number"
                                },
                                Types.STRING,
                                Types.INSTANT,
                                Types.STRING,
                                Types.INT));
        table.createTemporaryView(
                "swapped_changes",
                table.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("commit_timestamp", DataTypes.TIMESTAMP_LTZ(9))
                                .column("record_sequence", DataTypes.STRING())
                                .column("mod_number", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        table.executeSql(
                "CREATE TABLE swapped_spanner_rows ("
                        + "id STRING NOT NULL, "
                        + "change_sequence ROW<commit_timestamp TIMESTAMP_LTZ(9),"
                        + " mod_number INT, record_sequence STRING> "
                        + "METADATA FROM 'spanner-change-sequence', "
                        + "PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'connector'='bigquery', 'project'='p', 'dataset'='d', "
                        + "'table'='swapped_spanner_rows', 'sink.cdc.enabled'='true', "
                        + "'emulator-endpoint'='localhost:1')");

        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "INSERT INTO swapped_spanner_rows SELECT id,"
                                                + " ROW(commit_timestamp, record_sequence,"
                                                + " mod_number) FROM swapped_changes"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("change_sequence");
    }
}
