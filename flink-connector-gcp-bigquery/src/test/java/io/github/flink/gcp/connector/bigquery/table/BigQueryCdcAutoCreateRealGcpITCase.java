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

import com.google.cloud.bigquery.FieldValueList;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-service acceptance for connector-managed CDC table provisioning. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(600)
class BigQueryCdcAutoCreateRealGcpITCase {

    private static final String TABLE = "cdc_auto_created_" + TestNames.runId();

    @AfterAll
    static void dropTable() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void createsConfiguresAndWritesCdcTable() throws Exception {
        writeChanges(
                Arrays.asList(
                        Row.ofKind(RowKind.UPDATE_AFTER, "kept", 2L, "2"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "kept", 1L, "1"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "removed", 2L, "2"),
                        Row.ofKind(RowKind.DELETE, "removed", null, "3"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "removed", 1L, "1")),
                "sink.create-disposition",
                "create-if-needed",
                "sink.cdc.max-staleness",
                "1 ms");

        assertThat(RealBigQuery.tableConstraints(TABLE).getPrimaryKey().getColumns())
                .containsExactly("id");
        Map<String, String> labels = RealBigQuery.tableLabels(TABLE);
        assertThat(labels.get("flink_gcp_cdc")).startsWith("complete_");
        assertMaximumStaleness("INTERVAL 1 MILLISECOND");

        RealBigQuery.queryRows(
                "ALTER TABLE "
                        + RealBigQuery.tablePath(TABLE)
                        + " SET OPTIONS (max_staleness = INTERVAL 2 MILLISECOND)");
        writeChanges(
                Collections.singletonList(Row.ofKind(RowKind.UPDATE_AFTER, "kept", 3L, "3")),
                "sink.create-disposition",
                "create-never",
                "sink.cdc.max-staleness",
                "1 ms",
                "sink.cdc.table-reconciliation",
                "reconcile");
        assertMaximumStaleness("INTERVAL 1 MILLISECOND");

        writeChanges(
                Collections.singletonList(Row.ofKind(RowKind.UPDATE_AFTER, "kept", 4L, "4")),
                "sink.create-disposition",
                "create-never",
                "sink.cdc.clear-max-staleness",
                "true",
                "sink.cdc.table-reconciliation",
                "reconcile");
        assertMaximumStaleness(null);

        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT id, amount FROM " + RealBigQuery.tablePath(TABLE) + " ORDER BY id");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("id").getStringValue()).isEqualTo("kept");
        assertThat(rows.get(0).get("amount").getLongValue()).isEqualTo(4L);
    }

    private static void writeChanges(List<Row> rows, String... cdcTableOptions) throws Exception {
        String[] options =
                TableDdl.concat(
                        new String[] {
                            "sink.cdc.enabled",
                            "true",
                            "sink.create-disposition",
                            "create-if-needed",
                            "sink.location",
                            RealBigQuery.datasetLocation()
                        },
                        cdcTableOptions);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        DataStream<Row> changes =
                env.fromData(
                        rows,
                        Types.ROW_NAMED(
                                new String[] {"id", "amount", "sequence"},
                                Types.STRING,
                                Types.LONG,
                                Types.STRING));
        table.createTemporaryView(
                "changes",
                table.fromChangelogStream(
                        changes,
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
                        + "PRIMARY KEY (id) NOT ENFORCED) "
                        + TableDdl.withOptions(
                                RealBigQuery.project(), RealBigQuery.dataset(), TABLE, options));

        table.executeSql("INSERT INTO current_rows SELECT id, amount, sequence FROM changes")
                .await();
    }

    private static void assertMaximumStaleness(String expected) throws InterruptedException {
        String optionsView =
                String.format(
                        "`%s.%s.INFORMATION_SCHEMA.TABLE_OPTIONS`",
                        RealBigQuery.project(), RealBigQuery.dataset());
        String predicate =
                expected == null
                        ? "(COUNT(*) = 0 OR (COUNT(*) = 1 AND COUNTIF(CAST(option_value AS"
                                + " INTERVAL) = INTERVAL 0 MICROSECOND) = 1))"
                        : "COUNT(*) = 1 AND COUNTIF(CAST(option_value AS INTERVAL) = "
                                + expected
                                + ") = 1";
        List<FieldValueList> maxStaleness =
                RealBigQuery.queryRows(
                        "SELECT "
                                + predicate
                                + " AS matches FROM "
                                + optionsView
                                + " WHERE table_name = '"
                                + TABLE
                                + "' AND option_name = 'max_staleness'");
        assertThat(maxStaleness).hasSize(1);
        assertThat(maxStaleness.get(0).get("matches").getBooleanValue()).isTrue();
    }
}
