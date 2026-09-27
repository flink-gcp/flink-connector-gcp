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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.TableAnswer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A catalog table in the planner: what {@code getTable} returns has to plan as a hand-written
 * {@code CREATE TABLE} would.
 */
class BigQueryCatalogPlanTest {

    /** More answers than any one statement's lookups, since the planner looks a table up often. */
    private static final int LOOKUPS = 20;

    private static StreamTableEnvironment tableEnvironment(TableAnswer answer) {
        StubBigQuery bigquery = new StubBigQuery();
        TableAnswer[] answers = new TableAnswer[LOOKUPS];
        Arrays.fill(answers, answer);
        bigquery.tablesAnswering(answers);
        // A query's name resolution asks whether the database exists before it asks for the table.
        bigquery.locatedDataset(DatasetId.of("p", "analytics"), "US");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.registerCatalog(
                "bq",
                new BigQueryCatalog(
                        "bq",
                        "p",
                        "analytics",
                        // Plan only; an endpoint keeps any client the planner builds off ADC.
                        Map.of("emulator-endpoint", "localhost:1"),
                        () -> bigquery));
        DataStream<Row> changelog =
                env.fromData(
                        Arrays.asList(
                                Row.ofKind(RowKind.UPDATE_AFTER, "a", 1L),
                                Row.ofKind(RowKind.DELETE, "b", null)),
                        Types.ROW_NAMED(new String[] {"id", "amount"}, Types.STRING, Types.LONG));
        table.createTemporaryView(
                "changes",
                table.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return table;
    }

    private static TableAnswer keyedTable() {
        // The connector's own CDC tables carry a NULLABLE key column unless
        // sink.derive-required-columns is set.
        return TableAnswer.described(
                StandardTableDefinition.of(
                        com.google.cloud.bigquery.Schema.of(
                                Field.of("id", StandardSQLTypeName.STRING),
                                Field.of("amount", StandardSQLTypeName.INT64))),
                null,
                Collections.singletonList("id"));
    }

    @Test
    void anUpsertIntoAKeyedCatalogTablePlansOntoTheCdcSinkWhenAHintEnablesIt() {
        StreamTableEnvironment table = tableEnvironment(keyedTable());

        String plan =
                table.explainSql(
                        "INSERT INTO bq.analytics.current_rows"
                                + " /*+ OPTIONS('sink.cdc.enabled' = 'true') */"
                                + " SELECT id, amount FROM changes",
                        ExplainDetail.CHANGELOG_MODE);

        // The sink receives updates and deletes keyed on the catalog's primary key, with nothing
        // materializing or dropping them in between. Its input is the source's upsert stream on
        // Flink 2.x (partial deletes) and a ChangelogNormalize over it on 1.20 (full deletes).
        assertThat(plan)
                .containsPattern(
                        "Sink\\(table=\\[bq\\.analytics\\.current_rows\\].*\\n"
                                + "\\+- \\w+\\(.*changelogMode=\\[I,UA,P?D\\]\\)")
                .doesNotContain("upsertMaterialize")
                .doesNotContain("DropUpdateBefore");
    }

    @Test
    void withoutTheHintTheSameUpsertIsRefusedAsForAHandWrittenTable() {
        // The catalog carries no sink option: CDC stays a per-statement choice.
        StreamTableEnvironment table = tableEnvironment(keyedTable());

        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "INSERT INTO bq.analytics.current_rows"
                                                + " SELECT id, amount FROM changes"))
                .hasStackTraceContaining("update and delete changes");
    }

    @Test
    void aBoundedReadOfACatalogTablePlansAgainstTheConnectorSource() {
        StreamTableEnvironment table = tableEnvironment(keyedTable());

        String plan = table.explainSql("SELECT id FROM bq.analytics.current_rows");

        assertThat(plan).contains("TableSourceScan").contains("current_rows");
    }
}
