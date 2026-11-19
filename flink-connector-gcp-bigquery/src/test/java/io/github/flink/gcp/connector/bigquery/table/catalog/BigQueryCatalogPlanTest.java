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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.StubBigQuery.TableAnswer;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

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
        // Plan only; an endpoint keeps any client the planner builds off ADC.
        return tableEnvironment(
                answer, Map.of("emulator-endpoint", "localhost:1"), catalog -> catalog);
    }

    /**
     * A planner with the catalog registered as {@code bq} and an upsert view named {@code changes}.
     *
     * @param answer what every table lookup answers
     * @param carried the options the catalog carries
     * @param registered what the planner is given in place of the catalog
     */
    private static StreamTableEnvironment tableEnvironment(
            TableAnswer answer, Map<String, String> carried, UnaryOperator<Catalog> registered) {
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
                registered.apply(
                        new BigQueryCatalog("bq", "p", "analytics", carried, () -> bigquery)));
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
    void aHintAloneCannotMakeACatalogTableACdcSink() {
        // A catalog table has no column to carry the change sequence a CDC sink requires.
        StreamTableEnvironment table = tableEnvironment(keyedTable());

        assertThatThrownBy(
                        () ->
                                table.explainSql(
                                        "INSERT INTO bq.analytics.current_rows"
                                                + " /*+ OPTIONS('sink.cdc.enabled' = 'true') */"
                                                + " SELECT id, amount FROM changes"))
                .hasStackTraceContaining("requires a change sequence");
    }

    @Test
    void aTableLikeAKeyedCatalogTablePlansOntoTheCdcSinkWithASequenceColumn() {
        StreamTableEnvironment table = tableEnvironment(keyedTable());
        table.executeSql(
                "CREATE TABLE current_rows_cdc ("
                        + " sequence STRING METADATA FROM 'change-sequence-number'"
                        + ") WITH ('sink.cdc.enabled' = 'true')"
                        + " LIKE bq.analytics.current_rows");

        String plan =
                table.explainSql(
                        "INSERT INTO current_rows_cdc SELECT id, amount, 'A' FROM changes",
                        ExplainDetail.CHANGELOG_MODE);

        // The sink receives updates and deletes keyed on the catalog's primary key, with nothing
        // materializing or dropping them in between. Its input is the source's upsert stream on
        // Flink 2.x (partial deletes) and a ChangelogNormalize over it on 1.20 (full deletes).
        assertThat(plan)
                .containsPattern(
                        "Sink\\(table=\\[default_catalog\\.default_database\\.current_rows_cdc\\].*\\n"
                                + "\\+- \\w+\\(.*changelogMode=\\[I,UA,P?D\\]\\)")
                .doesNotContain("upsertMaterialize")
                .doesNotContain("DropUpdateBefore");
    }

    @Test
    void withoutTheHintTheSameUpsertIsRefusedAsForAHandWrittenTable() {
        // The catalog carries no CDC option, so an upsert into the catalog table itself has no CDC
        // path at all.
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

    @Test
    void aFileLoadsInsertRunsItsJobsInTheCarriedProjectUnlessAHintNamesAnother() {
        // The write method is per statement, so a FILE_LOADS insert always carries a hint; the job
        // project is the part of it the catalog supplies.
        String fileLoads =
                "'sink.write-method' = 'file-loads',"
                        + " 'sink.file-loads.staging-path' = 'gs://bucket/prefix'";

        assertThat(sinkParentProject("/*+ OPTIONS(" + fileLoads + ") */")).isEqualTo("jobs");
        assertThat(
                        sinkParentProject(
                                "/*+ OPTIONS("
                                        + fileLoads
                                        + ", 'sink.parent-project' = 'other') */"))
                .isEqualTo("other");
    }

    @Test
    void aTableLikeACatalogTableCopiesTheCarriedSinkParentProject() throws Exception {
        // The CDC path runs through such a table. This holds only the copy; that the CDC sink runs
        // its statements in the option's project is BigQueryDynamicTableFactoryTest's.
        StreamTableEnvironment table =
                tableEnvironment(
                        keyedTable(),
                        BigQueryCatalog.carriedOptions(null, "localhost:1", null, null, "jobs"),
                        catalog -> catalog);
        table.executeSql(
                "CREATE TABLE current_rows_cdc ("
                        + " sequence STRING METADATA FROM 'change-sequence-number'"
                        + ") WITH ('sink.cdc.enabled' = 'true')"
                        + " LIKE bq.analytics.current_rows");

        assertThat(
                        table.getCatalog("default_catalog")
                                .orElseThrow()
                                .getTable(new ObjectPath("default_database", "current_rows_cdc"))
                                .getOptions())
                .containsEntry("sink.parent-project", "jobs");
    }

    /**
     * Plans an insert into a catalog table that carries {@code sink.parent-project = 'jobs'} and
     * returns the job project of the sink the planner built.
     */
    private static String sinkParentProject(String hint) {
        RecordingSinkFactory factory = new RecordingSinkFactory();
        // Through carriedOptions rather than a literal map, so a catalog that stops carrying the
        // key fails here too. No emulator endpoint: FILE_LOADS refuses one, and planning builds no
        // client.
        StreamTableEnvironment table =
                tableEnvironment(
                        unkeyedTable(),
                        BigQueryCatalog.carriedOptions(null, null, null, null, "jobs"),
                        catalog -> recordingSinks(catalog, factory));
        // FILE_LOADS refuses a streaming plan without checkpointing, or with an interval below its
        // minimum.
        table.getConfig().set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(10));

        table.explainSql(
                "INSERT INTO bq.analytics.events " + hint + " VALUES ('a', CAST(1 AS BIGINT))");

        Sink<?> sink =
                ((SinkV2Provider)
                                factory.last()
                                        .getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                        .createSink();
        return ((BigQueryFileLoadsSink<?>) sink).getConfig().getParentProject();
    }

    private static TableAnswer unkeyedTable() {
        return TableAnswer.described(
                StandardTableDefinition.of(
                        com.google.cloud.bigquery.Schema.of(
                                Field.of("id", StandardSQLTypeName.STRING),
                                Field.of("amount", StandardSQLTypeName.INT64))),
                null,
                null);
    }

    /**
     * The catalog, except that the factory it hands the planner keeps the sinks it builds, so a
     * test reads what the carried options and a statement's hint produced together.
     */
    private static Catalog recordingSinks(Catalog catalog, RecordingSinkFactory factory) {
        return (Catalog)
                Proxy.newProxyInstance(
                        Catalog.class.getClassLoader(),
                        new Class<?>[] {Catalog.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getFactory")) {
                                return Optional.of(factory);
                            }
                            try {
                                return method.invoke(catalog, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }

    /** The connector's factory, keeping the last sink it built. */
    private static final class RecordingSinkFactory implements DynamicTableSinkFactory {

        private final BigQueryDynamicTableFactory delegate = new BigQueryDynamicTableFactory();
        @Nullable private DynamicTableSink last;

        @Override
        public String factoryIdentifier() {
            return delegate.factoryIdentifier();
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return delegate.requiredOptions();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return delegate.optionalOptions();
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            last = delegate.createDynamicTableSink(context);
            return last;
        }

        DynamicTableSink last() {
            assertThat(last).as("a sink the planner built").isNotNull();
            return last;
        }
    }
}
