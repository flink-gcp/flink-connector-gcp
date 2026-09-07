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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamEnvelopeSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.DestinationOrder;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.SinkKind;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.SourceKind;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.assertLogical;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.assertPhysical;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.roundTrip;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.sink;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.source;
import static org.assertj.core.api.Assertions.assertThat;

class BigtableLineageGraphTest {
    @TempDir Path directory;

    @Test
    void graphExtractsEveryBuilderReturnedSourceAndSinkWithoutStartingRuntime() throws Exception {
        for (SourceKind sourceKind : SourceKind.values()) {
            for (SinkKind sinkKind : SinkKind.values()) {
                for (DestinationOrder order :
                        List.of(DestinationOrder.FIXED, DestinationOrder.DYNAMIC_LAST)) {
                    var env = StreamExecutionEnvironment.getExecutionEnvironment();
                    String key = directory.resolve("missing.json").toString();
                    var source = source(sourceKind, key);
                    env.fromSource(
                                    roundTrip(source),
                                    WatermarkStrategy.noWatermarks(),
                                    "Bigtable source",
                                    Types.STRING)
                            .sinkTo(roundTrip(sink(sinkKind, order, key)));
                    var graph = env.getStreamGraph().getLineageGraph();
                    assertThat(graph.sources())
                            .singleElement()
                            .satisfies(
                                    vertex -> {
                                        assertPhysical(vertex);
                                        assertThat(vertex.boundedness())
                                                .isEqualTo(source.getBoundedness());
                                    });
                    assertThat(graph.sinks())
                            .singleElement()
                            .satisfies(
                                    vertex -> {
                                        if (order == DestinationOrder.DYNAMIC_LAST) {
                                            assertThat(vertex.datasets()).isEmpty();
                                        } else {
                                            assertPhysical(vertex);
                                        }
                                    });
                    assertThat(graph.relations()).hasSize(1);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "upsert",
                "keep-latest",
                "aggregate",
                "insert-if-absent",
                "append",
                "increment"
            })
    void sqlRetainsLogicalAndPhysicalIdentityForEveryWriteModeAndScanPushdown(String mode) {
        for (boolean filtered : List.of(false, true)) {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);
            var table = StreamTableEnvironment.create(env);
            table.getConfig().set("table.exec.sink.require-on-conflict", "false");
            String type = mode.equals("append") ? "STRING" : "BIGINT";
            table.executeSql(
                    "CREATE TABLE input_table (rowkey STRING, cf ROW<q "
                            + type
                            + ">, ignored ROW<q INT>) "
                            + options(
                                    "'scan.parallelism'='2', 'scan.app-profile-id'='scan-profile', 'scan.row-prefix'='r'"));
            createSink(table, mode, type);
            String query =
                    "SELECT rowkey, cf FROM input_table"
                            + (filtered ? " WHERE rowkey >= 'r' AND rowkey < 's'" : "");
            String plan = table.explainSql(query);
            assertThat(plan).contains("project=[rowkey, cf]");
            if (filtered) {
                assertThat(plan).containsIgnoringCase("filter=[and(");
            }
            table.createStatementSet()
                    .addInsertSql("INSERT INTO output_table " + query)
                    .attachAsDataStream();
            StreamGraph graph = env.getStreamGraph();
            assertSqlGraph(graph, Boundedness.BOUNDED);
            assertThat(
                            graph.getStreamNodes().stream()
                                    .filter(node -> graph.getSourceIDs().contains(node.getId())))
                    .singleElement()
                    .satisfies(node -> assertThat(node.getParallelism()).isEqualTo(2));
            assertThat(
                            graph.getStreamNodes().stream()
                                    .filter(
                                            node ->
                                                    node.getOperatorFactory()
                                                            instanceof
                                                            org.apache.flink.streaming.runtime
                                                                    .operators.sink
                                                                    .SinkWriterOperatorFactory))
                    .singleElement()
                    .satisfies(node -> assertThat(node.getParallelism()).isEqualTo(4));
        }
    }

    @ParameterizedTest
    @CsvSource({"envelope,false", "envelope,true", "selected-cell,false", "selected-cell,true"})
    void sqlChangeStreamsRetainTheConfiguredTableAndBoundedness(String mode, boolean bounded) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.exec.sink.require-on-conflict", "false");
        String extra =
                "'scan.mode'='change-stream', 'scan.change-stream.changelog-mode'='"
                        + mode
                        + "', 'scan.app-profile-id'='single-cluster'"
                        + (bounded ? ", 'scan.bounded.timestamp-millis'='1767312000000'" : "");
        String query;
        if (mode.equals("envelope")) {
            var descriptor =
                    TableDescriptor.forConnector("bigtable")
                            .schema(
                                    Schema.newBuilder()
                                            .fromRowDataType(
                                                    BigtableChangeStreamEnvelopeSchema.DATA_TYPE)
                                            .build())
                            .option("project", "lineage-project")
                            .option("instance", "lineage-instance")
                            .option("table", "events")
                            .option(
                                    "service-account-key-file",
                                    directory.resolve("missing.json").toString())
                            .option("scan.mode", "change-stream")
                            .option("scan.change-stream.changelog-mode", "envelope")
                            .option("scan.app-profile-id", "single-cluster");
            if (bounded) {
                descriptor.option("scan.bounded.timestamp-millis", "1767312000000");
            }
            table.createTemporaryTable("input_table", descriptor.build());
            query = "SELECT CAST(row_key AS STRING), ROW(CAST(1 AS BIGINT)) FROM input_table";
        } else {
            table.executeSql(
                    "CREATE TABLE input_table (rowkey STRING, score BIGINT, PRIMARY KEY(rowkey) NOT ENFORCED) "
                            + options(
                                    extra
                                            + ", 'scan.change-stream.selected-cell.family'='cf', 'scan.change-stream.selected-cell.qualifier-base64'='cQ==', "
                                            + "'scan.change-stream.selected-cell.source-cluster-id'='cluster', 'value.format'='json'"));
            query = "SELECT rowkey, ROW(score) FROM input_table";
        }
        createSink(table, "upsert", "BIGINT");
        table.createStatementSet()
                .addInsertSql("INSERT INTO output_table " + query)
                .attachAsDataStream();
        assertSqlGraph(
                env.getStreamGraph(),
                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void lookupTableDoesNotBecomeASourceInTheLineageGraph() {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.exec.sink.require-on-conflict", "false");
        table.executeSql(
                "CREATE TABLE input_table (rowkey STRING, pt AS PROCTIME()) WITH ('connector'='datagen')");
        table.executeSql(
                "CREATE TABLE dimension_table (rowkey STRING, cf ROW<q BIGINT>) "
                        + options("'lookup.async'='true'").replace("'events'", "'dimension'"));
        createSink(table, "upsert", "BIGINT");
        String query =
                "INSERT INTO output_table SELECT s.rowkey, d.cf FROM input_table AS s "
                        + "JOIN dimension_table FOR SYSTEM_TIME AS OF s.pt AS d ON s.rowkey=d.rowkey";
        assertThat(table.explainSql(query)).contains("LookupJoin");
        table.createStatementSet().addInsertSql(query).attachAsDataStream();
        var graph = env.getStreamGraph().getLineageGraph();
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        vertex ->
                                assertThat(vertex.datasets())
                                        .allSatisfy(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .doesNotContain("dimension_table");
                                                    assertThat(dataset.facets())
                                                            .doesNotContainKey("gcp");
                                                }));
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        vertex ->
                                assertLogical(
                                        vertex, "default_catalog.default_database.output_table"));
    }

    private void createSink(StreamTableEnvironment table, String mode, String type) {
        table.executeSql(
                "CREATE TABLE output_table (rowkey STRING, cf ROW<q "
                        + type
                        + ">, PRIMARY KEY (rowkey) NOT ENFORCED) "
                        + options(
                                "'sink.write-mode'='"
                                        + mode
                                        + "', 'sink.parallelism'='4'"
                                        + (mode.equals("aggregate")
                                                ? ", 'sink.aggregate.column-family-types'='cf:int64-sum'"
                                                : "")));
    }

    private String options(String extra) {
        return "WITH ('connector'='bigtable', 'project'='lineage-project', 'instance'='lineage-instance', 'table'='events', "
                + "'service-account-key-file'='"
                + directory.resolve("missing.json")
                + "', "
                + extra
                + ")";
    }

    private static void assertSqlGraph(StreamGraph streamGraph, Boundedness boundedness) {
        var graph = streamGraph.getLineageGraph();
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        vertex -> {
                            assertLogical(vertex, "default_catalog.default_database.input_table");
                            assertThat(vertex.boundedness()).isEqualTo(boundedness);
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        vertex ->
                                assertLogical(
                                        vertex, "default_catalog.default_database.output_table"));
        assertThat(graph.relations()).hasSize(1);
    }
}
