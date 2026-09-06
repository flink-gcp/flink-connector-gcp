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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.ConstantUserResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.TABLE;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.assertPhysical;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.name;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.sinkBuilder;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.source;
import static org.assertj.core.api.Assertions.assertThat;

class BigQueryLineageGraphTest {
    private static final TableDestination OUTPUT =
            TableDestination.of("output-project", "Output", "Result");
    @TempDir Path directory;

    static Stream<Arguments> paths() {
        return Stream.of("table", "view", "query", "filtered")
                .flatMap(
                        mode ->
                                Arrays.stream(WriteMethod.values())
                                        .map(method -> Arguments.of(mode, method)));
    }

    @ParameterizedTest
    @MethodSource("paths")
    void dataStreamExtractsActualPublicBuilderObjectsWithoutExecutingThem(
            String mode, WriteMethod method) {
        LineageGraph graph = graph(mode, method, false);
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        input -> {
                            assertThat(input.boundedness()).isEqualTo(Boundedness.BOUNDED);
                            if (mode.equals("query")) {
                                assertThat(input.datasets()).isEmpty();
                            } else {
                                assertThat(input.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertPhysical(
                                                                dataset, name(TABLE), TABLE));
                            }
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        output ->
                                assertThat(output.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertPhysical(
                                                                dataset, name(OUTPUT), OUTPUT)));
        assertThat(graph.relations()).hasSize(1);
    }

    @Test
    void dataStreamCdcRetainsItsOutput() {
        LineageGraph graph = graph("table", WriteMethod.STORAGE_API_AT_LEAST_ONCE, true);
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        output ->
                                assertThat(output.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertPhysical(
                                                                dataset, name(OUTPUT), OUTPUT)));
    }

    @ParameterizedTest
    @EnumSource(WriteMethod.class)
    void dataStreamRetainsUnknownSinkWithoutEvaluatingItsResolver(WriteMethod method) {
        ConstantUserResolver resolver = new ConstantUserResolver();
        LineageGraph graph =
                graph(
                        "table",
                        sinkBuilder(method, directory.resolve("missing.json").toString())
                                .destinationResolver(resolver)
                                .build());
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        input ->
                                assertThat(input.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertPhysical(
                                                                dataset, name(TABLE), TABLE)));
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(output -> assertThat(output.datasets()).isEmpty());
        assertThat(graph.relations()).hasSize(1);
        assertThat(resolver.calls).isZero();
    }

    private LineageGraph graph(String mode, WriteMethod method, boolean cdc) {
        BigQuerySinkBuilder<String> builder =
                sinkBuilder(method, directory.resolve("missing.json").toString()).table(OUTPUT);
        if (cdc) {
            builder.cdcOptions(
                    CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build());
        }
        return graph(mode, builder.build());
    }

    private LineageGraph graph(String mode, Sink<String> output) {
        StreamExecutionEnvironment env = environment();
        String keyFile = directory.resolve("missing.json").toString();
        env.fromSource(
                        source(mode, keyFile),
                        WatermarkStrategy.noWatermarks(),
                        "input",
                        Types.STRING)
                .sinkTo(output);
        return env.getStreamGraph().getLineageGraph();
    }

    @ParameterizedTest
    @MethodSource("paths")
    void sqlPlannerRetainsPhysicalFacetsBesideCatalogNames(String mode, WriteMethod method) {
        assertTableGraph(plan(mode, method, false), mode.equals("query"));
    }

    @Test
    void sqlCdcRetainsTheFixedPhysicalOutput() {
        assertTableGraph(plan("table", WriteMethod.STORAGE_API_AT_LEAST_ONCE, true), false);
    }

    private LineageGraph plan(String mode, WriteMethod method, boolean cdc) {
        StreamExecutionEnvironment env = environment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.registerCatalog("catalog", new GenericInMemoryCatalog("catalog", "db"));
        table.useCatalog("catalog");
        String inputOptions = "'project'='resource-project', 'dataset'='Dataset', 'table'='Events'";
        if (mode.equals("query")) {
            inputOptions =
                    "'scan.query'='SELECT id FROM private_dataset.private_table',"
                        + " 'scan.parent-project'='billing-project', 'scan.query-location'='US',"
                        + " 'scan.query-result-dataset'='scratch'";
        } else if (mode.equals("view")) {
            inputOptions += ", 'scan.materialize-views'='true'";
        } else if (mode.equals("filtered")) {
            inputOptions +=
                    ", 'scan.row-restriction'='id > 0',"
                            + " 'scan.snapshot-time'='2026-09-01T00:00:00Z'";
        }
        String credentials =
                ", 'service-account-key-file'='" + directory.resolve("missing.json") + "'";
        table.executeSql(
                "CREATE TABLE input (id BIGINT, unused STRING) WITH ('connector'='bigquery', "
                        + inputOptions
                        + credentials
                        + ")");
        String sinkOptions = "'sink.write-method'='" + method + "'";
        if (method == WriteMethod.FILE_LOADS) {
            sinkOptions += ", 'sink.file-loads.staging-path'='gs://lineage-never-opened/staging'";
        }
        if (cdc) {
            sinkOptions += ", 'sink.cdc.enabled'='true'";
        }
        table.executeSql(
                "CREATE TABLE output (id BIGINT"
                        + (cdc ? ", PRIMARY KEY (id) NOT ENFORCED" : "")
                        + ") WITH ('connector'='bigquery', 'project'='output-project',"
                        + " 'dataset'='Output', 'table'='Result', "
                        + sinkOptions
                        + credentials
                        + ")");
        table.createStatementSet()
                .addInsertSql("INSERT INTO output SELECT id FROM input WHERE id > 1")
                .attachAsDataStream();
        return env.getStreamGraph().getLineageGraph();
    }

    private static void assertTableGraph(LineageGraph graph, boolean query) {
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        input -> {
                            assertThat(input.boundedness()).isEqualTo(Boundedness.BOUNDED);
                            assertThat(input.datasets())
                                    .singleElement()
                                    .satisfies(
                                            dataset -> {
                                                if (query) {
                                                    assertThat(dataset.name())
                                                            .isEqualTo("catalog.db.input");
                                                    assertThat(dataset.namespace())
                                                            .isEqualTo("bigquery");
                                                    assertThat(dataset.facets()).containsKey("gcp");
                                                    assertThat(
                                                                    ((PhysicalResourceFacet)
                                                                                    dataset.facets()
                                                                                            .get(
                                                                                                    "gcp"))
                                                                            .resources())
                                                            .isEmpty();
                                                } else {
                                                    assertPhysical(
                                                            dataset, "catalog.db.input", TABLE);
                                                }
                                            });
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        output ->
                                assertThat(output.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertPhysical(
                                                                dataset,
                                                                "catalog.db.output",
                                                                OUTPUT)));
        assertThat(graph.relations()).hasSize(1);
    }

    private static StreamExecutionEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        return env;
    }
}
