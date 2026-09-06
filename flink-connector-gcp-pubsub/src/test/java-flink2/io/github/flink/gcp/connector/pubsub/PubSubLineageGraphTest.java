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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.FIRST;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.SECOND;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.TOPIC;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.resources;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.roundTrip;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.sinkBuilder;
import static io.github.flink.gcp.connector.pubsub.PubSubLineageTest.sourceBuilder;
import static org.assertj.core.api.Assertions.assertThat;

class PubSubLineageGraphTest {
    @TempDir Path directory;

    @Test
    void graphExtractsBuilderReturnedObjectsWithoutStartingThePubSubRuntime() throws Exception {
        for (boolean dynamic : List.of(false, true)) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            String missingKey = directory.resolve("missing.json").toString();
            var source =
                    sourceBuilder(new PubSubLineageTest.FakeSchema())
                            .subscriptions(SECOND, FIRST)
                            .serviceAccountKeyFile(missingKey)
                            .build();
            var builder =
                    sinkBuilder(new PubSubLineageTest.FakeSchema())
                            .serviceAccountKeyFile(missingKey);
            var sink =
                    dynamic
                            ? builder.destinationResolver(new PubSubLineageTest.FakeResolver())
                                    .build()
                            : builder.topic(TOPIC).build();
            env.fromSource(
                            roundTrip(source),
                            WatermarkStrategy.noWatermarks(),
                            "Pub/Sub source",
                            Types.STRING)
                    .sinkTo(roundTrip(sink));
            LineageGraph graph = env.getStreamGraph().getLineageGraph();
            assertThat(graph.sources())
                    .singleElement()
                    .satisfies(
                            vertex -> {
                                assertThat(vertex.boundedness()).isEqualTo(source.getBoundedness());
                                assertThat(vertex.datasets())
                                        .extracting(LineageDataset::name)
                                        .containsExactly(
                                                "subscription:a-project:first",
                                                "subscription:z-project:second");
                            });
            assertThat(graph.sinks())
                    .singleElement()
                    .satisfies(
                            vertex -> {
                                if (dynamic) {
                                    assertThat(vertex.datasets()).isEmpty();
                                } else {
                                    assertThat(vertex.datasets())
                                            .extracting(LineageDataset::name)
                                            .containsExactly("topic:out-project:events");
                                }
                            });
            assertThat(graph.relations()).hasSize(1);
        }
    }

    @ParameterizedTest
    @MethodSource("tableCases")
    void plannerRetainsPhysicalResourcesAndOrderingRouting(
            boolean multiple, boolean ordered, int parallelism) {
        StreamGraph graph = plan(multiple, ordered, parallelism, false);
        assertTableLineage(graph, multiple);
        long keyedEdges =
                graph.getStreamNodes().stream()
                        .flatMap(node -> node.getOutEdges().stream())
                        .filter(edge -> edge.getPartitioner() instanceof KeyGroupStreamPartitioner)
                        .count();
        assertThat(keyedEdges).isEqualTo(ordered && parallelism != 1 ? 1 : 0);
        // Sink V2 expands to one writer node here: exposing createSink must not attach a second
        // sink.
        assertThat(
                        graph.getStreamNodes().stream()
                                .filter(
                                        node ->
                                                node.getOperatorFactory()
                                                        instanceof SinkWriterOperatorFactory)
                                .collect(Collectors.toList()))
                .singleElement()
                .satisfies(
                        node ->
                                assertThat(node.getParallelism())
                                        .isEqualTo(parallelism == 0 ? 3 : parallelism));
    }

    @Test
    void plannerDoesNotTurnCreationSettingsIntoAdditionalPhysicalResources() {
        assertTableLineage(plan(true, true, 3, true), true);
    }

    static Stream<Arguments> tableCases() {
        return Stream.of(false, true)
                .flatMap(
                        multiple ->
                                Stream.of(false, true)
                                        .flatMap(
                                                ordered ->
                                                        Stream.of(0, 1, 3)
                                                                .map(
                                                                        parallelism ->
                                                                                Arguments.of(
                                                                                        multiple,
                                                                                        ordered,
                                                                                        parallelism))));
    }

    private StreamGraph plan(boolean multiple, boolean ordered, int parallelism, boolean create) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        String keyFile = directory.resolve("missing.json").toString();
        table.executeSql(
                "CREATE TABLE source_table (id INT) WITH ('connector' = 'pubsub', 'project' ="
                    + " 'sql-project', 'subscription' = '"
                        + (multiple ? "second;first" : "first")
                        + "', 'format' = 'json', 'service-account-key-file' = '"
                        + keyFile
                        + "'"
                        + (create
                                ? ", 'scan.auto-create.topics' ="
                                      + " 'first:backing-first,second:backing-second'"
                                : "")
                        + ")");
        table.executeSql(
                "CREATE TABLE sink_table (id INT"
                        + (ordered ? ", order_key STRING METADATA FROM 'ordering-key'" : "")
                        + ") WITH ('connector' = 'pubsub', 'project' = 'sql-project', 'topic' ="
                        + " 'events', 'format' = 'json', 'service-account-key-file' = '"
                        + keyFile
                        + "'"
                        + (ordered ? ", 'sink.message-ordering.enabled' = 'true'" : "")
                        + (parallelism == 0 ? "" : ", 'sink.parallelism' = '" + parallelism + "'")
                        + (create ? ", 'sink.create-disposition' = 'create-if-needed'" : "")
                        + ")");
        table.createStatementSet()
                .addInsertSql(
                        "INSERT INTO sink_table SELECT id"
                                + (ordered ? ", CAST(id AS STRING)" : "")
                                + " FROM source_table")
                .attachAsDataStream();
        return env.getStreamGraph();
    }

    private static void assertTableLineage(StreamGraph streamGraph, boolean multiple) {
        LineageGraph graph = streamGraph.getLineageGraph();
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        vertex -> {
                            assertThat(vertex.boundedness())
                                    .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
                            assertThat(vertex.datasets())
                                    .singleElement()
                                    .satisfies(
                                            dataset -> {
                                                assertThat(dataset.name())
                                                        .isEqualTo(
                                                                "default_catalog.default_database.source_table");
                                                assertThat(dataset.namespace()).isEqualTo("pubsub");
                                                assertThat(resources(dataset))
                                                        .extracting(ResourceIdentifier::name)
                                                        .containsExactlyElementsOf(
                                                                multiple
                                                                        ? List.of(
                                                                                "subscription:sql-project:first",
                                                                                "subscription:sql-project:second")
                                                                        : List.of(
                                                                                "subscription:sql-project:first"));
                                            });
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        vertex ->
                                assertThat(vertex.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .isEqualTo(
                                                                    "default_catalog.default_database.sink_table");
                                                    assertThat(dataset.namespace())
                                                            .isEqualTo("pubsub");
                                                    assertThat(resources(dataset))
                                                            .extracting(ResourceIdentifier::name)
                                                            .containsExactly(
                                                                    "topic:sql-project:events");
                                                }));
        assertThat(graph.relations()).hasSize(1);
    }
}
