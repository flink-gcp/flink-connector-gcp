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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.testutils.lineage.LineageListenerCapture;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LineageGraphITCase {
    @Test
    void flinkDeliversExtractedMetadataToItsConfiguredListener() throws Exception {
        ResourceIdentifier input = LineageIdentifiers.pubSubSubscription("p", "in");
        ResourceIdentifier output = LineageIdentifiers.bigQueryTable("p", "d", "out");
        LineageTestFixtures.FakeSource source =
                roundTrip(
                        new LineageTestFixtures.FakeSource(
                                List.of(input), Boundedness.BOUNDED, false));
        LineageTestFixtures.FakeSink sink =
                roundTrip(new LineageTestFixtures.FakeSink(List.of(output), false));
        LineageTestFixtures.READERS.set(0);
        LineageTestFixtures.ENUMERATORS.set(0);
        LineageTestFixtures.WRITERS.set(0);
        try (LineageListenerCapture capture = new LineageListenerCapture()) {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironment(1, capture.configuration());
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "input", rowType())
                    .sinkTo(sink)
                    .name("output");
            StreamGraph streamGraph = env.getStreamGraph(false);
            assertGraph(streamGraph.getLineageGraph(), input, output);
            assertThat(LineageTestFixtures.READERS).hasValue(0);
            assertThat(LineageTestFixtures.ENUMERATORS).hasValue(0);
            assertThat(LineageTestFixtures.WRITERS).hasValue(0);
            env.execute("lineage listener test");
            JobCreatedEvent event = capture.awaitCreated();
            assertGraph(event.lineageGraph(), input, output);
            assertThat(LineageTestFixtures.READERS.get()).isPositive();
            assertThat(LineageTestFixtures.ENUMERATORS.get()).isPositive();
            assertThat(LineageTestFixtures.WRITERS.get()).isPositive();
        }
    }

    @Test
    void unboundedAndUnknownMetadataAreStillExtractedWithoutRuntimeCreation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(
                        new LineageTestFixtures.FakeSource(
                                List.of(), Boundedness.CONTINUOUS_UNBOUNDED, false),
                        WatermarkStrategy.noWatermarks(),
                        "unknown input",
                        rowType())
                .sinkTo(new LineageTestFixtures.FakeSink(List.of(), false));
        LineageGraph graph = env.getStreamGraph().getLineageGraph();
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.boundedness())
                                    .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
                            assertThat(source.datasets()).isEmpty();
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(sink -> assertThat(sink.datasets()).isEmpty());
        assertThat(graph.relations()).hasSize(1);
    }

    static InternalTypeInfo<org.apache.flink.table.data.RowData> rowType() {
        return InternalTypeInfo.of(
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())).getLogicalType());
    }

    private static void assertGraph(
            LineageGraph graph, ResourceIdentifier input, ResourceIdentifier output) {
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.boundedness()).isEqualTo(Boundedness.BOUNDED);
                            assertThat(source.datasets())
                                    .singleElement()
                                    .satisfies(dataset -> assertResource(dataset, input));
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        sink ->
                                assertThat(sink.datasets())
                                        .singleElement()
                                        .satisfies(dataset -> assertResource(dataset, output)));
        assertThat(graph.relations())
                .singleElement()
                .satisfies(
                        edge -> {
                            assertThat(edge.source()).isSameAs(graph.sources().get(0));
                            assertThat(edge.sink()).isSameAs(graph.sinks().get(0));
                        });
    }

    private static void assertResource(LineageDataset dataset, ResourceIdentifier resource) {
        assertThat(dataset.namespace()).isEqualTo(resource.namespace());
        assertThat(dataset.name()).isEqualTo(resource.name());
        assertThat(((PhysicalResourceFacet) dataset.facets().get("gcp")).resources())
                .containsExactly(resource);
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }
}
