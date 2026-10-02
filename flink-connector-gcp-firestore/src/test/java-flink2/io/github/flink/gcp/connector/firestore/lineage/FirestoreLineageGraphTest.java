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

package io.github.flink.gcp.connector.firestore.lineage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Flink 2.x extraction from the real Firestore source and sink, without starting a client. */
class FirestoreLineageGraphTest {

    @Test
    void theDataStreamGraphCarriesTheSinkWithNoDataset() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 1)
                .sinkTo(
                        InstantiationUtil.clone(
                                FirestoreSink.<Long>builder()
                                        .database(DatabaseDestination.of("p"))
                                        .serializer(
                                                (element, context) ->
                                                        FirestoreWrite.set(
                                                                "c/" + element, Map.of()))
                                        .serviceAccountKeyFile(
                                                "/lineage-test/must-not-read-credentials.json")
                                        .build()));

        LineageGraph graph = env.getStreamGraph().getLineageGraph();

        assertThat(graph.sinks())
                .singleElement()
                .satisfies(vertex -> assertThat(vertex.datasets()).isEmpty());
    }

    @Test
    void theDataStreamGraphCarriesTheScannedCollectionGroup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(
                        InstantiationUtil.clone(
                                FirestoreSource.<String>builder()
                                        .database(DatabaseDestination.of("p"))
                                        .collectionGroup("orders")
                                        .deserializer(new PathDeserializer())
                                        .serviceAccountKeyFile(
                                                "/lineage-test/must-not-read-credentials.json")
                                        .build()),
                        WatermarkStrategy.noWatermarks(),
                        "firestore")
                .print();

        LineageGraph graph = env.getStreamGraph().getLineageGraph();

        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        vertex ->
                                assertThat(vertex.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.namespace())
                                                            .isEqualTo("firestore://p/(default)");
                                                    assertThat(dataset.name())
                                                            .isEqualTo("orders");
                                                }));
    }

    private static final class PathDeserializer
            implements FirestoreDocumentDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                DocumentSnapshot document,
                Collector<String> out) {
            out.collect(document.getReference().getPath());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
