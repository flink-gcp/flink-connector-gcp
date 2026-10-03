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


package io.github.flink.gcp.connector.datastore.lineage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Flink 2.x extraction from the real Datastore source and sink, without starting a client. */
class DatastoreLineageGraphTest {

    @Test
    void theDataStreamGraphCarriesTheSinkWithNoDataset() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 1)
                .sinkTo(
                        InstantiationUtil.clone(
                                DatastoreSink.<Long>builder()
                                        .database(DatabaseDestination.of("p"))
                                        .serializer(
                                                (element, context) ->
                                                        DatastoreMutation.delete(
                                                                Key.newBuilder("p", "K", element)
                                                                        .build()))
                                        .serviceAccountKeyFile(
                                                "/lineage-test/must-not-read-credentials.json")
                                        .build()));

        LineageGraph graph = env.getStreamGraph().getLineageGraph();

        assertThat(graph.sinks())
                .singleElement()
                .satisfies(vertex -> assertThat(vertex.datasets()).isEmpty());
    }

    @Test
    void theDataStreamGraphCarriesTheReadKind() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(
                        InstantiationUtil.clone(
                                DatastoreSource.<String>builder()
                                        .database(DatabaseDestination.of("p"))
                                        .kind("Task")
                                        .namespace("tenant")
                                        .deserializer(new KeyNameDeserializer())
                                        .serviceAccountKeyFile(
                                                "/lineage-test/must-not-read-credentials.json")
                                        .build()),
                        WatermarkStrategy.noWatermarks(),
                        "datastore")
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
                                                            .isEqualTo(
                                                                    "datastore://p/(default)/tenant");
                                                    assertThat(dataset.name()).isEqualTo("Task");
                                                }));
    }

    private static final class KeyNameDeserializer
            implements DatastoreEntityDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<String> out) {
            out.collect(entity.getKey().getName());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
