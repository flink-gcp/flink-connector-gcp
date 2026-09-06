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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksLineageTest.QUEUE;
import static io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksLineageTest.assertPhysicalQueue;
import static io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksLineageTest.assertQueue;
import static io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksLineageTest.builder;
import static org.assertj.core.api.Assertions.assertThat;

class CloudTasksLineageGraphTest {
    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    void dataStreamExtractsTheSerializedBuilderResultWithoutOpeningIt(
            boolean dynamic, boolean staged)
            throws Exception {
        CloudTasksSinkBuilder<String> builder = builder().queue(QUEUE);
        if (staged) {
            builder.deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE);
        }
        if (dynamic) {
            builder.destinationResolver(new CloudTasksLineageTest.ThrowingResolver());
        }
        Sink<String> sink =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(builder.build()),
                        getClass().getClassLoader());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (staged) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000);
        }
        env.fromData("never serialized").sinkTo(sink);
        LineageGraph graph = env.getStreamGraph().getLineageGraph();
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        vertex -> {
                            if (dynamic) {
                                assertThat(vertex.datasets()).isEmpty();
                            } else {
                                assertThat(vertex.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset ->
                                                        assertQueue(
                                                                dataset,
                                                                "project",
                                                                "location",
                                                                "queue"));
                            }
                        });
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void tablePlanningKeepsTheLogicalNameAndPhysicalQueue(boolean appEngine, boolean named) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.executeSql("CREATE DATABASE lineage_db");
        table.useDatabase("lineage_db");
        table.executeSql(
                "CREATE TABLE outgoing (payload STRING"
                        + (named ? ", task_key STRING METADATA FROM 'task-id'" : "")
                        + ") WITH ('connector' = 'cloud-tasks', 'project' = 'project', 'location' = 'location',"
                        + " 'queue' = 'queue', 'format' = 'json', 'sink.parallelism' = '3',"
                        + " 'service-account-key-file' = '/lineage-test/missing-service-account.json',"
                        + (appEngine
                                ? " 'target.type' = 'app-engine', 'app-engine.relative-uri' = '/handler'"
                                : " 'http.url' = 'https://example.com/handler'")
                        + ")");
        table.createStatementSet()
                .addInsertSql(
                        "INSERT INTO outgoing VALUES ('payload'"
                                + (named ? ", 'task-key'" : "")
                                + ")")
                .attachAsDataStream();
        LineageGraph graph = env.getStreamGraph().getLineageGraph();
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
                                                                    "default_catalog.lineage_db.outgoing");
                                                    assertThat(dataset.namespace())
                                                            .isEqualTo(
                                                                    "cloudtasks://project/location");
                                                    assertPhysicalQueue(
                                                            dataset,
                                                            "project",
                                                            "location",
                                                            "queue");
                                                }));
    }
}
