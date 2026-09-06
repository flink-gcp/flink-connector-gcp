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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.testutils.lineage.LineageListenerCapture;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CloudTasksLineageListenerITCase extends AbstractCloudTasksEmulatorITCase {
    @Test
    void theConfiguredListenerReceivesTheProductionSinksQueue() throws Exception {
        QueueDestination queue = createQueue("lineage-listener");
        try (LineageListenerCapture capture = new LineageListenerCapture()) {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironment(1, capture.configuration());
            env.fromData("lineage-payload")
                    .sinkTo(
                            CloudTasksSink.<String>builder()
                                    .queue(queue)
                                    .serializer(
                                            CloudTasksSerializationSchema.httpTarget(
                                                            targetUrl("/lineage"))
                                                    .withBody(new SimpleStringSchema()))
                                    .emulatorEndpoint(emulatorEndpoint())
                                    .build());
            env.execute("cloudtasks lineage listener");
            assertThat(capture.awaitCreated().lineageGraph().sinks())
                    .singleElement()
                    .satisfies(
                            vertex ->
                                    assertThat(vertex.datasets())
                                            .singleElement()
                                            .satisfies(
                                                    dataset -> {
                                                        assertThat(dataset.name())
                                                                .isEqualTo(queue.getQueue());
                                                        assertThat(dataset.namespace())
                                                                .isEqualTo(
                                                                        "cloudtasks://"
                                                                                + queue.getProject()
                                                                                + "/"
                                                                                + queue
                                                                                        .getLocation());
                                                        assertThat(dataset.facets())
                                                                .containsOnlyKeys("gcp");
                                                        assertThat(
                                                                        ((PhysicalResourceFacet)
                                                                                        dataset.facets()
                                                                                                .get(
                                                                                                        "gcp"))
                                                                                .resources())
                                                                .singleElement()
                                                                .satisfies(
                                                                        resource -> {
                                                                            assertThat(
                                                                                            resource
                                                                                                    .kind())
                                                                                    .isEqualTo(
                                                                                            "cloudtasks-queue");
                                                                            assertThat(
                                                                                            resource
                                                                                                    .name())
                                                                                    .isEqualTo(
                                                                                            queue
                                                                                                    .getQueue());
                                                                            assertThat(
                                                                                            resource
                                                                                                    .namespace())
                                                                                    .isEqualTo(
                                                                                            dataset
                                                                                                    .namespace());
                                                                            assertThat(
                                                                                            resource
                                                                                                    .identity())
                                                                                    .containsExactlyInAnyOrderEntriesOf(
                                                                                            Map.of(
                                                                                                    "project",
                                                                                                    queue
                                                                                                            .getProject(),
                                                                                                    "location",
                                                                                                    queue
                                                                                                            .getLocation(),
                                                                                                    "queue",
                                                                                                    queue
                                                                                                            .getQueue()));
                                                                        });
                                                    }));
            assertThat(awaitDistinctBodies("/lineage", 1)).containsExactly("lineage-payload");
        }
    }
}
