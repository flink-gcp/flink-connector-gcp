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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksLineageTest {
    static final QueueDestination QUEUE = QueueDestination.of("project", "location", "queue");

    @ParameterizedTest
    @CsvSource({
        "project,location,queue",
        "other,location,queue",
        "project,elsewhere,queue",
        "project,location,other"
    })
    void reportsTheExactFixedQueue(String project, String location, String queue) {
        Sink<String> sink = builder().queue(QueueDestination.of(project, location, queue)).build();
        LineageVertex first = vertex(sink);
        assertThat(first).isNotInstanceOf(SourceLineageVertex.class);
        assertThat(first.datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, project, location, queue));
        assertThat(vertex(sink)).isNotSameAs(first);
        assertThat(vertex(sink).datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, project, location, queue));
        assertThatThrownBy(() -> first.datasets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void stagedCreationRetainsFixedQueueLineageAcrossSerializationWithoutOpeningTheSink()
            throws Exception {
        Sink<String> sink =
                InstantiationUtil.clone(
                        builder()
                                .queue(QUEUE)
                                .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                                .build());
        assertThat(vertex(sink).datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
    }

    @Test
    void anUnknownQueueHasANonNullEmptyVertexWithoutCallingTheResolver() {
        Sink<String> sink = builder().destinationResolver(new ThrowingResolver()).build();
        assertThat(vertex(sink).datasets()).isEmpty();
        assertThat(vertex(sink)).isNotInstanceOf(SourceLineageVertex.class);
    }

    @Test
    void inspectsTheEffectiveResolverInBothSetterOrders() {
        assertThat(
                        vertex(
                                        builder()
                                                .queue(QUEUE)
                                                .destinationResolver(new ThrowingResolver())
                                                .build())
                                .datasets())
                .isEmpty();
        assertThat(
                        vertex(
                                        builder()
                                                .destinationResolver(new ThrowingResolver())
                                                .queue(QUEUE)
                                                .build())
                                .datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
        assertThat(
                        vertex(
                                        builder()
                                                .queue(QueueDestination.of("old", "old", "old"))
                                                .queue(QUEUE)
                                                .build())
                                .datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
        assertThat(
                        vertex(
                                        builder()
                                                .destinationResolver(
                                                        new FixedDestinationResolver(QUEUE))
                                                .build())
                                .datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
    }

    @ParameterizedTest
    @CsvSource({"false,false,1", "false,true,17", "true,false,31", "true,true,7"})
    void targetsNamingAndWriterSettingsDoNotAddDatasets(
            boolean appEngine, boolean named, int concurrency) {
        CloudTasksSerializationSchema<String> serializer =
                appEngine
                        ? CloudTasksSerializationSchema.appEngineTarget("/private/handler")
                                .withBody(new ThrowingBody())
                                .build()
                        : CloudTasksSerializationSchema.httpTarget(
                                        "https://example.com/private/handler")
                                .withBody(new ThrowingBody())
                                .withOidcToken("dispatch@example.com");
        CloudTasksSinkBuilder<String> builder =
                builder()
                        .queue(QUEUE)
                        .serializer(serializer)
                        .writerOptions(
                                CloudTasksWriterOptions.builder()
                                        .maxInFlightTasks(concurrency)
                                        .channelPoolSize(2)
                                        .recoveryMaxAttempts(3)
                                        .build());
        if (named) {
            builder.taskIdExtractor(new ThrowingTaskId());
        }
        assertThat(vertex(builder.build()).datasets())
                .singleElement()
                .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void serializationPreservesConfigurationAndMetadataWithoutSyntheticLambdas(boolean dynamic)
            throws Exception {
        CloudTasksSinkBuilder<String> builder =
                builder()
                        .queue(QUEUE)
                        .taskIdExtractor(new ThrowingTaskId())
                        .writerOptions(
                                CloudTasksWriterOptions.builder()
                                        .maxInFlightTasks(7)
                                        .recoveryMaxAttempts(3)
                                        .build());
        if (dynamic) {
            builder.destinationResolver(new ThrowingResolver());
        }
        Sink<String> sink = builder.build();
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("java.lang.invoke.SerializedLambda");
        Sink<String> restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        CloudTasksSinkConfig<String> config =
                ((CloudTasksCreateTaskSink<String>) restored).getConfig();
        assertThat(config.getWriterOptions())
                .isEqualTo(
                        ((CloudTasksCreateTaskSink<String>) sink).getConfig().getWriterOptions());
        assertThat(config.getServiceAccountKeyFile())
                .isEqualTo("/lineage-test/missing-service-account.json");
        assertThat(config.getSerializer()).isInstanceOf(ThrowingSerializer.class);
        assertThat(config.getTaskIdExtractor()).isInstanceOf(ThrowingTaskId.class);
        if (dynamic) {
            assertThat(vertex(restored).datasets()).isEmpty();
        } else {
            assertThat(vertex(restored).datasets())
                    .singleElement()
                    .satisfies(dataset -> assertQueue(dataset, "project", "location", "queue"));
        }
    }

    @Test
    void metadataNeverEntersTheClientFactoryPath() {
        CloudTasksCreateTaskSink<String> built =
                (CloudTasksCreateTaskSink<String>) builder().queue(QUEUE).build();
        CloudTasksCreateTaskSink<String> guarded = new FactoryRejectingSink(built.getConfig());
        assertThat(guarded.getLineageVertex().datasets()).hasSize(1);
    }

    static CloudTasksSinkBuilder<String> builder() {
        return CloudTasksSink.<String>builder()
                .serializer(new ThrowingSerializer())
                .serviceAccountKeyFile("/lineage-test/missing-service-account.json");
    }

    static LineageVertex vertex(Sink<?> sink) {
        assertThat(sink).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = ((LineageVertexProvider) sink).getLineageVertex();
        assertThat(vertex).isNotNull();
        return vertex;
    }

    static void assertQueue(LineageDataset dataset, String project, String location, String queue) {
        assertThat(dataset.namespace()).isEqualTo("cloudtasks://" + project + "/" + location);
        assertThat(dataset.name()).isEqualTo(queue);
        assertPhysicalQueue(dataset, project, location, queue);
    }

    static void assertPhysicalQueue(
            LineageDataset dataset, String project, String location, String queue) {
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        assertThat(dataset.facets().get("gcp")).isInstanceOf(PhysicalResourceFacet.class);
        PhysicalResourceFacet facet = (PhysicalResourceFacet) dataset.facets().get("gcp");
        assertThat(facet.name()).isEqualTo("gcp");
        assertThat(facet.resources())
                .singleElement()
                .satisfies(
                        resource -> {
                            assertThat(resource.kind()).isEqualTo("cloudtasks-queue");
                            assertThat(resource.namespace())
                                    .isEqualTo("cloudtasks://" + project + "/" + location);
                            assertThat(resource.name()).isEqualTo(queue);
                            assertThat(resource.identity())
                                    .containsExactlyInAnyOrderEntriesOf(
                                            Map.of(
                                                    "project",
                                                    project,
                                                    "location",
                                                    location,
                                                    "queue",
                                                    queue));
                        });
    }

    static final class ThrowingResolver implements DestinationResolver<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public QueueDestination resolve(String value, SinkWriter.Context context) {
            throw new AssertionError("Lineage must not resolve a destination");
        }
    }

    private static final class ThrowingSerializer implements CloudTasksSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            throw new AssertionError("Lineage must not open a serializer");
        }

        @Override
        public Task serialize(String value) {
            throw new AssertionError("Lineage must not create a task");
        }
    }

    private static final class ThrowingBody implements SerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(InitializationContext context) {
            throw new AssertionError("Lineage must not open the body encoder");
        }

        @Override
        public byte[] serialize(String value) {
            throw new AssertionError("Lineage must not serialize a body");
        }
    }

    private static final class ThrowingTaskId implements TaskIdExtractor<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String extractTaskId(String value) {
            throw new AssertionError("Lineage must not extract a task ID");
        }
    }

    private static final class FactoryRejectingSink extends CloudTasksCreateTaskSink<String> {
        private static final long serialVersionUID = 1L;

        private FactoryRejectingSink(CloudTasksSinkConfig<String> config) {
            super(config);
        }

        @Override
        public DefaultTaskCreatorFactory taskCreatorFactory() {
            throw new AssertionError("Lineage must not construct a client factory");
        }
    }
}
