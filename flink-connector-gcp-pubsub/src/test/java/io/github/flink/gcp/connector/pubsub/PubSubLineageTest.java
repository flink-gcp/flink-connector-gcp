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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.pubsub.sink.DestinationResolver;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PubSubLineageTest {
    static final SubscriptionDestination FIRST = SubscriptionDestination.of("a-project", "first");
    static final SubscriptionDestination SECOND = SubscriptionDestination.of("z-project", "second");
    static final TopicDestination TOPIC = TopicDestination.of("out-project", "events");

    @TempDir Path directory;

    @Test
    void publicBuildersExposeExactPhysicalIdentitiesWithoutExternalAccess() throws Exception {
        String missingKey = directory.resolve("missing.json").toString();
        FakeSchema schema = new FakeSchema();
        Object source =
                sourceBuilder(schema)
                        .subscriptions(SECOND, FIRST)
                        .serviceAccountKeyFile(missingKey)
                        .build();
        Object sink = sinkBuilder(schema).topic(TOPIC).serviceAccountKeyFile(missingKey).build();
        for (Object candidate : List.of(source, roundTrip(source))) {
            SourceLineageVertex vertex = (SourceLineageVertex) lineage(candidate);
            assertThat(vertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
            assertThat(vertex.boundedness())
                    .isEqualTo(((PubSubStreamingPullSource<?>) candidate).getBoundedness());
            assertThat(vertex.datasets())
                    .extracting(LineageDataset::name)
                    .containsExactly(
                            "subscription:a-project:first", "subscription:z-project:second");
            assertResource(
                    vertex.datasets().get(0),
                    "pubsub-subscription",
                    "subscription:a-project:first",
                    Map.of("project", "a-project", "subscription", "first"));
            assertResource(
                    vertex.datasets().get(1),
                    "pubsub-subscription",
                    "subscription:z-project:second",
                    Map.of("project", "z-project", "subscription", "second"));
            assertThat(((PubSubStreamingPullSource<?>) candidate).getConfig().getSubscriptions())
                    .containsExactly(SECOND, FIRST);
            assertThat(
                            ((PubSubStreamingPullSource<?>) candidate)
                                    .getConfig()
                                    .getServiceAccountKeyFile())
                    .isEqualTo(missingKey);
        }
        for (Object candidate : List.of(sink, roundTrip(sink))) {
            LineageVertex vertex = lineage(candidate);
            assertThat(vertex).isNotInstanceOf(SourceLineageVertex.class);
            assertThat(vertex.datasets())
                    .singleElement()
                    .satisfies(
                            dataset ->
                                    assertResource(
                                            dataset,
                                            "pubsub-topic",
                                            "topic:out-project:events",
                                            Map.of("project", "out-project", "topic", "events")));
            assertThat(((PubSubPublisherSink<?>) candidate).getConfig().getServiceAccountKeyFile())
                    .isEqualTo(missingKey);
        }
        assertThat(schema.calls).isZero();
        // A runtime credentials load would fail before any authenticated client or RPC could start.
        assertThatThrownBy(() -> PubSubCredentials.load(missingKey))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> schema.serialize("record")).isInstanceOf(AssertionError.class);
        assertThat(schema.calls).isEqualTo(1);
    }

    @Test
    void lastDestinationSetterWinsWithoutEvaluatingTheResolver() throws Exception {
        FakeResolver resolver = new FakeResolver();
        List<Sink<String>> dynamic =
                List.of(
                        sinkBuilder(new FakeSchema()).destinationResolver(resolver).build(),
                        sinkBuilder(new FakeSchema())
                                .topic(TOPIC)
                                .destinationResolver(resolver)
                                .build());
        for (Sink<String> sink : dynamic) {
            assertThat(lineage(sink).datasets()).isEmpty();
            assertThat(lineage(roundTrip(sink)).datasets()).isEmpty();
        }
        Sink<String> fixed =
                sinkBuilder(new FakeSchema()).destinationResolver(resolver).topic(TOPIC).build();
        assertThat(lineage(fixed).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("topic:out-project:events");
        assertThat(lineage(roundTrip(fixed)).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("topic:out-project:events");
        assertThat(resolver.calls).isZero();
        assertThatThrownBy(() -> resolver.resolve("record", null))
                .isInstanceOf(AssertionError.class);
        assertThat(resolver.calls).isEqualTo(1);
    }

    @Test
    void creationSettingsDoNotReportBackingTopicsOrDeadLetterResources() throws Exception {
        SubscriptionCreateOptions creation =
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of("backing-project", "backing-topic"))
                        .deadLetterPolicy(TopicDestination.of("dlq-project", "dlq"), 5)
                        .build();
        Object source = sourceBuilder(new FakeSchema()).subscription(FIRST, creation).build();
        Object sink =
                sinkBuilder(new FakeSchema())
                        .topic(TOPIC)
                        .topicCreateOptions(
                                TopicCreateOptions.builder()
                                        .messageRetention(Duration.ofDays(1))
                                        .build())
                        .publisherOptions(
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .build())
                        .build();
        assertThat(lineage(roundTrip(source)).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("subscription:a-project:first");
        assertThat(lineage(roundTrip(sink)).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("topic:out-project:events");
    }

    @Test
    void tableCopiesRetainCompleteResourcesAndLeaveDataStreamMetadataUnchanged() throws Exception {
        PubSubStreamingPullSource<String> source =
                (PubSubStreamingPullSource<String>)
                        sourceBuilder(new FakeSchema()).subscriptions(SECOND, FIRST).build();
        PubSubStreamingPullSource<String> tableSource = source.withTableLineage("catalog.db.input");
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>) sinkBuilder(new FakeSchema()).topic(TOPIC).build();
        PubSubPublisherSink<String> tableSink = sink.withTableLineage("catalog.db.output");
        assertThat(tableSource.getConfig()).isSameAs(source.getConfig());
        assertThat(tableSink.getConfig()).isSameAs(sink.getConfig());
        assertThat(lineage(source).datasets()).hasSize(2);
        assertThat(lineage(sink).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("topic:out-project:events");
        for (Object candidate : List.of(tableSource, roundTrip(tableSource))) {
            assertThat(lineage(candidate).datasets())
                    .singleElement()
                    .satisfies(
                            dataset -> {
                                assertThat(dataset.name()).isEqualTo("catalog.db.input");
                                assertThat(dataset.namespace()).isEqualTo("pubsub");
                                assertThat(resources(dataset))
                                        .extracting(ResourceIdentifier::name)
                                        .containsExactly(
                                                "subscription:a-project:first",
                                                "subscription:z-project:second");
                            });
        }
        assertThat(lineage(roundTrip(tableSink)).datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("catalog.db.output");
                            assertThat(resources(dataset))
                                    .extracting(ResourceIdentifier::name)
                                    .containsExactly("topic:out-project:events");
                        });
        PubSubPublisherSink<String> dynamic =
                (PubSubPublisherSink<String>)
                        sinkBuilder(new FakeSchema())
                                .destinationResolver(new FakeResolver())
                                .build();
        assertThat(lineage(roundTrip(dynamic.withTableLineage("catalog.db.unknown"))).datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("catalog.db.unknown");
                            assertThat(resources(dataset)).isEmpty();
                        });
    }

    @Test
    void repeatedInspectionReturnsImmutableSnapshots() {
        Object source = sourceBuilder(new FakeSchema()).subscriptions(SECOND, FIRST).build();
        LineageVertex vertex = lineage(source);
        assertThatThrownBy(() -> vertex.datasets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        LineageDataset dataset = vertex.datasets().get(0);
        assertThatThrownBy(() -> dataset.facets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> resources(dataset).clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> resources(dataset).get(0).identity().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(lineage(source).datasets())
                .extracting(LineageDataset::name)
                .containsExactly("subscription:a-project:first", "subscription:z-project:second");
    }

    @Test
    void duplicateSubscriptionsRemainInvalid() {
        assertThatThrownBy(
                        () -> sourceBuilder(new FakeSchema()).subscriptions(FIRST, FIRST).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Subscriptions must be distinct");
    }

    static PubSubSourceBuilder<String> sourceBuilder(FakeSchema schema) {
        return PubSubSource.<String>builder().deserializer(schema);
    }

    static PubSubSinkBuilder<String> sinkBuilder(FakeSchema schema) {
        return PubSubSink.<String>builder().serializer(schema);
    }

    static LineageVertex lineage(Object object) {
        assertThat(object).isInstanceOf(LineageVertexProvider.class);
        return ((LineageVertexProvider) object).getLineageVertex();
    }

    static List<ResourceIdentifier> resources(LineageDataset dataset) {
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        PhysicalResourceFacet facet = (PhysicalResourceFacet) dataset.facets().get("gcp");
        assertThat(facet.name()).isEqualTo("gcp");
        return facet.resources();
    }

    static void assertResource(
            LineageDataset dataset, String kind, String name, Map<String, String> identity) {
        assertThat(dataset.namespace()).isEqualTo("pubsub");
        assertThat(dataset.name()).isEqualTo(name);
        assertThat(resources(dataset))
                .singleElement()
                .satisfies(
                        resource -> {
                            assertThat(resource.kind()).isEqualTo(kind);
                            assertThat(resource.namespace()).isEqualTo("pubsub");
                            assertThat(resource.name()).isEqualTo(name);
                            assertThat(resource.identity()).isEqualTo(identity);
                        });
    }

    static <T> T roundTrip(T object) throws Exception {
        byte[] bytes = InstantiationUtil.serializeObject(object);
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("java.lang.invoke.SerializedLambda");
        return InstantiationUtil.deserializeObject(bytes, PubSubLineageTest.class.getClassLoader());
    }

    static final class FakeSchema
            implements PubSubSerializationSchema<String>, PubSubDeserializationSchema<String> {
        private static final long serialVersionUID = 1L;
        private int calls;

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            fail();
        }

        @Override
        public void open(DeserializationSchema.InitializationContext context) {
            fail();
        }

        @Override
        public PubsubMessage serialize(String value) {
            fail();
            return null;
        }

        @Override
        public void deserialize(
                PubsubMessage message,
                SubscriptionDestination subscription,
                Collector<String> out) {
            fail();
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        private void fail() {
            calls++;
            throw new AssertionError("Lineage must not invoke a schema");
        }
    }

    static final class FakeResolver implements DestinationResolver<String> {
        private static final long serialVersionUID = 1L;
        private int calls;

        @Override
        public TopicDestination resolve(String value, SinkWriter.Context context) {
            calls++;
            throw new AssertionError("Lineage must not evaluate a destination");
        }
    }
}
