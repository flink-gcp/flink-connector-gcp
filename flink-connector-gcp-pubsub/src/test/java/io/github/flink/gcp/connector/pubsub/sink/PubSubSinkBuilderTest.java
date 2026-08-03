/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubSinkBuilder}. */
class PubSubSinkBuilderTest {

    private static final TopicDestination TOPIC = TopicDestination.of("my-project", "my-topic");

    private static PubSubSerializationSchema<String> serializer() {
        return PubSubSerializationSchema.dataOnly(new SimpleStringSchema());
    }

    @Test
    void rejectsMissingSerializer() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().topic(TOPIC).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A serializer is required.");
    }

    @Test
    void rejectsMissingDestination() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().serializer(serializer()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "A destination is required: set topic(...) or destinationResolver(...).");
    }

    @Test
    void topicWrapsFixedDestinationResolver() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        DestinationResolver<? super String> resolver = sink.getConfig().getDestinationResolver();
        assertThat(resolver).isInstanceOf(FixedDestinationResolver.class);
        assertThat(((FixedDestinationResolver) resolver).getDestination()).isEqualTo(TOPIC);
    }

    @Test
    void lastDestinationCallWins() {
        DestinationResolver<Object> resolver =
                (element, context) -> TopicDestination.of("my-project", "resolved");

        PubSubPublisherSink<String> resolverLast =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .destinationResolver(resolver)
                                .serializer(serializer())
                                .build();
        assertThat(resolverLast.getConfig().getDestinationResolver()).isSameAs(resolver);

        PubSubPublisherSink<String> topicLast =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .destinationResolver(resolver)
                                .topic(TOPIC)
                                .serializer(serializer())
                                .build();
        assertThat(topicLast.getConfig().getDestinationResolver())
                .isInstanceOf(FixedDestinationResolver.class);
    }

    @Test
    void createDispositionDefaultsToCreateIfNeeded() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        assertThat(sink.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
    }

    @Test
    void rejectsNullCreateDisposition() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().createDisposition(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("createDisposition must not be null");
    }

    @Test
    void carriesTopicCreateOptionsIntoTheConfig() throws Exception {
        TopicCreateOptions createOptions = TopicCreateOptionsTest.fullyPopulated();
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .serializer(serializer())
                                .topicCreateOptions(createOptions)
                                .build();

        assertThat(sink.getConfig().getTopicCreateOptions()).isEqualTo(createOptions);

        // The options ship in the job graph (the region list included), so they must survive
        // Java serialization. The main round-trip test cannot carry them: it uses CREATE_NEVER,
        // which rejects them by design.
        PubSubPublisherSink<String> copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink), getClass().getClassLoader());
        assertThat(copy.getConfig().getTopicCreateOptions()).isEqualTo(createOptions);
    }

    @Test
    void topicCreateOptionsAreUnsetByDefault() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        assertThat(sink.getConfig().getTopicCreateOptions()).isNull();
    }

    @Test
    void rejectsTopicCreateOptionsAlongsideCreateNever() {
        // Whichever order the two are set in: the check is at build(), not in the setters.
        assertThatThrownBy(
                        () ->
                                PubSubSink.<String>builder()
                                        .topic(TOPIC)
                                        .serializer(serializer())
                                        .topicCreateOptions(TopicCreateOptions.builder().build())
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("topicCreateOptions(...)")
                .hasMessageContaining("CREATE_NEVER");
        assertThatThrownBy(
                        () ->
                                PubSubSink.<String>builder()
                                        .topic(TOPIC)
                                        .serializer(serializer())
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .topicCreateOptions(TopicCreateOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CREATE_NEVER");
    }

    @Test
    void rejectsNullTopicCreateOptions() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().topicCreateOptions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("topicCreateOptions must not be null");
    }

    @Test
    void publisherOptionsDefaultToDefaults() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        assertThat(sink.getConfig().getPublisherOptions())
                .isEqualTo(PubSubPublisherOptions.defaults());
    }

    @Test
    void rejectsNullPublisherOptions() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().publisherOptions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("publisherOptions must not be null");
    }

    @Test
    void theFailedMessageHandlerDefaultsToFailJob() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        assertThat(sink.getConfig().getFailedMessageHandler()).isSameAs(FailureHandler.failJob());
    }

    @Test
    void theFailedMessageHandlerPropagatesToConfig() {
        FailureHandler<FailedMessage> handler = message -> {};

        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .serializer(serializer())
                                .failedMessageHandler(handler)
                                .build();

        assertThat(sink.getConfig().getFailedMessageHandler()).isSameAs(handler);
    }

    @Test
    void acceptsACrossConnectorHandlerWithoutACast() {
        // The contravariant parameter is the point: one handler written against the shared contract
        // serves every connector in this repository.
        FailureHandler<FailedElement> shared = FailureHandler.logAndDrop();

        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .serializer(serializer())
                                .failedMessageHandler(shared)
                                .build();

        assertThat(sink.getConfig().getFailedMessageHandler()).isSameAs(shared);
    }

    @Test
    void rejectsNullFailedMessageHandler() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().failedMessageHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("failedMessageHandler must not be null");
    }

    @Test
    void acceptsADroppingFailedMessageHandlerAlongsideMessageOrdering() {
        // The combination #215 settled. What makes it safe is the writer, not the builder: a
        // dropped message leaves its ordering key paused in the SDK publisher, and PubSubWriter
        // hands that key to the repair. Whether to drop at all is the user's call.
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .serializer(serializer())
                                .failedMessageHandler(FailureHandler.logAndDrop())
                                .publisherOptions(
                                        PubSubPublisherOptions.builder()
                                                .enableMessageOrdering(true)
                                                .build())
                                .build();

        assertThat(sink.getConfig().getFailedMessageHandler())
                .isSameAs(FailureHandler.logAndDrop());
        assertThat(sink.getConfig().getPublisherOptions().isEnableMessageOrdering()).isTrue();
    }

    @Test
    void theFailedMessageHandlerSurvivesJavaSerialization() throws Exception {
        // The handler travels to the task managers inside the sink. Ordering is enabled so the
        // combination #215 settled is the one round-tripped, rather than the handler alone.
        Sink<String> sink =
                PubSubSink.<String>builder()
                        .topic(TOPIC)
                        .serializer(serializer())
                        .failedMessageHandler(FailureHandler.logAndDrop())
                        .publisherOptions(
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .build())
                        .build();

        byte[] bytes = InstantiationUtil.serializeObject(sink);
        PubSubPublisherSink<String> copy =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertThat(copy.getConfig().getFailedMessageHandler())
                .isSameAs(FailureHandler.logAndDrop());
        assertThat(copy.getConfig().getPublisherOptions().isEnableMessageOrdering()).isTrue();
    }

    @Test
    void emulatorEndpointDefaultsToNull() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        assertThat(sink.getConfig().getEmulatorEndpoint()).isNull();
    }

    @Test
    void emulatorEndpointPropagatesToConfig() {
        PubSubPublisherSink<String> sink =
                (PubSubPublisherSink<String>)
                        PubSubSink.<String>builder()
                                .topic(TOPIC)
                                .serializer(serializer())
                                .emulatorEndpoint("localhost:8085")
                                .build();

        assertThat(sink.getConfig().getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8085"));
    }

    @Test
    void rejectsNullEmulatorEndpoint() {
        assertThatThrownBy(() -> PubSubSink.<String>builder().emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorEndpoint must not be null");
    }

    @Test
    void rejectsAMalformedEmulatorEndpoint() {
        // Parsed at the setter, so a typo fails on the client rather than at connect time on a
        // TaskManager; the full parse table is EmulatorEndpointTest's.
        assertThatThrownBy(() -> PubSubSink.<String>builder().emulatorEndpoint("localhost8085"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost8085'");
    }

    @Test
    void builtSinkRoundTripsJavaSerialization() throws Exception {
        // Shared fully-populated fixture: a knob added there is automatically covered here.
        PubSubPublisherOptions options = PubSubPublisherOptionsTest.fullyPopulated();
        Sink<String> sink =
                PubSubSink.<String>builder()
                        .topic(TOPIC)
                        .serializer(serializer())
                        .createDisposition(CreateDisposition.CREATE_NEVER)
                        .publisherOptions(options)
                        .emulatorEndpoint("localhost:8085")
                        .build();

        byte[] bytes = InstantiationUtil.serializeObject(sink);
        PubSubPublisherSink<String> copy =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertThat(
                        ((FixedDestinationResolver) copy.getConfig().getDestinationResolver())
                                .getDestination())
                .isEqualTo(TOPIC);
        assertThat(copy.getConfig().getSerializer()).isNotNull();
        assertThat(copy.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_NEVER);
        assertThat(copy.getConfig().getPublisherOptions()).isEqualTo(options);
        assertThat(copy.getConfig().getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8085"));
    }
}
