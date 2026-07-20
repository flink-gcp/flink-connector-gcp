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

import io.github.flink.gcp.connector.pubsub.sink.publisher.PubSubPublisherSink;
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
    void builtSinkRoundTripsJavaSerialization() throws Exception {
        Sink<String> sink =
                PubSubSink.<String>builder().topic(TOPIC).serializer(serializer()).build();

        byte[] bytes = InstantiationUtil.serializeObject(sink);
        PubSubPublisherSink<String> copy =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertThat(
                        ((FixedDestinationResolver) copy.getConfig().getDestinationResolver())
                                .getDestination())
                .isEqualTo(TOPIC);
        assertThat(copy.getConfig().getSerializer()).isNotNull();
    }
}
