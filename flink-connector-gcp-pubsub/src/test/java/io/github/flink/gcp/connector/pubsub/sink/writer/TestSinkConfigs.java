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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.DestinationResolver;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

/**
 * Builds writer test configs through the public sink builder (keeping the build path covered), so
 * builder/config signature changes touch one place instead of every writer test class.
 */
final class TestSinkConfigs {

    static PubSubSinkConfig<String> forTopic(
            TopicDestination topic,
            PubSubSerializationSchema<String> serializer,
            CreateDisposition disposition,
            PubSubPublisherOptions options) {
        return config(
                PubSubSink.<String>builder()
                        .topic(topic)
                        .serializer(serializer)
                        .createDisposition(disposition)
                        .publisherOptions(options));
    }

    static PubSubSinkConfig<String> forResolver(
            DestinationResolver<? super String> resolver,
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options) {
        return config(
                PubSubSink.<String>builder()
                        .destinationResolver(resolver)
                        .serializer(serializer)
                        .publisherOptions(options));
    }

    private static PubSubSinkConfig<String> config(PubSubSinkBuilder<String> builder) {
        return ((PubSubPublisherSink<String>) builder.build()).getConfig();
    }

    private TestSinkConfigs() {}
}
