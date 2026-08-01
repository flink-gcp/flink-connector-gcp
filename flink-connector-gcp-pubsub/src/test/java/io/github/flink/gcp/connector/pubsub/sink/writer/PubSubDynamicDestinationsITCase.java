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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.pubsub.sink.DestinationResolver;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for dynamic per-record destinations against the Pub/Sub emulator: one writer
 * fanning out to several topics through a {@link DestinationResolver}, and the interplay of dynamic
 * destinations with topic auto-creation.
 */
class PubSubDynamicDestinationsITCase extends AbstractPubSubEmulatorITCase {

    /** Routes {@code "<topic>:<n>"} payloads to the topic named by their prefix. */
    private static final DestinationResolver<String> BY_PREFIX =
            (element, context) -> TopicDestination.of(PROJECT, element.split(":")[0]);

    private static PubSubWriter<String> writer() {
        return newWriter(
                TestSinkConfigs.forResolver(
                        BY_PREFIX,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        PubSubPublisherOptions.defaults()),
                new FakeMailboxExecutor());
    }

    @Test
    void dynamicDestinationsFanOutToMultipleTopics() throws Exception {
        TopicDestination topicA = TopicDestination.of(PROJECT, "fanout-a");
        TopicDestination topicB = TopicDestination.of(PROJECT, "fanout-b");
        createTopic(topicA);
        createTopic(topicB);
        createSubscription(topicA, "fanout-a-sub");
        createSubscription(topicB, "fanout-b-sub");

        PubSubWriter<String> writer = writer();
        try {
            writer.write("fanout-a:1", CONTEXT);
            writer.write("fanout-b:1", CONTEXT);
            writer.write("fanout-a:2", CONTEXT);
            writer.write("fanout-b:2", CONTEXT);
            writer.write("fanout-a:3", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(pullDistinctPayloadsUntil("fanout-a-sub", 3, Duration.ofSeconds(30)))
                .containsExactlyInAnyOrder("fanout-a:1", "fanout-a:2", "fanout-a:3");
        assertThat(pullDistinctPayloadsUntil("fanout-b-sub", 2, Duration.ofSeconds(30)))
                .containsExactlyInAnyOrder("fanout-b:1", "fanout-b:2");
    }

    @Test
    void dynamicDestinationsAutoCreateMultipleTopics() throws Exception {
        TopicDestination topicA = TopicDestination.of(PROJECT, "fanout-auto-a");
        TopicDestination topicB = TopicDestination.of(PROJECT, "fanout-auto-b");
        assertThat(topicExists(topicA)).isFalse();
        assertThat(topicExists(topicB)).isFalse();

        PubSubWriter<String> writer = writer();
        try {
            writer.write("fanout-auto-a:1", CONTEXT);
            writer.write("fanout-auto-b:1", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        // Delivery of the trigger records is not asserted: the emulator retains no messages
        // published before a subscription exists, and the topics did not exist to subscribe to.
        assertThat(topicExists(topicA)).isTrue();
        assertThat(topicExists(topicB)).isTrue();
    }
}
