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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.publisher.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for topic auto-creation against the Pub/Sub emulator: the at-least-once writer
 * publishing to a topic that does not exist, end-to-end through the SDK publisher and the admin
 * topic-creation path.
 */
class PubSubTopicAutoCreationITCase extends AbstractPubSubEmulatorITCase {

    private static PubSubWriter<String> writer(
            TopicDestination destination, CreateDisposition disposition) throws IOException {
        PubSubSinkConfig<String> config =
                ((PubSubPublisherSink<String>)
                                PubSubSink.<String>builder()
                                        .topic(destination)
                                        .serializer(
                                                PubSubSerializationSchema.dataOnly(
                                                        new SimpleStringSchema()))
                                        .createDisposition(disposition)
                                        .build())
                        .getConfig();
        return new PubSubWriter<>(
                config,
                new EmulatorPublisherFactory(emulatorEndpoint()),
                newTopicAdmin(),
                new FakeMailboxExecutor(),
                PubSubPublisherOptions.defaults().getMaxInFlightMessages(),
                new RetrySchedule(100, 1_000, 30, 0));
    }

    @Test
    void createIfNeededCreatesMissingTopicAndPublishesEndToEnd() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "auto-created");
        PubSubWriter<String> writer = writer(destination, CreateDisposition.CREATE_IF_NEEDED);
        try {
            writer.write("first", CONTEXT);
            writer.flush(false);
            assertThat(topicExists(destination)).isTrue();

            // Verify end-to-end delivery after the repair: a subscription only sees messages
            // published after it exists, so the first record (published before the topic did) is
            // unreceivable by design — subscribe now and publish a second record.
            createSubscription(destination, "auto-created-sub");
            writer.write("second", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(pullPayloads("auto-created-sub", 10)).containsExactly("second");
    }

    @Test
    void createNeverFailsFastOnMissingTopic() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "never-created");
        PubSubWriter<String> writer = writer(destination, CreateDisposition.CREATE_NEVER);
        try {
            assertThatThrownBy(
                            () -> {
                                writer.write("first", CONTEXT);
                                writer.flush(false);
                            })
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CREATE_NEVER");
        } finally {
            writer.close();
        }

        assertThat(topicExists(destination)).isFalse();
    }

    @Test
    void createTopicIsIdempotentForExistingTopic() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "idempotent-topic");
        try (TopicAdmin admin = newTopicAdmin()) {
            admin.createTopic(destination);
            admin.createTopic(destination);
        }

        assertThat(topicExists(destination)).isTrue();
    }
}
