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

import com.google.pubsub.v1.Topic;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for topic auto-creation against the Pub/Sub emulator: the at-least-once writer
 * publishing to a topic that does not exist, end-to-end through the SDK publisher and the admin
 * topic-creation path.
 */
class PubSubTopicAutoCreationITCase extends AbstractPubSubEmulatorITCase {

    private static PubSubWriter<String> writer(
            TopicDestination destination, CreateDisposition disposition) {
        return newWriter(
                TestSinkConfigs.forTopic(
                        destination,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        disposition,
                        PubSubPublisherOptions.defaults()),
                new FakeMailboxExecutor());
    }

    private static PubSubWriter<String> writerWithCreateOptions(
            TopicDestination destination, TopicCreateOptions createOptions) {
        return newWriter(
                TestSinkConfigs.forTopic(
                        destination,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        CreateDisposition.CREATE_IF_NEEDED,
                        createOptions,
                        PubSubPublisherOptions.defaults()),
                new FakeMailboxExecutor());
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

    /**
     * End to end through the writer's repair path: a fully-populated {@link TopicCreateOptions}
     * reaches the created topic and reads back field for field. The emulator (google-cloud-cli
     * 441.0.0) stores all four knobs verbatim — what it cannot show is their <em>effect</em>
     * (actual CMEK encryption, residency enforcement, retention-driven replay), and it validates
     * nothing (the KMS key here does not exist); those semantics belong to the real-GCP suite
     * (issue #82).
     */
    @Test
    void theRepairCreatesTheTopicWithTheConfiguredSettings() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "created-with-settings");
        TopicCreateOptions options =
                TopicCreateOptions.builder()
                        .messageRetention(Duration.ofHours(2))
                        .kmsKeyName("projects/p/locations/l/keyRings/r/cryptoKeys/k")
                        .allowedPersistenceRegions(Arrays.asList("us-central1"))
                        .enforceInTransit(true)
                        .build();
        PubSubWriter<String> writer = writerWithCreateOptions(destination, options);
        try {
            writer.write("first", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        Topic created = describeTopic(destination);
        assertThat(created.getName())
                .isEqualTo("projects/" + PROJECT + "/topics/" + destination.getTopic());
        assertThat(created.getMessageRetentionDuration().getSeconds())
                .isEqualTo(Duration.ofHours(2).getSeconds());
        assertThat(created.getKmsKeyName())
                .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
        assertThat(created.getMessageStoragePolicy().getAllowedPersistenceRegionsList())
                .containsExactly("us-central1");
        assertThat(created.getMessageStoragePolicy().getEnforceInTransit()).isTrue();
    }

    @Test
    void createTopicIsIdempotentForExistingTopic() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "idempotent-topic");
        try (TopicAdmin admin = newTopicAdmin()) {
            admin.createTopic(destination, null);
            admin.createTopic(destination, null);
        }

        assertThat(topicExists(destination)).isTrue();
    }
}
