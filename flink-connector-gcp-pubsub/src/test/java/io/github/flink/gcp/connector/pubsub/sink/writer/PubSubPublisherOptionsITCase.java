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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the #20 surface against the Pub/Sub emulator: message attributes and
 * ordering keys end to end (the issue's acceptance criterion), ordering across the NOT_FOUND
 * topic-creation repair, and publishing with overridden batching settings.
 */
class PubSubPublisherOptionsITCase extends AbstractPubSubEmulatorITCase {

    private static PubSubWriter<String> writer(
            TopicDestination destination,
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options)
            throws IOException {
        return writer(destination, serializer, options, new FakeMailboxExecutor());
    }

    private static PubSubWriter<String> writer(
            TopicDestination destination,
            PubSubSerializationSchema<String> serializer,
            PubSubPublisherOptions options,
            FakeMailboxExecutor mailbox)
            throws IOException {
        return newWriter(
                TestSinkConfigs.forTopic(
                        destination, serializer, CreateDisposition.CREATE_IF_NEEDED, options),
                mailbox);
    }

    /**
     * Publishes a warm-up record so the auto-creation repair creates the topic (a subscription can
     * only be created on an existing topic), then creates the subscription. Messages published
     * before the subscription exists — including the warm-up record — are not retained for it.
     */
    private static void warmUpAndSubscribe(
            PubSubWriter<String> writer,
            TopicDestination destination,
            String subscriptionId,
            boolean ordered,
            String warmUpRecord)
            throws Exception {
        writer.write(warmUpRecord, CONTEXT);
        writer.flush(false);
        if (ordered) {
            createOrderedSubscription(destination, subscriptionId);
        } else {
            createSubscription(destination, subscriptionId);
        }
    }

    private static PubSubSerializationSchema<String> payload() {
        return PubSubSerializationSchema.payload(new SimpleStringSchema());
    }

    @Test
    void attributesAreDeliveredEndToEnd() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "attributes-topic");
        PubSubWriter<String> writer =
                writer(
                        destination,
                        payload()
                                .withAttributes(
                                        element ->
                                                Collections.singletonMap(
                                                        "length",
                                                        String.valueOf(element.length()))),
                        PubSubPublisherOptions.defaults());
        try {
            warmUpAndSubscribe(writer, destination, "attributes-sub", false, "warm-up");

            writer.write("hello", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        List<PubsubMessage> messages = pullMessagesUntil("attributes-sub", 1, PULL_DEADLINE);
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getData().toString(StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(messages.get(0).getAttributesMap()).containsExactly(Map.entry("length", "5"));
    }

    @Test
    void orderingKeysAreDeliveredInPerKeyOrder() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "ordering-topic");
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder().enableMessageOrdering(true).build();
        PubSubWriter<String> writer =
                writer(
                        destination,
                        payload().withOrderingKey(element -> element.split(":")[0]),
                        options);
        try {
            warmUpAndSubscribe(writer, destination, "ordering-sub", true, "warm-up:0");

            writer.write("k1:1", CONTEXT);
            writer.write("k2:1", CONTEXT);
            writer.write("k1:2", CONTEXT);
            writer.write("k2:2", CONTEXT);
            writer.write("k1:3", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        List<PubsubMessage> messages = pullMessagesUntil("ordering-sub", 5, PULL_DEADLINE);
        assertThat(perKeyPayloads(messages, "k1")).containsExactly("k1:1", "k1:2", "k1:3");
        assertThat(perKeyPayloads(messages, "k2")).containsExactly("k2:1", "k2:2");
    }

    @Test
    void orderingSurvivesTopicAutoCreationRepair() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "ordering-auto-created");
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder().enableMessageOrdering(true).build();
        PubSubWriter<String> writer =
                writer(
                        destination,
                        payload().withOrderingKey(element -> element.split(":")[0]),
                        options);
        try {
            // Several same-key messages to a nonexistent topic: the first publish fails with
            // NOT_FOUND and the SDK cancels the key's queued publishes (cascades), all of which
            // must be parked, resumed and republished in order by the repair.
            writer.write("k1:1", CONTEXT);
            writer.write("k1:2", CONTEXT);
            writer.write("k1:3", CONTEXT);
            writer.flush(false);
            assertThat(topicExists(destination)).isTrue();

            // Delivery is verifiable only for messages published after the subscription exists.
            createOrderedSubscription(destination, "ordering-auto-created-sub");
            writer.write("k1:4", CONTEXT);
            writer.write("k1:5", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        // Retry the pull rather than assuming one returns both: a single pull is not guaranteed to
        // return everything outstanding, which is a flake independent of what this test asserts.
        List<PubsubMessage> messages =
                pullMessagesUntil("ordering-auto-created-sub", 2, PULL_DEADLINE);
        assertThat(perKeyPayloads(messages, "k1")).containsExactly("k1:4", "k1:5");
    }

    @Test
    void overriddenBatchingSettingsStillPublish() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "batching-topic");
        // elementCount 1 sends each message immediately even though the delay threshold would
        // hold it for an hour; the settings-application wiring itself is asserted in
        // DefaultPublisherFactoryTest.
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchElementCountThreshold(1)
                        .batchDelayThreshold(java.time.Duration.ofHours(1))
                        .build();
        FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
        PubSubWriter<String> writer = writer(destination, payload(), options, mailbox);
        try {
            warmUpAndSubscribe(writer, destination, "batching-sub", false, "warm-up");

            writer.write("prompt", CONTEXT);
            // No flush: the element-count threshold of 1 must send the message on its own.
            while (writer.getInFlightMessages() > 0) {
                mailbox.drain();
                Thread.sleep(10);
            }
        } finally {
            writer.close();
        }

        assertThat(pullPayloads("batching-sub", 10)).containsExactly("prompt");
    }

    private static List<String> perKeyPayloads(List<PubsubMessage> messages, String orderingKey) {
        return messages.stream()
                .filter(message -> message.getOrderingKey().equals(orderingKey))
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }
}
