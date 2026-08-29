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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubRealGcpITCase;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the capacity-eviction and lazy-recreation outcome against real Cloud Pub/Sub.
 *
 * <p>The sink is built through the public builder and production {@code createWriter}, so its
 * publishers use application-default credentials and the real SDK publish path. A capacity of one
 * makes each destination transition synchronous: the package-private state assertions prove that
 * the old publisher state was removed and a new state was created, while the subscription pulls
 * prove that neither transition lost an accepted record.
 *
 * <p>The messages are unkeyed because this class owns publisher lifecycle and non-loss. The gated
 * source ordering class separately owns Cloud Pub/Sub's ordered-delivery guarantee.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubPublisherLifecycleRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;
    private static final Duration EMPTY_PULL_PAUSE = Duration.ofMillis(200);

    private static final class RoutedMessage {

        private final TopicDestination destination;
        private final String payload;

        private RoutedMessage(TopicDestination destination, String payload) {
            this.destination = destination;
            this.payload = payload;
        }

        private TopicDestination destination() {
            return destination;
        }

        private String payload() {
            return payload;
        }
    }

    @Test
    // One synchronous Pull may outlive the collection deadline by about a minute. Together with
    // three publisher-release budgets, 420 seconds preserves enough room for setup and for the
    // payload assertions to report a shortfall instead of the inherited 300-second bare timeout.
    @Timeout(value = 420, threadMode = ThreadMode.SEPARATE_THREAD)
    void capacityEvictionRecreatesAPublisherWithoutLosingAcceptedRecords() throws Exception {
        TopicDestination topicA = createTopic("publisher-lifecycle-a");
        SubscriptionDestination subscriptionA = createSubscription(topicA, "publisher-lifecycle-a");
        TopicDestination topicB = createTopic("publisher-lifecycle-b");
        SubscriptionDestination subscriptionB = createSubscription(topicB, "publisher-lifecycle-b");

        PubSubWriter<RoutedMessage> writer =
                (PubSubWriter<RoutedMessage>)
                        PubSubSink.<RoutedMessage>builder()
                                .destinationResolver((message, unused) -> message.destination())
                                .serializer(
                                        message ->
                                                PubsubMessage.newBuilder()
                                                        .setData(
                                                                ByteString.copyFromUtf8(
                                                                        message.payload()))
                                                        .build())
                                // Both topics are pre-created and registered for cleanup above.
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .publisherOptions(
                                        PubSubPublisherOptions.builder()
                                                .maxActivePublishers(1)
                                                .build())
                                .build()
                                .createWriter(new StubWriterInitContext(0));
        try {
            writer.write(new RoutedMessage(topicA, "a-before-0"), CONTEXT);
            writer.write(new RoutedMessage(topicA, "a-before-1"), CONTEXT);
            writer.flush(false);

            DestinationState originalTopicA = writer.getDestinationState(topicA);
            assertThat(originalTopicA).isNotNull();
            assertThat(writer.getActivePublishers()).isEqualTo(1);

            writer.write(new RoutedMessage(topicB, "b-after-eviction"), CONTEXT);
            assertThat(writer.getDestinationState(topicA)).isNull();
            assertThat(writer.getDestinationState(topicB)).isNotNull();
            assertThat(writer.getActivePublishers()).isEqualTo(1);

            writer.write(new RoutedMessage(topicA, "a-after-recreation"), CONTEXT);
            assertThat(writer.getDestinationState(topicB)).isNull();
            assertThat(writer.getDestinationState(topicA)).isNotNull().isNotSameAs(originalTopicA);
            assertThat(writer.getActivePublishers()).isEqualTo(1);
            writer.flush(false);
        } finally {
            writer.close();
        }

        Set<String> topicARecords = new LinkedHashSet<>();
        Set<String> topicBRecords = new LinkedHashSet<>();
        pullBothUntil(subscriptionA, 3, topicARecords, subscriptionB, 1, topicBRecords);

        assertThat(topicARecords)
                .containsExactlyInAnyOrder("a-before-0", "a-before-1", "a-after-recreation");
        assertThat(topicBRecords).containsExactly("b-after-eviction");
    }

    /** Pulls both subscriptions round-robin under one service-paced collection deadline. */
    private static void pullBothUntil(
            SubscriptionDestination subscriptionA,
            int expectedA,
            Set<String> topicARecords,
            SubscriptionDestination subscriptionB,
            int expectedB,
            Set<String> topicBRecords)
            throws InterruptedException {
        long deadline = System.nanoTime() + COLLECT_TIMEOUT.toNanos();
        while ((topicARecords.size() < expectedA || topicBRecords.size() < expectedB)
                && System.nanoTime() < deadline) {
            boolean receivedNewPayload = pullInto(subscriptionA, expectedA, topicARecords);
            // A synchronous pull against an empty subscription can long-poll for about a minute.
            // Do not start the second one after the collection deadline has passed, or two final
            // long polls can consume the class-level timeout before the assertions report what
            // arrived.
            if (System.nanoTime() < deadline) {
                receivedNewPayload |= pullInto(subscriptionB, expectedB, topicBRecords);
            }
            if (!receivedNewPayload) {
                Thread.sleep(EMPTY_PULL_PAUSE.toMillis());
            }
        }
    }

    /** Pulls once when a subscription still needs payloads and reports whether the set grew. */
    private static boolean pullInto(
            SubscriptionDestination subscription, int expected, Set<String> payloads) {
        if (payloads.size() >= expected) {
            return false;
        }
        int before = payloads.size();
        pullMessagesAndAck(subscription, expected - before)
                .forEach(message -> payloads.add(message.getData().toStringUtf8()));
        return payloads.size() > before;
    }
}
