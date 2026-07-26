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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.PubSubTopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared harness for integration tests against the Pub/Sub emulator: the container, plaintext
 * admin/subscriber clients pointed at it, and a no-op {@link SinkWriter.Context}. Writers under
 * test use the production {@code DefaultPublisherFactory} and {@code PubSubTopicAdmin} in their
 * emulator-endpoint mode.
 */
@Testcontainers
@Timeout(180)
abstract class AbstractPubSubEmulatorITCase {

    static final String PROJECT = "it-project";

    static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    /**
     * How long a retrying pull waits for the messages it expects. Generous: it is only ever fully
     * spent by a test that is about to fail anyway.
     */
    static final Duration PULL_DEADLINE = Duration.ofSeconds(30);

    /** Fast auto-creation recovery backoff so repair paths converge quickly on the emulator. */
    static final RetrySchedule EMULATOR_RECOVERY_SCHEDULE = new RetrySchedule(100, 1_000, 30, 0);

    @Container
    private static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    private static ManagedChannel channel;
    private static TransportChannelProvider channelProvider;
    static TopicAdminClient topicAdminClient;
    static SubscriptionAdminClient subscriptionAdminClient;
    private static SubscriberStub subscriberStub;

    @BeforeAll
    static void createClients() throws IOException {
        channel =
                ManagedChannelBuilder.forTarget(EMULATOR.getEmulatorEndpoint())
                        .usePlaintext()
                        .build();
        // A fixed provider is not auto-closed by the clients, so all of them can share this
        // one channel.
        channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        topicAdminClient = newTopicAdminClient();
        subscriptionAdminClient =
                SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
        subscriberStub =
                GrpcSubscriberStub.create(
                        SubscriberStubSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
    }

    @AfterAll
    static void closeClients() {
        if (subscriberStub != null) {
            subscriberStub.close();
        }
        if (subscriptionAdminClient != null) {
            subscriptionAdminClient.close();
        }
        if (topicAdminClient != null) {
            topicAdminClient.close();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    static String emulatorEndpoint() {
        return EMULATOR.getEmulatorEndpoint();
    }

    /**
     * Returns a fresh emulator-backed {@link TopicAdmin} for a writer under test (the writer closes
     * its admin, so it must not receive the harness-owned client). Uses the production admin's
     * emulator-endpoint mode, so the integration tests exercise its client construction.
     */
    static TopicAdmin newTopicAdmin() {
        return new PubSubTopicAdmin(emulatorEndpoint());
    }

    private static TopicAdminClient newTopicAdminClient() throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build());
    }

    /**
     * Creates a writer under test wired to the emulator through the production publisher factory
     * and topic admin, with a fast auto-creation recovery schedule suited to the emulator.
     */
    static PubSubWriter<String> newWriter(
            PubSubSinkConfig<String> config, FakeMailboxExecutor mailbox) {
        return new PubSubWriter<>(
                config,
                new DefaultPublisherFactory(config.getPublisherOptions(), emulatorEndpoint()),
                newTopicAdmin(),
                mailbox,
                config.getPublisherOptions().getMaxInFlightMessages(),
                config.getPublisherOptions().getMaxInFlightBytes(),
                EMULATOR_RECOVERY_SCHEDULE);
    }

    /** Creates the topic through the harness-owned admin client. */
    static void createTopic(TopicDestination destination) {
        topicAdminClient.createTopic(
                TopicName.of(destination.getProject(), destination.getTopic()));
    }

    static boolean topicExists(TopicDestination destination) {
        try {
            topicAdminClient.getTopic(
                    TopicName.of(destination.getProject(), destination.getTopic()));
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    static void createSubscription(TopicDestination topic, String subscriptionId) {
        subscriptionAdminClient.createSubscription(
                SubscriptionName.of(PROJECT, subscriptionId),
                TopicName.of(topic.getProject(), topic.getTopic()),
                PushConfig.getDefaultInstance(),
                10);
    }

    /** Creates a subscription that preserves ordering-key delivery order. */
    static void createOrderedSubscription(TopicDestination topic, String subscriptionId) {
        subscriptionAdminClient.createSubscription(
                Subscription.newBuilder()
                        .setName(SubscriptionName.format(PROJECT, subscriptionId))
                        .setTopic(TopicName.format(topic.getProject(), topic.getTopic()))
                        .setAckDeadlineSeconds(10)
                        .setEnableMessageOrdering(true)
                        .build());
    }

    /** Pulls up to {@code maxMessages} from the subscription and returns their UTF-8 payloads. */
    static List<String> pullPayloads(String subscriptionId, int maxMessages) {
        return pullMessages(subscriptionId, maxMessages).stream()
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    /**
     * Pulls repeatedly until {@code expectedDistinct} distinct payloads have been seen or the
     * deadline expires, and returns the accumulated set. A single pull is not guaranteed to return
     * everything outstanding; pulled messages are acked so the next pull returns the remainder
     * immediately instead of after the ack-deadline redelivery.
     */
    static Set<String> pullDistinctPayloadsUntil(
            String subscriptionId, int expectedDistinct, Duration deadline)
            throws InterruptedException {
        Set<String> payloads = new LinkedHashSet<>();
        long deadlineNanos = System.nanoTime() + deadline.toNanos();
        while (payloads.size() < expectedDistinct && System.nanoTime() < deadlineNanos) {
            List<ReceivedMessage> received = pull(subscriptionId, 1_000);
            if (received.isEmpty()) {
                Thread.sleep(100);
                continue;
            }
            acknowledge(subscriptionId, received);
            received.stream()
                    .map(m -> m.getMessage().getData().toString(StandardCharsets.UTF_8))
                    .forEach(payloads::add);
        }
        return payloads;
    }

    /**
     * Pulls repeatedly until {@code expectedCount} distinct messages have been seen or the deadline
     * expires, and returns them in arrival order — the message-level counterpart of {@link
     * #pullDistinctPayloadsUntil}, for assertions that need the ordering key or other metadata.
     *
     * <p>Deduplicated by message id, so neither a redelivery nor the sink's own at-least-once
     * republish can break an ordering assertion made on the result.
     */
    static List<PubsubMessage> pullMessagesUntil(
            String subscriptionId, int expectedCount, Duration deadline)
            throws InterruptedException {
        Map<String, PubsubMessage> byMessageId = new LinkedHashMap<>();
        long deadlineNanos = System.nanoTime() + deadline.toNanos();
        while (byMessageId.size() < expectedCount && System.nanoTime() < deadlineNanos) {
            List<ReceivedMessage> received = pull(subscriptionId, 1_000);
            if (received.isEmpty()) {
                Thread.sleep(100);
                continue;
            }
            acknowledge(subscriptionId, received);
            received.forEach(
                    m -> byMessageId.putIfAbsent(m.getMessage().getMessageId(), m.getMessage()));
        }
        return new ArrayList<>(byMessageId.values());
    }

    /** Pulls up to {@code maxMessages} from the subscription and returns the full messages. */
    static List<PubsubMessage> pullMessages(String subscriptionId, int maxMessages) {
        return pull(subscriptionId, maxMessages).stream()
                .map(received -> received.getMessage())
                .collect(Collectors.toList());
    }

    /** Acks a pulled batch so the next pull returns the remainder rather than a redelivery. */
    private static void acknowledge(String subscriptionId, List<ReceivedMessage> received) {
        subscriberStub
                .acknowledgeCallable()
                .call(
                        AcknowledgeRequest.newBuilder()
                                .setSubscription(
                                        ProjectSubscriptionName.format(PROJECT, subscriptionId))
                                .addAllAckIds(
                                        received.stream()
                                                .map(ReceivedMessage::getAckId)
                                                .collect(Collectors.toList()))
                                .build());
    }

    private static List<ReceivedMessage> pull(String subscriptionId, int maxMessages) {
        return subscriberStub
                .pullCallable()
                .call(
                        PullRequest.newBuilder()
                                .setSubscription(
                                        ProjectSubscriptionName.format(PROJECT, subscriptionId))
                                .setMaxMessages(maxMessages)
                                .build())
                .getReceivedMessagesList();
    }
}
