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

package io.github.flink.gcp.connector.pubsub.source;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.PubSubSubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Shared harness for source integration tests against the Pub/Sub emulator: the container,
 * plaintext admin clients, and helpers to create topics and subscriptions and to publish into them.
 * Sources under test use the production {@code DefaultSubscriberFactory} in its emulator-endpoint
 * mode.
 *
 * <p>Separate from the sink's harness, which is package-private in its writer package; extracting a
 * shared one is tracked in issue #27. Only the container image is shared, through {@link
 * PubSubEmulatorContainers}.
 */
@Testcontainers
@Timeout(180)
public abstract class AbstractPubSubSourceEmulatorITCase {

    /** The project every emulator topic and subscription lives in. */
    public static final String PROJECT = "it-project";

    @Container
    private static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    private static ManagedChannel channel;
    private static TransportChannelProvider channelProvider;
    private static TopicAdminClient topicAdminClient;
    private static SubscriptionAdminClient subscriptionAdminClient;

    @BeforeAll
    static void createClients() throws IOException {
        channel =
                ManagedChannelBuilder.forTarget(EMULATOR.getEmulatorEndpoint())
                        .usePlaintext()
                        .build();
        // A fixed provider is not auto-closed by the clients, so all of them can share this one
        // channel.
        channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        topicAdminClient =
                TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
        subscriptionAdminClient =
                SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
    }

    @AfterAll
    static void closeClients() {
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

    /** Returns the emulator endpoint to pass to {@code emulatorEndpoint(...)}. */
    public static String emulatorEndpoint() {
        return EMULATOR.getEmulatorEndpoint();
    }

    /**
     * Creates a topic and a subscription on it, and returns the subscription. The emulator retains
     * nothing published before a subscription exists, so tests must create both before publishing.
     *
     * @param name used for both the topic and the subscription id
     * @param ackDeadlineSeconds the subscription's acknowledgement deadline
     * @return the created subscription
     */
    public static SubscriptionDestination createTopicAndSubscription(
            String name, int ackDeadlineSeconds) {
        TopicName topic = TopicName.of(PROJECT, name);
        topicAdminClient.createTopic(topic);
        subscriptionAdminClient.createSubscription(
                SubscriptionName.of(PROJECT, name),
                topic,
                PushConfig.getDefaultInstance(),
                ackDeadlineSeconds);
        return SubscriptionDestination.of(PROJECT, name);
    }

    /** Creates a topic and an ordering-enabled subscription on it. */
    public static SubscriptionDestination createTopicAndOrderedSubscription(
            String name, int ackDeadlineSeconds) {
        TopicName topic = TopicName.of(PROJECT, name);
        topicAdminClient.createTopic(topic);
        subscriptionAdminClient.createSubscription(
                Subscription.newBuilder()
                        .setName(SubscriptionName.format(PROJECT, name))
                        .setTopic(topic.toString())
                        .setAckDeadlineSeconds(ackDeadlineSeconds)
                        .setEnableMessageOrdering(true)
                        .build());
        return SubscriptionDestination.of(PROJECT, name);
    }

    /** Creates a topic without any subscription on it. */
    public static TopicDestination createTopic(String name) {
        topicAdminClient.createTopic(TopicName.of(PROJECT, name));
        return TopicDestination.of(PROJECT, name);
    }

    /**
     * Returns the production admin, pointed at the emulator. The enumerator closes the admin it is
     * given, so it must not receive a harness-owned client.
     */
    public static SubscriptionAdmin newSubscriptionAdmin() {
        return new PubSubSubscriptionAdmin(emulatorEndpoint());
    }

    /** Returns whether the subscription exists. */
    public static boolean subscriptionExists(SubscriptionDestination subscription) {
        try {
            subscriptionAdminClient.getSubscription(subscription.toSubscriptionPath());
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    /** Returns the subscription as the service reports it. */
    public static Subscription describeSubscription(SubscriptionDestination subscription) {
        return subscriptionAdminClient.getSubscription(subscription.toSubscriptionPath());
    }

    /**
     * Pulls and acknowledges until {@code expected} distinct payloads have arrived or the deadline
     * passes, returning what did arrive.
     *
     * <p>A single pull is not guaranteed to return everything outstanding even when more is
     * available, so an exact-count assertion on one pull would be flaky.
     */
    public static Set<String> pullAndAckUntil(
            SubscriptionDestination subscription, int expected, Duration timeout)
            throws InterruptedException {
        Set<String> payloads = new LinkedHashSet<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (payloads.size() < expected && System.nanoTime() < deadline) {
            List<String> pulled = pullAndAck(subscription, expected);
            if (pulled.isEmpty()) {
                Thread.sleep(100);
            }
            payloads.addAll(pulled);
        }
        return payloads;
    }

    /**
     * Pulls up to {@code maxMessages} from the subscription and acknowledges them, returning their
     * payloads. One pull only — use {@link #pullAndAckUntil} to assert on a known count.
     */
    public static List<String> pullAndAck(SubscriptionDestination subscription, int maxMessages) {
        PullResponse response =
                subscriptionAdminClient
                        .getStub()
                        .pullCallable()
                        .call(
                                PullRequest.newBuilder()
                                        .setSubscription(subscription.toSubscriptionPath())
                                        .setMaxMessages(maxMessages)
                                        .build());
        List<String> payloads = new ArrayList<>(response.getReceivedMessagesCount());
        List<String> ackIds = new ArrayList<>(response.getReceivedMessagesCount());
        for (ReceivedMessage received : response.getReceivedMessagesList()) {
            payloads.add(received.getMessage().getData().toStringUtf8());
            ackIds.add(received.getAckId());
        }
        if (!ackIds.isEmpty()) {
            subscriptionAdminClient.acknowledge(subscription.toSubscriptionPath(), ackIds);
        }
        return payloads;
    }

    /**
     * Publishes the given payloads to the topic of the same name and waits for the
     * acknowledgements.
     */
    public static void publish(String topicName, String... payloads)
            throws IOException, InterruptedException, ExecutionException {
        publishOrdered(topicName, null, payloads);
    }

    /**
     * Publishes the given payloads, optionally under one ordering key, and waits for the
     * acknowledgements so the messages are durable before the source starts.
     */
    public static void publishOrdered(String topicName, String orderingKey, String... payloads)
            throws IOException, InterruptedException, ExecutionException {
        Publisher.Builder builder =
                Publisher.newBuilder(TopicName.of(PROJECT, topicName))
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(NoCredentialsProvider.create());
        if (orderingKey != null) {
            builder.setEnableMessageOrdering(true);
        }
        Publisher publisher = builder.build();
        try {
            List<ApiFuture<String>> published = new ArrayList<>(payloads.length);
            for (String payload : payloads) {
                PubsubMessage.Builder message =
                        PubsubMessage.newBuilder()
                                .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8));
                if (orderingKey != null) {
                    message.setOrderingKey(orderingKey);
                }
                published.add(publisher.publish(message.build()));
            }
            publisher.publishAllOutstanding();
            for (ApiFuture<String> future : published) {
                future.get();
            }
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}
