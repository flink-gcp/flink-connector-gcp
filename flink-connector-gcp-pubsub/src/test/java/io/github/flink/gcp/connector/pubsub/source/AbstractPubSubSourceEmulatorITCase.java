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

import com.google.api.gax.rpc.NotFoundException;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.PubSubSubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Shared harness for integration tests against the Pub/Sub emulator: the container, the
 * emulator-transport {@link PubSubTestClients}, and connector-typed helpers to create topics and
 * subscriptions, publish into them and pull back out. Connectors under test use the production
 * factories in their emulator-endpoint mode.
 *
 * <p>Named for the source because that is where it started; the table tests build on it too, since
 * asserting what a sink published means pulling from a subscription. The sink's own harness is
 * still separate — it is package-private in the writer package — but both delegate their client
 * work to the same {@link PubSubTestClients}, so only the connector-typed conveniences differ.
 */
@Testcontainers
@Timeout(180)
public abstract class AbstractPubSubSourceEmulatorITCase {

    /** The project every emulator topic and subscription lives in. */
    public static final String PROJECT = "it-project";

    /**
     * Bound on every drain of a running job's output — comfortably inside the 180 s class timeout,
     * so a shortfall fails the assertion that asked for the elements rather than the whole test.
     */
    public static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(60);

    @Container
    private static final PubSubEmulatorContainer EMULATOR = PubSubEmulatorContainers.newContainer();

    private static PubSubTestClients clients;

    @BeforeAll
    static void createClients() throws IOException {
        clients = PubSubTestClients.forEmulator(EMULATOR.getEmulatorEndpoint());
    }

    @AfterAll
    static void closeClients() {
        if (clients != null) {
            clients.close();
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
        clients.topicAdmin().createTopic(topic);
        clients.subscriptionAdmin()
                .createSubscription(
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
        clients.topicAdmin().createTopic(topic);
        clients.subscriptionAdmin()
                .createSubscription(
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
        clients.topicAdmin().createTopic(TopicName.of(PROJECT, name));
        return TopicDestination.of(PROJECT, name);
    }

    /**
     * Returns the production admin, pointed at the emulator. The enumerator closes the admin it is
     * given, so it must not receive a harness-owned client.
     */
    public static SubscriptionAdmin newSubscriptionAdmin() {
        return new PubSubSubscriptionAdmin(EmulatorEndpoint.parse(emulatorEndpoint()));
    }

    /** Returns whether the topic exists. */
    public static boolean topicExists(String name) {
        try {
            clients.topicAdmin().getTopic(TopicName.of(PROJECT, name));
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    /** Returns the topic as the service reports it. */
    public static com.google.pubsub.v1.Topic describeTopic(String name) {
        return clients.topicAdmin().getTopic(TopicName.of(PROJECT, name));
    }

    /** Returns whether the subscription exists. */
    public static boolean subscriptionExists(SubscriptionDestination subscription) {
        try {
            clients.subscriptionAdmin().getSubscription(subscription.toSubscriptionPath());
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    /** Returns the subscription as the service reports it. */
    public static Subscription describeSubscription(SubscriptionDestination subscription) {
        return clients.subscriptionAdmin().getSubscription(subscription.toSubscriptionPath());
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
        return clients.pullAndAckUntil(subscription.toSubscriptionPath(), expected, timeout);
    }

    /**
     * Pulls and acknowledges until {@code expected} <em>distinct</em> messages have arrived or the
     * deadline passes, returning them whole — attributes and ordering key included, which {@link
     * #pullAndAckUntil} discards.
     */
    public static List<PubsubMessage> pullMessagesUntil(
            SubscriptionDestination subscription, int expected, Duration timeout)
            throws InterruptedException {
        return clients.pullMessagesUntil(subscription.toSubscriptionPath(), expected, timeout);
    }

    /**
     * Pulls up to {@code maxMessages} from the subscription and acknowledges them, returning the
     * messages. One pull only — use {@link #pullMessagesUntil} to assert on a known count.
     */
    public static List<PubsubMessage> pullMessagesAndAck(
            SubscriptionDestination subscription, int maxMessages) {
        return clients.pullMessagesAndAck(subscription.toSubscriptionPath(), maxMessages);
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
        clients.publishOrdered(TopicName.of(PROJECT, topicName), orderingKey, null, payloads);
    }
}
