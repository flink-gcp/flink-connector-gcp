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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.rpc.NotFoundException;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.PubSubTopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubEmulatorContainers;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared harness for integration tests against the Pub/Sub emulator: the container, the
 * emulator-transport {@link PubSubTestClients}, and a no-op {@link SinkWriter.Context}. Writers
 * under test use the production {@code DefaultPublisherFactory} and {@code PubSubTopicAdmin} in
 * their emulator-endpoint mode.
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

    static String emulatorEndpoint() {
        return EMULATOR.getEmulatorEndpoint();
    }

    /**
     * Returns a fresh emulator-backed {@link TopicAdmin} for a writer under test (the writer closes
     * its admin, so it must not receive the harness-owned client). Uses the production admin's
     * emulator-endpoint mode, so the integration tests exercise its client construction.
     */
    static TopicAdmin newTopicAdmin() {
        return new PubSubTopicAdmin(EmulatorEndpoint.parse(emulatorEndpoint()));
    }

    /**
     * Creates a writer under test wired to the emulator through the production publisher factory
     * and topic admin, with a fast auto-creation recovery schedule suited to the emulator.
     */
    static PubSubWriter<String> newWriter(
            PubSubSinkConfig<String> config, FakeMailboxExecutor mailbox) {
        return new PubSubWriter<>(
                config,
                new DefaultPublisherFactory(
                        config.getPublisherOptions(), EmulatorEndpoint.parse(emulatorEndpoint())),
                newTopicAdmin(),
                mailbox,
                TestSinkWriterMetricGroup.create(),
                EMULATOR_RECOVERY_SCHEDULE);
    }

    /** Creates the topic through the harness-owned admin client. */
    static void createTopic(TopicDestination destination) {
        clients.topicAdmin()
                .createTopic(TopicName.of(destination.getProject(), destination.getTopic()));
    }

    static boolean topicExists(TopicDestination destination) {
        try {
            clients.topicAdmin()
                    .getTopic(TopicName.of(destination.getProject(), destination.getTopic()));
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    /** Returns the topic as the service reports it. */
    static com.google.pubsub.v1.Topic describeTopic(TopicDestination destination) {
        return clients.topicAdmin()
                .getTopic(TopicName.of(destination.getProject(), destination.getTopic()));
    }

    static void createSubscription(TopicDestination topic, String subscriptionId) {
        clients.subscriptionAdmin()
                .createSubscription(
                        SubscriptionName.of(PROJECT, subscriptionId),
                        TopicName.of(topic.getProject(), topic.getTopic()),
                        PushConfig.getDefaultInstance(),
                        10);
    }

    /** Creates a subscription that preserves ordering-key delivery order. */
    static void createOrderedSubscription(TopicDestination topic, String subscriptionId) {
        clients.subscriptionAdmin()
                .createSubscription(
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
        return clients.pullAndAckUntil(
                subscriptionPath(subscriptionId), expectedDistinct, deadline);
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
        return clients.pullMessagesUntil(subscriptionPath(subscriptionId), expectedCount, deadline);
    }

    /** Pulls up to {@code maxMessages} from the subscription and returns the full messages. */
    static List<PubsubMessage> pullMessages(String subscriptionId, int maxMessages) {
        return clients.pullMessages(subscriptionPath(subscriptionId), maxMessages);
    }

    private static String subscriptionPath(String subscriptionId) {
        return ProjectSubscriptionName.format(PROJECT, subscriptionId);
    }
}
