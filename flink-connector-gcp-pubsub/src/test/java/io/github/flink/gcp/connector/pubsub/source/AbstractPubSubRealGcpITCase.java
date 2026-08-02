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

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.pubsub.PubSubTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.UnaryOperator;

/**
 * Shared harness for the gated integration tests that run against real Cloud Pub/Sub — the
 * properties the emulator cannot verify: ordered dispatch, dead-letter forwarding, seek on an
 * ordering-enabled subscription, retention and expiration settings taking effect, and IAM.
 *
 * <p>Clients authenticate with application-default credentials through the ADC transport of {@link
 * PubSubTestClients}; the project comes from {@code PUBSUB_IT_PROJECT}. Topics and subscriptions
 * are created under per-run UUID-suffixed names and deleted in {@link AfterAll}, so runs cannot
 * collide and a crash leaves at most one run's resources behind (subscriptions auto-created by a
 * source under test are registered with {@link #trackSubscription} so the same cleanup covers
 * them).
 *
 * <p>The {@code @EnabledIfEnvironmentVariable} gate lives on every concrete class, never here:
 * {@code scripts/e2e-gated-its.sh} discovers the suite by grepping for the annotation literal and
 * then expects a surefire report per matching file, which an abstract class never produces. The
 * {@code gated} tag beside it (issue #245) has to stay on the concrete classes for the same reason,
 * even though JUnit would inherit it from here: {@code --check-tags} greps both literals per file,
 * so hoisting one leaves the other unpaired.
 *
 * <p>Timeouts are looser than the emulator harness's: the service is remote, and dead-letter
 * forwarding in particular is service-paced.
 *
 * <p>The clients and the cleanup registries are static and shared by every concrete class in the
 * fork — one class's {@link AfterAll} clears and closes, the next class's {@link BeforeAll}
 * re-creates. That hand-off assumes surefire's default sequential class execution in one fork;
 * enabling class-level parallelism would let one class's cleanup delete another's live resources.
 */
@Timeout(300)
public abstract class AbstractPubSubRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPubSubRealGcpITCase.class);

    /** The project the suite runs against; null when the gate is off (the tests then skip). */
    protected static final String PROJECT = System.getenv("PUBSUB_IT_PROJECT");

    /**
     * Regional endpoint for publishers that set an ordering key: ordered publishing is only
     * guaranteed through a regional endpoint, and every publisher of the run using the same one is
     * what makes the guarantee hold across them.
     */
    protected static final String ORDERING_PUBLISH_ENDPOINT =
            "asia-northeast1-pubsub.googleapis.com:443";

    /** Bound on waits against the real service — network-paced, so looser than the emulator's. */
    protected static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(120);

    private static final List<TopicName> createdTopics = new CopyOnWriteArrayList<>();
    private static final List<SubscriptionName> createdSubscriptions = new CopyOnWriteArrayList<>();

    private static PubSubTestClients clients;

    @BeforeAll
    static void createClients() throws IOException {
        clients = PubSubTestClients.withApplicationDefaultCredentials();
    }

    @AfterAll
    static void deleteCreatedResourcesAndCloseClients() {
        // Subscriptions before topics: a subscription without its topic lingers detached.
        for (SubscriptionName subscription : createdSubscriptions) {
            try {
                clients.subscriptionAdmin().deleteSubscription(subscription);
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete subscription {}", subscription, e);
            }
        }
        for (TopicName topic : createdTopics) {
            try {
                clients.topicAdmin().deleteTopic(topic);
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete topic {}", topic, e);
            }
        }
        createdSubscriptions.clear();
        createdTopics.clear();
        if (clients != null) {
            clients.close();
        }
    }

    /** Returns a name no other run can collide with. */
    protected static String uniqueName(String prefix) {
        return TestNames.unique(prefix);
    }

    /** Creates a topic under a unique name and registers it for deletion. */
    protected static TopicDestination createTopic(String prefix) {
        TopicName topic = TopicName.of(PROJECT, uniqueName(prefix));
        clients.topicAdmin().createTopic(topic);
        createdTopics.add(topic);
        return TopicDestination.of(PROJECT, topic.getTopic());
    }

    /** Creates a plain pull subscription on the topic and registers it for deletion. */
    protected static SubscriptionDestination createSubscription(
            TopicDestination topic, String prefix) {
        return createSubscription(topic, prefix, UnaryOperator.identity());
    }

    /**
     * Creates a subscription on the topic, letting the caller adjust the settings, and registers it
     * for deletion.
     */
    protected static SubscriptionDestination createSubscription(
            TopicDestination topic, String prefix, UnaryOperator<Subscription.Builder> customize) {
        SubscriptionName name = SubscriptionName.of(PROJECT, uniqueName(prefix));
        Subscription.Builder builder =
                Subscription.newBuilder()
                        .setName(name.toString())
                        .setTopic(topic.toTopicPath())
                        .setPushConfig(PushConfig.getDefaultInstance());
        clients.subscriptionAdmin().createSubscription(customize.apply(builder).build());
        createdSubscriptions.add(name);
        return SubscriptionDestination.of(PROJECT, name.getSubscription());
    }

    /**
     * Registers a subscription some code under test creates — the source's auto-creation, most
     * likely — so the {@link AfterAll} cleanup deletes it too.
     */
    protected static SubscriptionDestination trackSubscription(
            SubscriptionDestination subscription) {
        createdSubscriptions.add(
                SubscriptionName.of(subscription.getProject(), subscription.getSubscription()));
        return subscription;
    }

    /** Returns the subscription as the service reports it. */
    protected static Subscription describeSubscription(SubscriptionDestination subscription) {
        return clients.subscriptionAdmin().getSubscription(subscription.toSubscriptionPath());
    }

    /** Publishes the payloads and waits for the acknowledgements. */
    protected static void publish(TopicDestination topic, String... payloads)
            throws IOException, InterruptedException, ExecutionException {
        publishOrdered(topic, null, payloads);
    }

    /**
     * Publishes the payloads, optionally under one ordering key, and waits for the acknowledgements
     * so they are durable before a source starts. Ordered publishes go through {@link
     * #ORDERING_PUBLISH_ENDPOINT}.
     */
    protected static void publishOrdered(
            TopicDestination topic, String orderingKey, String... payloads)
            throws IOException, InterruptedException, ExecutionException {
        clients.publishOrdered(
                TopicName.of(topic.getProject(), topic.getTopic()),
                orderingKey,
                orderingKey != null ? ORDERING_PUBLISH_ENDPOINT : null,
                payloads);
    }

    /**
     * Pulls and acknowledges until {@code expected} distinct payloads have arrived or the deadline
     * passes, returning what did arrive.
     */
    protected static Set<String> pullAndAckUntil(
            SubscriptionDestination subscription, int expected, Duration timeout)
            throws InterruptedException {
        return clients.pullAndAckUntil(subscription.toSubscriptionPath(), expected, timeout);
    }

    /**
     * Pulls and acknowledges until {@code expected} distinct messages have arrived or the deadline
     * passes, returning them whole — publish time and ordering key included.
     */
    protected static List<PubsubMessage> pullMessagesUntil(
            SubscriptionDestination subscription, int expected, Duration timeout)
            throws InterruptedException {
        return clients.pullMessagesUntil(subscription.toSubscriptionPath(), expected, timeout);
    }

    /** One pull, acknowledging whatever arrives. */
    protected static List<PubsubMessage> pullMessagesAndAck(
            SubscriptionDestination subscription, int maxMessages) {
        return clients.pullMessagesAndAck(subscription.toSubscriptionPath(), maxMessages);
    }
}
