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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL reads that create their own subscription or seek it, against the Pub/Sub emulator.
 *
 * <p>These two features are tested together because they share a lifecycle: both are applied by the
 * enumerator's startup check, once, before any split is assigned. Neither is observable through the
 * table source's own API — a start position leaves no getter and an auto-created subscription
 * exists only on the service — so every assertion here goes through the emulator.
 *
 * <p>Every subscription here is unordered, because <b>the emulator does not support seek on an
 * ordering-enabled subscription</b>; {@code PubSubSourceStartupITCase} records the same constraint.
 *
 * <p>Each seek test drains the subscription's backlog before the query runs, so the seek is the
 * only thing that can produce a row. That is what makes the assertion decisive rather than a race
 * against delivery order, which the emulator does not guarantee: without it, a seek that never
 * happened would deliver the same rows plus some older ones, and whether the older ones arrived
 * first would decide the result.
 */
class PubSubTableAutoCreateITCase extends PubSubTableTestBase {

    /** Long enough for the enumerator's startup check, short against the 180 s method timeout. */
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void createsTheSubscriptionItWasPointedAtAndConsumesIt() throws Exception {
        String topic = "table-autocreate-topic";
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "table-autocreate-sub");
        createTopic(topic);
        assertThat(subscriptionExists(subscription)).isFalse();

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events (id STRING) "
                        + withOptions(
                                "subscription",
                                subscription.getSubscription(),
                                "format",
                                "json",
                                "scan.auto-create.topics." + subscription.getSubscription(),
                                topic,
                                "scan.auto-create.ack-deadline",
                                "45 s",
                                "scan.auto-create.retain-acked-messages",
                                "true"));

        TableResult result = tEnv.executeSql("SELECT id FROM events");
        awaitSubscription(subscription);

        // The settings reached CreateSubscription rather than only authorising the call.
        assertThat(describeSubscription(subscription).getAckDeadlineSeconds()).isEqualTo(45);
        assertThat(describeSubscription(subscription).getRetainAckedMessages()).isTrue();

        // Published only now: a subscription retains nothing from before it existed, so a message
        // published before the startup check would be lost rather than late.
        publish(topic, "{\"id\":\"a\"}", "{\"id\":\"b\"}");

        List<Row> rows = collect(result, 2, r -> r.getField("id"));

        assertThat(rows).extracting(r -> r.getField("id")).containsAll(Arrays.asList("a", "b"));
    }

    @Test
    void createsSeveralSubscriptionsWithTheirOwnTopicsAndConsumesThem() throws Exception {
        TopicDestination ordersTopic = createTopic("table-multi-orders-topic");
        TopicDestination returnsTopic = createTopic("table-multi-returns-topic");
        SubscriptionDestination orders =
                SubscriptionDestination.of(PROJECT, "table-multi-orders-sub");
        SubscriptionDestination returns =
                SubscriptionDestination.of(PROJECT, "table-multi-returns-sub");
        assertThat(subscriptionExists(orders)).isFalse();
        assertThat(subscriptionExists(returns)).isFalse();

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE events (id STRING) "
                        + withOptions(
                                "subscription",
                                orders.getSubscription() + ";" + returns.getSubscription(),
                                "format",
                                "json",
                                "scan.auto-create.topics." + orders.getSubscription(),
                                ordersTopic.getTopic(),
                                "scan.auto-create.topics." + returns.getSubscription(),
                                returnsTopic.getTopic()));

        TableResult result = tEnv.executeSql("SELECT id FROM events");
        awaitSubscription(orders);
        awaitSubscription(returns);

        assertThat(describeSubscription(orders).getTopic()).isEqualTo(ordersTopic.toTopicPath());
        assertThat(describeSubscription(returns).getTopic()).isEqualTo(returnsTopic.toTopicPath());

        publish(ordersTopic.getTopic(), "{\"id\":\"order\"}");
        publish(returnsTopic.getTopic(), "{\"id\":\"return\"}");

        List<Row> rows = collect(result, 2, r -> r.getField("id"));

        assertThat(rows)
                .extracting(r -> r.getField("id"))
                .containsExactlyInAnyOrder("order", "return");
    }

    @Test
    void earliestRetainedReplaysABacklogThatWasAlreadyAcknowledged() throws Exception {
        String topic = "table-earliest-topic";
        SubscriptionDestination subscription = retainingSubscription("table-earliest-sub", topic);
        publish(topic, "{\"id\":\"first\"}", "{\"id\":\"second\"}");
        drain(subscription, 2);

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE replayed (id STRING) "
                        + withOptions(
                                "subscription",
                                subscription.getSubscription(),
                                "format",
                                "json",
                                "scan.startup.mode",
                                "earliest-retained"));

        List<Row> rows =
                collect(tEnv.executeSql("SELECT id FROM replayed"), 2, r -> r.getField("id"));

        assertThat(rows)
                .extracting(r -> r.getField("id"))
                .containsExactlyInAnyOrder("first", "second");
    }

    @Test
    void timestampReplaysOnlyWhatWasPublishedAfterIt() throws Exception {
        String topic = "table-timestamp-topic";
        SubscriptionDestination subscription = retainingSubscription("table-timestamp-sub", topic);

        publish(topic, "{\"id\":\"old-1\"}", "{\"id\":\"old-2\"}");
        Instant cutoff = latestPublishTime(drain(subscription, 2)).plusMillis(1);
        publish(topic, "{\"id\":\"new-1\"}", "{\"id\":\"new-2\"}");
        List<PubsubMessage> fresh = drain(subscription, 2);

        // The premise, asserted rather than assumed: publish times are stored in milliseconds, so a
        // cutoff one millisecond after the older pair only separates the two if the newer pair
        // really did land later. A failure here is a clock-granularity problem, not a seek bug.
        assertThat(earliestPublishTime(fresh)).isAfterOrEqualTo(cutoff);

        TableEnvironment tEnv = checkpointingTableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE seeked (id STRING) "
                        + withOptions(
                                "subscription",
                                subscription.getSubscription(),
                                "format",
                                "json",
                                "scan.startup.mode",
                                "timestamp",
                                "scan.startup.timestamp-millis",
                                String.valueOf(cutoff.toEpochMilli())));

        List<Row> rows =
                collect(tEnv.executeSql("SELECT id FROM seeked"), 2, r -> r.getField("id"));

        // Exactly the pair after the cutoff: 'earliest-retained' would have brought back all four,
        // so this also proves the mode is the one the DDL named rather than any seek at all.
        assertThat(rows)
                .extracting(r -> r.getField("id"))
                .containsExactlyInAnyOrder("new-1", "new-2");
    }

    /**
     * Creates a topic and a subscription of it that retains acknowledged messages, which is what
     * makes a backwards seek replay them at all.
     */
    private static SubscriptionDestination retainingSubscription(String name, String topic)
            throws Exception {
        createTopic(topic);
        SubscriptionDestination subscription = SubscriptionDestination.of(PROJECT, name);
        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(TopicDestination.of(PROJECT, topic))
                            .retainAckedMessages(true)
                            .build());
        }
        return subscription;
    }

    /**
     * Consumes and acknowledges the expected messages outside Flink, leaving the backlog empty so
     * that only a seek can produce a row afterwards.
     *
     * <p>Empty by construction — the caller publishes exactly {@code expected} messages and this
     * acknowledges exactly that many distinct ones — rather than by a confirming pull. A pull
     * against an empty subscription long-polls for about a minute before returning nothing, which
     * cost this class three minutes to assert something already known.
     */
    private static List<PubsubMessage> drain(SubscriptionDestination subscription, int expected)
            throws InterruptedException {
        List<PubsubMessage> messages = pullMessagesUntil(subscription, expected, STARTUP_TIMEOUT);
        assertThat(messages).hasSize(expected);
        return messages;
    }

    private static Instant earliestPublishTime(List<PubsubMessage> messages) {
        return messages.stream()
                .map(PubSubTableAutoCreateITCase::publishTime)
                .min(Instant::compareTo)
                .orElseThrow(() -> new AssertionError("no messages"));
    }

    private static Instant latestPublishTime(List<PubsubMessage> messages) {
        return messages.stream()
                .map(PubSubTableAutoCreateITCase::publishTime)
                .max(Instant::compareTo)
                .orElseThrow(() -> new AssertionError("no messages"));
    }

    private static Instant publishTime(PubsubMessage message) {
        return Instant.ofEpochSecond(
                message.getPublishTime().getSeconds(), message.getPublishTime().getNanos());
    }

    /** Waits for the enumerator's startup check to have created the subscription. */
    private static void awaitSubscription(SubscriptionDestination subscription)
            throws InterruptedException {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline && !subscriptionExists(subscription)) {
            Thread.sleep(100);
        }
        assertThat(subscriptionExists(subscription))
                .as("the source did not create '%s'", subscription)
                .isTrue();
    }
}
