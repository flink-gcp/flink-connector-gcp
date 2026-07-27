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

import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Drives the production {@link SubscriptionAdmin} against the Pub/Sub emulator. */
class PubSubSubscriptionAdminITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final Duration PULL_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void describingAnAbsentSubscriptionReturnsNothing() throws Exception {
        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            assertThat(admin.describe(SubscriptionDestination.of(PROJECT, "never-created")))
                    .isNull();
        }
    }

    @Test
    void createsASubscriptionWithTheConfiguredSettings() throws Exception {
        TopicDestination topic = createTopic("admin-create-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "admin-create-sub");

        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(topic)
                            .ackDeadline(Duration.ofSeconds(45))
                            .retainAckedMessages(true)
                            .messageRetention(Duration.ofHours(2))
                            .build());
        }

        Subscription created = describeSubscription(subscription);
        assertThat(created.getTopic()).isEqualTo(topic.toTopicPath());
        assertThat(created.getAckDeadlineSeconds()).isEqualTo(45);
        assertThat(created.getRetainAckedMessages()).isTrue();
        assertThat(created.getMessageRetentionDuration().getSeconds()).isEqualTo(2 * 3600);
    }

    @Test
    void createsAnOrderedSubscriptionTheStartupCheckAccepts() throws Exception {
        TopicDestination topic = createTopic("admin-ordered-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "admin-ordered-sub");

        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(topic)
                            .enableMessageOrdering(true)
                            .build());

            SubscriptionInfo info = admin.describe(subscription);
            assertThat(info).isNotNull();
            assertThat(info.isMessageOrderingEnabled()).isTrue();
            assertThat(info.isExactlyOnceDeliveryEnabled()).isFalse();
        }
    }

    @Test
    void creatingAnExistingSubscriptionSucceedsAndLeavesItAlone() throws Exception {
        TopicDestination topic = createTopic("admin-idempotent-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "admin-idempotent-sub");
        SubscriptionCreateOptions first =
                SubscriptionCreateOptions.builder()
                        .topic(topic)
                        .ackDeadline(Duration.ofSeconds(20))
                        .build();
        SubscriptionCreateOptions second =
                SubscriptionCreateOptions.builder()
                        .topic(topic)
                        .ackDeadline(Duration.ofSeconds(90))
                        .build();

        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(subscription, first);
            admin.create(subscription, second);
        }

        assertThat(subscriptionExists(subscription)).isTrue();
        // ALREADY_EXISTS is success, not an update: the settings of the first creation stand.
        assertThat(describeSubscription(subscription).getAckDeadlineSeconds()).isEqualTo(20);
    }

    @Test
    void seekingBackwardsReplaysAcknowledgedMessages() throws Exception {
        // Unordered on purpose: the emulator does not support seek on ordering-enabled
        // subscriptions, so ordered seek belongs to the real-GCP suite (#82).
        TopicDestination topic = createTopic("admin-seek-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "admin-seek-sub");
        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(topic)
                            .retainAckedMessages(true)
                            .build());

            publish("admin-seek-topic", "one", "two", "three");
            // After this the backlog is empty by construction — exactly three messages were
            // published and pullAndAckUntil returns only once it has acknowledged three distinct
            // ones — so the post-seek pull can only be satisfied by a replay. Confirming the
            // emptiness with another pull would long-poll an empty subscription for about a
            // minute to re-assert something already known (issue #151).
            assertThat(pullAndAckUntil(subscription, 3, PULL_TIMEOUT))
                    .containsExactlyInAnyOrder("one", "two", "three");

            admin.seek(subscription, Instant.EPOCH);

            assertThat(pullAndAckUntil(subscription, 3, PULL_TIMEOUT))
                    .containsExactlyInAnyOrder("one", "two", "three");
        }
    }
}
