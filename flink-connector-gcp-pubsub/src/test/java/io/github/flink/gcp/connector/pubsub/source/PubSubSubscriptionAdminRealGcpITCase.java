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
import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.PubSubSubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The production {@link PubSubSubscriptionAdmin} against real Cloud Pub/Sub, covering what the
 * emulator stores but does not honour: the create-option knobs actually persisting on the service,
 * and seek on an ordering-enabled subscription, which the emulator refuses outright (its seek
 * support is unordered-only, measured in issue #81's PR).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubSubscriptionAdminRealGcpITCase extends AbstractPubSubRealGcpITCase {

    /**
     * Every settings knob {@code SubscriptionCreateOptions} carries survives the round trip to the
     * service. Values sit at the service-side minimums so the test never bumps into a limit:
     * retention floors at 10 minutes, the expiration TTL at 1 day.
     */
    @Test
    void createPersistsEverySettingsKnobOnTheService() throws Exception {
        TopicDestination topic = createTopic("admin-settings");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, uniqueName("admin-settings"));

        try (SubscriptionAdmin admin = new PubSubSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(topic)
                            .ackDeadline(Duration.ofSeconds(30))
                            .messageRetention(Duration.ofMinutes(10))
                            .retainAckedMessages(true)
                            .expirationTtl(Duration.ofDays(1))
                            .filter("attributes.route = \"it\"")
                            .build());
        }
        trackSubscription(subscription);

        Subscription created = describeSubscription(subscription);
        assertThat(created.getAckDeadlineSeconds()).isEqualTo(30);
        assertThat(created.getMessageRetentionDuration().getSeconds())
                .isEqualTo(Duration.ofMinutes(10).getSeconds());
        assertThat(created.getRetainAckedMessages()).isTrue();
        assertThat(created.getExpirationPolicy().getTtl().getSeconds())
                .isEqualTo(Duration.ofDays(1).getSeconds());
        assertThat(created.getFilter()).isEqualTo("attributes.route = \"it\"");
    }

    /**
     * {@code neverExpire()} translates to an expiration policy with no TTL — the shape Pub/Sub
     * documents for "never expires" — and the service must keep it that way rather than fall back
     * to the 31-day default.
     */
    @Test
    void neverExpirePersistsAsAnExpirationPolicyWithoutTtl() throws Exception {
        TopicDestination topic = createTopic("admin-never-expire");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, uniqueName("admin-never-expire"));

        try (SubscriptionAdmin admin = new PubSubSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder().topic(topic).neverExpire().build());
        }
        trackSubscription(subscription);

        Subscription created = describeSubscription(subscription);
        assertThat(created.hasExpirationPolicy()).isTrue();
        assertThat(created.getExpirationPolicy().hasTtl()).isFalse();
    }

    /**
     * Seek-to-timestamp on an ordering-enabled subscription — the {@code
     * StartPosition.fromTimestamp} path under {@code orderingMode(PER_KEY)}, with no coverage
     * anywhere else: the emulator only seeks unordered subscriptions. The target timestamp comes
     * from the service's own publish times, not this machine's clock — backed off by one second,
     * because the boundary is exclusive: a message published exactly at the seek time counts as
     * "before" it and stays acknowledged (measured — a batch publish gives all its messages one
     * publish time, and seeking to precisely that time replayed none of them).
     */
    @Test
    void seekToTimestampReplaysAcknowledgedMessagesOnAnOrderedSubscription() throws Exception {
        TopicDestination topic = createTopic("admin-ordered-seek");
        SubscriptionDestination subscription =
                createSubscription(
                        topic,
                        "admin-ordered-seek",
                        builder ->
                                builder.setEnableMessageOrdering(true)
                                        .setRetainAckedMessages(true));
        publishOrdered(topic, "seek-key", "first-0", "first-1", "first-2");
        List<PubsubMessage> firstBatch = pullMessagesUntil(subscription, 3, Duration.ofSeconds(60));
        assertThat(firstBatch).hasSize(3);
        publishOrdered(topic, "seek-key", "second-0", "second-1");
        assertThat(pullAndAckUntil(subscription, 2, Duration.ofSeconds(60))).hasSize(2);

        Instant earliestPublish =
                firstBatch.stream()
                        .map(
                                message ->
                                        Instant.ofEpochSecond(
                                                message.getPublishTime().getSeconds(),
                                                message.getPublishTime().getNanos()))
                        .min(Instant::compareTo)
                        .orElseThrow();
        try (SubscriptionAdmin admin = new PubSubSubscriptionAdmin()) {
            admin.seek(subscription, earliestPublish.minusSeconds(1));
        }

        assertThat(pullAndAckUntil(subscription, 5, Duration.ofSeconds(60)))
                .containsExactlyInAnyOrder("first-0", "first-1", "first-2", "second-0", "second-1");
    }
}
