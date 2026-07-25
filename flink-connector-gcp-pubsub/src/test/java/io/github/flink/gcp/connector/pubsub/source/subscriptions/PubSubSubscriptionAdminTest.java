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

package io.github.flink.gcp.connector.pubsub.source.subscriptions;

import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the option-to-protobuf translation in {@link PubSubSubscriptionAdmin}. */
class PubSubSubscriptionAdminTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("project", "orders");
    private static final TopicDestination TOPIC = TopicDestination.of("other-project", "topic");
    private static final TopicDestination DEAD_LETTER =
            TopicDestination.of("project", "dead-letter");

    @Test
    void unsetKnobsLeaveTheirProtoFieldsAlone() {
        Subscription subscription =
                PubSubSubscriptionAdmin.toSubscription(
                        SUBSCRIPTION, SubscriptionCreateOptions.builder().topic(TOPIC).build());

        assertThat(subscription.getName()).isEqualTo("projects/project/subscriptions/orders");
        assertThat(subscription.getTopic()).isEqualTo("projects/other-project/topics/topic");
        assertThat(subscription.getAckDeadlineSeconds()).isZero();
        assertThat(subscription.getEnableMessageOrdering()).isFalse();
        assertThat(subscription.hasMessageRetentionDuration()).isFalse();
        assertThat(subscription.getRetainAckedMessages()).isFalse();
        assertThat(subscription.hasExpirationPolicy()).isFalse();
        assertThat(subscription.hasDeadLetterPolicy()).isFalse();
        assertThat(subscription.getFilter()).isEmpty();
        // Never offered on the options, so it can never be set and then rejected by the check.
        assertThat(subscription.getEnableExactlyOnceDelivery()).isFalse();
    }

    @Test
    void translatesEveryConfiguredKnob() {
        Subscription subscription =
                PubSubSubscriptionAdmin.toSubscription(
                        SUBSCRIPTION,
                        SubscriptionCreateOptions.builder()
                                .topic(TOPIC)
                                .ackDeadline(Duration.ofSeconds(45))
                                .enableMessageOrdering(true)
                                .messageRetention(Duration.ofHours(3))
                                .retainAckedMessages(true)
                                .expirationTtl(Duration.ofDays(2))
                                .deadLetterPolicy(DEAD_LETTER, 12)
                                .filter("attributes.kind = \"order\"")
                                .build());

        assertThat(subscription.getAckDeadlineSeconds()).isEqualTo(45);
        assertThat(subscription.getEnableMessageOrdering()).isTrue();
        assertThat(subscription.getMessageRetentionDuration().getSeconds()).isEqualTo(3 * 3600);
        assertThat(subscription.getRetainAckedMessages()).isTrue();
        assertThat(subscription.getExpirationPolicy().getTtl().getSeconds())
                .isEqualTo(2 * 24 * 3600);
        assertThat(subscription.getDeadLetterPolicy().getDeadLetterTopic())
                .isEqualTo("projects/project/topics/dead-letter");
        assertThat(subscription.getDeadLetterPolicy().getMaxDeliveryAttempts()).isEqualTo(12);
        assertThat(subscription.getFilter()).isEqualTo("attributes.kind = \"order\"");
    }

    @Test
    void neverExpireBecomesAnExpirationPolicyWithNoTtl() {
        Subscription subscription =
                PubSubSubscriptionAdmin.toSubscription(
                        SUBSCRIPTION,
                        SubscriptionCreateOptions.builder().topic(TOPIC).neverExpire().build());

        // An expiration policy present but empty is how Pub/Sub spells "never expires"; leaving it
        // unset would take the 31-day default instead.
        assertThat(subscription.hasExpirationPolicy()).isTrue();
        assertThat(subscription.getExpirationPolicy().hasTtl()).isFalse();
    }

    @Test
    void readsBackOnlyTheSettingsTheStartupCheckActsOn() {
        SubscriptionInfo info =
                PubSubSubscriptionAdmin.toInfo(
                        Subscription.newBuilder()
                                .setName("projects/project/subscriptions/orders")
                                .setEnableMessageOrdering(true)
                                .setEnableExactlyOnceDelivery(true)
                                .setRetainAckedMessages(true)
                                .setDeadLetterPolicy(
                                        com.google.pubsub.v1.DeadLetterPolicy.newBuilder()
                                                .setDeadLetterTopic(
                                                        "projects/project/topics/dead-letter")
                                                .setMaxDeliveryAttempts(5))
                                .setTopicMessageRetentionDuration(
                                        com.google.protobuf.Duration.newBuilder().setSeconds(60))
                                .build());

        assertThat(info.isMessageOrderingEnabled()).isTrue();
        assertThat(info.isExactlyOnceDeliveryEnabled()).isTrue();
        assertThat(info.isRetainAckedMessages()).isTrue();
        assertThat(info.isDeadLetterPolicyConfigured()).isTrue();
        assertThat(info.isTopicMessageRetentionConfigured()).isTrue();
    }

    @Test
    void aPlainSubscriptionReadsBackAsAllDefaults() {
        SubscriptionInfo info = PubSubSubscriptionAdmin.toInfo(Subscription.getDefaultInstance());

        assertThat(info).isEqualTo(SubscriptionInfo.builder().build());
    }
}
