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

package io.github.flink.gcp.connector.pubsub.source.subscriptions;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link PubSubSubscriptionAdmin}'s option-to-protobuf translation and the failure mappings
 * of the two calls {@link PubSubSubscriptionAdmin#create} makes.
 */
class PubSubSubscriptionAdminTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("project", "orders");
    private static final TopicDestination TOPIC = TopicDestination.of("other-project", "topic");
    private static final TopicDestination DEAD_LETTER =
            TopicDestination.of("project", "dead-letter");

    @Test
    void configuredCredentialsReachTheAdminSettings() throws Exception {
        NoCredentialsProvider credentials = NoCredentialsProvider.create();

        assertThat(
                        new PubSubSubscriptionAdmin(null, credentials)
                                .clientSettings()
                                .getCredentialsProvider())
                .isSameAs(credentials);
    }

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

    /** The create call that reports the subscription already exists, so the read-back runs. */
    private static PubSubSubscriptionAdmin.SubscriptionCreator loser() {
        return requested -> {
            throw new AlreadyExistsException(
                    new IllegalStateException("exists"),
                    GrpcStatusCode.of(Status.Code.ALREADY_EXISTS),
                    false);
        };
    }

    private static <T extends RuntimeException> PubSubSubscriptionAdmin.SubscriptionLookup throwing(
            T failure) {
        return path -> {
            throw failure;
        };
    }

    @Test
    void winningTheCreateReturnsWhatTheServiceCreated() throws Exception {
        List<Subscription> requested = new ArrayList<>();

        SubscriptionInfo info =
                PubSubSubscriptionAdmin.createWith(
                        subscription -> {
                            requested.add(subscription);
                            return subscription;
                        },
                        throwing(new IllegalStateException("the read-back must not run")),
                        SUBSCRIPTION,
                        SubscriptionCreateOptions.builder()
                                .topic(TOPIC)
                                .enableMessageOrdering(true)
                                .build());

        assertThat(requested)
                .singleElement()
                .extracting(Subscription::getName)
                .isEqualTo("projects/project/subscriptions/orders");
        assertThat(info.isMessageOrderingEnabled()).isTrue();
    }

    @Test
    void losingTheCreateReturnsTheSettingsTheWinnerCreatedTheSubscriptionWith() throws Exception {
        List<String> lookedUp = new ArrayList<>();

        SubscriptionInfo info =
                PubSubSubscriptionAdmin.createWith(
                        loser(),
                        path -> {
                            lookedUp.add(path);
                            // The winner's settings, not the ones this call asked for.
                            return Subscription.newBuilder()
                                    .setName(path)
                                    .setEnableMessageOrdering(true)
                                    .build();
                        },
                        SUBSCRIPTION,
                        SubscriptionCreateOptions.builder().topic(TOPIC).build());

        assertThat(lookedUp).containsExactly("projects/project/subscriptions/orders");
        assertThat(info.isMessageOrderingEnabled()).isTrue();
    }

    @Test
    void aSubscriptionDeletedBetweenTheCreateAndTheReadBackFailsAsIoException() {
        NotFoundException gone =
                new NotFoundException(
                        new IllegalStateException("gone"),
                        GrpcStatusCode.of(Status.Code.NOT_FOUND),
                        false);

        assertThatThrownBy(
                        () ->
                                PubSubSubscriptionAdmin.createWith(
                                        loser(),
                                        throwing(gone),
                                        SUBSCRIPTION,
                                        SubscriptionCreateOptions.builder().topic(TOPIC).build()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("project/orders")
                .hasMessageContaining("was gone when its settings were read back")
                .hasCause(gone);
    }

    @Test
    void aReadBackForbiddenByIamNamesThePermissionItNeeds() {
        PermissionDeniedException denied =
                new PermissionDeniedException(
                        new IllegalStateException("denied"),
                        GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                        false);

        assertThatThrownBy(
                        () ->
                                PubSubSubscriptionAdmin.createWith(
                                        loser(),
                                        throwing(denied),
                                        SUBSCRIPTION,
                                        SubscriptionCreateOptions.builder().topic(TOPIC).build()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("pubsub.subscriptions.get")
                .hasCause(denied);
    }

    @Test
    void aMissingSubscriptionStaysAnAnswerForDescribeRatherThanAnError() {
        NotFoundException absent =
                new NotFoundException(
                        new IllegalStateException("absent"),
                        GrpcStatusCode.of(Status.Code.NOT_FOUND),
                        false);

        // describe() turns this into null. The exemption lives in describeWith rather than in
        // describe, so this is what keeps a missing subscription from failing the startup check.
        assertThatThrownBy(
                        () -> PubSubSubscriptionAdmin.describeWith(throwing(absent), SUBSCRIPTION))
                .isSameAs(absent);
    }
}
