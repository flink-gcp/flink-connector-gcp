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
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.pubsub.EventLoopChannelChecks;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
                                .productionSettings()
                                .getCredentialsProvider())
                .isSameAs(credentials);
    }

    /**
     * The enumerator's close cancels a call still in flight and closes its client (ADR-0208,
     * #2263). On the JobManager Flink releases the job's class loader without waiting for the
     * enumerator, so a call left to run out its own deadline would close its client, and stop the
     * event-loop group, on a closed loader.
     *
     * <p>The emulator here accepts the connection and never answers, which holds the call in flight
     * for as long as the test needs; the accepted connection is what shows the call has started.
     */
    @Test
    @Timeout(60)
    void closeCancelsTheCallInFlightAndReleasesItsClient() throws Exception {
        try (ServerSocket silent = new ServerSocket(0)) {
            silent.setSoTimeout(10_000);
            PubSubSubscriptionAdmin admin =
                    new PubSubSubscriptionAdmin(
                            EmulatorEndpoint.parse(
                                    "127.0.0.1:" + silent.getLocalPort(), "emulatorEndpoint"));
            CompletableFuture<SubscriptionInfo> describe = new CompletableFuture<>();
            Thread worker =
                    new Thread(
                            () -> {
                                try {
                                    describe.complete(admin.describe(SUBSCRIPTION));
                                } catch (Throwable t) {
                                    describe.completeExceptionally(t);
                                }
                            },
                            "describe");
            worker.setDaemon(true);
            worker.start();
            try {
                try (Socket accepted = silent.accept()) {
                    assertThat(describe).as("the call is waiting on the silent server").isNotDone();
                    // The call's channel connects on the connector's group, not gRPC's.
                    assertThat(GrpcEventLoopThreads.alive())
                            .as("a %s* thread serves the call", GrpcEventLoopThreads.PREFIX)
                            .isTrue();

                    long started = System.nanoTime();
                    admin.close();

                    assertThatThrownBy(() -> describe.get(10, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .cause()
                            .isInstanceOf(IOException.class)
                            .hasMessageContaining("cancelled; the source is shutting down");
                    // The cancellation is what ends it. Without one the call would still end, but
                    // only when the client's close gave up waiting for it and forced the channel
                    // down, five seconds in (GrpcEventLoops' bounded close), or at
                    // GetSubscription's own 60 s deadline.
                    assertThat(Duration.ofNanos(System.nanoTime() - started))
                            .isLessThan(Duration.ofSeconds(4));
                }
                worker.join(10_000);
                assertThat(worker.isAlive()).as("the describing thread has returned").isFalse();
                assertThat(GrpcEventLoops.isRunning())
                        .as("the client released its lease")
                        .isFalse();

                // One admin belongs to one enumerator, so a closed one refuses what comes after.
                assertThatThrownBy(() -> admin.describe(SUBSCRIPTION))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("was closed");
                assertThat(GrpcEventLoops.isRunning()).isFalse();
            } finally {
                // Whatever failed above, the call and its lease must not outlive this test into
                // the next class in the fork.
                admin.close();
                worker.interrupt();
            }
        }
    }

    @Test
    void aClientForEachCallRunsOnTheEventLoopGroup() throws Exception {
        // Both arms. No credentials for production: its channels connect lazily, so nothing here
        // talks to the service; the emulator arm's endpoint is never dialled either.
        for (EmulatorEndpoint endpoint :
                new EmulatorEndpoint[] {
                    null, EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint")
                }) {
            GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client =
                    new PubSubSubscriptionAdmin(endpoint, NoCredentialsProvider.create())
                            .newClient();
            try {
                assertThat(GrpcEventLoops.isRunning()).as("the client holds a lease").isTrue();
                TransportChannelProvider provider =
                        client.client().getSettings().getTransportChannelProvider();
                if (endpoint == null) {
                    EventLoopChannelChecks.assertBuildsChannelsOnTheGroup(provider);
                } else {
                    assertThat(provider.shouldAutoClose())
                            .as("the emulator channel is the holder's, not the client's")
                            .isFalse();
                }
            } finally {
                client.close();
            }
            assertThat(GrpcEventLoops.isRunning()).as("closing released the lease").isFalse();
        }
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
