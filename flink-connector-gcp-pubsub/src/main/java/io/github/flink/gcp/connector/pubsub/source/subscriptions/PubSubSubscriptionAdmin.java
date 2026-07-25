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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ExpirationPolicy;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Default {@link SubscriptionAdmin} backed by the Pub/Sub {@link SubscriptionAdminClient}.
 *
 * <p>Every call opens its own client and closes it — together with its gRPC channel — when the call
 * returns, so {@link #close()} has nothing to release. That is not only about cost: the
 * enumerator's {@code close()} runs on the scheduler thread while a startup check may still be
 * running on a worker thread, and a client owned across calls would be torn out from under it. The
 * check is a handful of calls at job start, so a per-call client is cheap enough to buy that. With
 * an emulator endpoint the clients connect to it over a plaintext channel with no credentials.
 */
@Internal
public class PubSubSubscriptionAdmin implements SubscriptionAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriptionAdmin.class);

    @Nullable private final String emulatorEndpoint;

    /** Creates an admin using application-default credentials. */
    public PubSubSubscriptionAdmin() {
        this(null);
    }

    /**
     * Creates the admin.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port} (plaintext, no
     *     credentials), or {@code null} for production Pub/Sub with application-default credentials
     */
    public PubSubSubscriptionAdmin(@Nullable String emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    @Nullable
    public SubscriptionInfo describe(SubscriptionDestination subscription) throws IOException {
        SubscriptionAdminClient client = newClient();
        try {
            return describeWith(client, subscription);
        } catch (NotFoundException e) {
            return null;
        } finally {
            closeQuietly(client);
        }
    }

    /**
     * Reads a subscription's settings through an already-open client, wrapping every failure but
     * {@link NotFoundException} — which only {@link #describe} treats as an answer rather than an
     * error.
     */
    private static SubscriptionInfo describeWith(
            SubscriptionAdminClient client, SubscriptionDestination subscription)
            throws IOException {
        try {
            return toInfo(client.getSubscription(subscription.toSubscriptionPath()));
        } catch (NotFoundException e) {
            throw e;
        } catch (PermissionDeniedException e) {
            throw new IOException(
                    "Not allowed to read the settings of Pub/Sub subscription "
                            + subscription
                            + ". The job manager's credentials need the"
                            + " pubsub.subscriptions.get permission (roles/pubsub.viewer): the"
                            + " source verifies every subscription before it starts consuming.",
                    e);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to read the settings of Pub/Sub subscription " + subscription, e);
        }
    }

    @Override
    public SubscriptionInfo create(
            SubscriptionDestination subscription, SubscriptionCreateOptions options)
            throws IOException {
        SubscriptionAdminClient client = newClient();
        try {
            SubscriptionInfo created =
                    toInfo(client.createSubscription(toSubscription(subscription, options)));
            LOG.info("Created Pub/Sub subscription {} with options {}", subscription, options);
            return created;
        } catch (AlreadyExistsException e) {
            LOG.info(
                    "Pub/Sub subscription {} already exists, not creating it; its existing settings"
                            + " apply.",
                    subscription);
            // Whoever won the race decided the settings, so read them back rather than assume the
            // requested options took effect. Through the helper, because a failure here is in a
            // sibling catch block and so would escape the wrap below unwrapped — plausible, since
            // creating and describing are different permissions.
            return describeWith(client, subscription);
        } catch (RuntimeException e) {
            throw new IOException("Failed to create Pub/Sub subscription " + subscription, e);
        } finally {
            closeQuietly(client);
        }
    }

    @Override
    public void seek(SubscriptionDestination subscription, Instant timestamp) throws IOException {
        SeekRequest request =
                SeekRequest.newBuilder()
                        .setSubscription(subscription.toSubscriptionPath())
                        .setTime(toTimestamp(timestamp))
                        .build();
        SubscriptionAdminClient client = newClient();
        try {
            client.seek(request);
            LOG.info("Sought Pub/Sub subscription {} to {}", subscription, timestamp);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to seek Pub/Sub subscription "
                            + subscription
                            + " to "
                            + timestamp
                            + ". Seeking needs the pubsub.subscriptions.update permission"
                            + " (roles/pubsub.editor).",
                    e);
        } finally {
            closeQuietly(client);
        }
    }

    @Override
    public void close() {
        // Clients are short-lived within each call; there is nothing to release here.
    }

    /**
     * Shuts the per-call client down, logging rather than propagating a failure.
     *
     * <p>Not try-with-resources, which would close before the catch clauses run and so report a
     * channel that would not shut down cleanly as a failed call. That matters most for {@link
     * #seek}: the restarted job re-seeks a subscription whose seek is wrongly reported as failed,
     * and under a start position resolved against the clock that discards more messages each time.
     */
    private static void closeQuietly(SubscriptionAdminClient client) {
        try {
            client.close();
        } catch (RuntimeException e) {
            LOG.warn("Failed to close a Pub/Sub subscription admin client.", e);
        }
    }

    /** Translates the options into the subscription to create. */
    @VisibleForTesting
    static Subscription toSubscription(
            SubscriptionDestination subscription, SubscriptionCreateOptions options) {
        Subscription.Builder builder =
                Subscription.newBuilder()
                        .setName(subscription.toSubscriptionPath())
                        .setTopic(options.getTopic().toTopicPath());
        Duration ackDeadline = options.getAckDeadline();
        if (ackDeadline != null) {
            builder.setAckDeadlineSeconds((int) ackDeadline.getSeconds());
        }
        if (options.isEnableMessageOrdering()) {
            builder.setEnableMessageOrdering(true);
        }
        Duration messageRetention = options.getMessageRetention();
        if (messageRetention != null) {
            builder.setMessageRetentionDuration(toProtoDuration(messageRetention));
        }
        if (options.isRetainAckedMessages()) {
            builder.setRetainAckedMessages(true);
        }
        Duration expirationTtl = options.getExpirationTtl();
        if (expirationTtl != null) {
            builder.setExpirationPolicy(
                    ExpirationPolicy.newBuilder().setTtl(toProtoDuration(expirationTtl)));
        } else if (options.isNeverExpire()) {
            // An expiration policy with no TTL is how Pub/Sub spells "never expires"; leaving the
            // policy unset would instead take the 31-day default.
            builder.setExpirationPolicy(ExpirationPolicy.getDefaultInstance());
        }
        if (options.getDeadLetterTopic() != null) {
            builder.setDeadLetterPolicy(
                    DeadLetterPolicy.newBuilder()
                            .setDeadLetterTopic(options.getDeadLetterTopic().toTopicPath())
                            .setMaxDeliveryAttempts(options.getDeadLetterMaxDeliveryAttempts()));
        }
        if (options.getFilter() != null) {
            builder.setFilter(options.getFilter());
        }
        return builder.build();
    }

    /** Extracts the settings the enumerator's startup check acts on. */
    @VisibleForTesting
    static SubscriptionInfo toInfo(Subscription subscription) {
        return SubscriptionInfo.builder()
                .messageOrderingEnabled(subscription.getEnableMessageOrdering())
                .exactlyOnceDeliveryEnabled(subscription.getEnableExactlyOnceDelivery())
                .retainAckedMessages(subscription.getRetainAckedMessages())
                .deadLetterPolicyConfigured(subscription.hasDeadLetterPolicy())
                // Output-only on GetSubscription: set when the subscription's topic retains
                // messages, which lets a backwards seek reach past the subscription's own state.
                .topicMessageRetentionConfigured(subscription.hasTopicMessageRetentionDuration())
                .build();
    }

    private static com.google.protobuf.Duration toProtoDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private SubscriptionAdminClient newClient() throws IOException {
        try {
            if (emulatorEndpoint == null) {
                return SubscriptionAdminClient.create();
            }
            // The instantiating provider is auto-closed by the client, so the try-with-resources in
            // each call closes the emulator channel together with the client.
            return SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(NoCredentialsProvider.create())
                            .setTransportChannelProvider(
                                    SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
                                            .setEndpoint(emulatorEndpoint)
                                            .setChannelConfigurator(
                                                    ManagedChannelBuilder::usePlaintext)
                                            .build())
                            .build());
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Pub/Sub subscription admin client", e);
        }
    }
}
