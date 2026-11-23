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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ExpirationPolicy;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.Subscription;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Default {@link SubscriptionAdmin} backed by the Pub/Sub {@link SubscriptionAdminClient}.
 *
 * <p>Every call opens its own client and closes it — together with its gRPC channel — when the call
 * returns. That is not only about cost: the enumerator's {@code close()} runs on the scheduler
 * thread while a startup check may still be running on a worker thread, and a client owned across
 * calls would be torn out from under it. The check is a handful of calls at job start, so a
 * per-call client is cheap enough to buy that.
 *
 * <p>The client's channels run on the connector's gRPC event-loop group ({@link GrpcEventLoops},
 * ADR-0208), and closing the client waits for them before it releases its lease. That close has to
 * come before Flink releases the job's class loader, which on the JobManager it does without
 * waiting for the enumerator to close (Flink 2.2.1, #2237); a client still open by then stops the
 * group on a closed loader. So {@link #close()} cancels the call in flight, retries included,
 * through its future, and closes its client: the waiting call then returns at once rather than when
 * the service answers. With an emulator endpoint the client is handed a plaintext channel opened
 * for it, with no credentials, and closed after it.
 *
 * <p>One admin belongs to one enumerator, which is what makes the one-way {@code closed} flag
 * correct. The monitor is this object's own and is held while a client is opened and a call
 * started, never while a call runs.
 */
@Internal
public class PubSubSubscriptionAdmin implements SubscriptionAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriptionAdmin.class);

    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final CredentialsProvider credentialsOverride;

    /** The client of the call in flight, or {@code null} between calls. */
    @GuardedBy("this")
    @Nullable
    private GrpcEventLoops.LeasedClient<SubscriptionAdminClient> inFlight;

    /** The call {@code inFlight} is waiting on, which {@link #close()} cancels. */
    @GuardedBy("this")
    @Nullable
    private ApiFuture<?> inFlightCall;

    @GuardedBy("this")
    private boolean closed;

    /** Creates an admin using application-default credentials. */
    public PubSubSubscriptionAdmin() {
        this(null);
    }

    /**
     * Creates the admin.
     *
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Pub/Sub with application-default credentials
     */
    public PubSubSubscriptionAdmin(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(emulatorEndpoint, null);
    }

    /**
     * Creates an admin whose production clients authenticate with the given credentials instead of
     * application-default ones. The source's configured service-account key and the real-GCP
     * permission-denied tests share this injection point.
     *
     * @param emulatorEndpoint see {@link #PubSubSubscriptionAdmin(EmulatorEndpoint)}; the override
     *     is ignored against an emulator, whose channel carries no credentials at all
     * @param credentialsOverride the credentials to use, or {@code null} for application-default
     */
    public PubSubSubscriptionAdmin(
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
    }

    /**
     * One {@code GetSubscription} call, as a functional value so a test can drive the failure
     * mappings below without a client.
     *
     * <p>{@link SubscriptionAdminClient} is a generated final-in-practice type — the seam the rest
     * of this module already uses for {@code Publisher} and {@code Subscriber} (ADR-0007,
     * ADR-0012), for the same reason.
     */
    @FunctionalInterface
    interface SubscriptionLookup {
        Subscription get(String subscriptionPath) throws IOException;
    }

    /** One {@code CreateSubscription} call; see {@link SubscriptionLookup}. */
    @FunctionalInterface
    interface SubscriptionCreator {
        Subscription create(Subscription subscription) throws IOException;
    }

    @Override
    @Nullable
    public SubscriptionInfo describe(SubscriptionDestination subscription) throws IOException {
        GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client = open();
        try {
            return describeWith(lookup(client), subscription);
        } catch (NotFoundException e) {
            return null;
        } finally {
            release(client);
        }
    }

    /**
     * Reads a subscription's settings, wrapping every failure but {@link NotFoundException} — which
     * {@link #describe} treats as an answer rather than an error. Its other caller, {@link
     * #createWith}, wraps it.
     */
    @VisibleForTesting
    static SubscriptionInfo describeWith(
            SubscriptionLookup lookup, SubscriptionDestination subscription) throws IOException {
        try {
            return toInfo(lookup.get(subscription.toSubscriptionPath()));
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
        GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client = open();
        try {
            return createWith(
                    toCreate ->
                            call(
                                    client,
                                    admin ->
                                            admin.createSubscriptionCallable()
                                                    .futureCall(toCreate)),
                    lookup(client),
                    subscription,
                    options);
        } finally {
            release(client);
        }
    }

    /**
     * The body of {@link #create}, over the two calls it makes rather than over a client, so that
     * the branch below is reachable from a test: it needs a deletion to land in the window between
     * the two calls, and that window is inside this method, so no test driving a real client — an
     * emulator's included — can force it rather than race for it.
     *
     * <p>A {@link NotFoundException} from the read-back means the winner deleted the subscription
     * between the two calls. That is an answer for {@link #describe} and a failed creation here, so
     * it cannot travel as the raw vendor type — and the wrap has to be <em>inside</em> the {@code
     * catch} rather than beside it, because a sibling {@code catch} does not see what another one
     * throws. Unwrapped it would leave this method violating its own {@code throws IOException}
     * contract and replace the startup check's named message with a bare gax stack trace.
     */
    @VisibleForTesting
    static SubscriptionInfo createWith(
            SubscriptionCreator creator,
            SubscriptionLookup lookup,
            SubscriptionDestination subscription,
            SubscriptionCreateOptions options)
            throws IOException {
        try {
            SubscriptionInfo created =
                    toInfo(creator.create(toSubscription(subscription, options)));
            LOG.info("Created Pub/Sub subscription {} with options {}", subscription, options);
            return created;
        } catch (AlreadyExistsException e) {
            LOG.info(
                    "Pub/Sub subscription {} already exists, not creating it; its existing settings"
                            + " apply.",
                    subscription);
            // Whoever won the race decided the settings, so read them back rather than assume the
            // requested options took effect.
            try {
                return describeWith(lookup, subscription);
            } catch (NotFoundException gone) {
                throw new IOException(
                        "Pub/Sub subscription "
                                + subscription
                                + " already existed, but was gone when its settings were read back:"
                                + " whoever created it deleted it again between the two calls."
                                + " Retry, or create the subscription before starting the job.",
                        gone);
            }
        } catch (RuntimeException e) {
            throw new IOException("Failed to create Pub/Sub subscription " + subscription, e);
        }
    }

    @Override
    public void seek(SubscriptionDestination subscription, Instant timestamp) throws IOException {
        SeekRequest request =
                SeekRequest.newBuilder()
                        .setSubscription(subscription.toSubscriptionPath())
                        .setTime(toTimestamp(timestamp))
                        .build();
        GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client = open();
        try {
            call(client, admin -> admin.seekCallable().futureCall(request));
            LOG.info("Sought Pub/Sub subscription {} to {}", subscription, timestamp);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to seek Pub/Sub subscription "
                            + subscription
                            + " to "
                            + timestamp
                            + ". Seeking needs the pubsub.subscriptions.consume permission"
                            + " (roles/pubsub.subscriber).",
                    e);
        } finally {
            release(client);
        }
    }

    /**
     * Cancels the call in flight and closes its client, and refuses every later call. The waiting
     * call then fails at once, so the worker thread running it does not outlive the source.
     */
    @Override
    public void close() {
        GrpcEventLoops.LeasedClient<SubscriptionAdminClient> open;
        ApiFuture<?> call;
        synchronized (this) {
            closed = true;
            open = inFlight;
            call = inFlightCall;
            inFlight = null;
            inFlightCall = null;
        }
        if (call != null) {
            // Cancels the call and any retry gax has scheduled for it, so the client below has
            // nothing in flight to wait for.
            call.cancel(true);
        }
        if (open != null) {
            closeQuietly(open);
        }
    }

    /** Opens a client for one admin call and records it as in flight. */
    private GrpcEventLoops.LeasedClient<SubscriptionAdminClient> open() throws IOException {
        synchronized (this) {
            if (closed) {
                throw closedFailure();
            }
            inFlight = newClient();
            return inFlight;
        }
    }

    /** Starts one call on the client, recording it so {@link #close()} can cancel it, and waits. */
    private <T> T call(
            GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client,
            Function<SubscriptionAdminClient, ApiFuture<T>> start)
            throws IOException {
        ApiFuture<T> call;
        synchronized (this) {
            if (inFlight != client) {
                // close() took the client away between two calls on it.
                throw closedFailure();
            }
            call = start.apply(client.client());
            inFlightCall = call;
        }
        try {
            return await(call);
        } finally {
            synchronized (this) {
                if (inFlightCall == call) {
                    inFlightCall = null;
                }
            }
        }
    }

    private SubscriptionLookup lookup(GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client) {
        return path ->
                call(
                        client,
                        admin ->
                                admin.getSubscriptionCallable()
                                        .futureCall(
                                                GetSubscriptionRequest.newBuilder()
                                                        .setSubscription(path)
                                                        .build()));
    }

    /**
     * Closes the call's client, then forgets it. It stays findable while it closes, so a close()
     * racing this finds it and waits for the same close rather than returning first.
     */
    private void release(GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client) {
        closeQuietly(client);
        synchronized (this) {
            if (inFlight == client) {
                inFlight = null;
                inFlightCall = null;
            }
        }
    }

    private static IOException closedFailure() {
        return new IOException(
                "The Pub/Sub subscription admin was closed; the source is shutting down.");
    }

    /**
     * Waits for a call, interruptibly. The SDK's own synchronous wait is uninterruptible, and a
     * retry gax had scheduled can be dropped by the client's shutdown without completing the call,
     * so a thread waiting that way could wait forever; Flink interrupts the coordinator's worker
     * when it closes the source, and this wait answers that by cancelling the call. A failure the
     * service answered with is rethrown as the SDK's synchronous call would throw it, so the
     * callers' mappings above see the same types.
     */
    private static <T> T await(ApiFuture<T> call) throws IOException {
        try {
            return call.get();
        } catch (InterruptedException e) {
            call.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(
                    "Interrupted during a Pub/Sub subscription admin call; the source is shutting"
                            + " down.");
        } catch (CancellationException e) {
            // close() cancelled it: the call did not fail, the source is going away. An
            // IOException rather than the RuntimeException get() threw, so the callers' mappings
            // above do not report it as a failed call with a permission hint.
            throw new IOException(
                    "A Pub/Sub subscription admin call was cancelled; the source is shutting"
                            + " down.",
                    e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // The failure may have been raised on a gax thread; this records where it was
                // waited for, as the SDK's own synchronous call does.
                cause.addSuppressed(new Exception("The call was awaited here"));
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        }
    }

    /**
     * Closes the per-call client, logging rather than propagating a failure.
     *
     * <p>Not try-with-resources, which would close before the catch clauses run and so report a
     * channel that would not shut down cleanly as a failed call. That matters most for {@link
     * #seek}: the restarted job re-seeks a subscription whose seek is wrongly reported as failed,
     * and under a start position resolved against the clock that discards more messages each time.
     */
    private static void closeQuietly(GrpcEventLoops.LeasedClient<SubscriptionAdminClient> client) {
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

    /** Opens a client for one call, on the connector's event-loop group; for tests too. */
    @VisibleForTesting
    GrpcEventLoops.LeasedClient<SubscriptionAdminClient> newClient() throws IOException {
        try {
            if (emulatorEndpoint == null) {
                SubscriptionAdminSettings.Builder settings = productionSettings();
                return GrpcEventLoops.open(
                        lease -> SubscriptionAdminClient.create(lease.applyTo(settings).build()));
            }
            return EmulatorChannels.openLeasedClient(
                    SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder(),
                    emulatorEndpoint,
                    channel ->
                            SubscriptionAdminClient.create(
                                    SubscriptionAdminSettings.newBuilder()
                                            .setCredentialsProvider(NoCredentialsProvider.create())
                                            .setTransportChannelProvider(channel)
                                            .build()));
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Pub/Sub subscription admin client", e);
        }
    }

    /** Returns the production client's settings, before the lease is applied; for tests too. */
    @VisibleForTesting
    SubscriptionAdminSettings.Builder productionSettings() {
        SubscriptionAdminSettings.Builder settings = SubscriptionAdminSettings.newBuilder();
        if (credentialsOverride != null) {
            settings.setCredentialsProvider(credentialsOverride);
        }
        return settings;
    }
}
