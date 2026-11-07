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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.DefaultSubscriberFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builder for Pub/Sub sources, obtained from {@link PubSubSource#builder()}.
 *
 * <p>Required settings: at least one subscription and a deserialization schema.
 *
 * @param <T> type of the records produced by the source
 */
@Public
public class PubSubSourceBuilder<T> {

    /**
     * How long the restored ledger is kept when acknowledgement-deadline extension is disabled
     * ({@code maxAckExtensionPeriod} zero): the longest acknowledgement deadline Pub/Sub accepts.
     */
    private static final Duration UNEXTENDED_LEASE_TIME_TO_LIVE = Duration.ofMinutes(10);

    private static final Duration MAX_EXPRESSIBLE_IN_NANOS = Duration.ofNanos(Long.MAX_VALUE);

    private final List<SubscriptionDestination> subscriptions = new ArrayList<>();
    private final Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions =
            new LinkedHashMap<>();
    private PubSubDeserializationSchema<T> deserializationSchema;
    private OrderingMode orderingMode = OrderingMode.NONE;
    private PubSubSubscriberOptions subscriberOptions = PubSubSubscriberOptions.defaults();
    private DeserializationFailurePolicy deserializationFailurePolicy =
            DeserializationFailurePolicy.FAIL;
    private PubSubStartPosition startPosition = PubSubStartPosition.continueFromSubscription();
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    private boolean restoreDeduplication;
    @Nullable private Duration restoreDeduplicationTtl;

    PubSubSourceBuilder() {}

    /**
     * Adds a subscription to consume. Calling this several times, or combining it with {@link
     * #subscriptions(SubscriptionDestination...)}, consumes every added subscription in one source.
     *
     * <p>The subscription must already exist. Use {@link #subscription(SubscriptionDestination,
     * SubscriptionCreateOptions)} to have the source create it when it does not.
     *
     * @param subscription the subscription
     * @return this builder
     */
    public PubSubSourceBuilder<T> subscription(SubscriptionDestination subscription) {
        this.subscriptions.add(
                Preconditions.checkNotNull(subscription, "subscription must not be null"));
        return this;
    }

    /**
     * Adds a subscription to consume, creating it with the given settings if it does not exist.
     *
     * <p>Passing options is what authorises creating this subscription; a subscription added
     * without them must already exist. The options are per subscription because they carry the
     * topic binding, and two subscriptions of one topic each receive a complete copy of its stream
     * — so sharing one options object would silently duplicate every message.
     *
     * <p>An existing subscription is left exactly as it is: these settings are not applied to it.
     *
     * @param subscription the subscription
     * @param createOptions the settings to create it with if it is absent
     * @return this builder
     */
    public PubSubSourceBuilder<T> subscription(
            SubscriptionDestination subscription, SubscriptionCreateOptions createOptions) {
        Preconditions.checkNotNull(subscription, "subscription must not be null");
        Preconditions.checkNotNull(createOptions, "createOptions must not be null");
        this.subscriptions.add(subscription);
        this.createOptions.put(subscription, createOptions);
        return this;
    }

    /**
     * Adds subscriptions to consume.
     *
     * @param subscriptions the subscriptions
     * @return this builder
     */
    public PubSubSourceBuilder<T> subscriptions(SubscriptionDestination... subscriptions) {
        Preconditions.checkNotNull(subscriptions, "subscriptions must not be null");
        for (SubscriptionDestination subscription : subscriptions) {
            subscription(subscription);
        }
        return this;
    }

    /**
     * Adds subscriptions to consume.
     *
     * @param subscriptions the subscriptions
     * @return this builder
     */
    public PubSubSourceBuilder<T> subscriptions(Collection<SubscriptionDestination> subscriptions) {
        Preconditions.checkNotNull(subscriptions, "subscriptions must not be null");
        subscriptions.forEach(this::subscription);
        return this;
    }

    /**
     * Sets the record deserializer. Required; one message may produce any number of records,
     * including none, which drops the message while still acknowledging it with the checkpoint that
     * covers it.
     *
     * @param deserializer the deserialization schema
     * @return this builder
     */
    public PubSubSourceBuilder<T> deserializer(PubSubDeserializationSchema<T> deserializer) {
        this.deserializationSchema =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Sets whether the source preserves ordering-key delivery order. Defaults to {@link
     * OrderingMode#NONE}.
     *
     * <p>{@link OrderingMode#PER_KEY} assigns each subscription to exactly one reader subtask, so
     * source parallelism beyond the subscription count leaves subtasks idle. See the enum for the
     * full guarantee and its cost.
     *
     * @param orderingMode the ordering mode
     * @return this builder
     */
    public PubSubSourceBuilder<T> orderingMode(OrderingMode orderingMode) {
        this.orderingMode =
                Preconditions.checkNotNull(orderingMode, "orderingMode must not be null");
        return this;
    }

    /**
     * Sets the subscriber tuning options: SDK flow control, the streaming-pull connection count and
     * the acknowledgement-deadline extension settings, plus the source's drain size, subscriber
     * shutdown budget and first-checkpoint watchdog. Optional; every knob left unset keeps the
     * SDK's (or the source's) default.
     *
     * @param subscriberOptions the subscriber options
     * @return this builder
     */
    public PubSubSourceBuilder<T> subscriberOptions(PubSubSubscriberOptions subscriberOptions) {
        this.subscriberOptions =
                Preconditions.checkNotNull(subscriberOptions, "subscriberOptions must not be null");
        return this;
    }

    /**
     * Sets what the source does with a message the deserialization schema cannot convert. Defaults
     * to {@link DeserializationFailurePolicy#FAIL}.
     *
     * @param deserializationFailurePolicy the failure policy
     * @return this builder
     */
    public PubSubSourceBuilder<T> deserializationFailurePolicy(
            DeserializationFailurePolicy deserializationFailurePolicy) {
        this.deserializationFailurePolicy =
                Preconditions.checkNotNull(
                        deserializationFailurePolicy,
                        "deserializationFailurePolicy must not be null");
        return this;
    }

    /**
     * Sets where the source starts consuming. Defaults to {@link
     * PubSubStartPosition#continueFromSubscription()}, which starts wherever the subscriptions
     * already are.
     *
     * <p>Every other position seeks, which rewrites state shared by every consumer of the
     * subscription — including other jobs. The seek runs once, at the first start of a job, and
     * never on a restore. See {@link PubSubStartPosition} for the full semantics.
     *
     * @param startPosition where to start consuming
     * @return this builder
     */
    public PubSubSourceBuilder<T> startPosition(PubSubStartPosition startPosition) {
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        return this;
    }

    /**
     * Authenticates the source with the service-account JSON key at the given path instead of
     * application-default credentials. The file is read on the JobManager for subscription
     * administration and on each TaskManager that creates a reader, so the same path must be
     * readable by every JobManager and TaskManager that can run this source. Optional; when unset
     * the source uses application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel deliberately carries no
     * credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public PubSubSourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the source at a Pub/Sub emulator instead of the production service. Subscribers
     * connect to the given {@code host:port} over a plaintext channel with no credentials, so this
     * must only ever be used against an emulator (for example a testcontainers {@code
     * PubSubEmulatorContainer}). Optional; when unset the source connects to Pub/Sub with
     * application-default credentials.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public PubSubSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Keeps a completed checkpoint's acknowledgements across a restart. Optional; off by default.
     *
     * <p>The source acknowledges a checkpoint's messages when the checkpoint's completion
     * notification reaches the reader. Flink delivers that notification on a best-effort basis: a
     * job that is cancelled or fails between completing a checkpoint and notifying its readers
     * restores that checkpoint, while Pub/Sub, never having heard the acknowledgements, redelivers
     * its messages, which are then processed a second time. The AdaptiveScheduler, including
     * reactive mode, triggers a rescale when a checkpoint completes, so every such rescale races
     * the notification, and one lost race duplicates a whole checkpoint interval.
     *
     * <p>With this on, each checkpoint also records the ids of the messages it covers that are not
     * acknowledged yet. After a restore, a redelivery of one of those messages is acknowledged
     * instead of emitted, for {@link #restoreDeduplicationTtl(Duration)}. This removes the
     * duplicates of a whole-job restore — which every AdaptiveScheduler restart is, whether it
     * rescales up or down — and loses nothing: every suppressed message was covered by the restored
     * checkpoint, and a redelivery that arrives after the ledger expires is emitted as it would be
     * without this setting. A region failover is covered for the subtasks it restores; a redelivery
     * that Pub/Sub routes to a source subtask the failover left running is emitted again.
     *
     * <p>The cost is checkpoint state proportional to the messages emitted per checkpoint interval,
     * and a restore that waits for the ledger to be redistributed before consuming. See the Pub/Sub
     * connector documentation for the details and caveats, including a seek during the ledger's
     * lifetime.
     *
     * @param restoreDeduplication whether to suppress redeliveries of restored messages
     * @return this builder
     */
    public PubSubSourceBuilder<T> restoreDeduplication(boolean restoreDeduplication) {
        this.restoreDeduplication = restoreDeduplication;
        return this;
    }

    /**
     * Sets how long a restored deduplication ledger is kept once the reader starts consuming.
     * Optional; requires {@link #restoreDeduplication(boolean)}. When unset, the ledger is kept for
     * the subscriber's {@link PubSubSubscriberOptions.Builder#maxAckExtensionPeriod(Duration)}, the
     * longest the previous execution could have kept a message leased, capped at {@code
     * Duration.ofNanos(Long.MAX_VALUE)}, or for ten minutes, the longest acknowledgement deadline,
     * when that extension is disabled.
     *
     * <p>A redelivery that arrives after the ledger expires is emitted, so a longer lifetime
     * suppresses late redeliveries, for example behind a backlog, at the cost of keeping the ids in
     * memory for longer.
     *
     * @param restoreDeduplicationTtl how long to keep the restored ledger; must be positive
     * @return this builder
     */
    public PubSubSourceBuilder<T> restoreDeduplicationTtl(Duration restoreDeduplicationTtl) {
        this.restoreDeduplicationTtl =
                OptionChecks.checkExpressibleInNanos(
                        OptionChecks.checkPositive(
                                restoreDeduplicationTtl, "restoreDeduplicationTtl"),
                        "restoreDeduplicationTtl");
        return this;
    }

    /**
     * Builds the source.
     *
     * <p>The returned source implements {@link
     * org.apache.flink.streaming.api.lineage.LineageVertexProvider}. It reports each configured
     * subscription as {@code pubsub} / {@code subscription:project:subscription}, with the source's
     * unboundedness and a {@code gcp} physical-resource facet. It does not look up backing topics
     * or infer them from subscription-creation settings. Inspection performs no authentication,
     * client creation, RPC, or deserialization.
     *
     * <p>Flink 2.2 and 2.3 extract this metadata natively. Flink 1.20 supports direct inspection
     * through the interface, without automatic FLIP-314 listener delivery.
     *
     * @return the source
     */
    public Source<T, SubscriptionSplit, PubSubEnumeratorState> build() {
        Preconditions.checkState(
                deserializationSchema != null,
                "A deserializer is required: set deserializer(...).");
        Preconditions.checkState(
                !subscriptions.isEmpty(),
                "At least one subscription is required: set subscription(...) or"
                        + " subscriptions(...).");
        Set<SubscriptionDestination> distinct = new HashSet<>(subscriptions);
        Preconditions.checkState(
                distinct.size() == subscriptions.size(),
                "Subscriptions must be distinct, but %s were given: %s. The split plan already"
                        + " opens several subscriber clients on a subscription when the parallelism"
                        + " exceeds the subscription count, so a repeated entry buys nothing and"
                        + " only skews the assignment — and under orderingMode(PER_KEY) it would"
                        + " put one subscription on two subtasks, which is exactly what that mode"
                        + " exists to prevent.",
                subscriptions.size(),
                subscriptions);
        Integer parallelPullCount = subscriberOptions.getParallelPullCount();
        Preconditions.checkState(
                orderingMode != OrderingMode.PER_KEY
                        || parallelPullCount == null
                        || parallelPullCount == 1,
                "parallelPullCount(%s) cannot be combined with orderingMode(PER_KEY): each"
                        + " streaming-pull connection has its own message dispatcher and"
                        + " per-ordering-key callback serialization is per dispatcher, so a second"
                        + " connection would deliver two messages of one key concurrently. Remove"
                        + " parallelPullCount(...) — ordered subscriptions always use exactly one"
                        + " connection — or use orderingMode(NONE).",
                parallelPullCount);
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        Preconditions.checkState(
                restoreDeduplication || restoreDeduplicationTtl == null,
                "restoreDeduplicationTtl(%s) is set but restoreDeduplication is off. Set"
                        + " restoreDeduplication(true), or remove the time-to-live.",
                restoreDeduplicationTtl);
        // Both settings are checked at startup and this source never updates a subscription it
        // created, so catching them here is what stops it creating one it then refuses to consume —
        // leaving an orphan behind and crash-looping. (Ordering is genuinely fixed at creation; a
        // dead-letter policy is not, which is why the enumerator's own rejection for it says to add
        // one to the subscription.)
        for (Map.Entry<SubscriptionDestination, SubscriptionCreateOptions> entry :
                createOptions.entrySet()) {
            Preconditions.checkState(
                    orderingMode != OrderingMode.PER_KEY
                            || entry.getValue().isEnableMessageOrdering(),
                    "orderingMode(PER_KEY) requires every auto-created subscription to be created"
                            + " with enableMessageOrdering(true), but the settings for %s leave it"
                            + " off.",
                    entry.getKey());
            Preconditions.checkState(
                    !deserializationFailurePolicy.requiresDeadLetterPolicy()
                            || entry.getValue().getDeadLetterTopic() != null,
                    "deserializationFailurePolicy(%s) requires every auto-created subscription to be"
                            + " created with deadLetterPolicy(...), but the settings for %s have"
                            + " none. Nacking does not fail the job, so without one a message the"
                            + " schema can never convert is redelivered forever.",
                    // The constant name, not toString(): this sentence spells builder calls, while
                    // toString() is the enum's DDL spelling for the table layer.
                    deserializationFailurePolicy.name(),
                    entry.getKey());
        }
        return new PubSubStreamingPullSource<>(
                new PubSubSourceConfig<>(
                        Collections.unmodifiableList(new ArrayList<>(subscriptions)),
                        Collections.unmodifiableMap(new LinkedHashMap<>(createOptions)),
                        deserializationSchema,
                        orderingMode,
                        subscriberOptions,
                        deserializationFailurePolicy,
                        startPosition,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        restoreDeduplication ? resolveRestoreDeduplicationTtl() : null));
    }

    private Duration resolveRestoreDeduplicationTtl() {
        if (restoreDeduplicationTtl != null) {
            return restoreDeduplicationTtl;
        }
        Duration maxAckExtension =
                DefaultSubscriberFactory.effectiveMaxAckExtensionPeriod(subscriberOptions);
        if (maxAckExtension.isZero()) {
            return UNEXTENDED_LEASE_TIME_TO_LIVE;
        }
        // maxAckExtensionPeriod has no upper bound of its own, and the ledger's clock counts in
        // nanoseconds (ADR-0068). A default past what nanoseconds express is unbounded in effect,
        // so it is capped rather than rejected.
        return maxAckExtension.compareTo(MAX_EXPRESSIBLE_IN_NANOS) > 0
                ? MAX_EXPRESSIBLE_IN_NANOS
                : maxAckExtension;
    }
}
