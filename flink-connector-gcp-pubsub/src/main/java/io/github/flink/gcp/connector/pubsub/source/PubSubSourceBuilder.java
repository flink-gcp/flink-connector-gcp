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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import javax.annotation.Nullable;

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
@PublicEvolving
public class PubSubSourceBuilder<T> {

    private final List<SubscriptionDestination> subscriptions = new ArrayList<>();
    private final Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions =
            new LinkedHashMap<>();
    private PubSubDeserializationSchema<T> deserializationSchema;
    private OrderingMode orderingMode = OrderingMode.NONE;
    private PubSubSubscriberOptions subscriberOptions = PubSubSubscriberOptions.defaults();
    private DeserializationFailurePolicy deserializationFailurePolicy =
            DeserializationFailurePolicy.FAIL;
    private StartPosition startPosition = StartPosition.continueFromSubscription();
    @Nullable private String emulatorEndpoint;

    PubSubSourceBuilder() {}

    /**
     * Adds a subscription to consume. Calling this several times, or combining it with {@link
     * #subscriptions}, consumes every added subscription in one source.
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
     * Sets the record deserialization schema.
     *
     * @param deserializationSchema the deserialization schema
     * @return this builder
     */
    public PubSubSourceBuilder<T> deserializationSchema(
            PubSubDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema =
                Preconditions.checkNotNull(
                        deserializationSchema, "deserializationSchema must not be null");
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
     * StartPosition#continueFromSubscription()}, which starts wherever the subscriptions already
     * are.
     *
     * <p>Every other position seeks, which rewrites state shared by every consumer of the
     * subscription — including other jobs. The seek runs once, at the first start of a job, and
     * never on a restore. See {@link StartPosition} for the full semantics.
     *
     * @param startPosition where to start consuming
     * @return this builder
     */
    public PubSubSourceBuilder<T> startPosition(StartPosition startPosition) {
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        return this;
    }

    /**
     * Points the source at a Pub/Sub emulator instead of the production service. Subscribers
     * connect to the given {@code host:port} over a plaintext channel with no credentials, so this
     * must only ever be used against an emulator (for example a testcontainers {@code
     * PubSubEmulatorContainer}). Optional; when unset the source connects to Pub/Sub with
     * application-default credentials.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     */
    public PubSubSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        Preconditions.checkNotNull(emulatorEndpoint, "emulatorEndpoint must not be null");
        Preconditions.checkArgument(
                !emulatorEndpoint.trim().isEmpty(), "emulatorEndpoint must not be blank");
        this.emulatorEndpoint = emulatorEndpoint;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     */
    public Source<T, SubscriptionSplit, PubSubEnumeratorState> build() {
        Preconditions.checkState(
                deserializationSchema != null, "A deserialization schema is required.");
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
        if (orderingMode == OrderingMode.PER_KEY) {
            for (Map.Entry<SubscriptionDestination, SubscriptionCreateOptions> entry :
                    createOptions.entrySet()) {
                Preconditions.checkState(
                        entry.getValue().isEnableMessageOrdering(),
                        "orderingMode(PER_KEY) requires every auto-created subscription to be"
                                + " created with enableMessageOrdering(true), but the options for"
                                + " %s leave it off. A subscription's ordering setting is fixed at"
                                + " creation, so the source would create it and then have to reject"
                                + " it at startup.",
                        entry.getKey());
            }
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
                        emulatorEndpoint));
    }
}
