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

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Immutable configuration of a Pub/Sub source, assembled by {@link PubSubSourceBuilder} and shipped
 * in the job graph.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public final class PubSubSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SubscriptionDestination> subscriptions;
    private final Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions;
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final OrderingMode orderingMode;
    private final PubSubSubscriberOptions subscriberOptions;
    private final DeserializationFailurePolicy deserializationFailurePolicy;
    private final PubSubStartPosition startPosition;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    PubSubSourceConfig(
            List<SubscriptionDestination> subscriptions,
            Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions,
            PubSubDeserializationSchema<T> deserializationSchema,
            OrderingMode orderingMode,
            PubSubSubscriberOptions subscriberOptions,
            DeserializationFailurePolicy deserializationFailurePolicy,
            PubSubStartPosition startPosition,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.subscriptions = subscriptions;
        this.createOptions = createOptions;
        this.deserializationSchema = deserializationSchema;
        this.orderingMode = orderingMode;
        this.subscriberOptions = subscriberOptions;
        this.deserializationFailurePolicy = deserializationFailurePolicy;
        this.startPosition = startPosition;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the subscriptions to consume, in assignment order. */
    public List<SubscriptionDestination> getSubscriptions() {
        return subscriptions;
    }

    /**
     * Returns the settings each subscription is created with if it is absent, keyed by
     * subscription. A subscription missing from this map must already exist.
     */
    public Map<SubscriptionDestination, SubscriptionCreateOptions> getCreateOptions() {
        return createOptions;
    }

    /** Returns where the source starts consuming. */
    public PubSubStartPosition getStartPosition() {
        return startPosition;
    }

    /** Returns the deserialization schema. */
    public PubSubDeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }

    /** Returns the ordering mode. */
    public OrderingMode getOrderingMode() {
        return orderingMode;
    }

    /** Returns the subscriber tuning options. */
    public PubSubSubscriberOptions getSubscriberOptions() {
        return subscriberOptions;
    }

    /** Returns what to do with a message the deserialization schema cannot convert. */
    public DeserializationFailurePolicy getDeserializationFailurePolicy() {
        return deserializationFailurePolicy;
    }

    /** Returns the service-account key-file path, or {@code null} for ADC. */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** Returns the emulator endpoint, or {@code null} for production Pub/Sub. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
