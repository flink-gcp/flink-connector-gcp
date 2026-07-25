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

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

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
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final OrderingMode orderingMode;
    @Nullable private final String emulatorEndpoint;

    PubSubSourceConfig(
            List<SubscriptionDestination> subscriptions,
            PubSubDeserializationSchema<T> deserializationSchema,
            OrderingMode orderingMode,
            @Nullable String emulatorEndpoint) {
        this.subscriptions = subscriptions;
        this.deserializationSchema = deserializationSchema;
        this.orderingMode = orderingMode;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the subscriptions to consume, in assignment order. */
    public List<SubscriptionDestination> getSubscriptions() {
        return subscriptions;
    }

    /** Returns the deserialization schema. */
    public PubSubDeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }

    /** Returns the ordering mode. */
    public OrderingMode getOrderingMode() {
        return orderingMode;
    }

    /** Returns the emulator endpoint, or {@code null} for production Pub/Sub. */
    @Nullable
    public String getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
