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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The parts of a delivery other than the payload that a table can read, exposed as {@code METADATA}
 * columns.
 *
 * <p>The payload itself is not here — it is what the table's format decodes.
 *
 * <p>Converters return Flink's internal data structures ({@code StringData}, {@code TimestampData},
 * {@code GenericMapData}), which is what a metadata column is always represented by regardless of
 * the {@link DataType} declared beside it.
 */
@Internal
enum ReadableMetadata {

    /** The service-assigned message id, unique within the topic. */
    MESSAGE_ID(
            "message-id",
            DataTypes.STRING().notNull(),
            (message, subscription) -> StringData.fromString(message.getMessageId())),

    /**
     * The time the service received the message, truncated to milliseconds — Pub/Sub stamps
     * nanoseconds, and {@code TIMESTAMP_LTZ(3)} is the Kafka-compatible precision for the column a
     * {@code WATERMARK FOR} is usually declared on.
     */
    PUBLISH_TIME(
            "publish-time",
            DataTypes.TIMESTAMP_LTZ(3).notNull(),
            (message, subscription) -> {
                // An unstamped message yields the epoch rather than null, because the column is
                // NOT NULL and there is no honest alternative. The service always stamps a
                // delivered message, so this only shows up for a hand-built one. The record
                // emitter faces the same case on the *event time* it assigns and answers it
                // differently — it emits without a timestamp — because there the option exists.
                Timestamp publishTime = message.getPublishTime();
                return TimestampData.fromEpochMillis(
                        publishTime.getSeconds() * 1_000L + publishTime.getNanos() / 1_000_000);
            }),

    /**
     * The message attributes; never null, but empty when the message carries none.
     *
     * <p>Not always only what the publisher wrote: on a subscription with a dead-letter policy the
     * client library injects {@code googclient_deliveryattempt} before the message reaches the
     * deserialization schema. Passed through rather than stripped, so nothing the service sends is
     * hidden.
     */
    ATTRIBUTES(
            "attributes",
            DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING().notNull()).notNull(),
            (message, subscription) -> {
                Map<StringData, StringData> attributes =
                        new HashMap<>(message.getAttributesCount());
                message.getAttributesMap()
                        .forEach(
                                (key, value) ->
                                        attributes.put(
                                                StringData.fromString(key),
                                                StringData.fromString(value)));
                return new GenericMapData(attributes);
            }),

    /**
     * The ordering key, or {@code null} when the message carries none. Pub/Sub represents "no key"
     * as the empty string, which would be a wrong SQL value: an unordered message has no key rather
     * than an empty one.
     */
    ORDERING_KEY(
            "ordering-key",
            DataTypes.STRING().nullable(),
            (message, subscription) ->
                    message.getOrderingKey().isEmpty()
                            ? null
                            : StringData.fromString(message.getOrderingKey())),

    /**
     * The subscription the message arrived on, as its resource name {@code
     * projects/<project>/subscriptions/<subscription>} — the analogue of Kafka's {@code topic}
     * column, and the only way to tell deliveries apart when a table names several subscriptions.
     *
     * <p>The resource name rather than the bare id, because that is the only form Pub/Sub's own API
     * speaks in: it is the value of {@code Subscription.name}, it is what every RPC's {@code
     * subscription} field takes, and the bare id appears nowhere on the API surface. It is also the
     * <em>relative resource name</em> that <a href="https://google.aip.dev/122">AIP-122</a> makes
     * canonical for API fields, so it is what joins a stream against audit logs or Cloud Asset
     * Inventory. Pub/Sub publishes no URL or self-link of its own, and Google's two other spellings
     * are string operations on this one:
     *
     * <ul>
     *   <li>full resource name (IAM, Asset Inventory): {@code '//pubsub.googleapis.com/' || name}
     *   <li>resource URI: {@code 'https://pubsub.googleapis.com/v1/' || name}
     * </ul>
     *
     * <p>Note this does not equal what the {@code subscription} option was written with, which is
     * the bare id resolved against {@code project}.
     *
     * <p>The subscription is on neither the {@link PubsubMessage} nor anything the SDK's receiver
     * callback hands over — the source consumes through {@code Subscriber}, which delivers a
     * message and an ack handle and never surfaces the streaming-pull response. That is why {@code
     * PubSubDeserializationSchema.deserialize} carries a {@link SubscriptionDestination}.
     */
    SUBSCRIPTION(
            "subscription",
            DataTypes.STRING().notNull(),
            (message, subscription) -> StringData.fromString(subscription.toSubscriptionPath()));

    /** Reads one metadata value out of a delivery, as an internal data structure. */
    @FunctionalInterface
    interface MetadataConverter extends Serializable {

        Object read(PubsubMessage message, SubscriptionDestination subscription);
    }

    private final String key;
    private final DataType dataType;
    private final MetadataConverter converter;

    ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    MetadataConverter getConverter() {
        return converter;
    }

    /**
     * Returns the metadata this connector can read, keyed by metadata key, in declaration order.
     *
     * <p>Ordered for the same reason the sink's is: the planner echoes this iteration order back as
     * the selected keys and lays the produced row out from it, so any consistent order is correct,
     * and an ordered one makes a plan's column layout a property of this declaration.
     */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (ReadableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return metadata;
    }

    /** Returns the constant with the given metadata key, or {@code null} if it is not one. */
    @Nullable
    static ReadableMetadata find(String key) {
        for (ReadableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        return null;
    }

    /** Returns the constant with the given metadata key. */
    static ReadableMetadata of(String key) {
        ReadableMetadata found = find(key);
        if (found == null) {
            throw new IllegalArgumentException(
                    "Unknown Pub/Sub readable metadata key '" + key + "'.");
        }
        return found;
    }
}
