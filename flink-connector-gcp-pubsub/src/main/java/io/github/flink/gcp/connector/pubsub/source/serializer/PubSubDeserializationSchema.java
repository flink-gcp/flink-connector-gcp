/*
 * Copyright 2023 Google LLC
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

package io.github.flink.gcp.connector.pubsub.source.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Deserializes Pub/Sub messages into source records.
 *
 * <p>Implementations receive the full {@link PubsubMessage}, so the payload, message attributes,
 * the ordering key, the message id and the publish time are all available, along with the {@link
 * SubscriptionDestination} it arrived on — which the message itself does not carry and a source
 * consuming several subscriptions needs. Messages that only carry a payload can wrap a plain Flink
 * {@link DeserializationSchema} with {@link #dataOnly(DeserializationSchema)}.
 *
 * <p>Records are handed to a {@link Collector} rather than returned, so one message may produce any
 * number of records — including none, which drops the message (it is still acknowledged with the
 * checkpoint that covers it).
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * whose {@code deserialize} returns a single nullable record instead of collecting.
 *
 * @param <T> type of the records produced by the source
 */
@PublicEvolving
public interface PubSubDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization hook invoked once before deserialization starts, on the task that runs the
     * reader. The default implementation does nothing.
     *
     * @param context contextual information for initialization (metrics, user code class loader)
     * @throws Exception if initialization fails; fails the reader creation
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes the given Pub/Sub message, emitting zero or more records.
     *
     * @param message the received message
     * @param subscription the subscription the message arrived on, constant for the duration of a
     *     split but not for the schema, which serves every split the reader is assigned
     * @param out collector for the produced records
     * @throws IOException if the message cannot be deserialized; handling is governed by the
     *     source's deserialization failure policy
     */
    void deserialize(PubsubMessage message, SubscriptionDestination subscription, Collector<T> out)
            throws IOException;

    /**
     * Wraps a plain Flink {@link DeserializationSchema} into a schema that deserializes the message
     * payload and ignores attributes, the ordering key and every other message field.
     *
     * @param schema the payload schema
     * @param <T> type of the records produced by the source
     * @return the wrapping schema
     */
    static <T> PubSubDeserializationSchema<T> dataOnly(DeserializationSchema<T> schema) {
        return new DataOnlyDeserializationSchema<>(schema);
    }
}
