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

package io.github.flink.gcp.connector.pubsub.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Serializes sink records into Pub/Sub messages.
 *
 * <p>Implementations return a full {@link PubsubMessage}, so message attributes and ordering keys
 * are expressible in addition to the payload. Records that only carry a payload can wrap a plain
 * Flink {@link SerializationSchema} with {@link #dataOnly(SerializationSchema)}; attributes and an
 * ordering key extracted from the record can be layered onto any schema with {@link
 * #withAttributes(AttributesExtractor)} and {@link #withOrderingKey(OrderingKeyExtractor)}.
 *
 * <p>Returning {@code null} skips the record: it is written nowhere and is not a failure. Every
 * serializer of this connector family reads {@code null} that way, so a filter that depends on the
 * message being built belongs here rather than upstream of the sink. A {@code null} travels
 * unchanged through {@link #withAttributes(AttributesExtractor)} and {@link
 * #withOrderingKey(OrderingKeyExtractor)}, which extract nothing from a record they are not going
 * to send.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0).
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public interface PubSubSerializationSchema<T> extends Serializable {

    /**
     * Extracts message attributes from a record.
     *
     * @param <T> type of the records written by the sink
     */
    @PublicEvolving
    @FunctionalInterface
    interface AttributesExtractor<T> extends Serializable {

        /**
         * Returns the attributes of the message built for the given record. {@code null} or an
         * empty map adds no attributes; entries must have non-null keys and values.
         *
         * @param element the record
         * @return the attributes, or {@code null} for none
         */
        Map<String, String> extractAttributes(T element);
    }

    /**
     * Extracts the ordering key from a record.
     *
     * @param <T> type of the records written by the sink
     */
    @PublicEvolving
    @FunctionalInterface
    interface OrderingKeyExtractor<T> extends Serializable {

        /**
         * Returns the ordering key of the message built for the given record. {@code null} or an
         * empty string sets no ordering key.
         *
         * @param element the record
         * @return the ordering key, or {@code null} for none
         */
        String extractOrderingKey(T element);
    }

    /**
     * Initialization hook invoked once before serialization starts, on the task that runs the
     * writer. The default implementation does nothing.
     *
     * @param context contextual information for initialization (metrics, user code class loader)
     * @throws Exception if initialization fails; fails the writer creation
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes the given record into a Pub/Sub message.
     *
     * @param element the record
     * @return the message to publish, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized; the record is handed to the sink's
     *     failed-message handler, which fails the job by default
     */
    @Nullable
    PubsubMessage serialize(T element) throws IOException;

    /**
     * Wraps a plain Flink {@link SerializationSchema} into a schema producing messages whose
     * payload is the serialized record, with no attributes or ordering key.
     *
     * <p>A payload-only schema cannot skip a record: Flink's {@code SerializationSchema} contract
     * has no {@code null} in it, so a {@code null} payload is treated as a serialization failure
     * rather than as a skip. Implement this interface directly to skip.
     *
     * @param schema the payload schema
     * @param <T> type of the records written by the sink
     * @return the wrapping schema
     */
    static <T> PubSubSerializationSchema<T> dataOnly(SerializationSchema<T> schema) {
        return new DataOnlySerializationSchema<>(schema);
    }

    /**
     * Returns a schema producing this schema's messages with the extracted attributes added
     * (overwriting same-named attributes this schema already set; a {@code null}/empty extraction
     * leaves the message's attributes unchanged).
     *
     * @param extractor the attributes extractor
     * @return the composed schema
     */
    default PubSubSerializationSchema<T> withAttributes(AttributesExtractor<? super T> extractor) {
        return new MetadataSerializationSchema<>(this, extractor, null);
    }

    /**
     * Returns a schema producing this schema's messages with the extracted ordering key set
     * (overwriting an ordering key this schema already set; a {@code null}/empty extraction leaves
     * the message — including an ordering key this schema itself set — unchanged).
     *
     * <p>Ordering keys are only honored when {@code
     * PubSubPublisherOptions.builder().enableMessageOrdering(true)} is set on the sink; the writer
     * rejects messages carrying an ordering key while ordering is disabled.
     *
     * @param extractor the ordering-key extractor
     * @return the composed schema
     */
    default PubSubSerializationSchema<T> withOrderingKey(
            OrderingKeyExtractor<? super T> extractor) {
        return new MetadataSerializationSchema<>(this, null, extractor);
    }
}
