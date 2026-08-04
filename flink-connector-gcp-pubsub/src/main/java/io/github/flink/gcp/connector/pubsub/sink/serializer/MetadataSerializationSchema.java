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

package io.github.flink.gcp.connector.pubsub.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link PubSubSerializationSchema} layering extracted attributes and/or an ordering key onto the
 * messages of a wrapped schema. Composed by {@link
 * PubSubSerializationSchema#withAttributes(PubSubSerializationSchema.AttributesExtractor)} and
 * {@link
 * PubSubSerializationSchema#withOrderingKey(PubSubSerializationSchema.OrderingKeyExtractor)};
 * chained compositions nest wrappers (one message rebuild per layer), the outermost layer winning
 * for the ordering key and same-named attributes.
 *
 * <p>A {@code null} from the wrapped schema — the skip of {@link
 * PubSubSerializationSchema#serialize} — is returned unchanged, with no extractor called.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
final class MetadataSerializationSchema<T> implements PubSubSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final PubSubSerializationSchema<T> inner;
    @Nullable private final AttributesExtractor<? super T> attributesExtractor;
    @Nullable private final OrderingKeyExtractor<? super T> orderingKeyExtractor;

    MetadataSerializationSchema(
            PubSubSerializationSchema<T> inner,
            @Nullable AttributesExtractor<? super T> attributesExtractor,
            @Nullable OrderingKeyExtractor<? super T> orderingKeyExtractor) {
        this.inner = inner;
        this.attributesExtractor = attributesExtractor;
        this.orderingKeyExtractor = orderingKeyExtractor;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        inner.open(context);
    }

    @Nullable
    @Override
    public PubsubMessage serialize(T element) throws IOException {
        PubsubMessage message = inner.serialize(element);
        if (message == null) {
            // A skip passes through unchanged: there is nothing to layer metadata onto, and the
            // writer's check is the one place a record's fate is decided. Without this the
            // extractors would still run and the message be rebuilt, turning a skip into a
            // NullPointerException the writer routes to the failure handler — for the records an
            // extractor happened to fire on, and not for the others.
            return null;
        }
        PubsubMessage.Builder builder = null;
        if (attributesExtractor != null) {
            Map<String, String> attributes = attributesExtractor.extractAttributes(element);
            if (attributes != null && !attributes.isEmpty()) {
                builder = message.toBuilder();
                for (Map.Entry<String, String> attribute : attributes.entrySet()) {
                    // Explicit throws (not the varargs Preconditions overloads) keep the
                    // per-entry checks allocation-free on the per-record path.
                    if (attribute.getKey() == null) {
                        throw new NullPointerException(
                                "The attributes extractor returned a null key.");
                    }
                    if (attribute.getValue() == null) {
                        throw new NullPointerException(
                                "The attributes extractor returned a null value for key '"
                                        + attribute.getKey()
                                        + "'.");
                    }
                    builder.putAttributes(attribute.getKey(), attribute.getValue());
                }
            }
        }
        if (orderingKeyExtractor != null) {
            String orderingKey = orderingKeyExtractor.extractOrderingKey(element);
            if (orderingKey != null && !orderingKey.isEmpty()) {
                if (builder == null) {
                    builder = message.toBuilder();
                }
                builder.setOrderingKey(orderingKey);
            }
        }
        return builder == null ? message : builder.build();
    }

    /**
     * Chaining onto an unoccupied slot merges into this wrapper instead of nesting another one, so
     * the common {@code dataOnly(...).withAttributes(...).withOrderingKey(...)} chain pays one
     * message rebuild per record, not two. An occupied slot falls back to nesting, which keeps the
     * outermost-wins semantics.
     */
    @Override
    public PubSubSerializationSchema<T> withAttributes(AttributesExtractor<? super T> extractor) {
        if (attributesExtractor == null) {
            return new MetadataSerializationSchema<>(inner, extractor, orderingKeyExtractor);
        }
        return PubSubSerializationSchema.super.withAttributes(extractor);
    }

    @Override
    public PubSubSerializationSchema<T> withOrderingKey(OrderingKeyExtractor<? super T> extractor) {
        if (orderingKeyExtractor == null) {
            return new MetadataSerializationSchema<>(inner, attributesExtractor, extractor);
        }
        return PubSubSerializationSchema.super.withOrderingKey(extractor);
    }
}
