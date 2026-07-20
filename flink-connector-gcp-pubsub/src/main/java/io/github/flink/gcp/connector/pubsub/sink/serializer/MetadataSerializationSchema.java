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
import org.apache.flink.util.Preconditions;

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

    @Override
    public PubsubMessage serialize(T element) throws IOException {
        PubsubMessage message = inner.serialize(element);
        PubsubMessage.Builder builder = null;
        if (attributesExtractor != null) {
            Map<String, String> attributes = attributesExtractor.extractAttributes(element);
            if (attributes != null && !attributes.isEmpty()) {
                builder = message.toBuilder();
                for (Map.Entry<String, String> attribute : attributes.entrySet()) {
                    Preconditions.checkNotNull(
                            attribute.getKey(), "The attributes extractor returned a null key.");
                    Preconditions.checkNotNull(
                            attribute.getValue(),
                            "The attributes extractor returned a null value for key '%s'.",
                            attribute.getKey());
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
}
