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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.IOException;

/**
 * {@link PubSubDeserializationSchema} deserializing only the message payload through a plain Flink
 * {@link DeserializationSchema}, obtained from {@link PubSubDeserializationSchema#payload}.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0).
 *
 * @param <T> type of the records produced by the source
 */
@Internal
final class PayloadDeserializationSchema<T> implements PubSubDeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> schema;

    PayloadDeserializationSchema(DeserializationSchema<T> schema) {
        this.schema = schema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public void deserialize(
            PubsubMessage message, SubscriptionDestination subscription, Collector<T> out)
            throws IOException {
        // The Collector overload of DeserializationSchema drops a null deserialization result
        // instead of emitting it.
        schema.deserialize(message.getData().toByteArray(), out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return schema.getProducedType();
    }
}
