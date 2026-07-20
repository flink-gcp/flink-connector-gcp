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

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes sink records into Pub/Sub messages.
 *
 * <p>Implementations return a full {@link PubsubMessage}, so message attributes and ordering keys
 * are expressible in addition to the payload. Records that only carry a payload can wrap a plain
 * Flink {@link SerializationSchema} with {@link #dataOnly(SerializationSchema)}.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0).
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public interface PubSubSerializationSchema<T> extends Serializable {

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
     * @return the message to publish
     * @throws IOException if the record cannot be serialized; fails the ongoing write
     */
    PubsubMessage serialize(T element) throws IOException;

    /**
     * Wraps a plain Flink {@link SerializationSchema} into a schema producing messages whose
     * payload is the serialized record, with no attributes or ordering key.
     *
     * @param schema the payload schema
     * @param <T> type of the records written by the sink
     * @return the wrapping schema
     */
    static <T> PubSubSerializationSchema<T> dataOnly(SerializationSchema<T> schema) {
        return new DataOnlySerializationSchema<>(schema);
    }
}
