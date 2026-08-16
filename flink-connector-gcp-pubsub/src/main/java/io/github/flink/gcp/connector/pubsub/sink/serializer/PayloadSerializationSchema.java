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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;

/**
 * A {@link PubSubSerializationSchema} producing messages whose payload is the record serialized by
 * a wrapped Flink {@link SerializationSchema}, with no attributes or ordering key.
 *
 * <p>This schema never skips a record. Flink's {@code SerializationSchema} contract has no {@code
 * null} in it, so a {@code null} payload is reported as a serialization failure — reading it as a
 * skip would silently drop the records a payload schema failed on.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0).
 *
 * @param <T> type of the records written by the sink
 */
@Internal
final class PayloadSerializationSchema<T> implements PubSubSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<T> schema;

    PayloadSerializationSchema(SerializationSchema<T> schema) {
        this.schema = Preconditions.checkNotNull(schema, "schema must not be null");
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public PubsubMessage serialize(T element) throws IOException {
        byte[] payload = schema.serialize(element);
        if (payload == null) {
            throw new IOException(
                    "The payload schema "
                            + schema.getClass().getName()
                            + " returned null for a record. Flink's SerializationSchema contract"
                            + " has no null in it, so this is a serialization failure rather than"
                            + " a skip; implement PubSubSerializationSchema directly to skip a"
                            + " record.");
        }
        return PubsubMessage.newBuilder().setData(ByteString.copyFrom(payload)).build();
    }
}
