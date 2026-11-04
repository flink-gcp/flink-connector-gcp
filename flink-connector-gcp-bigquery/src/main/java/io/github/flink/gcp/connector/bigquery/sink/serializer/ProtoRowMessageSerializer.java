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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.Message;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializationSchema} that can hand over the row message it builds before
 * serializing it.
 *
 * <p>The built-in serializers build a protobuf message and serialize it, and {@code FILE_LOADS}
 * walks a message to build the staged record. Handing the message over spares that write method a
 * serialization and a parse per record. Serializers that do not implement this interface keep the
 * byte path, which {@code FILE_LOADS} parses once.
 *
 * <p>The returned message carries the same row that {@link
 * BigQueryProtoSerializationSchema#serialize(Object)} would return the bytes of, and {@code null}
 * skips the record in the same way. A caller uses the message only when its descriptor is the very
 * instance {@link BigQueryProtoSerializationSchema#getDescriptor} returns for the record's
 * destination, and serializes it otherwise.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public interface ProtoRowMessageSerializer<T> {

    /**
     * Converts a record into its protobuf row message.
     *
     * @param element the record
     * @return the row message, or {@code null} to skip the record
     * @throws IOException if the record cannot be converted, as {@link
     *     BigQueryProtoSerializationSchema#serialize(Object)} would throw
     */
    @Nullable
    Message serializeToMessage(T element) throws IOException;
}
