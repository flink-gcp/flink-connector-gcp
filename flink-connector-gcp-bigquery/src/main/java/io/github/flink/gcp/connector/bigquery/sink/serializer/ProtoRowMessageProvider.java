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

import org.apache.flink.annotation.PublicEvolving;

import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link BigQueryProtoSerializationSchema} that can hand over the row message it builds before
 * serializing it.
 *
 * <p>A serializer that builds a protobuf message and then encodes it can implement this interface
 * next to {@link BigQueryProtoSerializationSchema}, with the same type argument. The interface has
 * no effect on a class that is not a {@link BigQueryProtoSerializationSchema}. {@code FILE_LOADS}
 * walks a message to build the staged record, so a serializer that hands its message over spares
 * that write method an encoding and a parse per record. Serializers that do not implement this
 * interface keep the byte path, which {@code FILE_LOADS} parses once. The built-in serializers
 * implement it, so a serializer that delegates to one of them can forward this method to the
 * delegate as it forwards {@code serialize}.
 *
 * <p>Which of the two methods the sink calls for a record depends on the write method and on
 * whether additional or write-only fields are configured, so both must produce the same row: the
 * returned message carries the row that {@link BigQueryProtoSerializationSchema#serialize(Object)}
 * would return the bytes of, and {@code null} skips the record in the same way.
 *
 * <p>The message is used as it is only when its descriptor is the very instance {@link
 * BigQueryProtoSerializationSchema#getDescriptor(TableDestination)} returns for the record's
 * destination and it carries no unknown field. Any other message, including one of an equal
 * descriptor derived again, is encoded and its bytes are handled as the byte path handles them:
 * such a serializer loses the saving, and its row is accepted or refused as its bytes would be. A
 * message used as it is is refused, as its bytes would be, when it leaves a {@code required} field
 * unset. It must hold nested messages of that descriptor's own nested types, as a message built
 * through the descriptor's builders does; {@code FILE_LOADS} reads nested fields through those
 * types and fails a row whose nested message has another descriptor. Nested messages should carry
 * no unknown field, which is not checked.
 *
 * @param <T> type of the records written by the sink, the same as the serialization schema's
 */
@PublicEvolving
public interface ProtoRowMessageProvider<T> {

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
