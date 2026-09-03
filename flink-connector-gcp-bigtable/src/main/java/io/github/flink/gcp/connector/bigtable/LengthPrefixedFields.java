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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The length-prefixed fields the connector-owned {@code TypeSerializer}s share: a byte string or a
 * UTF-8 string as an {@code int} length followed by the bytes, and a non-negative {@code int}
 * count. Each serializer's format is its own; only the field encoding is common.
 *
 * <p>A read rejects a negative length or count with a named {@link IOException} before it
 * allocates. A positive length the stream cannot back is allocated first and then fails as the
 * truncation it is, so a corrupt or foreign stream can fail in that allocation or cost one
 * oversized allocation before it is refused: no field here has a bound both serializers could
 * share. The copy buffer is a {@link ThreadLocal} because a serializer's {@code duplicate()}
 * returns {@code this} and task threads therefore share one instance.
 */
@Internal
public final class LengthPrefixedFields {

    private static final ThreadLocal<byte[]> COPY_BUFFER =
            ThreadLocal.withInitial(() -> new byte[4 * 1024]);

    private LengthPrefixedFields() {}

    /**
     * Returns the calling thread's copy buffer, for a serializer's {@code copy} to pass through
     * every {@link #copyByteArray} of one record.
     *
     * @return the buffer
     */
    public static byte[] copyBuffer() {
        return COPY_BUFFER.get();
    }

    /**
     * Writes a string as its UTF-8 length and bytes.
     *
     * @param value the string
     * @param target the output
     * @throws IOException if the output fails
     */
    public static void writeString(String value, DataOutputView target) throws IOException {
        writeByteArray(value.getBytes(StandardCharsets.UTF_8), target);
    }

    /**
     * Reads a string written by {@link #writeString}.
     *
     * @param source the input
     * @return the string
     * @throws IOException if the input fails or the length is negative
     */
    public static String readString(DataInputView source) throws IOException {
        return new String(readByteArray(source, "string"), StandardCharsets.UTF_8);
    }

    /**
     * Writes a byte string as its length and bytes, chunk by chunk through the copy buffer so a
     * rope is never flattened.
     *
     * @param value the bytes
     * @param target the output
     * @throws IOException if the output fails
     */
    public static void writeBytes(ByteString value, DataOutputView target) throws IOException {
        target.writeInt(value.size());
        byte[] buffer = COPY_BUFFER.get();
        for (ByteBuffer chunk : value.asReadOnlyByteBufferList()) {
            while (chunk.hasRemaining()) {
                int copied = Math.min(chunk.remaining(), buffer.length);
                chunk.get(buffer, 0, copied);
                target.write(buffer, 0, copied);
            }
        }
    }

    /**
     * Reads a byte string written by {@link #writeBytes}.
     *
     * @param source the input
     * @return the bytes
     * @throws IOException if the input fails or the length is negative
     */
    public static ByteString readBytes(DataInputView source) throws IOException {
        return ByteString.copyFrom(readByteArray(source, "byte string"));
    }

    /**
     * Copies one length-prefixed field from the input to the output without decoding it.
     *
     * @param source the input
     * @param target the output
     * @param buffer the copy buffer, from {@link #copyBuffer()}
     * @param description what the field is, for the failure message
     * @throws IOException if the input or output fails or the length is negative
     */
    public static void copyByteArray(
            DataInputView source, DataOutputView target, byte[] buffer, String description)
            throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative " + description + " length: " + length);
        }
        target.writeInt(length);
        int remaining = length;
        while (remaining > 0) {
            int copied = Math.min(remaining, buffer.length);
            source.readFully(buffer, 0, copied);
            target.write(buffer, 0, copied);
            remaining -= copied;
        }
    }

    /**
     * Reads a count and checks it is not negative.
     *
     * @param source the input
     * @param description what is counted, for the failure message
     * @return the count
     * @throws IOException if the input fails or the count is negative
     */
    public static int readCount(DataInputView source, String description) throws IOException {
        int count = source.readInt();
        if (count < 0) {
            throw new IOException("Negative " + description + " count: " + count);
        }
        return count;
    }

    private static void writeByteArray(byte[] bytes, DataOutputView target) throws IOException {
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    private static byte[] readByteArray(DataInputView source, String description)
            throws IOException {
        int length = source.readInt();
        if (length < 0) {
            throw new IOException("Negative " + description + " length: " + length);
        }
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return bytes;
    }
}
