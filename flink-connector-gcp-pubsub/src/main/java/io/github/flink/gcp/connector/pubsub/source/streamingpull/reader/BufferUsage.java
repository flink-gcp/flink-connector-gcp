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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

/**
 * What one subscriber holds buffered and has not yet handed to the reader, as a single consistent
 * reading.
 *
 * <p>Both numbers together rather than two accessors, because the reader's decision and the line it
 * logs about that decision must quote the same state: read separately, a warning could report a
 * buffer that never existed at any instant.
 *
 * <p>{@link #bytes()} is in the client library's own unit — {@code
 * PubsubMessage.getSerializedSize()}, what {@code MessageDispatcher} reserves against flow control
 * and releases on {@code forget()} — so that a bound expressed here and the flow-control bound it
 * defaults to are measuring the same quantity.
 */
@Internal
public final class BufferUsage {

    private final int messages;
    private final long bytes;

    private BufferUsage(int messages, long bytes) {
        this.messages = messages;
        this.bytes = bytes;
    }

    /** Returns a reading of the given size. */
    public static BufferUsage of(int messages, long bytes) {
        return new BufferUsage(messages, bytes);
    }

    /** Returns how many messages are buffered. */
    public int messages() {
        return messages;
    }

    /** Returns their total serialized size. */
    public long bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return messages + " messages, " + bytes + " bytes";
    }
}
