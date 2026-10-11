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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.Mutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

/**
 * A write with the protobuf form a commit sends, encoded once when the write enters the buffer and
 * reused by every commit that carries it, a retry and a solo re-send included.
 *
 * <p>The write is kept beside its encoding because a routed write reaches the failure handler as
 * the serializer returned it, and the {@code ALREADY_EXISTS} lookup compares its entity. The two
 * share their strings, and the bytes of a blob held in one piece, which is every blob {@code
 * Blob.copyFrom} makes, so for those holding both costs the encoding's message objects rather than
 * a second copy of the data. A blob read by {@code Value.fromPb} from a protobuf whose bytes are a
 * concatenation is the exception: its encoding holds a copy of the bytes.
 */
@Internal
public final class EncodedMutation {

    private final DatastoreMutation write;
    private final Mutation encoding;
    private final long size;

    private EncodedMutation(DatastoreMutation write, Mutation encoding) {
        this.write = write;
        this.encoding = encoding;
        this.size = MutationSizeEstimator.sizeOf(encoding);
    }

    /**
     * Encodes the write.
     *
     * @param write the write
     * @return the write with its encoding
     * @throws RuntimeException if the write holds a value the encoding cannot carry
     */
    static EncodedMutation of(DatastoreMutation write) {
        Preconditions.checkNotNull(write, "write must not be null");
        return new EncodedMutation(write, EntityProtos.mutation(write));
    }

    /**
     * Returns the write as the serializer returned it.
     *
     * @return the write
     */
    public DatastoreMutation getWrite() {
        return write;
    }

    /**
     * Returns the write's protobuf form, as the {@code Commit} request carries it.
     *
     * @return the encoding
     */
    public Mutation getEncoding() {
        return encoding;
    }

    /**
     * Returns the bytes the encoding adds to a {@code Commit} request.
     *
     * @return its size on the wire, framing included
     */
    public long getSize() {
        return size;
    }
}
