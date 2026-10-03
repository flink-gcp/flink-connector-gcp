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

package io.github.flink.gcp.connector.firestore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.Timestamp;
import com.google.firestore.v1.RunQueryRequest;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

/**
 * Serializer for {@link QuerySplit}.
 *
 * <p>The split id and the read time are written in this connector's own format. The query is
 * written as the protobuf encoding of {@code google.firestore.v1.RunQueryRequest}, length-prefixed.
 * That is the service's published wire contract, whose field numbers are never reused, not a
 * library's Java serialization — and it is the form the client library itself serializes a query
 * to: re-encoding filters, orderings, projections and cursor values field by field would be a
 * second query model to keep in step with the service's ({@code docs/adr/0173}).
 */
@Internal
public final class QuerySplitSerializer implements SimpleVersionedSerializer<QuerySplit> {

    private static final int VERSION = 1;

    /** Enough for a split id, a read time and a small query; the serializer grows if needed. */
    private static final int INITIAL_BUFFER_SIZE = 256;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(QuerySplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public QuerySplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Firestore query split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        return readSplit(new DataInputDeserializer(serialized));
    }

    /**
     * Writes a split without a version tag, for the enumerator state serializer, which embeds its
     * pending splits and carries a version of its own.
     *
     * @param out the output to write to
     * @param split the split to write
     * @throws IOException if writing fails
     */
    static void writeSplit(DataOutputSerializer out, QuerySplit split) throws IOException {
        out.writeUTF(split.splitId());
        out.writeLong(split.getReadTime().getSeconds());
        out.writeInt(split.getReadTime().getNanos());
        byte[] query = split.getQuery().toByteArray();
        out.writeInt(query.length);
        out.write(query);
    }

    /**
     * Reads a split written by {@link #writeSplit}.
     *
     * @param in the input to read from
     * @return the split
     * @throws IOException if reading fails
     */
    static QuerySplit readSplit(DataInputDeserializer in) throws IOException {
        String splitId = in.readUTF();
        long seconds = in.readLong();
        int nanos = in.readInt();
        int length = in.readInt();
        if (length < 0) {
            throw new IOException(
                    "Corrupt Firestore query split "
                            + splitId
                            + ": negative query length "
                            + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        RunQueryRequest query;
        try {
            query = RunQueryRequest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Corrupt Firestore query split " + splitId + ": " + e, e);
        }
        Timestamp readTime;
        try {
            readTime = Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
            return new QuerySplit(splitId, query, readTime);
        } catch (IllegalArgumentException e) {
            throw new IOException("Corrupt Firestore query split " + splitId + ": " + e, e);
        }
    }
}
