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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Serializer for {@link RowRangeSplit}.
 *
 * <p>Hand-written, as every split serializer in this repository is, and for a reason that is
 * sharper here than usual: this format is checkpointed state, so it has to be one this connector
 * controls rather than one a client-library upgrade can move. The vendor's {@code ByteStringRange}
 * is {@code Serializable} and its {@code Query} carries a {@code ReadRowsRequest} wire form, but
 * delegating to either would pin their formats into every checkpoint.
 *
 * <p>Two details are load-bearing.
 *
 * <p>Row keys are written with an explicit length prefix, never through {@code writeUTF}. A row key
 * is up to 4 KB of <em>arbitrary bytes</em>, and modified UTF-8 mangles any key holding a {@code
 * 0x00} or a byte sequence that is not valid UTF-8 — silently, and only for the jobs whose keys are
 * not text.
 *
 * <p>Bound types are written as explicitly mapped codes rather than through {@code
 * BoundType.ordinal()}. The enum belongs to the client library; reordering its constants is a
 * source-compatible change there, and would reinterpret every checkpoint written before it here.
 */
@Internal
public final class RowRangeSplitSerializer implements SimpleVersionedSerializer<RowRangeSplit> {

    private static final int VERSION = 1;

    /** Enough for a split id and two row keys; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 256;

    private static final byte BOUND_UNBOUNDED = 0;
    private static final byte BOUND_CLOSED = 1;
    private static final byte BOUND_OPEN = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(RowRangeSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public RowRangeSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Bigtable row range split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        return readSplit(new DataInputDeserializer(serialized));
    }

    /**
     * Writes a split without a version tag.
     *
     * <p>Separate from {@link #serialize} for the enumerator state serializer, which embeds its
     * pending splits and carries a version of its own, so that the two formats can move
     * independently.
     *
     * @param out the output to write to
     * @param split the split to write
     * @throws IOException if writing fails
     */
    static void writeSplit(DataOutputSerializer out, RowRangeSplit split) throws IOException {
        out.writeUTF(split.splitId());
        ByteStringRange range = split.getRange();
        writeBound(out, range.getStartBound());
        if (range.getStartBound() != BoundType.UNBOUNDED) {
            writeKey(out, range.getStart());
        }
        writeBound(out, range.getEndBound());
        if (range.getEndBound() != BoundType.UNBOUNDED) {
            writeKey(out, range.getEnd());
        }
    }

    /**
     * Reads a split written by {@link #writeSplit}.
     *
     * @param in the input to read from
     * @return the split
     * @throws IOException if reading fails
     */
    static RowRangeSplit readSplit(DataInputDeserializer in) throws IOException {
        String splitId = in.readUTF();
        ByteStringRange range = ByteStringRange.unbounded();

        byte startBound = in.readByte();
        if (startBound != BOUND_UNBOUNDED) {
            ByteString start = readKey(in, "start");
            if (startBound == BOUND_CLOSED) {
                range.startClosed(start);
            } else if (startBound == BOUND_OPEN) {
                range.startOpen(start);
            } else {
                throw new IOException(
                        "Corrupt Bigtable row range split: unknown start bound code " + startBound);
            }
        }

        byte endBound = in.readByte();
        if (endBound != BOUND_UNBOUNDED) {
            ByteString end = readKey(in, "end");
            if (endBound == BOUND_CLOSED) {
                range.endClosed(end);
            } else if (endBound == BOUND_OPEN) {
                range.endOpen(end);
            } else {
                throw new IOException(
                        "Corrupt Bigtable row range split: unknown end bound code " + endBound);
            }
        }

        return new RowRangeSplit(splitId, range);
    }

    private static void writeBound(DataOutputSerializer out, BoundType bound) throws IOException {
        switch (bound) {
            case UNBOUNDED:
                out.writeByte(BOUND_UNBOUNDED);
                break;
            case CLOSED:
                out.writeByte(BOUND_CLOSED);
                break;
            case OPEN:
                out.writeByte(BOUND_OPEN);
                break;
            default:
                throw new IOException("Unknown row range bound type " + bound);
        }
    }

    private static void writeKey(DataOutputSerializer out, ByteString key) throws IOException {
        byte[] bytes = key.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static ByteString readKey(DataInputDeserializer in, String side) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException(
                    "Corrupt Bigtable row range split: negative " + side + " key length " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return ByteString.copyFrom(bytes);
    }
}
