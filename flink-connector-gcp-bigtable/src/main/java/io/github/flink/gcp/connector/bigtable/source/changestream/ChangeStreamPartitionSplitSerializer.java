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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Connector-owned checkpoint format for {@link ChangeStreamPartitionSplit}. */
@Internal
public final class ChangeStreamPartitionSplitSerializer
        implements SimpleVersionedSerializer<ChangeStreamPartitionSplit> {

    private static final int VERSION = 1;
    private static final int INITIAL_BUFFER_SIZE = 512;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ChangeStreamPartitionSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        writeSplit(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public ChangeStreamPartitionSplit deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Bigtable change-stream split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        return readSplit(new DataInputDeserializer(serialized));
    }

    static void writeSplit(DataOutputSerializer out, ChangeStreamPartitionSplit split)
            throws IOException {
        out.writeUTF(split.splitId());
        writePartition(out, split.getPartition());
        List<ChangeStreamContinuationToken> tokens = split.getContinuationTokens();
        out.writeInt(tokens.size());
        for (ChangeStreamContinuationToken token : tokens) {
            writeBytes(out, token.toByteString());
        }
        writeInstant(out, split.getLowWatermark());
    }

    static ChangeStreamPartitionSplit readSplit(DataInputDeserializer in) throws IOException {
        String splitId = in.readUTF();
        ByteStringRange partition = readPartition(in);
        int tokenCount = readCount(in, "continuation token");
        List<ChangeStreamContinuationToken> tokens = new ArrayList<>(Math.min(tokenCount, 1024));
        for (int i = 0; i < tokenCount; i++) {
            tokens.add(ChangeStreamContinuationToken.fromByteString(readBytes(in, "token")));
        }
        return new ChangeStreamPartitionSplit(splitId, partition, tokens, readInstant(in));
    }

    static void writePartition(DataOutputSerializer out, ByteStringRange partition)
            throws IOException {
        // The rule and its wording live in ChangeStreamPartitions; only the currency is ours, since
        // SimpleVersionedSerializer answers in IOException. Checked before the first write rather
        // than between the two, which is unobservable — every caller builds a local buffer and
        // returns it only on the success path — and leaves nothing half-written to reason about.
        String violation = ChangeStreamPartitions.partitionShapeViolation(partition);
        if (violation != null) {
            throw new IOException(violation);
        }

        if (partition.getStartBound() == BoundType.UNBOUNDED) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeBytes(out, partition.getStart());
        }

        if (partition.getEndBound() == BoundType.UNBOUNDED) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeBytes(out, partition.getEnd());
        }
    }

    static ByteStringRange readPartition(DataInputDeserializer in) throws IOException {
        ByteStringRange partition = ByteStringRange.unbounded();
        if (in.readBoolean()) {
            partition.startClosed(readBytes(in, "partition start"));
        }
        if (in.readBoolean()) {
            partition.endOpen(readBytes(in, "partition end"));
        }
        return partition;
    }

    static void writeInstant(DataOutputSerializer out, Instant instant) throws IOException {
        out.writeLong(instant.getEpochSecond());
        out.writeInt(instant.getNano());
    }

    static Instant readInstant(DataInputDeserializer in) throws IOException {
        long seconds = in.readLong();
        int nanos = in.readInt();
        try {
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: invalid instant "
                            + seconds
                            + ":"
                            + nanos,
                    e);
        }
    }

    static int readCount(DataInputDeserializer in, String noun) throws IOException {
        int count = in.readInt();
        if (count < 0) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: negative " + noun + " count " + count);
        }
        if (count > in.available()) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: "
                            + noun
                            + " count "
                            + count
                            + " exceeds the remaining "
                            + in.available()
                            + " byte(s)");
        }
        return count;
    }

    /**
     * Writes a length-prefixed byte string. Package-private, and matched with {@link #readBytes},
     * so that the enumerator state's serializer embeds the same bytes through the same pair rather
     * than hand-rolling the write half of it.
     */
    static void writeBytes(DataOutputSerializer out, ByteString bytes) throws IOException {
        out.writeInt(bytes.size());
        out.write(bytes.toByteArray());
    }

    static ByteString readBytes(DataInputDeserializer in, String noun) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: negative " + noun + " length " + length);
        }
        if (length > in.available()) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: "
                            + noun
                            + " length "
                            + length
                            + " exceeds the remaining "
                            + in.available()
                            + " byte(s)");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return ByteString.copyFrom(bytes);
    }
}
