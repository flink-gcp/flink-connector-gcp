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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Connector-owned checkpoint format for the Spanner Change Streams partition ledger. */
@Internal
public final class SpannerChangeStreamEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SpannerChangeStreamEnumeratorState> {

    private static final int VERSION = 4;
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SpannerChangeStreamEnumeratorState state) throws IOException {
        // Only a completed version-3 ledger has a bound it cannot name, and the coordinator never
        // snapshots one: it restores under the configured bound and checkpoints that.
        Preconditions.checkArgument(
                !state.isBounded() || state.getBoundedTimestamp() != null,
                "a bounded Spanner change-stream ledger must name its bound to be written");
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        ChangeStreamPartitionSplitSerializer.writeNullableInstant(out, state.getBoundedTimestamp());
        out.writeInt(state.getPartitions().size());
        for (ChangeStreamPartitionSplit partition : state.getPartitions()) {
            ChangeStreamPartitionSplitSerializer.writeSplit(out, partition);
        }
        out.writeInt(state.getFinishedParentProofs().size());
        for (String proof : state.getFinishedParentProofs()) {
            out.writeUTF(proof);
        }
        out.writeLong(state.getSourceWatermark());
        return out.getCopyOfBuffer();
    }

    @Override
    public SpannerChangeStreamEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version < 1 || version > VERSION) {
            throw new IOException(
                    "Unsupported Spanner change-stream enumerator state serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version >= 3) {
            return deserializeCompact(version, in);
        }
        return deserializeLegacy(version, in);
    }

    private static SpannerChangeStreamEnumeratorState deserializeCompact(
            int version, DataInputDeserializer in) throws IOException {
        boolean bounded;
        Instant boundedTimestamp = null;
        if (version == 3) {
            bounded = in.readBoolean();
        } else {
            boundedTimestamp = ChangeStreamPartitionSplitSerializer.readNullableInstant(in);
            bounded = boundedTimestamp != null;
        }
        int count = ChangeStreamPartitionSplitSerializer.readCount(in, "partition");
        List<ChangeStreamPartitionSplit> partitions = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            partitions.add(ChangeStreamPartitionSplitSerializer.readSplit(in));
        }
        int proofCount =
                ChangeStreamPartitionSplitSerializer.readCount(in, "finished parent proof");
        List<String> finishedParentProofs = new ArrayList<>(Math.min(proofCount, 1024));
        for (int i = 0; i < proofCount; i++) {
            finishedParentProofs.add(in.readUTF());
        }
        try {
            return version == 3
                    ? new SpannerChangeStreamEnumeratorState(
                            partitions, finishedParentProofs, bounded, in.readLong())
                    : SpannerChangeStreamEnumeratorState.versionFour(
                            partitions, finishedParentProofs, boundedTimestamp, in.readLong());
        } catch (RuntimeException e) {
            throw new IOException("Corrupt Spanner change-stream enumerator state.", e);
        }
    }

    private static SpannerChangeStreamEnumeratorState deserializeLegacy(
            int version, DataInputDeserializer in) throws IOException {
        int count = ChangeStreamPartitionSplitSerializer.readCount(in, "partition");
        List<ChangeStreamPartitionSplit> partitions = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            partitions.add(ChangeStreamPartitionSplitSerializer.readSplit(in));
        }
        try {
            return version == 1
                    ? new SpannerChangeStreamEnumeratorState(partitions)
                    : new SpannerChangeStreamEnumeratorState(partitions, in.readLong());
        } catch (RuntimeException e) {
            throw new IOException("Corrupt Spanner change-stream enumerator state.", e);
        }
    }
}
