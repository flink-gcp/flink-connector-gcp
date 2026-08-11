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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Connector-owned checkpoint format for {@link BigtableChangeStreamEnumeratorState}. */
@Internal
public final class BigtableChangeStreamEnumeratorStateSerializer
        implements SimpleVersionedSerializer<BigtableChangeStreamEnumeratorState> {

    private static final int VERSION = 1;
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigtableChangeStreamEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeBoolean(state.isInitialized());
        ChangeStreamPartitionSplitSerializer.writeInstant(out, state.getStartTime());
        out.writeLong(state.getNextSplitId());
        writeSplits(out, state.getUnassignedSplits());
        writeSplits(out, state.getAssignedSplits());
        out.writeInt(state.getPendingMerges().size());
        for (PendingMerge merge : state.getPendingMerges()) {
            ChangeStreamPartitionSplitSerializer.writePartition(out, merge.getPartition());
            out.writeInt(merge.getContinuationTokens().size());
            for (ChangeStreamContinuationToken token : merge.getContinuationTokens()) {
                byte[] bytes = token.toByteString().toByteArray();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            ChangeStreamPartitionSplitSerializer.writeInstant(out, merge.getLowWatermark());
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public BigtableChangeStreamEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Bigtable change-stream enumerator state serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        boolean initialized = in.readBoolean();
        Instant startTime = ChangeStreamPartitionSplitSerializer.readInstant(in);
        long nextSplitId = in.readLong();
        if (nextSplitId < 0) {
            throw new IOException(
                    "Corrupt Bigtable change-stream state: negative next split id " + nextSplitId);
        }
        List<ChangeStreamPartitionSplit> unassigned = readSplits(in);
        List<ChangeStreamPartitionSplit> assigned = readSplits(in);
        int mergeCount = ChangeStreamPartitionSplitSerializer.readCount(in, "pending merge");
        List<PendingMerge> pendingMerges = new ArrayList<>(Math.min(mergeCount, 1024));
        for (int i = 0; i < mergeCount; i++) {
            ByteStringRange partition = ChangeStreamPartitionSplitSerializer.readPartition(in);
            int tokenCount =
                    ChangeStreamPartitionSplitSerializer.readCount(in, "continuation token");
            List<ChangeStreamContinuationToken> tokens =
                    new ArrayList<>(Math.min(tokenCount, 1024));
            for (int j = 0; j < tokenCount; j++) {
                tokens.add(
                        ChangeStreamContinuationToken.fromByteString(
                                ChangeStreamPartitionSplitSerializer.readBytes(in, "token")));
            }
            pendingMerges.add(
                    new PendingMerge(
                            partition,
                            tokens,
                            ChangeStreamPartitionSplitSerializer.readInstant(in)));
        }
        return new BigtableChangeStreamEnumeratorState(
                initialized, startTime, nextSplitId, unassigned, assigned, pendingMerges);
    }

    private static void writeSplits(
            DataOutputSerializer out, List<ChangeStreamPartitionSplit> splits) throws IOException {
        out.writeInt(splits.size());
        for (ChangeStreamPartitionSplit split : splits) {
            ChangeStreamPartitionSplitSerializer.writeSplit(out, split);
        }
    }

    private static List<ChangeStreamPartitionSplit> readSplits(DataInputDeserializer in)
            throws IOException {
        int count = ChangeStreamPartitionSplitSerializer.readCount(in, "split");
        List<ChangeStreamPartitionSplit> splits = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            splits.add(ChangeStreamPartitionSplitSerializer.readSplit(in));
        }
        return splits;
    }
}
