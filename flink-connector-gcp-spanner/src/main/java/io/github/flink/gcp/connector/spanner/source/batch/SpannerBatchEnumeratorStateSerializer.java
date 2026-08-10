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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link SpannerBatchEnumeratorState}.
 *
 * <p>Splits are embedded through {@link PartitionSplitSerializer}'s version-free helpers rather
 * than through the serializer itself, so that the split format and this one carry separate version
 * numbers and can move independently.
 */
@Internal
public final class SpannerBatchEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SpannerBatchEnumeratorState> {

    private static final int VERSION = 1;

    /** Enough for a small plan; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 8192;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SpannerBatchEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeBoolean(state.isPlanned());
        List<PartitionSplit> splits = state.getPendingSplits();
        out.writeInt(splits.size());
        for (PartitionSplit split : splits) {
            PartitionSplitSerializer.writeSplit(out, split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public SpannerBatchEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Spanner batch enumerator state serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        boolean planned = in.readBoolean();
        int count = in.readInt();
        if (count < 0) {
            throw new IOException(
                    "Corrupt Spanner batch enumerator state: negative split count " + count);
        }
        List<PartitionSplit> splits = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            splits.add(PartitionSplitSerializer.readSplit(in));
        }
        return new SpannerBatchEnumeratorState(planned, splits);
    }
}
