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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Connector-owned checkpoint format for the Spanner Change Streams partition ledger. */
@Internal
public final class SpannerChangeStreamEnumeratorStateSerializer
        implements SimpleVersionedSerializer<SpannerChangeStreamEnumeratorState> {

    private static final int VERSION = 1;
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SpannerChangeStreamEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeInt(state.getPartitions().size());
        for (SpannerChangeStreamPartitionSplit partition : state.getPartitions()) {
            SpannerChangeStreamPartitionSplitSerializer.writeSplit(out, partition);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public SpannerChangeStreamEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Spanner change-stream enumerator state serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int count = SpannerChangeStreamPartitionSplitSerializer.readCount(in, "partition");
        List<SpannerChangeStreamPartitionSplit> partitions = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            partitions.add(SpannerChangeStreamPartitionSplitSerializer.readSplit(in));
        }
        try {
            return new SpannerChangeStreamEnumeratorState(partitions);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt Spanner change-stream enumerator state.", e);
        }
    }
}
