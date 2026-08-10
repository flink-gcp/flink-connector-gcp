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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplitSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link BigQueryReadEnumeratorState}.
 *
 * <p>Pending splits are written through the split's own serializer rather than field by field, so
 * the two formats can move independently. Which split layout a payload holds is derived from this
 * version rather than written beside it: a second version number inside the payload would be one
 * more thing to keep in step, for a mapping that is a single line.
 *
 * <p>Version 2 is version 1 with splits that carry the read session's expiry; version 1 is still
 * read.
 */
@Internal
public final class BigQueryReadEnumeratorStateSerializer
        implements SimpleVersionedSerializer<BigQueryReadEnumeratorState> {

    private static final int VERSION_WITHOUT_SPLIT_EXPIRY = 1;

    private static final int VERSION = 2;

    /** Enough for a session name and a few splits; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 4096;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigQueryReadEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeBoolean(state.isInitialized());
        out.writeBoolean(state.getSessionName() != null);
        if (state.getSessionName() != null) {
            out.writeUTF(state.getSessionName());
        }
        out.writeBoolean(state.getSessionExpireTime() != null);
        if (state.getSessionExpireTime() != null) {
            out.writeLong(state.getSessionExpireTime().getEpochSecond());
            out.writeInt(state.getSessionExpireTime().getNano());
        }
        out.writeInt(state.getPendingSplits().size());
        for (BigQueryReadStreamSplit split : state.getPendingSplits()) {
            BigQueryReadStreamSplitSerializer.writeSplit(out, split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public BigQueryReadEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION && version != VERSION_WITHOUT_SPLIT_EXPIRY) {
            throw new IOException(
                    "Unsupported BigQuery read enumerator state serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        int splitVersion =
                version == VERSION_WITHOUT_SPLIT_EXPIRY
                        ? BigQueryReadStreamSplitSerializer.VERSION_WITHOUT_EXPIRY
                        : BigQueryReadStreamSplitSerializer.VERSION_WITH_EXPIRY;
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        boolean initialized = in.readBoolean();
        String sessionName = in.readBoolean() ? in.readUTF() : null;
        Instant expireTime =
                in.readBoolean() ? Instant.ofEpochSecond(in.readLong(), in.readInt()) : null;
        int splitCount = in.readInt();
        if (splitCount < 0) {
            throw new IOException(
                    "Corrupt BigQuery read enumerator state: negative split count " + splitCount);
        }
        List<BigQueryReadStreamSplit> splits = new ArrayList<>(Math.min(splitCount, 1024));
        for (int i = 0; i < splitCount; i++) {
            splits.add(BigQueryReadStreamSplitSerializer.readSplit(in, splitVersion));
        }
        return new BigQueryReadEnumeratorState(initialized, sessionName, expireTime, splits);
    }
}
