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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for {@link BufferedStreamWriterState}. */
@Internal
public final class BufferedStreamWriterStateSerializer
        implements SimpleVersionedSerializer<BufferedStreamWriterState> {

    private static final int VERSION = 2;
    private static final int VERSION_WITHOUT_DESTINATION = 1;

    @Nullable private final TableDestination legacyFixedDestination;

    /** Creates a serializer for state written by this version. */
    public BufferedStreamWriterStateSerializer() {
        this(null);
    }

    /**
     * Creates a serializer that can migrate version-1 state from the given fixed destination.
     *
     * <p>Version 1 predates dynamic destinations and therefore did not carry the destination.
     */
    public BufferedStreamWriterStateSerializer(@Nullable TableDestination legacyFixedDestination) {
        this.legacyFixedDestination = legacyFixedDestination;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BufferedStreamWriterState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(state.getDestination().getProject());
            out.writeUTF(state.getDestination().getDataset());
            out.writeUTF(state.getDestination().getTable());
            out.writeUTF(state.getStreamName());
            out.writeLong(state.getNextOffset());
            out.writeLong(state.getCheckpointId());
        }
        return bytes.toByteArray();
    }

    @Override
    public BufferedStreamWriterState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION && version != VERSION_WITHOUT_DESTINATION) {
            throw new IOException("Unknown writer state version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            TableDestination destination;
            String streamName;
            if (version == VERSION) {
                destination = TableDestination.of(in.readUTF(), in.readUTF(), in.readUTF());
                streamName = in.readUTF();
            } else {
                if (legacyFixedDestination == null) {
                    throw new IOException(
                            "Writer state version 1 requires the sink's legacy fixed destination");
                }
                destination = legacyFixedDestination;
                streamName = in.readUTF();
                if (!streamName.equals(BufferedStreamWriterState.NO_STREAM)
                        && !streamName.startsWith(destination.toTablePath() + "/streams/")) {
                    throw new IOException(
                            "Writer state version 1 stream "
                                    + streamName
                                    + " does not belong to the configured fixed destination "
                                    + destination.toTablePath());
                }
            }
            return new BufferedStreamWriterState(
                    destination, streamName, in.readLong(), in.readLong());
        }
    }
}
