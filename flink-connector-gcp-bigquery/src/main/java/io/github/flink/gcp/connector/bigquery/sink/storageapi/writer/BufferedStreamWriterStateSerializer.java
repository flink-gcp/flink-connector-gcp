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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for {@link BufferedStreamWriterState}. */
@Internal
public final class BufferedStreamWriterStateSerializer
        implements SimpleVersionedSerializer<BufferedStreamWriterState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BufferedStreamWriterState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(state.getStreamName());
            out.writeLong(state.getNextOffset());
            out.writeLong(state.getCheckpointId());
        }
        return bytes.toByteArray();
    }

    @Override
    public BufferedStreamWriterState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown writer state version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            return new BufferedStreamWriterState(in.readUTF(), in.readLong(), in.readLong());
        }
    }
}
