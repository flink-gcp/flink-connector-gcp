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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for {@link FileLoadsCommittable}. */
@Internal
public final class FileLoadsCommittableSerializer
        implements SimpleVersionedSerializer<FileLoadsCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FileLoadsCommittable committable) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(committable.getDestination().getProject());
            out.writeUTF(committable.getDestination().getDataset());
            out.writeUTF(committable.getDestination().getTable());
            out.writeUTF(committable.getUri());
            out.writeLong(committable.getByteCount());
            out.writeLong(committable.getRowCount());
            Long checkpointId = committable.getCheckpointId();
            out.writeBoolean(checkpointId != null);
            if (checkpointId != null) {
                out.writeLong(checkpointId);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public FileLoadsCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown committable version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            TableDestination destination =
                    TableDestination.of(in.readUTF(), in.readUTF(), in.readUTF());
            String uri = in.readUTF();
            long byteCount = in.readLong();
            long rowCount = in.readLong();
            Long checkpointId = in.readBoolean() ? in.readLong() : null;
            return new FileLoadsCommittable(destination, uri, byteCount, rowCount, checkpointId);
        }
    }
}
