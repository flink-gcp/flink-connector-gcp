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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer for {@link FileLoadsCommittable}. Version 2 added the originating Flink job id and the
 * optional checkpoint id; version 1 (the pre-#69, batch-only layout) never survived a job, so it is
 * rejected instead of migrated.
 *
 * <p>Version 3 added the staging format. Version 2 <em>is</em> migrated, unlike version 1: it is a
 * layout {@code main} has produced since #69, so committables written by it are in committer state
 * across the upgrade that introduces {@link StagingFormat}, and every one of them is Avro by
 * construction — that was the only format then. Rejecting them would fail a restart for no reason.
 * (Version 1 predates #69 and never survived a job, which is why it is still rejected rather than
 * migrated.)
 */
@Internal
public final class FileLoadsCommittableSerializer
        implements SimpleVersionedSerializer<FileLoadsCommittable> {

    private static final int VERSION = 3;

    /** The layout before the staging format, whose committables are all Avro. */
    private static final int VERSION_WITHOUT_FORMAT = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FileLoadsCommittable committable) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(committable.getFlinkJobId());
            out.writeUTF(committable.getDestination().getProject());
            out.writeUTF(committable.getDestination().getDataset());
            out.writeUTF(committable.getDestination().getTable());
            out.writeUTF(committable.getUri());
            out.writeLong(committable.getByteCount());
            out.writeLong(committable.getRowCount());
            out.writeUTF(committable.getFormat().name());
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
        if (version != VERSION && version != VERSION_WITHOUT_FORMAT) {
            throw new IOException("Unknown committable version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String flinkJobId = in.readUTF();
            TableDestination destination =
                    TableDestination.of(in.readUTF(), in.readUTF(), in.readUTF());
            String uri = in.readUTF();
            long byteCount = in.readLong();
            long rowCount = in.readLong();
            // Version 2 predates the format entirely, and everything it wrote was Avro.
            StagingFormat format =
                    version == VERSION ? StagingFormat.valueOf(in.readUTF()) : StagingFormat.AVRO;
            Long checkpointId = in.readBoolean() ? in.readLong() : null;
            return new FileLoadsCommittable(
                    flinkJobId, destination, uri, byteCount, rowCount, format, checkpointId);
        }
    }
}
