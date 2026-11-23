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
 *
 * <p>Version 4 added the project the committable's BigQuery jobs run in (#2178). Versions 2 and 3
 * are migrated with none, as no run before it configured one; such a committable commits where the
 * restored job's configuration says.
 *
 * <p>Version 5 added the dataset of the destination's temporary tables (#2289). Its absence marks a
 * file a version before #2289 staged ({@link FileLoadsCommittable#isStagedByOlderVersion()}), whose
 * commit is planned under the temporary-table names and job ids that version derived. Versions 2 to
 * 4 are migrated without one, and a version-5 committable that records none, written back from such
 * state, keeps that absence. For version 3 as every release up to 1.4.0 wrote it, and for version
 * 4, those are the base names those runs used, so a restored commit derives the base
 * temporary-table names and job ids the attempt that may already have run it derived. Such
 * committables record no temporary dataset, and version 3 no job project, so restore them under the
 * {@code tempDataset} and job project that attempt used: a commit whose final copy succeeded then
 * re-attaches to it instead of reloading files that attempt deleted, and one whose final copy did
 * not still follows ADR-0183's layout rules, which can need the staged files. Before the first
 * release, version 2 and the earliest version 3 were written by code that appended a streaming
 * commit's overflow partitions directly to the destination; such a commit is planned through
 * temporary tables now and is not reproduced.
 */
@Internal
public final class FileLoadsCommittableSerializer
        implements SimpleVersionedSerializer<FileLoadsCommittable> {

    private static final int VERSION = 5;

    /** The layout before the temporary dataset, which no run before #2289 recorded. */
    private static final int VERSION_WITHOUT_TEMP_DATASET = 4;

    /** The layout before the job project, whose committables all used the default project. */
    private static final int VERSION_WITHOUT_JOB_PROJECT = 3;

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
            String jobProject = committable.getJobProject();
            out.writeBoolean(jobProject != null);
            if (jobProject != null) {
                out.writeUTF(jobProject);
            }
            String recordedTempDataset = committable.getRecordedTempDataset();
            out.writeBoolean(recordedTempDataset != null);
            if (recordedTempDataset != null) {
                out.writeUTF(recordedTempDataset);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public FileLoadsCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION
                && version != VERSION_WITHOUT_TEMP_DATASET
                && version != VERSION_WITHOUT_JOB_PROJECT
                && version != VERSION_WITHOUT_FORMAT) {
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
                    version == VERSION_WITHOUT_FORMAT
                            ? StagingFormat.AVRO
                            : StagingFormat.valueOf(in.readUTF());
            Long checkpointId = in.readBoolean() ? in.readLong() : null;
            // Versions 2 and 3 predate the job project, so no run then configured one.
            String jobProject =
                    version > VERSION_WITHOUT_JOB_PROJECT && in.readBoolean() ? in.readUTF() : null;
            // Versions 2 to 4 predate the recorded temporary dataset, and a version-5 committable
            // written back from their state keeps its absence: all of them were staged before
            // #2289 and are planned under the names their attempt used.
            String recordedTempDataset =
                    version == VERSION && in.readBoolean() ? in.readUTF() : null;
            if (recordedTempDataset == null) {
                return FileLoadsCommittable.stagedByOlderVersion(
                        flinkJobId,
                        destination,
                        uri,
                        byteCount,
                        rowCount,
                        format,
                        checkpointId,
                        jobProject);
            }
            return new FileLoadsCommittable(
                    flinkJobId,
                    destination,
                    uri,
                    byteCount,
                    rowCount,
                    format,
                    checkpointId,
                    jobProject,
                    recordedTempDataset);
        }
    }
}
