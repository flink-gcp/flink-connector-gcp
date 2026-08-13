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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One finalized staging file: the Flink job id of the run that staged it, its destination table,
 * its Cloud Storage URI, size counters used for load-job partitioning, and — in streaming execution
 * — the checkpoint that triggered it.
 *
 * <p>Load jobs reference exactly the URIs collected from committables — never a bucket prefix — so
 * objects orphaned by failed attempts can never leak into a load.
 *
 * <p>The originating Flink job id travels with the committable (rather than being read from the
 * runtime at commit time) so that BigQuery job ids and temporary table names stay deterministic
 * when committer state is restored under a <em>new</em> Flink job id (a savepoint or retained
 * checkpoint resubmitted with {@code flink run -s}): the re-commit reproduces the original ids and
 * re-attaches instead of double-loading.
 *
 * <p>The staging format travels here rather than being read from configuration at commit time,
 * because a committable recovered from state must be loaded as the format its file was actually
 * written in — which the configuration no longer says once the option changes, or once an upgrade
 * introduces the option at all and the in-flight committables are Avro. One load job cannot mix
 * formats, so the orchestrator groups on it (see {@code LoadJobOrchestrator}).
 *
 * <p>The Parquet <em>codec</em> deliberately does not travel: a Parquet footer names its own, and
 * the load job is never told, so carrying it would be state nobody reads.
 *
 * <p>The checkpoint id is stamped by the pre-commit stage (the writer does not know it); it stays
 * {@code null} in batch execution and selects the streaming behavior of the load-job orchestrator
 * (visible {@code -c<id>} job-id segment and checkpoint-scoped overflow tables).
 */
@Internal
public final class FileLoadsCommittable {

    private final String flinkJobId;
    private final TableDestination destination;
    private final String uri;
    private final long byteCount;
    private final long rowCount;
    private final StagingFormat format;
    @Nullable private final Long checkpointId;

    /**
     * Creates a committable without a checkpoint id (as emitted by the writer).
     *
     * @param flinkJobId the Flink job id (hex) of the run that staged the file
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     */
    public FileLoadsCommittable(
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format) {
        this(flinkJobId, destination, uri, byteCount, rowCount, format, null);
    }

    /**
     * Creates a committable.
     *
     * @param flinkJobId the Flink job id (hex) of the run that staged the file
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     * @param format the format the file was written in
     * @param checkpointId the checkpoint that triggered this file, or {@code null} in batch
     *     execution
     */
    public FileLoadsCommittable(
            String flinkJobId,
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            StagingFormat format,
            @Nullable Long checkpointId) {
        this.flinkJobId = flinkJobId;
        this.destination = destination;
        this.uri = uri;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.format = format;
        this.checkpointId = checkpointId;
    }

    /** Returns a copy of this committable stamped with the given checkpoint id. */
    public FileLoadsCommittable withCheckpointId(long checkpointId) {
        return new FileLoadsCommittable(
                flinkJobId, destination, uri, byteCount, rowCount, format, checkpointId);
    }

    /** Returns the Flink job id (hex) of the run that staged the file. */
    public String getFlinkJobId() {
        return flinkJobId;
    }

    /** Returns the destination table. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the staging object URI. */
    public String getUri() {
        return uri;
    }

    /** Returns the object size in bytes. */
    public long getByteCount() {
        return byteCount;
    }

    /** Returns the number of rows in the object. */
    public long getRowCount() {
        return rowCount;
    }

    /** Returns the format the file was written in. */
    public StagingFormat getFormat() {
        return format;
    }

    /**
     * Returns the checkpoint that triggered this file, or {@code null} in batch execution (or
     * before the pre-commit stage stamped it).
     */
    @Nullable
    public Long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileLoadsCommittable that = (FileLoadsCommittable) o;
        return byteCount == that.byteCount
                && rowCount == that.rowCount
                && flinkJobId.equals(that.flinkJobId)
                && destination.equals(that.destination)
                && uri.equals(that.uri)
                && format == that.format
                && Objects.equals(checkpointId, that.checkpointId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                flinkJobId, destination, uri, byteCount, rowCount, format, checkpointId);
    }

    @Override
    public String toString() {
        return "FileLoadsCommittable{flinkJobId="
                + flinkJobId
                + ", destination="
                + destination
                + ", uri="
                + uri
                + ", byteCount="
                + byteCount
                + ", rowCount="
                + rowCount
                + ", format="
                + format
                + ", checkpointId="
                + checkpointId
                + "}";
    }
}
