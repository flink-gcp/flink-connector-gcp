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
 * One finalized staging file: its destination table, its Cloud Storage URI, size counters used for
 * load-job partitioning, and — in streaming execution — the checkpoint that triggered it.
 *
 * <p>Load jobs reference exactly the URIs collected from committables — never a bucket prefix — so
 * objects orphaned by failed attempts can never leak into a load.
 *
 * <p>The checkpoint id is stamped by the pre-commit gather operator (the writer does not know it);
 * it stays {@code null} in batch execution and selects the streaming behavior of the load-job
 * orchestrator (visible {@code -c<id>} job-id segment, direct loads on overflow).
 */
@Internal
public final class FileLoadsCommittable {

    private final TableDestination destination;
    private final String uri;
    private final long byteCount;
    private final long rowCount;
    @Nullable private final Long checkpointId;

    /**
     * Creates a committable without a checkpoint id (as emitted by the writer).
     *
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     */
    public FileLoadsCommittable(
            TableDestination destination, String uri, long byteCount, long rowCount) {
        this(destination, uri, byteCount, rowCount, null);
    }

    /**
     * Creates a committable.
     *
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     * @param checkpointId the checkpoint that triggered this file, or {@code null} in batch
     *     execution
     */
    public FileLoadsCommittable(
            TableDestination destination,
            String uri,
            long byteCount,
            long rowCount,
            @Nullable Long checkpointId) {
        this.destination = destination;
        this.uri = uri;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.checkpointId = checkpointId;
    }

    /** Returns a copy of this committable stamped with the given checkpoint id. */
    public FileLoadsCommittable withCheckpointId(long checkpointId) {
        return new FileLoadsCommittable(destination, uri, byteCount, rowCount, checkpointId);
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

    /**
     * Returns the checkpoint that triggered this file, or {@code null} in batch execution (or
     * before the gather operator stamped it).
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
                && destination.equals(that.destination)
                && uri.equals(that.uri)
                && Objects.equals(checkpointId, that.checkpointId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, uri, byteCount, rowCount, checkpointId);
    }

    @Override
    public String toString() {
        return "FileLoadsCommittable{destination="
                + destination
                + ", uri="
                + uri
                + ", byteCount="
                + byteCount
                + ", rowCount="
                + rowCount
                + ", checkpointId="
                + checkpointId
                + "}";
    }
}
