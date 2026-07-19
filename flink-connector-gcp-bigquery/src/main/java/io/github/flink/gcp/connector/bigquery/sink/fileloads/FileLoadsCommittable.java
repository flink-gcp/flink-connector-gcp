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

import java.util.Objects;

/**
 * One finalized staging file: its destination table, its Cloud Storage URI, and size counters used
 * for load-job partitioning.
 *
 * <p>Load jobs reference exactly the URIs collected from committables — never a bucket prefix — so
 * objects orphaned by failed attempts can never leak into a load.
 */
@Internal
public final class FileLoadsCommittable {

    private final TableDestination destination;
    private final String uri;
    private final long byteCount;
    private final long rowCount;

    /**
     * Creates a committable.
     *
     * @param destination the destination table
     * @param uri the staging object URI ({@code gs://bucket/name})
     * @param byteCount the object size in bytes
     * @param rowCount the number of rows in the object
     */
    public FileLoadsCommittable(
            TableDestination destination, String uri, long byteCount, long rowCount) {
        this.destination = destination;
        this.uri = uri;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
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
                && uri.equals(that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, uri, byteCount, rowCount);
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
                + "}";
    }
}
