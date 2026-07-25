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

package io.github.flink.gcp.connector.bigquery.sink.storageapi;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/**
 * A flush instruction for one buffered write stream: make every row up to {@code flushOffset}
 * (inclusive, per {@code FlushRows} semantics) visible in the destination table.
 *
 * <p>The stream name and offset are all a commit needs — {@code FlushRows} is naturally idempotent
 * (re-flushing an already-flushed offset answers {@code ALREADY_EXISTS}, which the committer treats
 * as success), so unlike the FILE_LOADS committable no originating Flink job id or checkpoint id is
 * carried for deterministic re-attach.
 *
 * <p>The subtask id is diagnostic only: it names the writer that appended the rows in log messages
 * and error reports.
 */
@Internal
public final class BufferedStreamCommittable {

    private final String streamName;
    private final long flushOffset;
    private final int subtaskId;

    /**
     * Creates a committable.
     *
     * @param streamName the buffered write stream name (full resource path)
     * @param flushOffset the highest row offset to make visible, inclusive
     * @param subtaskId the writer subtask that appended the rows (diagnostics only)
     */
    public BufferedStreamCommittable(String streamName, long flushOffset, int subtaskId) {
        this.streamName = streamName;
        this.flushOffset = flushOffset;
        this.subtaskId = subtaskId;
    }

    /** Returns the buffered write stream name. */
    public String getStreamName() {
        return streamName;
    }

    /** Returns the highest row offset to make visible, inclusive. */
    public long getFlushOffset() {
        return flushOffset;
    }

    /** Returns the writer subtask that appended the rows. */
    public int getSubtaskId() {
        return subtaskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferedStreamCommittable that = (BufferedStreamCommittable) o;
        return flushOffset == that.flushOffset
                && subtaskId == that.subtaskId
                && streamName.equals(that.streamName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamName, flushOffset, subtaskId);
    }

    @Override
    public String toString() {
        return "BufferedStreamCommittable{streamName="
                + streamName
                + ", flushOffset="
                + flushOffset
                + ", subtaskId="
                + subtaskId
                + "}";
    }
}
