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

import java.util.Objects;

/**
 * The buffered-stream writer's checkpointed state: which stream the subtask owns and how many rows
 * it had appended (acknowledged) as of the checkpoint.
 *
 * <p>The checkpoint id resolves restores that hand a subtask more than one state (scale-down): the
 * state with the highest checkpoint id is adopted and the other streams are abandoned.
 */
@Internal
public final class BufferedStreamWriterState {

    /** The stream name of a writer that has not created its stream yet. */
    public static final String NO_STREAM = "";

    private final String streamName;
    private final long nextOffset;
    private final long checkpointId;

    /**
     * Creates a state snapshot.
     *
     * @param streamName the buffered write stream name, or {@link #NO_STREAM} if none was created
     * @param nextOffset the next append offset (== rows appended so far)
     * @param checkpointId the checkpoint this snapshot belongs to
     */
    public BufferedStreamWriterState(String streamName, long nextOffset, long checkpointId) {
        this.streamName = streamName;
        this.nextOffset = nextOffset;
        this.checkpointId = checkpointId;
    }

    /** Returns the buffered write stream name, or {@link #NO_STREAM} if none was created. */
    public String getStreamName() {
        return streamName;
    }

    /** Returns the next append offset (== rows appended as of the checkpoint). */
    public long getNextOffset() {
        return nextOffset;
    }

    /** Returns the checkpoint this snapshot belongs to. */
    public long getCheckpointId() {
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
        BufferedStreamWriterState that = (BufferedStreamWriterState) o;
        return nextOffset == that.nextOffset
                && checkpointId == that.checkpointId
                && streamName.equals(that.streamName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamName, nextOffset, checkpointId);
    }

    @Override
    public String toString() {
        return "BufferedStreamWriterState{streamName="
                + streamName
                + ", nextOffset="
                + nextOffset
                + ", checkpointId="
                + checkpointId
                + "}";
    }
}
