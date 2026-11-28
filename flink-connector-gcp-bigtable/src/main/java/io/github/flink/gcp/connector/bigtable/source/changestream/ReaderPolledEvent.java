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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

/**
 * Tells the coordinator that the reader polled after its snapshot of a checkpoint, so it is still
 * running: a source that a stop-with-savepoint stopped takes that snapshot but is never polled
 * again. Until a reader reports the coordinator's latest snapshot, the coordinator sends it
 * nothing, because the reader may finish once that checkpoint completes and Flink fails a task that
 * an event reaches after it finished.
 */
@Internal
public final class ReaderPolledEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;

    public ReaderPolledEvent(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    /** The latest checkpoint whose snapshot the reader took before this poll. */
    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public String toString() {
        return "ReaderPolledEvent{checkpointId=" + checkpointId + "}";
    }
}
