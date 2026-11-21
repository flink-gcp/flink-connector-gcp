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
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Names the unclosed splits a reader restored from its checkpointed state, sent once from {@code
 * start()}.
 *
 * <p>Flink restores those splits into the reader rather than returning them to the coordinator, so
 * after a region failover, which keeps the coordinator, this is how the coordinator learns that a
 * partition whose close it has not yet seen covered by a completed checkpoint is being read again.
 * A reader sends it before the task reports {@code RUNNING}, so it precedes every checkpoint of
 * that attempt and cannot fall between a coordinator snapshot and a barrier.
 */
@Internal
public final class PartitionsRestoredEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<String> splitIds;

    public PartitionsRestoredEvent(List<String> splitIds) {
        Preconditions.checkNotNull(splitIds, "splitIds must not be null");
        List<String> copy = new ArrayList<>(splitIds);
        Preconditions.checkArgument(!copy.contains(null), "splitIds must not contain null");
        this.splitIds = Collections.unmodifiableList(copy);
    }

    public List<String> getSplitIds() {
        return splitIds;
    }

    @Override
    public String toString() {
        return "PartitionsRestoredEvent{splitIds=" + splitIds.size() + '}';
    }
}
