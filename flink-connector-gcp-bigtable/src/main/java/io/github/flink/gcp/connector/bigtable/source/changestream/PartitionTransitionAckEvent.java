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

/**
 * Tells a reader that the coordinator has applied its {@link PartitionTransitionEvent}, so the
 * reader may drop the closed split from its checkpointed state.
 *
 * <p>The coordinator sends it as soon as it applies the transition. Flink holds a coordinator's
 * events to a subtask back between the coordinator's checkpoint and that subtask's barrier, so a
 * transition the coordinator applied after its snapshot is still in the reader's snapshot of the
 * same checkpoint.
 */
@Internal
public final class PartitionTransitionAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;

    public PartitionTransitionAckEvent(String splitId) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
    }

    public String getSplitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "PartitionTransitionAckEvent{splitId='" + splitId + "'}";
    }
}
