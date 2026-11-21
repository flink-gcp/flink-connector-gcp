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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/** Mutable task-thread state for one change-stream partition. */
@Internal
public final class ChangeStreamPartitionSplitState {

    private final ChangeStreamPartitionSplit original;
    private List<ChangeStreamContinuationToken> tokens;
    private Instant lowWatermark;
    @Nullable private List<PartitionTransitionEvent.Successor> closeSuccessors;

    /** Whether this attempt has emitted a record from the split; deliberately not checkpointed. */
    private boolean emittedRecords;

    public ChangeStreamPartitionSplitState(ChangeStreamPartitionSplit split) {
        this.original = split;
        this.tokens = split.getContinuationTokens();
        this.lowWatermark = split.getLowWatermark();
    }

    public ChangeStreamPartitionSplit toSplit() {
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        original.splitId(), original.getPartition(), tokens, lowWatermark);
        return closeSuccessors == null ? split : split.closedWith(closeSuccessors);
    }

    /**
     * Records the partition's {@code CloseStream}, which leaves the position where it was.
     *
     * @param successors the successors the record named
     */
    public void close(List<PartitionTransitionEvent.Successor> successors) {
        this.closeSuccessors = successors;
    }

    public void advance(ChangeStreamContinuationToken token, Instant watermark) {
        this.tokens = Collections.singletonList(token);
        this.lowWatermark = watermark;
    }

    /** Records that the reader emitted at least one output record from this split. */
    public void markEmittedRecords() {
        emittedRecords = true;
    }

    /**
     * Returns whether this attempt emitted any output record from the split. Records an earlier
     * attempt emitted before the restored checkpoint have reached downstream already, so only this
     * attempt's output can still be on its way when the split closes.
     */
    public boolean hasEmittedRecords() {
        return emittedRecords;
    }

    public List<ChangeStreamContinuationToken> getTokens() {
        return tokens;
    }

    public Instant getLowWatermark() {
        return lowWatermark;
    }
}
