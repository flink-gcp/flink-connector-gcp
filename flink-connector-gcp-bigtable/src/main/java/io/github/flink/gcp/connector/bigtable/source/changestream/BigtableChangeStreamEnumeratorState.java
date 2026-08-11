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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Checkpointed coordinator state for the Bigtable Change Streams partition topology. */
@Internal
public final class BigtableChangeStreamEnumeratorState {

    private final boolean initialized;
    private final Instant startTime;
    private final long nextSplitId;
    private final List<ChangeStreamPartitionSplit> unassignedSplits;
    private final List<ChangeStreamPartitionSplit> assignedSplits;
    private final List<PendingMerge> pendingMerges;

    public BigtableChangeStreamEnumeratorState(
            boolean initialized,
            Instant startTime,
            long nextSplitId,
            List<ChangeStreamPartitionSplit> unassignedSplits,
            List<ChangeStreamPartitionSplit> assignedSplits,
            List<PendingMerge> pendingMerges) {
        Preconditions.checkArgument(nextSplitId >= 0, "nextSplitId must not be negative");
        Preconditions.checkNotNull(unassignedSplits, "unassignedSplits must not be null");
        Preconditions.checkNotNull(assignedSplits, "assignedSplits must not be null");
        Preconditions.checkNotNull(pendingMerges, "pendingMerges must not be null");
        Preconditions.checkArgument(
                initialized || (unassignedSplits.isEmpty() && assignedSplits.isEmpty()),
                "an uninitialized enumerator cannot hold partitions");
        this.initialized = initialized;
        this.startTime = Preconditions.checkNotNull(startTime, "startTime must not be null");
        this.nextSplitId = nextSplitId;
        this.unassignedSplits = immutableCopy(unassignedSplits);
        this.assignedSplits = immutableCopy(assignedSplits);
        this.pendingMerges = Collections.unmodifiableList(new ArrayList<>(pendingMerges));
    }

    private static List<ChangeStreamPartitionSplit> immutableCopy(
            List<ChangeStreamPartitionSplit> splits) {
        return Collections.unmodifiableList(new ArrayList<>(splits));
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public long getNextSplitId() {
        return nextSplitId;
    }

    public List<ChangeStreamPartitionSplit> getUnassignedSplits() {
        return unassignedSplits;
    }

    public List<ChangeStreamPartitionSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public List<PendingMerge> getPendingMerges() {
        return pendingMerges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BigtableChangeStreamEnumeratorState)) {
            return false;
        }
        BigtableChangeStreamEnumeratorState other = (BigtableChangeStreamEnumeratorState) o;
        return initialized == other.initialized
                && nextSplitId == other.nextSplitId
                && startTime.equals(other.startTime)
                && unassignedSplits.equals(other.unassignedSplits)
                && assignedSplits.equals(other.assignedSplits)
                && pendingMerges.equals(other.pendingMerges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                initialized,
                startTime,
                nextSplitId,
                unassignedSplits,
                assignedSplits,
                pendingMerges);
    }
}
