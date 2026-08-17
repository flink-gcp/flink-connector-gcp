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
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

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
    private final List<MissingPartition> missingPartitions;
    private final List<ByteStringRange> completedPartitions;

    public BigtableChangeStreamEnumeratorState(
            boolean initialized,
            Instant startTime,
            long nextSplitId,
            List<ChangeStreamPartitionSplit> unassignedSplits,
            List<ChangeStreamPartitionSplit> assignedSplits,
            List<PendingMerge> pendingMerges) {
        this(
                initialized,
                startTime,
                nextSplitId,
                unassignedSplits,
                assignedSplits,
                pendingMerges,
                Collections.emptyList());
    }

    public BigtableChangeStreamEnumeratorState(
            boolean initialized,
            Instant startTime,
            long nextSplitId,
            List<ChangeStreamPartitionSplit> unassignedSplits,
            List<ChangeStreamPartitionSplit> assignedSplits,
            List<PendingMerge> pendingMerges,
            List<MissingPartition> missingPartitions) {
        this(
                initialized,
                startTime,
                nextSplitId,
                unassignedSplits,
                assignedSplits,
                pendingMerges,
                missingPartitions,
                Collections.emptyList());
    }

    public BigtableChangeStreamEnumeratorState(
            boolean initialized,
            Instant startTime,
            long nextSplitId,
            List<ChangeStreamPartitionSplit> unassignedSplits,
            List<ChangeStreamPartitionSplit> assignedSplits,
            List<PendingMerge> pendingMerges,
            List<MissingPartition> missingPartitions,
            List<ByteStringRange> completedPartitions) {
        Preconditions.checkArgument(nextSplitId >= 0, "nextSplitId must not be negative");
        Preconditions.checkNotNull(unassignedSplits, "unassignedSplits must not be null");
        Preconditions.checkNotNull(assignedSplits, "assignedSplits must not be null");
        Preconditions.checkNotNull(pendingMerges, "pendingMerges must not be null");
        Preconditions.checkNotNull(missingPartitions, "missingPartitions must not be null");
        Preconditions.checkNotNull(completedPartitions, "completedPartitions must not be null");
        Preconditions.checkArgument(
                initialized || (unassignedSplits.isEmpty() && assignedSplits.isEmpty()),
                "an uninitialized enumerator cannot hold partitions");
        this.initialized = initialized;
        this.startTime = Preconditions.checkNotNull(startTime, "startTime must not be null");
        this.nextSplitId = nextSplitId;
        this.unassignedSplits = immutableCopy(unassignedSplits);
        this.assignedSplits = immutableCopy(assignedSplits);
        this.pendingMerges = Collections.unmodifiableList(new ArrayList<>(pendingMerges));
        this.missingPartitions = Collections.unmodifiableList(new ArrayList<>(missingPartitions));
        this.completedPartitions =
                Collections.unmodifiableList(RowRanges.copyAll(completedPartitions));
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

    public List<MissingPartition> getMissingPartitions() {
        return missingPartitions;
    }

    /**
     * Returns the ranges a bounded run has already read to its end time.
     *
     * <p>These are the ledger's finished business rather than its live work. The reconciler counts
     * them as covered, because the service reports a partition's range for as long as the table
     * exists, while a bounded run hands that range back exactly once (#951).
     *
     * @return the completed ranges, empty for a continuous run
     */
    public List<ByteStringRange> getCompletedPartitions() {
        return RowRanges.copyAll(completedPartitions);
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
                && pendingMerges.equals(other.pendingMerges)
                && missingPartitions.equals(other.missingPartitions)
                && completedPartitions.equals(other.completedPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                initialized,
                startTime,
                nextSplitId,
                unassignedSplits,
                assignedSplits,
                pendingMerges,
                missingPartitions,
                completedPartitions);
    }
}
