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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable task-thread progress for one Spanner Change Streams partition.
 *
 * <p>Progress stops at the last record before the first child-partitions record, as the Debezium
 * Spanner connector's offset does. Spanner ends a partition at its children's start timestamp, so a
 * restored query must start before it to meet the child-partitions records again; what Spanner
 * returns for a query that starts exactly there is neither documented nor measured. The watermark
 * stops with the position, so a restored query that repeats records from that position never emits
 * them behind its own watermark. The cost is that a restore repeats the records from that position
 * on (ADR-0101).
 */
@Internal
public final class ChangeStreamPartitionSplitState {

    private final ChangeStreamPartitionSplit original;
    private Instant currentPosition;
    private Instant watermark;
    private final List<ChildPartitionsEvent> childReports = new ArrayList<>();

    /** Whether this attempt has emitted a record from the split; deliberately not checkpointed. */
    private boolean emittedRecords;

    public ChangeStreamPartitionSplitState(ChangeStreamPartitionSplit split) {
        this.original = Preconditions.checkNotNull(split, "split must not be null");
        this.currentPosition = split.getCurrentPosition();
        this.watermark = split.getWatermark();
    }

    public ChangeStreamPartitionSplit toSplit() {
        return original.withProgress(currentPosition, watermark);
    }

    public String splitId() {
        return original.splitId();
    }

    public boolean isInitialPartition() {
        return original.getPartitionToken() == null;
    }

    public void advance(Instant position, Instant newWatermark) {
        Instant advancedPosition = later(currentPosition, position, "position");
        Instant advancedWatermark = later(watermark, newWatermark, "watermark");
        if (childReports.isEmpty()) {
            currentPosition = advancedPosition;
            watermark = advancedWatermark;
        }
    }

    /**
     * Records a child-partitions report this attempt forwarded to the coordinator. The finished
     * split carries every one of them until the coordinator acknowledges the finish.
     */
    public void recordChildren(ChildPartitionsEvent report) {
        childReports.add(Preconditions.checkNotNull(report, "report must not be null"));
    }

    /** Returns the split as a reader keeps it after its query ended, until the acknowledgement. */
    public ChangeStreamPartitionSplit toFinishedSplit() {
        return toSplit().withFinishReport(childReports);
    }

    /** Records that the reader emitted at least one output record from this split. */
    public void markEmittedRecords() {
        emittedRecords = true;
    }

    /**
     * Returns whether this attempt emitted any output record from the split. Records an earlier
     * attempt emitted before the restored checkpoint have reached downstream already, so only this
     * attempt's count.
     */
    public boolean hasEmittedRecords() {
        return emittedRecords;
    }

    public Instant getCurrentPosition() {
        return currentPosition;
    }

    public Instant getWatermark() {
        return watermark;
    }

    private static Instant later(Instant current, Instant candidate, String name) {
        Preconditions.checkNotNull(candidate, name + " must not be null");
        return candidate.isAfter(current) ? candidate : current;
    }
}
