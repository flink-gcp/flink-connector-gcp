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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * What the split enumerator checkpoints: whether the read has been planned, and the splits nobody
 * is holding yet.
 *
 * <p>There is deliberately <b>no record of which subtask holds which split</b>. A split assigned to
 * a reader that later fails comes back through {@code addSplitsBack}, and a split covered by a
 * completed checkpoint is restored by the reader that held it — so a ledger here would be a third
 * account to reconcile against those two.
 *
 * <p>The snapshot itself is not a field. Every split carries the {@code BatchTransactionId} it
 * reads at, because a reader is handed splits and not this state; storing it a second time here
 * would be a copy that a restore could disagree with.
 *
 * <p>{@code planned} is not the same statement as "the pending list is non-empty", and the
 * difference is what makes a restore correct: a plan that has been fully handed out leaves an empty
 * pending list and must still never be recomputed. Planning again would open a <em>second</em>
 * batch transaction, at a second timestamp, and hand out partitions of it under split ids the
 * readers already hold — so one job would read two snapshots and call the result consistent.
 */
@Internal
public final class SpannerBatchEnumeratorState {

    private final boolean planned;
    private final List<PartitionSplit> pendingSplits;

    /**
     * Creates enumerator state.
     *
     * @param planned whether the read has already been planned into partitions
     * @param pendingSplits the splits not yet handed to a reader
     */
    public SpannerBatchEnumeratorState(boolean planned, List<PartitionSplit> pendingSplits) {
        Preconditions.checkNotNull(pendingSplits, "pendingSplits must not be null");
        Preconditions.checkArgument(
                planned || pendingSplits.isEmpty(),
                "an unplanned enumerator cannot hold pending splits, but it held %s",
                pendingSplits.size());
        this.planned = planned;
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
    }

    /**
     * Returns whether the read has already been planned into partitions.
     *
     * @return whether the read is planned
     */
    public boolean isPlanned() {
        return planned;
    }

    /**
     * Returns the splits not yet handed to a reader, in the order they will be handed out.
     *
     * @return the pending splits
     */
    public List<PartitionSplit> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SpannerBatchEnumeratorState)) {
            return false;
        }
        SpannerBatchEnumeratorState other = (SpannerBatchEnumeratorState) o;
        return planned == other.planned && pendingSplits.equals(other.pendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planned, pendingSplits);
    }

    @Override
    public String toString() {
        return "SpannerBatchEnumeratorState{planned="
                + planned
                + ", pendingSplits="
                + pendingSplits.size()
                + '}';
    }
}
