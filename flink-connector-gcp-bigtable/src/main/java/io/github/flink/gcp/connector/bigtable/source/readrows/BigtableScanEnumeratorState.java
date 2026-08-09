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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * What the split enumerator checkpoints: whether the table has been sampled and planned, and the
 * splits nobody is holding yet.
 *
 * <p>There is deliberately <b>no record of which subtask holds which split</b>. A split assigned to
 * a reader that later fails comes back through {@code addSplitsBack}, and a split covered by a
 * completed checkpoint is restored by the reader that held it — so a ledger here would be a third
 * account to reconcile against those two, and the reconciliation is where a hand-written enumerator
 * loses splits silently.
 *
 * <p>{@code planned} is not the same statement as "the pending list is non-empty", and the
 * difference is what makes a restore correct: a plan that has been fully handed out leaves an empty
 * pending list and must still never be recomputed. Re-sampling would renumber the splits — a tablet
 * may have split in the meantime, so the sample boundaries move — while the readers still hold
 * splits under the old numbering, and {@code addSplitsBack} and the restored readers would then
 * disagree about which range each id names.
 */
@Internal
public final class BigtableScanEnumeratorState {

    private final boolean planned;
    private final List<RowRangeSplit> pendingSplits;

    /**
     * Creates enumerator state.
     *
     * @param planned whether the table has already been sampled and cut into splits
     * @param pendingSplits the splits not yet handed to a reader
     */
    public BigtableScanEnumeratorState(boolean planned, List<RowRangeSplit> pendingSplits) {
        Preconditions.checkNotNull(pendingSplits, "pendingSplits must not be null");
        Preconditions.checkArgument(
                planned || pendingSplits.isEmpty(),
                "an unplanned enumerator cannot hold pending splits, but it held %s",
                pendingSplits.size());
        this.planned = planned;
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
    }

    /** Returns whether the table has already been sampled and cut into splits. */
    public boolean isPlanned() {
        return planned;
    }

    /** Returns the splits not yet handed to a reader, in the order they will be handed out. */
    public List<RowRangeSplit> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BigtableScanEnumeratorState)) {
            return false;
        }
        BigtableScanEnumeratorState other = (BigtableScanEnumeratorState) o;
        return planned == other.planned && pendingSplits.equals(other.pendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planned, pendingSplits);
    }

    @Override
    public String toString() {
        return "BigtableScanEnumeratorState{planned="
                + planned
                + ", pendingSplits="
                + pendingSplits.size()
                + '}';
    }
}
