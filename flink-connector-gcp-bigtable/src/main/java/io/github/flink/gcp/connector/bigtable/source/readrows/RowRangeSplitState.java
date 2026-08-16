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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

/**
 * How far through one {@link RowRangeSplit} the source has emitted.
 *
 * <p>Separate from the split because the two are touched by different threads: the split reader
 * runs on the fetcher thread and holds the split it was handed, while the record emitter runs on
 * the task thread and advances this state per row. A single mutable type would put a field the task
 * thread writes inside an object the fetcher thread reads.
 *
 * <p>It is also a different clock from the one the split reader keeps. This state tracks the last
 * row <em>successfully deserialized</em>, including one that produced no output, which is what a
 * checkpoint must record; the split reader tracks the last row it handed to the element queue,
 * which is what a reopen must resume from. The rows between the two are in flight at any instant,
 * so neither key can serve the other's purpose: reopening at this one would hand every in-flight
 * row over twice inside a single successful run, and checkpointing the reader's one would drop them
 * on restore.
 *
 * <p>Not thread-safe, and does not need to be: every method here is called from the task thread.
 */
@Internal
public final class RowRangeSplitState {

    private final RowRangeSplit split;

    @Nullable private ByteString lastEmittedKey;

    /**
     * Creates the state of a freshly assigned or restored split.
     *
     * <p>It starts with no progress, whatever the split's history: a restored split already carries
     * its previous progress in the range it was truncated to.
     *
     * @param split the split being read
     */
    public RowRangeSplitState(RowRangeSplit split) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
    }

    /**
     * Records that a row has been successfully deserialized.
     *
     * <p>Called once per row the split reader produced, whatever the deserializer made of it —
     * nothing, one record, or several. The unit of progress is the row, because the range is
     * resumed at a row key rather than at a count, so a row that produced no record still has to
     * move the resume point or a restore replays it together with everything after it.
     *
     * @param rowKey the key of the row that was handed to the emitter
     */
    public void recordEmitted(ByteString rowKey) {
        this.lastEmittedKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
    }

    /** Returns the key of the last successfully deserialized row, or null when none has been. */
    @VisibleForTesting
    @Nullable
    public ByteString getLastEmittedKey() {
        return lastEmittedKey;
    }

    /**
     * Returns the split as it should be checkpointed: the work that is left.
     *
     * @return the assigned split unchanged when no row has been successfully deserialized,
     *     otherwise a split over the range starting just past the last such row
     */
    public RowRangeSplit toSplit() {
        if (lastEmittedKey == null) {
            return split;
        }
        return new RowRangeSplit(
                split.splitId(), RowRanges.truncateStartOpen(split.getRange(), lastEmittedKey));
    }
}
