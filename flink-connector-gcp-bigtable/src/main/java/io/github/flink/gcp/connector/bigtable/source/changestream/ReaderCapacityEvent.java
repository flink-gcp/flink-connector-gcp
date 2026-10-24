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

/** One reader's free read slots, the splits it holds, and the splits it has received. */
@Internal
public final class ReaderCapacityEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    private final int freeSlots;
    private final int heldSplits;
    private final long receivedSplits;

    /**
     * Creates the event.
     *
     * @param freeSlots the reads this reader can still open, after its queued splits
     * @param heldSplits its queued splits plus its active reads that have not seen CloseStream
     * @param receivedSplits the splits assigned to this reader attempt since it started
     */
    public ReaderCapacityEvent(int freeSlots, int heldSplits, long receivedSplits) {
        Preconditions.checkArgument(freeSlots >= 0, "freeSlots must not be negative");
        Preconditions.checkArgument(heldSplits >= 0, "heldSplits must not be negative");
        Preconditions.checkArgument(receivedSplits >= 0, "receivedSplits must not be negative");
        this.freeSlots = freeSlots;
        this.heldSplits = heldSplits;
        this.receivedSplits = receivedSplits;
    }

    /** The reads this reader can still open, after its queued splits. */
    public int getFreeSlots() {
        return freeSlots;
    }

    /** Its queued splits plus its active reads that have not seen CloseStream. */
    public int getHeldSplits() {
        return heldSplits;
    }

    /** The splits assigned to this reader attempt since it started. */
    public long getReceivedSplits() {
        return receivedSplits;
    }
}
