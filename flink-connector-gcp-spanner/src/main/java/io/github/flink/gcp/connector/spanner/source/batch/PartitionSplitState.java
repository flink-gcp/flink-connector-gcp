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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * The mutable reader-side state of a {@link PartitionSplit}.
 *
 * <p>It holds no progress, and that is the design rather than an omission. Spanner offers no way to
 * resume a partition part-way: {@code execute(partition)} replays the whole partition, and the row
 * order it comes back in is not contractual for a partitioned query — a root-partitionable query
 * cannot carry a top-level {@code ORDER BY}, so nothing a reader could count would still mean the
 * same thing on a second execution. A checkpoint therefore records which partitions a reader still
 * holds, and a restore re-reads each of them whole.
 *
 * <p>The class exists all the same, because the reader base is generic over a split state and
 * because the next thing anyone will want to add here is a resume point — which should meet this
 * javadoc first.
 */
@Internal
public final class PartitionSplitState {

    private final PartitionSplit split;

    /**
     * Creates the state of a split a reader has just been assigned.
     *
     * @param split the split
     */
    public PartitionSplitState(PartitionSplit split) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
    }

    /**
     * Returns the split to checkpoint, which is the split as it was assigned.
     *
     * @return the split
     */
    public PartitionSplit toSplit() {
        return split;
    }
}
