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
import java.util.Objects;

/** Checkpointed timer for a service partition absent from the coordinator ledger. */
@Internal
public final class MissingPartition {

    private final ByteStringRange partition;
    private final Instant firstObserved;
    private final Instant lowWatermark;

    public MissingPartition(
            ByteStringRange partition, Instant firstObserved, Instant lowWatermark) {
        this.partition = RowRanges.copyOf(partition);
        this.firstObserved =
                Preconditions.checkNotNull(firstObserved, "firstObserved must not be null");
        this.lowWatermark =
                Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
    }

    public ByteStringRange getPartition() {
        return RowRanges.copyOf(partition);
    }

    public Instant getFirstObserved() {
        return firstObserved;
    }

    public Instant getLowWatermark() {
        return lowWatermark;
    }

    /**
     * Returns a copy restarting at {@code lowWatermark}, keeping the {@code firstObserved} timer.
     */
    public MissingPartition restartAt(Instant lowWatermark) {
        return new MissingPartition(partition, firstObserved, lowWatermark);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MissingPartition)) {
            return false;
        }
        MissingPartition other = (MissingPartition) object;
        return partition.equals(other.partition)
                && firstObserved.equals(other.firstObserved)
                && lowWatermark.equals(other.lowWatermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, firstObserved, lowWatermark);
    }
}
