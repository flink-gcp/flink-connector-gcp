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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Successor partition parked until its parent tokens cover the whole target range. */
@Internal
public final class PendingMerge {

    private final ByteStringRange partition;
    private final List<ChangeStreamContinuationToken> continuationTokens;
    private final Instant lowWatermark;

    public PendingMerge(
            ByteStringRange partition,
            List<ChangeStreamContinuationToken> continuationTokens,
            Instant lowWatermark) {
        this.partition =
                RowRanges.copyOf(
                        Preconditions.checkNotNull(partition, "partition must not be null"));
        this.continuationTokens =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        continuationTokens,
                                        "continuationTokens must not be null")));
        this.lowWatermark =
                Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
    }

    public ByteStringRange getPartition() {
        return RowRanges.copyOf(partition);
    }

    public List<ChangeStreamContinuationToken> getContinuationTokens() {
        return continuationTokens;
    }

    public Instant getLowWatermark() {
        return lowWatermark;
    }

    public PendingMerge add(ChangeStreamContinuationToken token, Instant parentLowWatermark) {
        List<ChangeStreamContinuationToken> combined = new ArrayList<>(continuationTokens);
        if (!combined.contains(token)) {
            combined.add(token);
        }
        Instant combinedLowWatermark =
                parentLowWatermark.isBefore(lowWatermark) ? parentLowWatermark : lowWatermark;
        return new PendingMerge(partition, combined, combinedLowWatermark);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PendingMerge)) {
            return false;
        }
        PendingMerge other = (PendingMerge) o;
        return partition.equals(other.partition)
                && continuationTokens.equals(other.continuationTokens)
                && lowWatermark.equals(other.lowWatermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, continuationTokens, lowWatermark);
    }
}
