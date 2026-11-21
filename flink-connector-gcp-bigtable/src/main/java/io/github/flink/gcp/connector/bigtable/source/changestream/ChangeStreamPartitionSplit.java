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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.RowRanges;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One Bigtable Change Streams partition and the exact position from which it resumes.
 *
 * <p>A split whose reader has seen the partition's {@code CloseStream} also carries the successors
 * that record named, until the coordinator acknowledges the transition: a reader keeps such a split
 * in its checkpointed state so that a restore can report the transition again.
 */
@Internal
public final class ChangeStreamPartitionSplit implements SourceSplit {

    private final String splitId;
    private final ByteStringRange partition;
    private final List<ChangeStreamContinuationToken> continuationTokens;
    private final Instant lowWatermark;
    @Nullable private final List<PartitionTransitionEvent.Successor> closeSuccessors;

    public ChangeStreamPartitionSplit(
            String splitId,
            ByteStringRange partition,
            List<ChangeStreamContinuationToken> continuationTokens,
            Instant lowWatermark) {
        this(splitId, partition, continuationTokens, lowWatermark, null);
    }

    private ChangeStreamPartitionSplit(
            String splitId,
            ByteStringRange partition,
            List<ChangeStreamContinuationToken> continuationTokens,
            Instant lowWatermark,
            @Nullable List<PartitionTransitionEvent.Successor> closeSuccessors) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
        this.partition =
                RowRanges.copyOf(
                        Preconditions.checkNotNull(partition, "partition must not be null"));
        Preconditions.checkNotNull(continuationTokens, "continuationTokens must not be null");
        this.continuationTokens = Collections.unmodifiableList(new ArrayList<>(continuationTokens));
        this.lowWatermark =
                Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
        this.closeSuccessors =
                closeSuccessors == null
                        ? null
                        : Collections.unmodifiableList(new ArrayList<>(closeSuccessors));
    }

    @Override
    public String splitId() {
        return splitId;
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

    public ChangeStreamPartitionSplit restartAt(Instant startTime) {
        Preconditions.checkState(!isClosed(), "a closed split %s cannot restart", splitId);
        return new ChangeStreamPartitionSplit(
                splitId, partition, Collections.emptyList(), startTime);
    }

    /**
     * Returns this split as its reader saw the partition's {@code CloseStream}.
     *
     * @param successors the successors the record named, empty for a partition that ended
     * @return the closed split
     */
    public ChangeStreamPartitionSplit closedWith(
            List<PartitionTransitionEvent.Successor> successors) {
        Preconditions.checkNotNull(successors, "successors must not be null");
        return new ChangeStreamPartitionSplit(
                splitId, partition, continuationTokens, lowWatermark, successors);
    }

    /** Whether the reader has seen this partition's {@code CloseStream}. */
    public boolean isClosed() {
        return closeSuccessors != null;
    }

    /**
     * Returns the transition this closed split reports.
     *
     * @param emittedRecords whether the reporting reader attempt emitted a record for this split
     * @return the event naming this split, its low watermark and its successors
     */
    public PartitionTransitionEvent toTransitionEvent(boolean emittedRecords) {
        Preconditions.checkState(isClosed(), "split %s has not closed", splitId);
        return new PartitionTransitionEvent(splitId, lowWatermark, closeSuccessors, emittedRecords);
    }

    /**
     * Returns this split without its close, at the position it closed at.
     *
     * @return the open split
     */
    public ChangeStreamPartitionSplit reopened() {
        return new ChangeStreamPartitionSplit(splitId, partition, continuationTokens, lowWatermark);
    }

    /** Returns the successors a closed split's {@code CloseStream} named. */
    public List<PartitionTransitionEvent.Successor> getCloseSuccessors() {
        Preconditions.checkState(isClosed(), "split %s has not closed", splitId);
        return closeSuccessors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChangeStreamPartitionSplit)) {
            return false;
        }
        ChangeStreamPartitionSplit other = (ChangeStreamPartitionSplit) o;
        return splitId.equals(other.splitId)
                && partition.equals(other.partition)
                && continuationTokens.equals(other.continuationTokens)
                && lowWatermark.equals(other.lowWatermark)
                && Objects.equals(closeSuccessors, other.closeSuccessors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, partition, continuationTokens, lowWatermark, closeSuccessors);
    }

    @Override
    public String toString() {
        return "ChangeStreamPartitionSplit{splitId='"
                + splitId
                + "', partition="
                + RowRanges.format(partition)
                + ", tokens="
                + continuationTokens.size()
                + ", lowWatermark="
                + lowWatermark
                + (closeSuccessors == null ? "" : ", closed, successors=" + closeSuccessors.size())
                + '}';
    }
}
