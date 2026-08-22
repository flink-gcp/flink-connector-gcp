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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/** Mutable coordinator-thread accumulator for one pending merge target. */
final class PendingMergeAccumulator {

    private static final Comparator<TokenEntry> TOKEN_ORDER =
            PendingMergeAccumulator::compareEntries;

    private final ByteStringRange partition;
    private final NavigableSet<TokenEntry> tokens = new TreeSet<>(TOKEN_ORDER);
    private Instant lowWatermark;
    private int disconnectedPairs;

    /**
     * How many neighbour comparisons {@link #add} has made, counted because its bound is the
     * property worth holding: an arriving token is compared against its two neighbours and against
     * the pair those two formed without it, so three per token, and an accumulator handed <i>n</i>
     * parents stays linear in <i>n</i>. A rewrite that rescanned the set would still produce the
     * right merge and would only read as a slow job; the test's ceiling of {@code 3n} on this
     * counter is what turns it into a failure. It is also why {@link #connected} is an instance
     * method where every other comparison this class needs is a static on {@code RowRanges}.
     */
    private long adjacencyEvaluations;

    /**
     * How many times a checkpoint has been taken of this accumulator, counted so the test can hold
     * the other half of the same bound: accumulating must not materialize, and a checkpoint must
     * materialize once.
     */
    private long materializations;

    PendingMergeAccumulator(ByteStringRange partition, Instant lowWatermark) {
        this.partition =
                RowRanges.copyOf(
                        Preconditions.checkNotNull(partition, "partition must not be null"));
        this.lowWatermark =
                Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
    }

    static PendingMergeAccumulator restore(PendingMerge merge) {
        PendingMergeAccumulator accumulator =
                new PendingMergeAccumulator(merge.getPartition(), merge.getLowWatermark());
        for (ChangeStreamContinuationToken token : merge.getContinuationTokens()) {
            accumulator.add(token, merge.getLowWatermark());
        }
        return accumulator;
    }

    void add(ChangeStreamContinuationToken token, Instant parentLowWatermark) {
        Preconditions.checkNotNull(token, "token must not be null");
        Preconditions.checkNotNull(parentLowWatermark, "parentLowWatermark must not be null");
        if (parentLowWatermark.isBefore(lowWatermark)) {
            lowWatermark = parentLowWatermark;
        }

        TokenEntry entry = new TokenEntry(token);
        TokenEntry previous = tokens.lower(entry);
        TokenEntry next = tokens.higher(entry);
        if (!tokens.add(entry)) {
            return;
        }
        if (previous != null && next != null && !connected(previous.partition, next.partition)) {
            disconnectedPairs--;
        }
        if (previous != null && !connected(previous.partition, entry.partition)) {
            disconnectedPairs++;
        }
        if (next != null && !connected(entry.partition, next.partition)) {
            disconnectedPairs++;
        }
    }

    boolean isComplete() {
        return !tokens.isEmpty()
                && disconnectedPairs == 0
                && RowRanges.sameStart(tokens.first().partition, partition)
                && RowRanges.sameEnd(tokens.last().partition, partition);
    }

    ByteStringRange partitionKey() {
        return RowRanges.copyOf(partition);
    }

    Instant getLowWatermark() {
        return lowWatermark;
    }

    boolean tokensAreContainedBy(List<ChangeStreamContinuationToken> candidates) {
        if (candidates.size() < tokens.size()) {
            return false;
        }
        for (TokenEntry token : tokens) {
            if (!candidates.contains(token.token)) {
                return false;
            }
        }
        return true;
    }

    PendingMerge toPendingMerge() {
        materializations++;
        List<ChangeStreamContinuationToken> snapshot = new ArrayList<>(tokens.size());
        for (TokenEntry entry : tokens) {
            snapshot.add(entry.token);
        }
        return new PendingMerge(partition, snapshot, lowWatermark);
    }

    @VisibleForTesting
    long getAdjacencyEvaluations() {
        return adjacencyEvaluations;
    }

    @VisibleForTesting
    long getMaterializations() {
        return materializations;
    }

    private boolean connected(ByteStringRange left, ByteStringRange right) {
        adjacencyEvaluations++;
        return !RowRanges.isUnboundedEnd(left)
                && !RowRanges.isUnboundedStart(right)
                && left.getEnd().equals(right.getStart())
                && left.getEndBound() != right.getStartBound();
    }

    private static int compareEntries(TokenEntry left, TokenEntry right) {
        int result = RowRanges.compareStarts(left.partition, right.partition);
        if (result != 0) {
            return result;
        }
        result = RowRanges.compareEnds(left.partition, right.partition);
        if (result != 0) {
            return result;
        }
        return left.token.getToken().compareTo(right.token.getToken());
    }

    private static final class TokenEntry {

        private final ChangeStreamContinuationToken token;
        private final ByteStringRange partition;

        private TokenEntry(ChangeStreamContinuationToken token) {
            this.token = token;
            this.partition = RowRanges.copyOf(token.getPartition());
        }
    }
}
