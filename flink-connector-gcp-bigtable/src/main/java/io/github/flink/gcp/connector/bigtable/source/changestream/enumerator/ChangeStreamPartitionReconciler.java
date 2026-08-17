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

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Pure keyspace reconciliation used by the enumerator's periodic service scan. */
@Internal
final class ChangeStreamPartitionReconciler {

    static final Duration TOKEN_GRACE = Duration.ofMinutes(2);
    static final Duration TOKENLESS_GRACE = Duration.ofMinutes(20);

    Result reconcile(
            List<ByteStringRange> servicePartitions,
            List<ChangeStreamPartitionSplit> ledger,
            List<ByteStringRange> completed,
            List<PendingMerge> pendingMerges,
            List<MissingPartition> previous,
            Instant now,
            Instant fallbackLowWatermark) {
        // A completed range covers the keyspace as firmly as a live split does. The service goes on
        // reporting that keyspace for as long as the table exists, but a bounded run hands its
        // range
        // back once, at the end time; without this the run's own success reads as a gap (#951).
        List<ByteStringRange> ledgerRanges = new ArrayList<>(completed);
        for (ChangeStreamPartitionSplit split : ledger) {
            ledgerRanges.add(split.getPartition());
        }
        List<MissingPartition> remaining = new ArrayList<>();
        List<Recovery> recoveries = new ArrayList<>();
        for (ByteStringRange servicePartition : servicePartitions) {
            List<ByteStringRange> gaps = uncovered(servicePartition, ledgerRanges);
            if (gaps.isEmpty()) {
                continue;
            }
            MissingPartition missing = find(servicePartition, previous);
            if (missing == null) {
                missing = new MissingPartition(servicePartition, now, fallbackLowWatermark);
            }
            Duration absentFor = Duration.between(missing.getFirstObserved(), now);
            boolean recoveredEveryGap = true;
            for (ByteStringRange gap : gaps) {
                List<ChangeStreamContinuationToken> compatible =
                        compatibleTokens(gap, pendingMerges);
                if (absentFor.compareTo(TOKEN_GRACE) >= 0 && tokensCover(gap, compatible)) {
                    recoveries.add(
                            new Recovery(
                                    gap,
                                    compatible,
                                    lowWatermark(compatible, pendingMerges, missing),
                                    false));
                } else if (absentFor.compareTo(TOKENLESS_GRACE) >= 0) {
                    recoveries.add(
                            new Recovery(
                                    gap, Collections.emptyList(), missing.getLowWatermark(), true));
                } else {
                    recoveredEveryGap = false;
                }
            }
            if (!recoveredEveryGap) {
                remaining.add(missing);
            }
        }
        return new Result(remaining, recoveries);
    }

    private static MissingPartition find(
            ByteStringRange partition, List<MissingPartition> missingPartitions) {
        for (MissingPartition missing : missingPartitions) {
            // Exact range equality, not RowRanges.format(): that renderer prints "*" both for an
            // unbounded bound and for a bound at the row key "*" (0x2A), so comparing renderings
            // could hand a partition another one's grace timer and low watermark.
            if (partition.equals(missing.getPartition())) {
                return missing;
            }
        }
        return null;
    }

    static List<ChangeStreamContinuationToken> compatibleTokens(
            ByteStringRange target, List<PendingMerge> merges) {
        List<ChangeStreamContinuationToken> tokens = new ArrayList<>();
        for (PendingMerge merge : merges) {
            for (ChangeStreamContinuationToken token : merge.getContinuationTokens()) {
                // copyOf, because a token's partition comes straight from the proto through
                // ByteStringRange.create and spells an absent bound as an empty key rather than as
                // UNBOUNDED. See RowRanges.copyOf.
                if (containedBy(RowRanges.copyOf(token.getPartition()), target)
                        && !tokens.contains(token)) {
                    tokens.add(token);
                }
            }
        }
        return tokens;
    }

    private static Instant lowWatermark(
            List<ChangeStreamContinuationToken> tokens,
            List<PendingMerge> merges,
            MissingPartition missing) {
        Instant result = missing.getLowWatermark();
        for (PendingMerge merge : merges) {
            for (ChangeStreamContinuationToken token : merge.getContinuationTokens()) {
                if (tokens.contains(token) && merge.getLowWatermark().isBefore(result)) {
                    result = merge.getLowWatermark();
                }
            }
        }
        return result;
    }

    private static List<ByteStringRange> uncovered(
            ByteStringRange target, List<ByteStringRange> ledgerRanges) {
        List<ByteStringRange> sorted = new ArrayList<>(ledgerRanges);
        sorted.sort(RowRanges::compareStarts);
        ByteString cursor = RowRanges.isUnboundedStart(target) ? null : target.getStart();
        ByteString targetEnd = RowRanges.isUnboundedEnd(target) ? null : target.getEnd();
        List<ByteStringRange> gaps = new ArrayList<>();
        for (ByteStringRange ledger : sorted) {
            ByteString ledgerStart = RowRanges.isUnboundedStart(ledger) ? null : ledger.getStart();
            ByteString ledgerEnd = RowRanges.isUnboundedEnd(ledger) ? null : ledger.getEnd();
            if (ledgerEnd != null
                    && cursor != null
                    && RowRanges.compareKeys(ledgerEnd, cursor) <= 0) {
                continue;
            }
            if (targetEnd != null
                    && ledgerStart != null
                    && RowRanges.compareKeys(ledgerStart, targetEnd) >= 0) {
                break;
            }
            if (ledgerStart != null
                    && (cursor == null || RowRanges.compareKeys(ledgerStart, cursor) > 0)) {
                gaps.add(range(cursor, min(ledgerStart, targetEnd)));
            }
            if (ledgerEnd == null) {
                return gaps;
            }
            if (cursor == null || RowRanges.compareKeys(ledgerEnd, cursor) > 0) {
                cursor = ledgerEnd;
            }
            if (targetEnd != null && RowRanges.compareKeys(cursor, targetEnd) >= 0) {
                return gaps;
            }
        }
        if (targetEnd == null || cursor == null || RowRanges.compareKeys(cursor, targetEnd) < 0) {
            gaps.add(range(cursor, targetEnd));
        }
        return gaps;
    }

    private static ByteString min(ByteString left, ByteString right) {
        return right == null || RowRanges.compareKeys(left, right) <= 0 ? left : right;
    }

    private static ByteStringRange range(ByteString start, ByteString end) {
        ByteStringRange range = ByteStringRange.unbounded();
        if (start != null) {
            range.startClosed(start);
        }
        if (end != null) {
            range.endOpen(end);
        }
        return range;
    }

    static boolean tokensCover(ByteStringRange target, List<ChangeStreamContinuationToken> tokens) {
        List<ByteStringRange> ranges = new ArrayList<>();
        for (ChangeStreamContinuationToken token : tokens) {
            ranges.add(RowRanges.copyOf(token.getPartition()));
        }
        return rangesCover(target, ranges);
    }

    private static boolean rangesCover(ByteStringRange target, List<ByteStringRange> ranges) {
        if (ranges.isEmpty()) {
            return false;
        }
        ranges.sort(RowRanges::compareStarts);
        if (!RowRanges.sameStart(ranges.get(0), target)) {
            return false;
        }
        for (int i = 1; i < ranges.size(); i++) {
            ByteStringRange left = ranges.get(i - 1);
            ByteStringRange right = ranges.get(i);
            if (!left.getEnd().equals(right.getStart())) {
                return false;
            }
        }
        return RowRanges.sameEnd(ranges.get(ranges.size() - 1), target);
    }

    private static boolean containedBy(ByteStringRange inner, ByteStringRange outer) {
        boolean startsInside =
                RowRanges.isUnboundedStart(outer)
                        || (!RowRanges.isUnboundedStart(inner)
                                && RowRanges.compareKeys(inner.getStart(), outer.getStart()) >= 0);
        boolean endsInside =
                RowRanges.isUnboundedEnd(outer)
                        || (!RowRanges.isUnboundedEnd(inner)
                                && RowRanges.compareKeys(inner.getEnd(), outer.getEnd()) <= 0);
        return startsInside && endsInside;
    }

    static final class Result {
        final List<MissingPartition> missing;
        final List<Recovery> recoveries;

        Result(List<MissingPartition> missing, List<Recovery> recoveries) {
            this.missing = missing;
            this.recoveries = recoveries;
        }
    }

    static final class Recovery {
        final ByteStringRange partition;
        final List<ChangeStreamContinuationToken> tokens;
        final Instant lowWatermark;
        final boolean tokenless;

        Recovery(
                ByteStringRange partition,
                List<ChangeStreamContinuationToken> tokens,
                Instant lowWatermark,
                boolean tokenless) {
            this.partition = RowRanges.copyOf(partition);
            this.tokens = new ArrayList<>(tokens);
            this.lowWatermark = lowWatermark;
            this.tokenless = tokenless;
        }
    }
}
