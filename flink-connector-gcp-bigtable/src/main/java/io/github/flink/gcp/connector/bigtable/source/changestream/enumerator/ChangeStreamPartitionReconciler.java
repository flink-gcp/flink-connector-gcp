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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
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
            List<PendingMerge> pendingMerges,
            List<MissingPartition> previous,
            Instant now,
            Instant fallbackLowWatermark) {
        List<ByteStringRange> ledgerRanges = new ArrayList<>();
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
            if (RowRanges.format(partition).equals(RowRanges.format(missing.getPartition()))) {
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
                if (containedBy(token.getPartition(), target) && !tokens.contains(token)) {
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
        sorted.sort(ChangeStreamPartitionReconciler::compareStarts);
        ByteString cursor = isUnboundedStart(target) ? null : target.getStart();
        ByteString targetEnd = isUnboundedEnd(target) ? null : target.getEnd();
        List<ByteStringRange> gaps = new ArrayList<>();
        for (ByteStringRange ledger : sorted) {
            ByteString ledgerStart = isUnboundedStart(ledger) ? null : ledger.getStart();
            ByteString ledgerEnd = isUnboundedEnd(ledger) ? null : ledger.getEnd();
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

    private static int compareStarts(ByteStringRange left, ByteStringRange right) {
        if (isUnboundedStart(left)) {
            return isUnboundedStart(right) ? 0 : -1;
        }
        if (isUnboundedStart(right)) {
            return 1;
        }
        return RowRanges.compareKeys(left.getStart(), right.getStart());
    }

    static boolean tokensCover(ByteStringRange target, List<ChangeStreamContinuationToken> tokens) {
        List<ByteStringRange> ranges = new ArrayList<>();
        for (ChangeStreamContinuationToken token : tokens) {
            ranges.add(token.getPartition());
        }
        return rangesCover(target, ranges);
    }

    private static boolean rangesCover(ByteStringRange target, List<ByteStringRange> ranges) {
        if (ranges.isEmpty()) {
            return false;
        }
        ranges.sort(
                (left, right) -> {
                    if (isUnboundedStart(left)) {
                        return isUnboundedStart(right) ? 0 : -1;
                    }
                    if (isUnboundedStart(right)) {
                        return 1;
                    }
                    return RowRanges.compareKeys(left.getStart(), right.getStart());
                });
        if (!sameStart(ranges.get(0), target)) {
            return false;
        }
        for (int i = 1; i < ranges.size(); i++) {
            ByteStringRange left = ranges.get(i - 1);
            ByteStringRange right = ranges.get(i);
            if (!left.getEnd().equals(right.getStart())) {
                return false;
            }
        }
        return sameEnd(ranges.get(ranges.size() - 1), target);
    }

    private static boolean containedBy(ByteStringRange inner, ByteStringRange outer) {
        boolean startsInside =
                isUnboundedStart(outer)
                        || (!isUnboundedStart(inner)
                                && RowRanges.compareKeys(inner.getStart(), outer.getStart()) >= 0);
        boolean endsInside =
                isUnboundedEnd(outer)
                        || (!isUnboundedEnd(inner)
                                && RowRanges.compareKeys(inner.getEnd(), outer.getEnd()) <= 0);
        return startsInside && endsInside;
    }

    private static boolean sameStart(ByteStringRange left, ByteStringRange right) {
        return (isUnboundedStart(left) && isUnboundedStart(right))
                || (left.getStartBound() == right.getStartBound()
                        && left.getStart().equals(right.getStart()));
    }

    private static boolean sameEnd(ByteStringRange left, ByteStringRange right) {
        return (isUnboundedEnd(left) && isUnboundedEnd(right))
                || (left.getEndBound() == right.getEndBound()
                        && left.getEnd().equals(right.getEnd()));
    }

    private static boolean isUnboundedStart(ByteStringRange range) {
        return range.getStartBound() == BoundType.UNBOUNDED || range.getStart().isEmpty();
    }

    private static boolean isUnboundedEnd(ByteStringRange range) {
        return range.getEndBound() == BoundType.UNBOUNDED || range.getEnd().isEmpty();
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
