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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * Cuts the configured row-key ranges at the boundaries the service sampled.
 *
 * <p>A pure function, and deliberately so: this is where the source's parallelism comes from, the
 * emulator's {@code SampleRowKeys} is too degenerate to exercise it, and running against the real
 * service costs a billed instance — so correctness here has to be a unit test.
 *
 * <p>The approach is the one Apache Beam's {@code BigtableIO} takes, and also the one the Bigtable
 * client library itself ships as {@code Query#shard}: the sampled keys are section boundaries, and
 * every configured range is cut wherever a boundary falls strictly inside it. Cutting at a boundary
 * on the exclusive side of a bound would produce an empty piece, which is why {@link
 * RowRanges#cuts} decides rather than a bare comparison. The convention for the two pieces a cut
 * produces — the left one ending exclusively at the boundary, the right one starting inclusively at
 * it — is the client's own, so a split boundary here and a split boundary there agree.
 *
 * <p>{@code Query#shard} is not used as the implementation. It answers with request segments, each
 * of which may hold several disjoint ranges when two configured ranges fall inside one section,
 * whereas a split here holds exactly one range; cutting per range gives a strictly finer plan and
 * never produces a split that spans a gap the user excluded. It is used as a test oracle instead.
 *
 * <p>Interpreting the sample list is this class's job too, so that it happens once: a sample whose
 * key is empty is the service's "end of table" marker rather than a boundary and is dropped,
 * duplicates are collapsed, and the order is made total. The service documents its samples sorted
 * and distinct; this normalisation costs nothing and means a planner test can state what a
 * misbehaving sampler produces.
 */
@Internal
public final class RowRangeSplitPlanner {

    private RowRangeSplitPlanner() {}

    /**
     * Plans the splits for a scan.
     *
     * @param ranges the configured ranges, already normalised and coalesced by the builder, in key
     *     order and none of them empty
     * @param samples what {@code SampleRowKeys} answered, in any order
     * @return one planned split per (range, section) piece, in key order, with ordinal ids
     */
    public static List<PlannedSplit> plan(
            List<ByteStringRange> ranges, List<RowKeySample> samples) {
        Preconditions.checkNotNull(ranges, "ranges must not be null");
        Preconditions.checkNotNull(samples, "samples must not be null");

        List<ByteString> splitPoints = new ArrayList<>(samples.size());
        List<Long> offsets = new ArrayList<>(samples.size());
        normalise(samples, splitPoints, offsets);

        List<PlannedSplit> planned = new ArrayList<>();
        for (ByteStringRange range : ranges) {
            cut(range, splitPoints, offsets, planned);
        }
        return planned;
    }

    /**
     * Collapses the sample list into ascending, distinct split points and their offsets.
     *
     * <p>A duplicated key keeps the larger offset, which is the one that keeps the offsets
     * non-decreasing — the property the size estimates are subtracted from.
     */
    private static void normalise(
            List<RowKeySample> samples, List<ByteString> splitPoints, List<Long> offsets) {
        Map<ByteString, Long> byKey = new TreeMap<>(RowRanges::compareKeys);
        for (RowKeySample sample : samples) {
            if (sample.getKey().isEmpty()) {
                // The service's end-of-table marker, not a boundary between two sections.
                continue;
            }
            byKey.merge(sample.getKey(), sample.getOffsetBytes(), Math::max);
        }
        for (Map.Entry<ByteString, Long> entry : byKey.entrySet()) {
            splitPoints.add(entry.getKey());
            offsets.add(entry.getValue());
        }
    }

    /**
     * Cuts one range at every split point strictly inside it, appending the pieces in key order.
     */
    private static void cut(
            ByteStringRange range,
            List<ByteString> splitPoints,
            List<Long> offsets,
            List<PlannedSplit> planned) {
        List<ByteString> interior = new ArrayList<>();
        for (ByteString splitPoint : splitPoints) {
            if (RowRanges.cuts(range, splitPoint)) {
                interior.add(splitPoint);
            }
        }

        // Each piece lies wholly within one section, because the range is cut at every boundary
        // that falls inside it. The first piece's section is the one holding the range's start;
        // the pieces after it run through the following sections one by one.
        int section = sectionOfRangeStart(range, splitPoints);

        ByteStringRange remaining = RowRanges.copyOf(range);
        for (ByteString splitPoint : interior) {
            ByteStringRange piece = RowRanges.copyOf(remaining).endOpen(splitPoint);
            planned.add(newSplit(planned.size(), piece, sectionBytes(section, offsets)));
            remaining.startClosed(splitPoint);
            section++;
        }
        planned.add(newSplit(planned.size(), remaining, sectionBytes(section, offsets)));
    }

    /**
     * Returns the index of the section a range begins in; an unbounded start begins in the first.
     */
    private static int sectionOfRangeStart(ByteStringRange range, List<ByteString> splitPoints) {
        if (range.getStartBound() == BoundType.UNBOUNDED) {
            return 0;
        }
        int section = 0;
        for (ByteString splitPoint : splitPoints) {
            if (RowRanges.compareKeys(splitPoint, range.getStart()) > 0) {
                break;
            }
            section++;
        }
        return section;
    }

    /**
     * Returns the approximate size of one section.
     *
     * <p>Section {@code i} runs from split point {@code i - 1} to split point {@code i}, so its
     * size is the difference between their cumulative offsets, with the section before the first
     * boundary measured from zero. The section past the last boundary has no second offset to
     * subtract and is therefore unknown, as is any section whose offsets do not increase — which
     * would mean the service answered something the arithmetic cannot be trusted on.
     */
    private static OptionalLong sectionBytes(int section, List<Long> offsets) {
        if (section >= offsets.size()) {
            return OptionalLong.empty();
        }
        long end = offsets.get(section);
        long start = section == 0 ? 0L : offsets.get(section - 1);
        return end < start ? OptionalLong.empty() : OptionalLong.of(end - start);
    }

    private static PlannedSplit newSplit(
            int ordinal, ByteStringRange range, OptionalLong estimatedBytes) {
        return new PlannedSplit(
                new RowRangeSplit(Integer.toString(ordinal), range), estimatedBytes);
    }
}
