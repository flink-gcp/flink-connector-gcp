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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RowRangeSplitPlanner}.
 *
 * <p>Every case is aimed at the planner quietly producing <em>less</em> than it should, because a
 * plan that loses the tail of a table reads exactly like a clean run: the job succeeds, and the
 * rows that were never read leave no trace. That is why the coverage assertions compare
 * reconstructed ranges rather than counting splits.
 */
@Timeout(30)
class RowRangeSplitPlannerTest {

    private static ByteString key(String text) {
        return ByteString.copyFromUtf8(text);
    }

    private static ByteStringRange range(String startClosed, String endOpen) {
        return ByteStringRange.unbounded().startClosed(startClosed).endOpen(endOpen);
    }

    private static RowKeySample sample(String key, long offsetBytes) {
        return RowKeySample.of(key(key), offsetBytes);
    }

    private static List<ByteStringRange> rangesOf(List<PlannedSplit> planned) {
        return planned.stream().map(p -> p.getSplit().getRange()).collect(Collectors.toList());
    }

    /**
     * Asserts that the pieces tile the range: they start where it starts, end where it ends, and
     * each one begins exactly where the previous one stopped.
     */
    private static void assertTiles(List<ByteStringRange> pieces, ByteStringRange whole) {
        assertThat(pieces).isNotEmpty();
        ByteStringRange first = pieces.get(0);
        assertThat(first.getStartBound()).isEqualTo(whole.getStartBound());
        if (whole.getStartBound() != BoundType.UNBOUNDED) {
            assertThat(first.getStart()).isEqualTo(whole.getStart());
        }
        ByteStringRange last = pieces.get(pieces.size() - 1);
        assertThat(last.getEndBound()).isEqualTo(whole.getEndBound());
        if (whole.getEndBound() != BoundType.UNBOUNDED) {
            assertThat(last.getEnd()).isEqualTo(whole.getEnd());
        }
        for (int i = 1; i < pieces.size(); i++) {
            ByteStringRange previous = pieces.get(i - 1);
            ByteStringRange next = pieces.get(i);
            assertThat(previous.getEndBound()).isEqualTo(BoundType.OPEN);
            assertThat(next.getStartBound()).isEqualTo(BoundType.CLOSED);
            assertThat(next.getStart()).isEqualTo(previous.getEnd());
        }
    }

    @Test
    void plansOneSplitPerRangeWhenNothingWasSampled() {
        // What the emulator always produces, and what a small table produces on the service.
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Collections.emptyList());

        assertThat(rangesOf(planned)).containsExactly(ByteStringRange.unbounded());
        assertThat(planned.get(0).getEstimatedBytes()).isEmpty();
    }

    @Test
    void cutsTheWholeTableAtEverySampledKey() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400)));

        assertThat(rangesOf(planned))
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("c"),
                        range("c", "m"),
                        range("m", "t"),
                        ByteStringRange.unbounded().startClosed("t"));
        assertTiles(rangesOf(planned), ByteStringRange.unbounded());
    }

    @Test
    void leavesARangeSpanningNoBoundaryAsOneSplit() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(range("d", "j")),
                        Arrays.asList(sample("c", 100), sample("m", 250)));

        assertThat(rangesOf(planned)).containsExactly(range("d", "j"));
    }

    @Test
    void keepsTheRangesOwnBoundsOnTheOuterPieces() {
        ByteStringRange whole = ByteStringRange.unbounded().startOpen("a").endClosed("z");

        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(whole),
                        Collections.singletonList(sample("m", 100)));

        assertThat(rangesOf(planned))
                .containsExactly(
                        ByteStringRange.unbounded().startOpen("a").endOpen("m"),
                        ByteStringRange.unbounded().startClosed("m").endClosed("z"));
        assertTiles(rangesOf(planned), whole);
    }

    @Test
    void doesNotCutAtAKeyEqualToTheRangesStart() {
        // Either bound type: the left-hand piece would hold nothing.
        assertThat(
                        RowRangeSplitPlanner.plan(
                                Collections.singletonList(range("m", "z")),
                                Collections.singletonList(sample("m", 100))))
                .hasSize(1);
        assertThat(
                        RowRangeSplitPlanner.plan(
                                Collections.singletonList(
                                        ByteStringRange.unbounded().startOpen("m").endOpen("z")),
                                Collections.singletonList(sample("m", 100))))
                .hasSize(1);
    }

    @Test
    void doesNotCutAtAnExclusiveEndButDoesCutAtAnInclusiveOne() {
        assertThat(
                        RowRangeSplitPlanner.plan(
                                Collections.singletonList(range("a", "m")),
                                Collections.singletonList(sample("m", 100))))
                .hasSize(1);

        // An inclusive end leaves a right-hand piece holding exactly the row at the boundary.
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(
                                ByteStringRange.unbounded().startClosed("a").endClosed("m")),
                        Collections.singletonList(sample("m", 100)));

        assertThat(rangesOf(planned))
                .containsExactly(
                        range("a", "m"),
                        ByteStringRange.unbounded().startClosed("m").endClosed("m"));
    }

    @Test
    void dropsTheEndOfTableMarkerRatherThanCuttingAtIt() {
        // An empty key is the service's "end of table" response, not a boundary. Cutting at it
        // would produce a split the SDK itself refuses to build.
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("m", 100), RowKeySample.of(ByteString.EMPTY, 200)));

        assertThat(planned).hasSize(2);
    }

    @Test
    void plansOneSplitWhenTheOnlySampleIsTheEndOfTableMarker() {
        // The service's documented answer for a table with no boundaries to offer. The emulator
        // this project pins answers such a table with no samples at all instead, so both shapes
        // have to reach the same plan.
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Collections.singletonList(RowKeySample.of(ByteString.EMPTY, 0)));

        assertThat(rangesOf(planned)).containsExactly(ByteStringRange.unbounded());
    }

    @Test
    void sortsSamplesThatArriveOutOfOrder() {
        List<PlannedSplit> unsorted =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("t", 400), sample("c", 100), sample("m", 250)));

        assertThat(rangesOf(unsorted))
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("c"),
                        range("c", "m"),
                        range("m", "t"),
                        ByteStringRange.unbounded().startClosed("t"));
    }

    @Test
    void collapsesDuplicateSamplesInsteadOfProducingAnEmptySplit() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("m", 250), sample("m", 250)));

        assertThat(planned).hasSize(2);
        assertThat(rangesOf(planned)).noneMatch(RowRanges::isEmpty);
    }

    @Test
    void sortsKeysAsUnsignedBytes() {
        // 0x80 sorts after 0x7f in Bigtable; a signed comparison would order these the other way
        // and the pieces would not tile the table.
        ByteString low = ByteString.copyFrom(new byte[] {(byte) 0x7F});
        ByteString high = ByteString.copyFrom(new byte[] {(byte) 0x80});

        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(RowKeySample.of(high, 200), RowKeySample.of(low, 100)));

        assertTiles(rangesOf(planned), ByteStringRange.unbounded());
        assertThat(rangesOf(planned).get(0).getEnd()).isEqualTo(low);
    }

    @Test
    void cutsSeveralRangesIndependentlyAndNumbersEverySplit() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Arrays.asList(range("a", "f"), range("p", "z")),
                        Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400)));

        assertThat(rangesOf(planned))
                .containsExactly(
                        range("a", "c"), range("c", "f"), range("p", "t"), range("t", "z"));
        assertThat(planned.stream().map(p -> p.getSplit().splitId()))
                .containsExactly("0", "1", "2", "3");
    }

    @Test
    void neverPlansAnEmptySplit() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Arrays.asList(range("a", "f"), range("f", "m"), range("m", "z")),
                        Arrays.asList(
                                sample("a", 10),
                                sample("f", 100),
                                sample("m", 250),
                                sample("z", 400)));

        assertThat(rangesOf(planned)).noneMatch(RowRanges::isEmpty);
    }

    @Test
    void handlesARangeLyingWhollyBeforeOrAfterEverySample() {
        assertThat(
                        rangesOf(
                                RowRangeSplitPlanner.plan(
                                        Collections.singletonList(range("a", "b")),
                                        Collections.singletonList(sample("m", 100)))))
                .containsExactly(range("a", "b"));
        assertThat(
                        rangesOf(
                                RowRangeSplitPlanner.plan(
                                        Collections.singletonList(range("x", "z")),
                                        Collections.singletonList(sample("m", 100)))))
                .containsExactly(range("x", "z"));
    }

    @Test
    void estimatesEachSectionFromTheOffsetDeltaAndLeavesTheTailUnknown() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400)));

        assertThat(planned.get(0).getEstimatedBytes()).hasValue(100);
        assertThat(planned.get(1).getEstimatedBytes()).hasValue(150);
        assertThat(planned.get(2).getEstimatedBytes()).hasValue(150);
        // Past the last boundary the samples say nothing, and reporting zero would make the most
        // likely place for a skewed table to hide look like the emptiest.
        assertThat(planned.get(3).getEstimatedBytes()).isEmpty();
    }

    @Test
    void estimatesAgainstTheSectionARangeStartsInRatherThanTheFirstOne() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(range("d", "z")),
                        Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400)));

        // [d, m) lies in the section [c, m), which the samples price at 250 - 100.
        assertThat(planned.get(0).getEstimatedBytes()).hasValue(150);
        assertThat(planned.get(1).getEstimatedBytes()).hasValue(150);
        assertThat(planned.get(2).getEstimatedBytes()).isEmpty();
    }

    @Test
    void estimatesARangeStartingExactlyAtABoundaryAgainstTheSectionThatBoundaryOpens() {
        // A range whose start is a sampled key lies in the section that *begins* there, not in the
        // one that ends there. Getting this off by one is invisible in the splits and wrong in the
        // only number that reports a skewed table before the job runs.
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(range("m", "z")),
                        Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400)));

        // [m, t) is the section [m, t), priced at 400 - 250.
        assertThat(planned.get(0).getEstimatedBytes()).hasValue(150);
        assertThat(planned.get(1).getEstimatedBytes()).isEmpty();
    }

    @Test
    void refusesToEstimateWhenTheOffsetsDoNotIncrease() {
        List<PlannedSplit> planned =
                RowRangeSplitPlanner.plan(
                        Collections.singletonList(ByteStringRange.unbounded()),
                        Arrays.asList(sample("c", 500), sample("m", 100)));

        assertThat(planned.get(0).getEstimatedBytes()).hasValue(500);
        assertThat(planned.get(1).getEstimatedBytes()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void agreesWithTheClientLibrarysOwnSharding() {
        // The design's "Beam's approach" is also the approach the Bigtable client ships as
        // Query#shard. Pinning the two together turns a client upgrade that changes the cut
        // convention into a failing test rather than into splits that silently disagree with the
        // service's own idea of a tablet boundary. KeyOffset#create is @InternalApi and is used
        // here, in test code, for exactly that comparison.
        List<ByteStringRange> configured =
                Arrays.asList(
                        ByteStringRange.unbounded(),
                        range("a", "z"),
                        ByteStringRange.unbounded().startOpen("c").endClosed("t"),
                        ByteStringRange.unbounded().startClosed("m"));
        List<RowKeySample> samples =
                Arrays.asList(sample("c", 100), sample("m", 250), sample("t", 400));
        List<KeyOffset> keyOffsets = new ArrayList<>();
        for (RowKeySample s : samples) {
            keyOffsets.add(KeyOffset.create(s.getKey(), s.getOffsetBytes()));
        }

        for (ByteStringRange range : configured) {
            List<ByteStringRange> ours =
                    rangesOf(RowRangeSplitPlanner.plan(Collections.singletonList(range), samples));
            List<ByteStringRange> theirs =
                    Query.create(TableId.of("t"))
                            .range(RowRanges.copyOf(range))
                            .shard(keyOffsets)
                            .stream()
                            .map(Query::getBound)
                            .collect(Collectors.toList());

            assertThat(ours).as("shards of %s", RowRanges.format(range)).isEqualTo(theirs);
        }
    }

    @Test
    void planningTheSameInputTwiceGivesTheSameSplits() {
        // The ids have to be stable for a job that restarts before its first checkpoint, and the
        // ranges have to be, since the plan is what a reader resumes against.
        List<ByteStringRange> ranges = Collections.singletonList(range("a", "z"));
        List<RowKeySample> samples = Arrays.asList(sample("c", 100), sample("m", 250));

        List<RowRangeSplit> first =
                RowRangeSplitPlanner.plan(ranges, samples).stream()
                        .map(PlannedSplit::getSplit)
                        .collect(Collectors.toList());
        List<RowRangeSplit> second =
                RowRangeSplitPlanner.plan(ranges, samples).stream()
                        .map(PlannedSplit::getSplit)
                        .collect(Collectors.toList());

        assertThat(second).isEqualTo(first);
    }

    @Test
    void doesNotMutateTheRangesItWasGiven() {
        ByteStringRange configured = range("a", "z");

        RowRangeSplitPlanner.plan(
                Collections.singletonList(configured), Collections.singletonList(sample("m", 100)));

        assertThat(configured).isEqualTo(range("a", "z"));
    }
}
