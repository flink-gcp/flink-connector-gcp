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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowRanges}. */
@Timeout(30)
class RowRangesTest {

    private static ByteString key(String text) {
        return ByteString.copyFromUtf8(text);
    }

    private static ByteString bytes(int... values) {
        byte[] raw = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            raw[i] = (byte) values[i];
        }
        return ByteString.copyFrom(raw);
    }

    private static ByteStringRange range(String startClosed, String endOpen) {
        return ByteStringRange.unbounded().startClosed(startClosed).endOpen(endOpen);
    }

    @Test
    void comparesKeysAsUnsignedBytesTheWayBigtableStoresThem() {
        // The signed ordering ByteString#compareTo would use puts 0x80 first, which is the whole
        // reason this method exists rather than a natural-order comparison.
        assertThat(RowRanges.compareKeys(bytes(0x80), bytes(0x7F))).isPositive();
        assertThat(RowRanges.compareKeys(bytes(0xFF), bytes(0x00))).isPositive();
        assertThat(RowRanges.compareKeys(ByteString.EMPTY, bytes(0x00))).isNegative();
        assertThat(RowRanges.compareKeys(key("a"), key("a"))).isZero();
    }

    @Test
    void copiesRangesRatherThanSharingThem() {
        ByteStringRange original = range("a", "z");

        ByteStringRange copy = RowRanges.copyOf(original);
        // Vendor ranges are mutable and their mutators return the receiver, so a copy that shared
        // state would let this call rewrite the original.
        copy.startClosed("m");

        assertThat(original.getStart()).isEqualTo(key("a"));
        assertThat(copy.getStart()).isEqualTo(key("m"));
    }

    @Test
    void copiesEveryBoundCombination() {
        List<ByteStringRange> originals =
                Arrays.asList(
                        ByteStringRange.unbounded(),
                        ByteStringRange.unbounded().startClosed("a"),
                        ByteStringRange.unbounded().startOpen("a"),
                        ByteStringRange.unbounded().endClosed("z"),
                        ByteStringRange.unbounded().endOpen("z"),
                        ByteStringRange.unbounded().startOpen("a").endClosed("z"));

        for (ByteStringRange original : originals) {
            assertThat(RowRanges.copyOf(original)).isEqualTo(original);
        }
    }

    @Test
    void treatsAnInvertedOrPointlessRangeAsEmpty() {
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().startClosed("z").endOpen("a")))
                .isTrue();
        assertThat(RowRanges.isEmpty(range("k", "k"))).isTrue();
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().startOpen("k").endClosed("k")))
                .isTrue();
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().startOpen("k").endOpen("k")))
                .isTrue();
    }

    @Test
    void treatsAOneKeyGapBetweenTwoOpenBoundsAsEmpty() {
        // The smallest key above an open start is that start with a 0x00 appended, and an open end
        // at exactly that key excludes it — so the range admits nothing at all.
        ByteStringRange range =
                ByteStringRange.unbounded().startOpen(key("k")).endOpen(key("k").concat(bytes(0)));

        assertThat(RowRanges.isEmpty(range)).isTrue();
        // One byte higher and the gap holds exactly one key, so it is not empty.
        assertThat(
                        RowRanges.isEmpty(
                                ByteStringRange.unbounded()
                                        .startOpen(key("k"))
                                        .endOpen(key("k").concat(bytes(1)))))
                .isFalse();
    }

    @Test
    void treatsRangesThatHoldSomethingAsNonEmpty() {
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded())).isFalse();
        assertThat(RowRanges.isEmpty(range("a", "z"))).isFalse();
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().startClosed("k").endClosed("k")))
                .isFalse();
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().startClosed("a"))).isFalse();
        assertThat(RowRanges.isEmpty(ByteStringRange.unbounded().endOpen("z"))).isFalse();
    }

    @Test
    void cutsOnlyWhereBothSidesWouldHoldSomething() {
        ByteStringRange closedOpen = range("b", "y");

        assertThat(RowRanges.cuts(closedOpen, key("m"))).isTrue();
        // At the start, either bound type, the left-hand piece would be empty.
        assertThat(RowRanges.cuts(closedOpen, key("b"))).isFalse();
        assertThat(
                        RowRanges.cuts(
                                ByteStringRange.unbounded().startOpen("b").endOpen("y"), key("b")))
                .isFalse();
        // At an exclusive end the right-hand piece would be empty.
        assertThat(RowRanges.cuts(closedOpen, key("y"))).isFalse();
        assertThat(RowRanges.cuts(closedOpen, key("a"))).isFalse();
        assertThat(RowRanges.cuts(closedOpen, key("z"))).isFalse();
    }

    @Test
    void cutsAtAnInclusiveEndBecauseThatLeavesOneRow() {
        ByteStringRange closedClosed = ByteStringRange.unbounded().startClosed("b").endClosed("y");

        assertThat(RowRanges.cuts(closedClosed, key("y"))).isTrue();
        assertThat(RowRanges.cuts(closedClosed, key("z"))).isFalse();
    }

    @Test
    void cutsAnUnboundedRangeAnywhere() {
        assertThat(RowRanges.cuts(ByteStringRange.unbounded(), key("anything"))).isTrue();
        assertThat(RowRanges.cuts(ByteStringRange.unbounded(), bytes(0))).isTrue();
    }

    @Test
    void containsClosedBoundsAndExcludesOpenBounds() {
        ByteStringRange closed = ByteStringRange.unbounded().startClosed("b").endClosed("y");
        ByteStringRange open = ByteStringRange.unbounded().startOpen("b").endOpen("y");

        assertThat(RowRanges.contains(closed, key("b"))).isTrue();
        assertThat(RowRanges.contains(closed, key("y"))).isTrue();
        assertThat(RowRanges.contains(open, key("b"))).isFalse();
        assertThat(RowRanges.contains(open, key("y"))).isFalse();
        assertThat(RowRanges.contains(open, key("m"))).isTrue();
    }

    @Test
    void containsKeysOnEitherSideOfAnUnboundedRange() {
        assertThat(RowRanges.contains(ByteStringRange.unbounded().endOpen("m"), key("a"))).isTrue();
        assertThat(RowRanges.contains(ByteStringRange.unbounded().startClosed("m"), key("z")))
                .isTrue();
    }

    @Test
    void truncatesToStartJustPastTheEmittedRowAndKeepsTheEnd() {
        ByteStringRange truncated = RowRanges.truncateStartOpen(range("a", "z"), key("m"));

        assertThat(truncated.getStartBound()).isEqualTo(BoundType.OPEN);
        assertThat(truncated.getStart()).isEqualTo(key("m"));
        assertThat(truncated.getEndBound()).isEqualTo(BoundType.OPEN);
        assertThat(truncated.getEnd()).isEqualTo(key("z"));
    }

    @Test
    void truncationPreservesAnInclusiveOrUnboundedEnd() {
        ByteStringRange closedEnd =
                RowRanges.truncateStartOpen(
                        ByteStringRange.unbounded().startClosed("a").endClosed("z"), key("m"));
        assertThat(closedEnd.getEndBound()).isEqualTo(BoundType.CLOSED);
        assertThat(closedEnd.getEnd()).isEqualTo(key("z"));

        ByteStringRange unboundedEnd =
                RowRanges.truncateStartOpen(ByteStringRange.unbounded(), key("m"));
        assertThat(unboundedEnd.getEndBound()).isEqualTo(BoundType.UNBOUNDED);
        assertThat(unboundedEnd.getStart()).isEqualTo(key("m"));
    }

    @Test
    void truncatingAtAnInclusiveEndLeavesAnEmptyRange() {
        // The normal end of a split: the last row of a closed-ended range has been emitted.
        ByteStringRange truncated =
                RowRanges.truncateStartOpen(
                        ByteStringRange.unbounded().startClosed("a").endClosed("z"), key("z"));

        assertThat(RowRanges.isEmpty(truncated)).isTrue();
    }

    @Test
    void truncationIsIdempotent() {
        ByteStringRange once = RowRanges.truncateStartOpen(range("a", "z"), key("m"));
        ByteStringRange twice = RowRanges.truncateStartOpen(once, key("m"));

        assertThat(twice).isEqualTo(once);
    }

    @Test
    void truncatesPastAnEmptyRowKeyWithoutWideningTheRange() {
        // Real Bigtable rejects an empty row key but the emulator accepts one, and the SDK turns
        // startOpen(EMPTY) into an unbounded start — which would replay the whole range forever.
        ByteStringRange truncated = RowRanges.truncateStartOpen(range("a", "z"), ByteString.EMPTY);

        assertThat(truncated.getStartBound()).isEqualTo(BoundType.CLOSED);
        assertThat(truncated.getStart()).isEqualTo(bytes(0));
        assertThat(truncated.getEnd()).isEqualTo(key("z"));
    }

    @Test
    void truncatesKeysThatAreNotText() {
        ByteString binary = bytes(0x00, 0xFF, 0x10);

        ByteStringRange truncated =
                RowRanges.truncateStartOpen(ByteStringRange.unbounded(), binary);

        assertThat(truncated.getStart()).isEqualTo(binary);
    }

    @Test
    void mergesOverlappingRanges() {
        List<ByteStringRange> merged =
                RowRanges.coalesce(Arrays.asList(range("a", "m"), range("f", "z")));

        assertThat(merged).containsExactly(range("a", "z"));
    }

    @Test
    void mergesNestedRanges() {
        // prefix("user") beside prefix("user1") is the accident this protects against: the
        // overlapping rows would otherwise be read by two subtasks and emitted twice.
        List<ByteStringRange> merged =
                RowRanges.coalesce(Arrays.asList(range("user1", "user2"), range("user", "usez")));

        assertThat(merged).containsExactly(range("user", "usez"));
    }

    @Test
    void mergesRangesThatMeetWhereTheKeyBetweenThemBelongsToOne() {
        assertThat(RowRanges.coalesce(Arrays.asList(range("a", "m"), range("m", "z"))))
                .containsExactly(range("a", "z"));
        assertThat(
                        RowRanges.coalesce(
                                Arrays.asList(
                                        ByteStringRange.unbounded().startClosed("a").endClosed("m"),
                                        ByteStringRange.unbounded().startOpen("m").endOpen("z"))))
                .containsExactly(range("a", "z"));
    }

    @Test
    void leavesRangesApartWhenTheKeyBetweenThemBelongsToNeither() {
        // Both bounds exclude "m", so merging would put a row back that the user removed.
        List<ByteStringRange> ranges =
                Arrays.asList(
                        ByteStringRange.unbounded().startClosed("a").endOpen("m"),
                        ByteStringRange.unbounded().startOpen("m").endOpen("z"));

        assertThat(RowRanges.coalesce(ranges)).hasSize(2);
    }

    @Test
    void leavesDisjointRangesAloneAndSortsThem() {
        List<ByteStringRange> merged =
                RowRanges.coalesce(
                        Arrays.asList(range("p", "q"), range("a", "b"), range("h", "i")));

        assertThat(merged).containsExactly(range("a", "b"), range("h", "i"), range("p", "q"));
    }

    @Test
    void swallowsEverythingIntoAnUnboundedRange() {
        assertThat(RowRanges.coalesce(Arrays.asList(range("a", "b"), ByteStringRange.unbounded())))
                .containsExactly(ByteStringRange.unbounded());
        assertThat(
                        RowRanges.coalesce(
                                Arrays.asList(
                                        ByteStringRange.unbounded().startClosed("a"),
                                        range("b", "c"))))
                .containsExactly(ByteStringRange.unbounded().startClosed("a"));
    }

    @Test
    void coalescingCopiesSoTheCallersRangesAreNeverTouched() {
        ByteStringRange first = range("a", "m");
        ByteStringRange second = range("f", "z");

        RowRanges.coalesce(Arrays.asList(first, second));

        assertThat(first).isEqualTo(range("a", "m"));
        assertThat(second).isEqualTo(range("f", "z"));
    }

    @Test
    void coalescesAnEmptyListToAnEmptyList() {
        assertThat(RowRanges.coalesce(java.util.Collections.emptyList())).isEmpty();
    }

    @Test
    void intersectsTwoRangeUnionsWithoutChangingTheirInputs() {
        List<ByteStringRange> left = Arrays.asList(range("a", "f"), range("m", "z"));
        List<ByteStringRange> right = Arrays.asList(range("d", "p"), range("x", "zz"));

        assertThat(RowRanges.intersect(left, right))
                .containsExactly(range("d", "f"), range("m", "p"), range("x", "z"));
        assertThat(left).containsExactly(range("a", "f"), range("m", "z"));
        assertThat(right).containsExactly(range("d", "p"), range("x", "zz"));
    }

    @Test
    void intersectionUsesTheNarrowerBoundAtTheSameKey() {
        ByteStringRange closed = ByteStringRange.unbounded().startClosed("b").endClosed("y");
        ByteStringRange open = ByteStringRange.unbounded().startOpen("b").endOpen("y");

        assertThat(
                        RowRanges.intersect(
                                java.util.Collections.singletonList(closed),
                                java.util.Collections.singletonList(open)))
                .containsExactly(open);
    }

    @Test
    void intersectionOmitsDisjointAndOpenPointRanges() {
        assertThat(
                        RowRanges.intersect(
                                java.util.Collections.singletonList(range("a", "b")),
                                java.util.Collections.singletonList(range("c", "d"))))
                .isEmpty();
        assertThat(
                        RowRanges.intersect(
                                java.util.Collections.singletonList(
                                        ByteStringRange.unbounded()
                                                .startClosed("a")
                                                .endClosed("m")),
                                java.util.Collections.singletonList(
                                        ByteStringRange.unbounded().startOpen("m").endOpen("z"))))
                .isEmpty();
    }

    @Test
    void rendersRangesReadablyForLogs() {
        assertThat(RowRanges.format(range("row-1", "row-9"))).isEqualTo("[row-1, row-9)");
        assertThat(RowRanges.format(ByteStringRange.unbounded().startOpen("a").endClosed("b")))
                .isEqualTo("(a, b]");
        assertThat(RowRanges.format(ByteStringRange.unbounded())).isEqualTo("(*, *)");
        assertThat(RowRanges.format(ByteStringRange.unbounded().startClosed(bytes(0x00, 0xFF))))
                .isEqualTo("[\\x00\\xff, *)");
    }

    @Test
    void separatesTheUnboundedSentinelFromARowKeyStar() {
        // 0x2A is printable and is also the sentinel format() prints for an absent bound, so it is
        // escaped like an unprintable byte. Without that, each pair below renders one string, and
        // an operator reading a warning cannot tell the two partitions apart.
        ByteStringRange unboundedEnd = ByteStringRange.unbounded().startClosed("a");
        ByteStringRange endsAtStar = ByteStringRange.unbounded().startClosed("a").endOpen("*");
        assertThat(RowRanges.format(unboundedEnd)).isEqualTo("[a, *)");
        assertThat(RowRanges.format(endsAtStar)).isEqualTo("[a, \\x2a)");

        ByteStringRange unboundedStart = ByteStringRange.unbounded().endOpen("z");
        ByteStringRange startsAfterStar = ByteStringRange.unbounded().startOpen("*").endOpen("z");
        assertThat(RowRanges.format(unboundedStart)).isEqualTo("(*, z)");
        assertThat(RowRanges.format(startsAfterStar)).isEqualTo("(\\x2a, z)");
    }

    @Test
    void rendersEveryDistinctRangeToItsOwnString() {
        // Injectivity as a property rather than as examples. Two things are needed for this to be
        // able to fail for the reason its name gives, and the first draft had only one of them.
        //
        // Every byte escape() spends on structure must appear inside a key: "\", "*" and ",".
        // And for the comma the alphabet must also be *closed under the split* — a key holding
        // ", " only collides with the pair of keys either side of it, so "a, b" and "b, c" catch
        // nothing unless "b" and "c" are here to be a bound on their own. Dropping them lets the
        // comma escape be deleted with this test still green, which is how the control caught it.
        List<String> keys = Arrays.asList("a", "b", "c", "*", ",", "\\", "a, b", "b, c", "*x");
        List<ByteStringRange> ranges = new ArrayList<>();
        ranges.add(ByteStringRange.unbounded());
        for (String start : keys) {
            ranges.add(ByteStringRange.unbounded().startClosed(start));
            ranges.add(ByteStringRange.unbounded().startOpen(start));
            ranges.add(ByteStringRange.unbounded().endClosed(start));
            ranges.add(ByteStringRange.unbounded().endOpen(start));
            for (String end : keys) {
                ranges.add(ByteStringRange.unbounded().startClosed(start).endOpen(end));
                ranges.add(ByteStringRange.unbounded().startOpen(start).endClosed(end));
            }
        }
        ranges.add(ByteStringRange.create(ByteString.EMPTY, key("m")));
        ranges.add(ByteStringRange.create(key("m"), ByteString.EMPTY));

        Map<String, ByteStringRange> byRendering = new LinkedHashMap<>();
        for (ByteStringRange range : ranges) {
            String rendering = RowRanges.format(range);
            ByteStringRange clash = byRendering.put(rendering, range);
            // Only distinct ranges count as a clash: the loops build some ranges twice, and an
            // empty key normalises to an unbounded bound whichever setter produced it.
            if (clash != null) {
                assertThat(clash).as("two ranges render as %s", rendering).isEqualTo(range);
            }
        }
    }

    @Test
    void copyOfFoldsTheEmptyKeySpellingTheServiceUsesIntoAnUnboundedBound() {
        // ByteStringRange.create is how every Change Streams partition and continuation token
        // reaches this connector, and unlike the setters it does not fold an empty key. copyOf
        // rebuilds through the setters, which is the connector's one normalisation point, and the
        // reconciler's correctness rests on it.
        ByteStringRange serviceFirst = ByteStringRange.create(ByteString.EMPTY, key("m"));
        ByteStringRange serviceLast = ByteStringRange.create(key("m"), ByteString.EMPTY);
        assertThat(serviceFirst.getStartBound()).isEqualTo(BoundType.CLOSED);
        assertThat(serviceLast.getEndBound()).isEqualTo(BoundType.OPEN);
        assertThat(serviceFirst).isNotEqualTo(ByteStringRange.unbounded().endOpen("m"));

        assertThat(RowRanges.copyOf(serviceFirst))
                .isEqualTo(ByteStringRange.unbounded().endOpen("m"));
        assertThat(RowRanges.copyOf(serviceLast))
                .isEqualTo(ByteStringRange.unbounded().startClosed("m"));
        assertThat(RowRanges.copyOf(ByteStringRange.create(ByteString.EMPTY, ByteString.EMPTY)))
                .isEqualTo(ByteStringRange.unbounded());
    }

    @Test
    void reportsWhetherABoundIsAbsent() {
        assertThat(RowRanges.isUnboundedStart(ByteStringRange.unbounded().endOpen("m"))).isTrue();
        assertThat(RowRanges.isUnboundedStart(range("a", "m"))).isFalse();
        assertThat(RowRanges.isUnboundedEnd(ByteStringRange.unbounded().startClosed("m"))).isTrue();
        assertThat(RowRanges.isUnboundedEnd(range("a", "m"))).isFalse();
    }

    @Test
    void sameStartAndSameEndSeparateBoundTypeAsWellAsKey() {
        assertThat(RowRanges.sameStart(range("a", "m"), range("a", "z"))).isTrue();
        assertThat(RowRanges.sameStart(range("a", "m"), range("b", "m"))).isFalse();
        assertThat(
                        RowRanges.sameStart(
                                ByteStringRange.unbounded().startClosed("a"),
                                ByteStringRange.unbounded().startOpen("a")))
                .isFalse();
        assertThat(
                        RowRanges.sameStart(
                                ByteStringRange.unbounded(),
                                ByteStringRange.unbounded().endOpen("m")))
                .isTrue();
        assertThat(RowRanges.sameStart(ByteStringRange.unbounded(), range("a", "m"))).isFalse();

        assertThat(RowRanges.sameEnd(range("a", "m"), range("b", "m"))).isTrue();
        assertThat(
                        RowRanges.sameEnd(
                                ByteStringRange.unbounded().endOpen("m"),
                                ByteStringRange.unbounded().endClosed("m")))
                .isFalse();
        assertThat(
                        RowRanges.sameEnd(
                                ByteStringRange.unbounded(),
                                ByteStringRange.unbounded().startClosed("a")))
                .isTrue();
        assertThat(RowRanges.sameEnd(ByteStringRange.unbounded(), range("a", "m"))).isFalse();
    }

    @Test
    void sameStartAndSameEndAgreeWithTheComparatorsOverEveryBoundShape() {
        // sameStart's javadoc claims it is the equality compareStarts induces. Measured over every
        // pair of bound shapes rather than asserted.
        List<ByteStringRange> shapes =
                Arrays.asList(
                        ByteStringRange.unbounded(),
                        ByteStringRange.unbounded().startClosed("m"),
                        ByteStringRange.unbounded().startOpen("m"),
                        ByteStringRange.unbounded().startClosed("z"),
                        ByteStringRange.unbounded().startOpen("z"),
                        ByteStringRange.unbounded().endOpen("m"),
                        ByteStringRange.unbounded().endClosed("m"),
                        range("a", "m"),
                        range("a", "z"));

        for (ByteStringRange left : shapes) {
            for (ByteStringRange right : shapes) {
                assertThat(RowRanges.sameStart(left, right))
                        .as("sameStart %s %s", RowRanges.format(left), RowRanges.format(right))
                        .isEqualTo(RowRanges.compareStarts(left, right) == 0);
                assertThat(RowRanges.sameEnd(left, right))
                        .as("sameEnd %s %s", RowRanges.format(left), RowRanges.format(right))
                        .isEqualTo(RowRanges.compareEnds(left, right) == 0);
            }
        }
    }

    @Test
    void ordersEqualKeysByWhichBoundReachesFurther() {
        ByteStringRange closedStart = ByteStringRange.unbounded().startClosed("m");
        ByteStringRange openStart = ByteStringRange.unbounded().startOpen("m");
        // An inclusive start begins first — the opposite of BoundType's own declaration order
        // (OPEN, CLOSED, UNBOUNDED), so a comparator keyed on the enum's ordinal sorts these the
        // other way round. That is what the enumerator's deleted copy used to do.
        assertThat(RowRanges.compareStarts(closedStart, openStart)).isNegative();
        assertThat(RowRanges.compareStarts(openStart, closedStart)).isPositive();
        assertThat(BoundType.OPEN.compareTo(BoundType.CLOSED)).isNegative();

        // An inclusive end reaches further.
        ByteStringRange openEnd = ByteStringRange.unbounded().endOpen("m");
        ByteStringRange closedEnd = ByteStringRange.unbounded().endClosed("m");
        assertThat(RowRanges.compareEnds(openEnd, closedEnd)).isNegative();
        assertThat(RowRanges.compareEnds(closedEnd, openEnd)).isPositive();

        // An unbounded start sorts first; an unbounded end sorts last.
        assertThat(RowRanges.compareStarts(ByteStringRange.unbounded(), closedStart)).isNegative();
        assertThat(RowRanges.compareEnds(ByteStringRange.unbounded(), closedEnd)).isPositive();
    }
}
