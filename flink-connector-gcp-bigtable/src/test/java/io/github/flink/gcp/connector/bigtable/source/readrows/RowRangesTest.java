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

import java.util.Arrays;
import java.util.List;

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
}
