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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The row-key range algebra the scan source is built on: one definition of "empty", one of "these
 * two ranges can be merged", one of "this key cuts this range".
 *
 * <p>Collected here rather than spread over the builder, the planner, the split state and the split
 * reader, because the four would otherwise each have to get the bound types right and would each
 * get them wrong differently. Row keys are compared as <em>unsigned</em> bytes, which is the order
 * Bigtable stores them in; the natural ordering of {@link ByteString} is not that order, and a
 * signed comparison sorts every key whose first byte is above {@code 0x7F} before every key whose
 * first byte is below it.
 *
 * <p>Three facts about the vendor's {@link ByteStringRange}, measured against google-cloud-bigtable
 * 2.80.0 on 2026-08-09 and relied on below:
 *
 * <ul>
 *   <li><b>Ranges are mutable.</b> {@code startClosed}/{@code endOpen} and their siblings assign to
 *       the receiver and return it, despite javadoc reading "Creates a new Range". Every range this
 *       connector hands out or stores is therefore built by {@link #copyOf}, and no range the user
 *       supplied is ever mutated.
 *   <li><b>{@code clone()} is not public API.</b> It is {@code protected} on the package-private
 *       superclass, so a copy has to be rebuilt from the four accessors — which is what {@link
 *       #copyOf} does.
 *   <li><b>An empty key on any bound is normalised to {@code UNBOUNDED} by the SDK itself.</b>
 *       {@code startClosed(EMPTY)}, {@code startOpen(EMPTY)}, {@code endOpen(EMPTY)} and {@code
 *       endClosed(EMPTY)} all produce an unbounded side. That is why this class never has to
 *       special-case the empty key on input — and why {@link #truncateStartOpen} has to
 *       special-case it on <em>output</em>, where silently widening a range to the whole table
 *       would make a restored split re-read everything it had already emitted.
 * </ul>
 */
@Internal
public final class RowRanges {

    /**
     * The smallest non-empty row key.
     *
     * <p>Row keys are ordered as unsigned byte strings, so the empty key sorts before every other
     * key and the single {@code 0x00} byte is its immediate successor. That makes "every key after
     * the empty key" exactly "every key from {@code 0x00} onwards", which is how {@link
     * #truncateStartOpen} expresses progress past an empty row key.
     */
    private static final ByteString SMALLEST_NON_EMPTY_KEY = ByteString.copyFrom(new byte[] {0});

    private static final Comparator<ByteString> KEYS =
            ByteString.unsignedLexicographicalComparator();

    private RowRanges() {}

    /**
     * Compares two row keys in Bigtable's own order.
     *
     * @param left the first key
     * @param right the second key
     * @return a negative number, zero or a positive number as {@code left} sorts before, at or
     *     after {@code right}
     */
    public static int compareKeys(ByteString left, ByteString right) {
        return KEYS.compare(left, right);
    }

    /**
     * Returns an independent copy of a range.
     *
     * <p>Every range that crosses into this connector — from a builder setter, from the planner,
     * from a deserialised split — goes through here, because a {@link ByteStringRange} is mutable
     * and shared references would let a caller change a plan after it was made.
     *
     * @param range the range to copy
     * @return a range equal to it, sharing no mutable state with it
     */
    public static ByteStringRange copyOf(ByteStringRange range) {
        Preconditions.checkNotNull(range, "range must not be null");
        ByteStringRange copy = ByteStringRange.unbounded();
        switch (range.getStartBound()) {
            case CLOSED:
                copy.startClosed(range.getStart());
                break;
            case OPEN:
                copy.startOpen(range.getStart());
                break;
            case UNBOUNDED:
                break;
            default:
                throw new IllegalArgumentException("Unknown start bound " + range.getStartBound());
        }
        switch (range.getEndBound()) {
            case CLOSED:
                copy.endClosed(range.getEnd());
                break;
            case OPEN:
                copy.endOpen(range.getEnd());
                break;
            case UNBOUNDED:
                break;
            default:
                throw new IllegalArgumentException("Unknown end bound " + range.getEndBound());
        }
        return copy;
    }

    /**
     * Returns independent copies of every range in a list.
     *
     * @param ranges the ranges to copy
     * @return copies, in the same order, sharing no mutable state with the originals
     */
    public static List<ByteStringRange> copyAll(List<ByteStringRange> ranges) {
        Preconditions.checkNotNull(ranges, "ranges must not be null");
        List<ByteStringRange> copies = new ArrayList<>(ranges.size());
        for (ByteStringRange range : ranges) {
            copies.add(copyOf(range));
        }
        return copies;
    }

    /**
     * Returns whether a range provably holds no row key at all.
     *
     * <p>Four shapes qualify, the last of which is easy to write by accident and impossible to
     * spot: a start at or after the end; a start equal to a non-inclusive end; and {@code
     * startOpen(k)} paired with {@code endOpen(k + 0x00)}, where the only key the bounds admit is
     * the one they both exclude.
     *
     * <p>A user-configured empty range is rejected by the builder, because a range that reads
     * nothing under a green job is indistinguishable from a job with nothing to read. A
     * <em>truncated</em> range is a different matter and is normal — see {@link
     * #truncateStartOpen}.
     *
     * @param range the range to test
     * @return true when no row key lies inside it
     */
    public static boolean isEmpty(ByteStringRange range) {
        Preconditions.checkNotNull(range, "range must not be null");
        if (range.getStartBound() == BoundType.UNBOUNDED
                || range.getEndBound() == BoundType.UNBOUNDED) {
            return false;
        }
        ByteString start = range.getStart();
        ByteString end = range.getEnd();
        int cmp = compareKeys(start, end);
        if (cmp > 0) {
            return true;
        }
        if (cmp == 0) {
            return range.getStartBound() != BoundType.CLOSED
                    || range.getEndBound() != BoundType.CLOSED;
        }
        // start < end, so only the one-key gap can still be empty: the smallest key above an open
        // start is that start with a 0x00 appended, and an open end at exactly that key excludes
        // it.
        return range.getStartBound() == BoundType.OPEN
                && range.getEndBound() == BoundType.OPEN
                && end.equals(start.concat(SMALLEST_NON_EMPTY_KEY));
    }

    /**
     * Returns whether a sampled row key cuts a range into two non-empty pieces.
     *
     * <p>A split point {@code k} means "rows below {@code k} belong to the section before it, rows
     * from {@code k} onwards to the section after it", so cutting at {@code k} is worthwhile
     * exactly when both sides would hold something. Both start bound types answer the same way —
     * cutting at a key equal to the start yields an empty left-hand piece whether the start
     * includes that key or not — while the end bounds differ, because a cut at an inclusive end
     * leaves a right-hand piece holding exactly that one row.
     *
     * @param range the range being cut
     * @param key the candidate split point
     * @return true when the key lies strictly inside the range
     */
    public static boolean cuts(ByteStringRange range, ByteString key) {
        Preconditions.checkNotNull(range, "range must not be null");
        Preconditions.checkNotNull(key, "key must not be null");
        if (range.getStartBound() != BoundType.UNBOUNDED
                && compareKeys(key, range.getStart()) <= 0) {
            return false;
        }
        if (range.getEndBound() == BoundType.UNBOUNDED) {
            return true;
        }
        int cmp = compareKeys(key, range.getEnd());
        return range.getEndBound() == BoundType.CLOSED ? cmp <= 0 : cmp < 0;
    }

    /**
     * Returns whether a row key belongs to a range.
     *
     * <p>This is deliberately separate from {@link #cuts}: a key on a closed start belongs to the
     * range but cannot cut a non-empty left-hand piece from it. Point lookups need membership,
     * while split planning needs the stricter cut relation.
     *
     * @param range the range to test
     * @param key the row key
     * @return true when the range contains the key
     */
    public static boolean contains(ByteStringRange range, ByteString key) {
        Preconditions.checkNotNull(range, "range must not be null");
        Preconditions.checkNotNull(key, "key must not be null");
        if (range.getStartBound() != BoundType.UNBOUNDED) {
            int cmp = compareKeys(key, range.getStart());
            if (cmp < 0 || (cmp == 0 && range.getStartBound() == BoundType.OPEN)) {
                return false;
            }
        }
        if (range.getEndBound() == BoundType.UNBOUNDED) {
            return true;
        }
        int cmp = compareKeys(key, range.getEnd());
        return cmp < 0 || (cmp == 0 && range.getEndBound() == BoundType.CLOSED);
    }

    /**
     * Returns the work a split has left after emitting a row, as a range starting just past it.
     *
     * <p>The end bound is carried over untouched and the original start bound is discarded, which
     * is safe because it sits strictly below the emitted key and so constrains nothing. An
     * <em>exclusive</em> start is what makes a restore resume rather than replay, and it is also
     * what the client's own resumption strategy uses when it reconnects a broken stream mid-range.
     *
     * <p>The result may be empty — a range ending {@code endClosed(e)} whose row {@code e} has been
     * emitted has nothing left — and that is a normal end-of-split state, not an error. The split
     * reader finishes such a split without opening a stream, so an inverted range is never sent to
     * the service.
     *
     * <p>The empty-key case is not hypothetical enough to leave out: real Bigtable rejects an empty
     * row key, but the emulator accepts one, and {@code startOpen(EMPTY)} is silently turned into
     * an unbounded start by the SDK — which would widen the split back to the whole table and
     * replay it forever. Progress past the empty key is expressed as an inclusive start at its
     * successor instead.
     *
     * @param range the range the split was assigned
     * @param lastEmittedKey the key of the last row handed downstream
     * @return the remaining range
     */
    public static ByteStringRange truncateStartOpen(
            ByteStringRange range, ByteString lastEmittedKey) {
        Preconditions.checkNotNull(range, "range must not be null");
        Preconditions.checkNotNull(lastEmittedKey, "lastEmittedKey must not be null");
        ByteStringRange truncated = copyOf(range);
        if (lastEmittedKey.isEmpty()) {
            return truncated.startClosed(SMALLEST_NON_EMPTY_KEY);
        }
        return truncated.startOpen(lastEmittedKey);
    }

    /**
     * Merges ranges that overlap or run into one another, and returns them in key order.
     *
     * <p>Overlapping ranges are easy to configure by accident — {@code prefix("user")} beside
     * {@code prefix("user1")} is enough — and left alone they are not merely wasteful: the
     * overlapping rows land in two different splits, which two different subtasks read, so a single
     * successful run emits them twice. Deduplication inside one request does not reach across
     * splits, so it has to happen here.
     *
     * <p>Two ranges that merely touch are merged when the key between them belongs to one of them,
     * and left apart when it belongs to neither: {@code endOpen(k)} beside {@code startOpen(k)}
     * excludes {@code k} deliberately, and merging would put a row back that the user removed.
     *
     * @param ranges the ranges to merge, in any order
     * @return the merged ranges, sorted by start
     */
    public static List<ByteStringRange> coalesce(List<ByteStringRange> ranges) {
        Preconditions.checkNotNull(ranges, "ranges must not be null");
        List<ByteStringRange> sorted = copyAll(ranges);
        sorted.sort(RowRanges::compareStarts);

        List<ByteStringRange> merged = new ArrayList<>(sorted.size());
        for (ByteStringRange range : sorted) {
            if (merged.isEmpty()) {
                merged.add(range);
                continue;
            }
            ByteStringRange last = merged.get(merged.size() - 1);
            if (!runsInto(last, range)) {
                merged.add(range);
                continue;
            }
            if (compareEnds(range, last) > 0) {
                copyEndInto(last, range);
            }
        }
        return merged;
    }

    /**
     * Intersects two unions of row-key ranges.
     *
     * <p>The inputs may overlap and arrive in any order. Each side is coalesced first, then the two
     * sorted lists are walked once. Empty intersections are omitted; the result is therefore an
     * empty list when the two unions share no row key.
     *
     * @param left the first range union
     * @param right the second range union
     * @return independent, coalesced ranges present in both unions
     */
    public static List<ByteStringRange> intersect(
            List<ByteStringRange> left, List<ByteStringRange> right) {
        Preconditions.checkNotNull(left, "left must not be null");
        Preconditions.checkNotNull(right, "right must not be null");
        List<ByteStringRange> a = coalesce(left);
        List<ByteStringRange> b = coalesce(right);
        List<ByteStringRange> intersections = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < a.size() && j < b.size()) {
            ByteStringRange overlap = intersectionOf(a.get(i), b.get(j));
            if (!isEmpty(overlap)) {
                intersections.add(overlap);
            }
            int ends = compareEnds(a.get(i), b.get(j));
            if (ends <= 0) {
                i++;
            }
            if (ends >= 0) {
                j++;
            }
        }
        return coalesce(intersections);
    }

    /** Returns the possibly empty intersection of two ranges. */
    private static ByteStringRange intersectionOf(ByteStringRange left, ByteStringRange right) {
        ByteStringRange intersection = ByteStringRange.unbounded();
        copyLaterStartInto(intersection, left, right);
        copyEarlierEndInto(intersection, left, right);
        return intersection;
    }

    /** Copies the later, narrower start bound onto {@code target}. */
    private static void copyLaterStartInto(
            ByteStringRange target, ByteStringRange left, ByteStringRange right) {
        if (left.getStartBound() == BoundType.UNBOUNDED
                && right.getStartBound() == BoundType.UNBOUNDED) {
            return;
        }
        ByteStringRange source;
        if (left.getStartBound() == BoundType.UNBOUNDED) {
            source = right;
        } else if (right.getStartBound() == BoundType.UNBOUNDED) {
            source = left;
        } else {
            int cmp = compareKeys(left.getStart(), right.getStart());
            if (cmp > 0) {
                source = left;
            } else if (cmp < 0) {
                source = right;
            } else {
                ByteString key = left.getStart();
                if (left.getStartBound() == BoundType.OPEN
                        || right.getStartBound() == BoundType.OPEN) {
                    target.startOpen(key);
                } else {
                    target.startClosed(key);
                }
                return;
            }
        }
        if (source.getStartBound() == BoundType.OPEN) {
            target.startOpen(source.getStart());
        } else {
            target.startClosed(source.getStart());
        }
    }

    /** Copies the earlier, narrower end bound onto {@code target}. */
    private static void copyEarlierEndInto(
            ByteStringRange target, ByteStringRange left, ByteStringRange right) {
        if (left.getEndBound() == BoundType.UNBOUNDED
                && right.getEndBound() == BoundType.UNBOUNDED) {
            return;
        }
        ByteStringRange source;
        if (left.getEndBound() == BoundType.UNBOUNDED) {
            source = right;
        } else if (right.getEndBound() == BoundType.UNBOUNDED) {
            source = left;
        } else {
            int cmp = compareKeys(left.getEnd(), right.getEnd());
            if (cmp < 0) {
                source = left;
            } else if (cmp > 0) {
                source = right;
            } else {
                ByteString key = left.getEnd();
                if (left.getEndBound() == BoundType.OPEN || right.getEndBound() == BoundType.OPEN) {
                    target.endOpen(key);
                } else {
                    target.endClosed(key);
                }
                return;
            }
        }
        if (source.getEndBound() == BoundType.OPEN) {
            target.endOpen(source.getEnd());
        } else {
            target.endClosed(source.getEnd());
        }
    }

    /**
     * Renders a range the way a log reader needs to see it.
     *
     * <p>{@link ByteStringRange} inherits {@link Object#toString()}, so a range in a log line is
     * otherwise an identity hash. Printable ASCII is shown as itself and every other byte as {@code
     * \xNN}, so a key that is not text stays readable and a key that is text stays recognisable.
     *
     * @param range the range to render
     * @return a rendering such as {@code [row-1, row-9)} or {@code (\x00ff, *]}
     */
    public static String format(ByteStringRange range) {
        Preconditions.checkNotNull(range, "range must not be null");
        StringBuilder text = new StringBuilder();
        text.append(range.getStartBound() == BoundType.CLOSED ? '[' : '(');
        text.append(range.getStartBound() == BoundType.UNBOUNDED ? "*" : escape(range.getStart()));
        text.append(", ");
        text.append(range.getEndBound() == BoundType.UNBOUNDED ? "*" : escape(range.getEnd()));
        text.append(range.getEndBound() == BoundType.CLOSED ? ']' : ')');
        return text.toString();
    }

    /** Orders two ranges by where they begin; an unbounded start comes first. */
    private static int compareStarts(ByteStringRange left, ByteStringRange right) {
        if (left.getStartBound() == BoundType.UNBOUNDED
                || right.getStartBound() == BoundType.UNBOUNDED) {
            return Boolean.compare(
                    left.getStartBound() != BoundType.UNBOUNDED,
                    right.getStartBound() != BoundType.UNBOUNDED);
        }
        int cmp = compareKeys(left.getStart(), right.getStart());
        if (cmp != 0) {
            return cmp;
        }
        // At the same key an inclusive start begins first.
        return Boolean.compare(
                left.getStartBound() == BoundType.OPEN, right.getStartBound() == BoundType.OPEN);
    }

    /** Orders two ranges by where they end; an unbounded end comes last. */
    private static int compareEnds(ByteStringRange left, ByteStringRange right) {
        if (left.getEndBound() == BoundType.UNBOUNDED
                || right.getEndBound() == BoundType.UNBOUNDED) {
            return Boolean.compare(
                    left.getEndBound() == BoundType.UNBOUNDED,
                    right.getEndBound() == BoundType.UNBOUNDED);
        }
        int cmp = compareKeys(left.getEnd(), right.getEnd());
        if (cmp != 0) {
            return cmp;
        }
        // At the same key an inclusive end reaches further.
        return Boolean.compare(
                left.getEndBound() == BoundType.CLOSED, right.getEndBound() == BoundType.CLOSED);
    }

    /** Returns whether {@code next} starts before {@code first} ends, or immediately after it. */
    private static boolean runsInto(ByteStringRange first, ByteStringRange next) {
        if (first.getEndBound() == BoundType.UNBOUNDED
                || next.getStartBound() == BoundType.UNBOUNDED) {
            return true;
        }
        int cmp = compareKeys(next.getStart(), first.getEnd());
        if (cmp != 0) {
            return cmp < 0;
        }
        // They meet at one key: contiguous unless both bounds exclude it.
        return first.getEndBound() == BoundType.CLOSED || next.getStartBound() == BoundType.CLOSED;
    }

    /** Copies {@code source}'s end onto {@code target}, which is a range this class owns. */
    private static void copyEndInto(ByteStringRange target, ByteStringRange source) {
        switch (source.getEndBound()) {
            case CLOSED:
                target.endClosed(source.getEnd());
                break;
            case OPEN:
                target.endOpen(source.getEnd());
                break;
            case UNBOUNDED:
                target.endUnbounded();
                break;
            default:
                throw new IllegalArgumentException("Unknown end bound " + source.getEndBound());
        }
    }

    private static String escape(ByteString key) {
        StringBuilder text = new StringBuilder(key.size());
        for (int i = 0; i < key.size(); i++) {
            int b = key.byteAt(i) & 0xFF;
            if (b >= 0x20 && b < 0x7F && b != '\\') {
                text.append((char) b);
            } else {
                text.append(String.format("\\x%02x", b));
            }
        }
        return text.toString();
    }
}
