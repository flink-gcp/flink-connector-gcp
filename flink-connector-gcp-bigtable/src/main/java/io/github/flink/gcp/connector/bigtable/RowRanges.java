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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The module's row-key range algebra: emptiness, containment, cutting, coalescing, intersection,
 * truncation and the unsigned key comparison they all rest on, each defined once here.
 *
 * <p>It began as the bounded scan source's and now serves both source directions and both halves of
 * the table layer — the scan source's builder, planner, split state and split reader, the Change
 * Streams partition model, the table source's range parsing, filter pushdown, point lookups and
 * decode-failure guards, and the table sink's empty-mutation refusal — which is why it sits at the
 * module root rather than under one of them (ADR-0055): 26 importers in the main tree, across ten
 * packages. It lives in one place because every one of those would otherwise have to get the bound
 * types right, and would each get them wrong differently. Row keys are compared as
 * <em>unsigned</em> bytes, which is the order Bigtable stores them in; the natural ordering of
 * {@link ByteString} is not that order, and a signed comparison sorts every key whose first byte is
 * above {@code 0x7F} before every key whose first byte is below it.
 *
 * <h2>Turning bytes into text: which form, and why there is more than one</h2>
 *
 * <p>A row key, a qualifier and a cell value are all arbitrary bytes, and this module turns them
 * into text four different ways. That is not drift: <b>the reader chooses the form</b>, and
 * choosing by package or by habit is how the wrong one gets used. Before rendering a byte string,
 * ask who reads the result. This is about <em>rendering for something to read</em> and not about
 * decoding a stored value back into what it was — a serializer's {@code readString} is neither
 * governed nor contradicted by it.
 *
 * <ul>
 *   <li><b>A person, in a log line or an exception message → {@link #format(ByteString)} or {@link
 *       #format(ByteStringRange)}.</b> Printable ASCII stays itself so a text key is recognisable,
 *       and every other byte — plus the three that carry structure — becomes {@code \xNN}. The
 *       rendering is injective, so an operator can tell two of them apart (ADR-0080).
 *   <li><b>A pattern the user wrote → Base64</b>, canonical padded RFC 4648. {@code
 *       BigtableChangeStreamMutationFilter} matches user regexes against {@code
 *       family:qualifierBase64}. A pattern needs a form the user can <em>write</em>, which an
 *       escape sequence is not. The row-key options take Base64 too, but only when {@code
 *       scan.row-key-encoding} asks for it — its default is {@code UTF8} — so this arm is about the
 *       filter identifier, and an option's own encoding is whatever that option declares. Note it
 *       runs the other way from the three below, which are all about output; where the two meet,
 *       {@code RowRangeParser} is the precedent, and it names the entry number rather than echoing
 *       the value back at all.
 *   <li><b>Anyone, when the value is text by construction → {@code toStringUtf8()}.</b> A qualifier
 *       built from a DDL field name is valid UTF-8 and is the identifier the reader must match
 *       against their own DDL; escaping it would render a non-ASCII column as hex, and unlike the
 *       family name printed beside it.
 *   <li><b>A user's own code, which is likely to log the object → keep it out of that object's own
 *       rendering.</b> A row's key and cell values are that row's data: {@code FailedMutation}
 *       prints the failed mutation's size, and {@code BigtableChangeStreamMutation} has no {@code
 *       toString} at all. An exception message is the deliberate exception, having one chance to
 *       name the offending row and no accessors — <b>so this arm bounds a {@code toString} and not
 *       a whole object graph</b>: a {@code FailedMutation} whose {@code getCause()} is a
 *       serialization failure may carry that message, escaped key and all, into any handler that
 *       logs the cause — a message doing its job through this arm's object, not a leak to close.
 *       <em>May</em>, because it is per message: of this connector's own refusals only the
 *       empty-mutation one names the key, and a {@code FailedMutation} can equally wrap whatever a
 *       user's own serializer threw. {@code getRowKey()} is null for all of them.
 * </ul>
 *
 * <p><b>{@code toStringUtf8()} on a value that is not text by construction is always wrong.</b>
 * Decoding invalid UTF-8 substitutes U+FFFD rather than failing, so {@code 0xFE} and {@code 0xFF}
 * arrive as one character — the value is exposed and destroyed in the same breath.
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
 *   <li><b>An empty key on any bound is normalised to {@code UNBOUNDED} by the four setters, and
 *       <em>not</em> by {@code ByteStringRange.create}.</b> {@code startClosed(EMPTY)}, {@code
 *       startOpen(EMPTY)}, {@code endOpen(EMPTY)} and {@code endClosed(EMPTY)} all produce an
 *       unbounded side, but {@code create(EMPTY, k)} produces a <em>closed</em> start at the empty
 *       key. The two spellings are not {@link Object#equals(Object) equal} and do not render alike,
 *       so a range that arrives in the second spelling has to be converted before anything here
 *       compares it — which is what {@link #copyOf} does, and why the change-stream code copies
 *       every range the service hands it. It is also why {@link #truncateStartOpen} has to
 *       special-case the empty key on <em>output</em>, where silently widening a range to the whole
 *       table would make a restored split re-read everything it had already emitted.
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
     * Returns an independent, normalised copy of a range.
     *
     * <p>Every range that crosses into this connector — from a builder setter, from the planner,
     * from a deserialised split, from the Change Streams service — goes through here, for two
     * reasons. A {@link ByteStringRange} is mutable, so shared references would let a caller change
     * a plan after it was made. And it is the connector's one <b>normalisation</b> point:
     * rebuilding through the four setters folds an empty key on a bounded side into {@code
     * UNBOUNDED}, which is the spelling everything else here assumes. A range built by {@code
     * ByteStringRange.create} — every partition and continuation token the service returns — uses
     * the other spelling, and the two are not equal to one another.
     *
     * @param range the range to copy
     * @return a normalised range denoting the same rows, sharing no mutable state with it
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
     * Returns the work a split has left after successfully deserializing a row, as a range starting
     * just past it.
     *
     * <p>The end bound is carried over untouched and the original start bound is discarded, which
     * is safe because it sits strictly below the processed key and so constrains nothing. An
     * <em>exclusive</em> start is what makes a restore resume rather than replay, and it is also
     * what the client's own resumption strategy uses when it reconnects a broken stream mid-range.
     *
     * <p>The result may be empty — a range ending {@code endClosed(e)} whose row {@code e} was
     * successfully deserialized has nothing left, even if it produced no output — and that is a
     * normal end-of-split state, not an error. The split reader finishes such a split without
     * opening a stream, so an inverted range is never sent to the service.
     *
     * <p>The empty-key case is not hypothetical enough to leave out: {@code startOpen(EMPTY)} is
     * silently turned into an unbounded start by the SDK — which would widen the split back to the
     * whole table and replay it forever. Progress past the empty key is expressed as an inclusive
     * start at its successor instead. That is written against the SDK's normalisation rather than
     * against what a server admits, which is what keeps it correct on both ends.
     *
     * @param range the range the split was assigned
     * @param lastEmittedKey the key of the last successfully deserialized row
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
     * <p>Three printable bytes are shown escaped rather than as themselves, because each carries
     * structure here: {@code \} introduces an escape, {@code *} is the sentinel for an absent
     * bound, and {@code ,} separates the two bounds. A key holding one of them would otherwise make
     * two different ranges render as one string — {@code [a, b, c)} is both "from {@code a, b} to
     * {@code c}" and "from {@code a} to {@code b, c}" — and these strings are what an operator
     * reads to tell two ranges apart in a warning. The rendering is therefore injective, and a test
     * asserts that as a property.
     *
     * <p>That is a readability property, not a contract: <b>nothing decides identity from a
     * rendering, and nothing should</b>. Range identity is {@code ByteStringRange.equals}.
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

    /**
     * Renders one row key under {@link #format(ByteStringRange)}'s escaping, for a caller holding a
     * key rather than a range.
     *
     * <p>Escaping only — no sentinel, and the empty key renders as the empty string. Every caller
     * that names a row in a message quotes the value ({@code '%s'}), where an empty rendering reads
     * as {@code ''} and needs no marker. A caller that does <em>not</em> quote, and for which an
     * empty key carries a meaning, supplies its own: {@code RowKeySample} marks it {@code *},
     * because there it is the service's "end of table" rather than a key. Deciding that here would
     * impose one caller's meaning on the rest, which is the whole reason this method does not.
     *
     * <p>Same caveat as the range form: a rendering is what a person reads in a log, and nothing
     * decides identity from one.
     *
     * @param key the key to render
     * @return a rendering such as {@code row-1} or {@code \x00\xff}; empty for the empty key
     */
    public static String format(ByteString key) {
        Preconditions.checkNotNull(key, "key must not be null");
        return escape(key);
    }

    /**
     * Orders two ranges by where they begin; an unbounded start comes first.
     *
     * <p>Like {@link #compareKeys(ByteString, ByteString)}, and unlike the range predicates above,
     * this does not null-check. It is called once per comparison inside a sort or a {@code
     * TreeSet}, over ranges the caller has already accepted.
     *
     * @param left the first range
     * @param right the second range
     * @return a negative number, zero or a positive number as {@code left} begins before, at or
     *     after {@code right}
     */
    public static int compareStarts(ByteStringRange left, ByteStringRange right) {
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

    /**
     * Orders two ranges by where they end; an unbounded end comes last.
     *
     * @param left the first range
     * @param right the second range
     * @return a negative number, zero or a positive number as {@code left} ends before, at or after
     *     {@code right}
     */
    public static int compareEnds(ByteStringRange left, ByteStringRange right) {
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

    /**
     * Returns whether two ranges begin at exactly the same point.
     *
     * <p>Two unbounded starts are the same start. Otherwise the bound type has to match as well as
     * the key, because {@code startClosed(k)} and {@code startOpen(k)} disagree about the one row
     * {@code k}. This is the equality {@link #compareStarts(ByteStringRange, ByteStringRange)}
     * induces, written out directly so that a caller asking "is this the same edge?" does not have
     * to read a comparator's result against zero.
     *
     * @param left the first range
     * @param right the second range
     * @return true when both begin at the same key with the same bound type, or both are unbounded
     */
    public static boolean sameStart(ByteStringRange left, ByteStringRange right) {
        // getStart() throws on an unbounded bound, so the bound types have to settle it first.
        if (left.getStartBound() != right.getStartBound()) {
            return false;
        }
        return left.getStartBound() == BoundType.UNBOUNDED
                || left.getStart().equals(right.getStart());
    }

    /**
     * Returns whether two ranges end at exactly the same point.
     *
     * @param left the first range
     * @param right the second range
     * @return true when both end at the same key with the same bound type, or both are unbounded
     */
    public static boolean sameEnd(ByteStringRange left, ByteStringRange right) {
        if (left.getEndBound() != right.getEndBound()) {
            return false;
        }
        return left.getEndBound() == BoundType.UNBOUNDED || left.getEnd().equals(right.getEnd());
    }

    /**
     * Returns whether a range begins before every row key.
     *
     * <p>The bound type alone answers this, which is correct only for a <em>normalised</em> range.
     * Copies of this helper elsewhere in the module used to add {@code ||
     * range.getStart().isEmpty()}, and that disjunct was not decoration: it absorbed the spelling
     * {@code ByteStringRange.create} produces, in which an absent bound is a bounded one at the
     * empty key. The copies are gone and the disjunct with them, so every caller owes a range that
     * has been through {@link #copyOf(ByteStringRange)} — see the third measured fact above.
     *
     * @param range the range to test
     * @return true when the range has no lower bound
     */
    public static boolean isUnboundedStart(ByteStringRange range) {
        return range.getStartBound() == BoundType.UNBOUNDED;
    }

    /**
     * Returns whether a range continues past every row key.
     *
     * @param range the range to test
     * @return true when the range has no upper bound
     */
    public static boolean isUnboundedEnd(ByteStringRange range) {
        return range.getEndBound() == BoundType.UNBOUNDED;
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

    /**
     * Renders a key readably, escaping every byte that carries structure in {@link
     * #format(ByteStringRange)}'s output as well as every unprintable one.
     *
     * <p>Three printable bytes are structural, and a key holding one has to be escaped or the
     * rendering becomes ambiguous: {@code \} introduces an escape, {@code *} is the sentinel for an
     * absent bound, and {@code ,} separates the two bounds. Escaping the comma is what leaves
     * {@code ", "} occurring exactly once, so the two halves can always be told apart.
     */
    private static String escape(ByteString key) {
        StringBuilder text = new StringBuilder(key.size());
        for (int i = 0; i < key.size(); i++) {
            int b = key.byteAt(i) & 0xFF;
            if (b >= 0x20 && b < 0x7F && b != '\\' && b != '*' && b != ',') {
                text.append((char) b);
            } else {
                text.append(String.format("\\x%02x", b));
            }
        }
        return text.toString();
    }
}
