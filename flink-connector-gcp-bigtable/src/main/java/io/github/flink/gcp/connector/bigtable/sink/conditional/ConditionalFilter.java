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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable selection of cells used as a conditional predicate. A predicate matches when its
 * selection contains any cell. A chain filters one cell set in order; it is not boolean AND across
 * different cells. No nested conditional filters are supported.
 */
@PublicEvolving
public final class ConditionalFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    enum Kind {
        ROW_EXISTS,
        FAMILY,
        QUALIFIER,
        VALUE,
        TIMESTAMP,
        COLUMN_LIMIT,
        ROW_LIMIT,
        CHAIN,
        INTERLEAVE
    }

    final Kind kind;
    final String family;
    final ByteString bytes;
    final long start;
    final long end;
    final int count;
    final List<ConditionalFilter> children;

    private ConditionalFilter(
            Kind kind,
            String family,
            ByteString bytes,
            long start,
            long end,
            int count,
            List<ConditionalFilter> children) {
        this.kind = kind;
        this.family = family;
        this.bytes = bytes;
        this.start = start;
        this.end = end;
        this.count = count;
        this.children = children;
    }

    /**
     * Tests whether any cell exists anywhere in the stored row, including undeclared families.
     *
     * @return the predicate
     */
    public static ConditionalFilter rowExists() {
        return new ConditionalFilter(Kind.ROW_EXISTS, null, null, 0, 0, 0, List.of());
    }

    /**
     * Selects one column family by exact name.
     *
     * @param family the family name
     * @return the filter
     */
    public static ConditionalFilter familyEquals(String family) {
        return new ConditionalFilter(Kind.FAMILY, checkedFamily(family), null, 0, 0, 0, List.of());
    }

    /**
     * Selects qualifiers by exact bytes; an empty qualifier is valid.
     *
     * @param qualifier the qualifier bytes
     * @return the filter
     */
    public static ConditionalFilter qualifierEquals(ByteString qualifier) {
        return new ConditionalFilter(
                Kind.QUALIFIER,
                null,
                Preconditions.checkNotNull(qualifier, "qualifier must not be null"),
                0,
                0,
                0,
                List.of());
    }

    /**
     * Selects cell values by exact bytes, including empty or non-UTF-8 values.
     *
     * @param value the value bytes
     * @return the filter
     */
    public static ConditionalFilter valueEquals(ByteString value) {
        return new ConditionalFilter(
                Kind.VALUE,
                null,
                Preconditions.checkNotNull(value, "value must not be null"),
                0,
                0,
                0,
                List.of());
    }

    /**
     * Selects timestamps in a half-open microsecond range without rounding.
     *
     * @param startInclusive the nonnegative inclusive start
     * @param endExclusive the exclusive end, greater than the start
     * @return the filter
     */
    public static ConditionalFilter timestampRange(long startInclusive, long endExclusive) {
        Preconditions.checkArgument(startInclusive >= 0, "startInclusive must be nonnegative");
        Preconditions.checkArgument(
                endExclusive > startInclusive, "endExclusive must exceed startInclusive");
        return new ConditionalFilter(
                Kind.TIMESTAMP, null, null, startInclusive, endExclusive, 0, List.of());
    }

    /**
     * Selects the newest versions of each remaining column.
     *
     * @param count the positive number of versions per column
     * @return the filter
     */
    public static ConditionalFilter cellsPerColumn(int count) {
        Preconditions.checkArgument(count > 0, "count must be positive");
        return new ConditionalFilter(Kind.COLUMN_LIMIT, null, null, 0, 0, count, List.of());
    }

    /**
     * Selects the first cells of the remaining row in Bigtable's cell order.
     *
     * @param count the positive number of cells
     * @return the filter
     */
    public static ConditionalFilter cellsPerRow(int count) {
        Preconditions.checkArgument(count > 0, "count must be positive");
        return new ConditionalFilter(Kind.ROW_LIMIT, null, null, 0, 0, count, List.of());
    }

    /**
     * Applies filters in their supplied order to one cell set.
     *
     * @param filters at least one filter, with no null elements
     * @return the composed filter
     */
    public static ConditionalFilter chain(ConditionalFilter... filters) {
        return compose(Kind.CHAIN, filters);
    }

    /**
     * Combines each filter's selection from the same input cell set, retaining duplicates.
     *
     * @param filters at least one filter, with no null elements
     * @return the composed filter
     */
    public static ConditionalFilter interleave(ConditionalFilter... filters) {
        return compose(Kind.INTERLEAVE, filters);
    }

    /**
     * Tests for any version of one column.
     *
     * @param family the family name
     * @param qualifier the qualifier bytes
     * @return the predicate
     */
    public static ConditionalFilter cellExists(String family, ByteString qualifier) {
        return chain(familyEquals(family), qualifierEquals(qualifier));
    }

    /**
     * Tests the newest version of one column. A matching historical value cannot satisfy it.
     *
     * @param family the family name
     * @param qualifier the qualifier bytes
     * @param value the expected bytes
     * @return the predicate
     */
    public static ConditionalFilter latestCellValueEquals(
            String family, ByteString qualifier, ByteString value) {
        return chain(
                familyEquals(family),
                qualifierEquals(qualifier),
                cellsPerColumn(1),
                valueEquals(value));
    }

    static String checkedFamily(String family) {
        Preconditions.checkNotNull(family, "family must not be null");
        Preconditions.checkArgument(!family.isBlank(), "family must not be blank");
        return family;
    }

    private static ConditionalFilter compose(Kind kind, ConditionalFilter[] filters) {
        List<ConditionalFilter> children = List.of(filters);
        Preconditions.checkArgument(!children.isEmpty(), "filters must not be empty");
        return new ConditionalFilter(kind, null, null, 0, 0, 0, children);
    }
}
