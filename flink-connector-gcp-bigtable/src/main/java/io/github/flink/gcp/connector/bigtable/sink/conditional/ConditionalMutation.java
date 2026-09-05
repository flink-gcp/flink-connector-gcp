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

import javax.annotation.Nullable;

import java.io.Serializable;

/** Immutable mutation in an ordered conditional branch. Explicit timestamps are never rounded. */
@PublicEvolving
public final class ConditionalMutation implements Serializable {
    private static final long serialVersionUID = 1L;

    enum Kind {
        SET_CELL,
        DELETE_CELLS,
        DELETE_FAMILY,
        DELETE_ROW,
        ADD_TO_CELL,
        MERGE_TO_CELL
    }

    final Kind kind;
    final String family;
    final ByteString qualifier;
    final ByteString value;
    final long timestamp;
    final Long start;
    final Long end;
    final AggregateValue aggregate;

    private ConditionalMutation(
            Kind kind,
            String family,
            ByteString qualifier,
            ByteString value,
            long timestamp,
            Long start,
            Long end,
            AggregateValue aggregate) {
        this.kind = kind;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
        this.start = start;
        this.end = end;
        this.aggregate = aggregate;
    }

    /**
     * Sets a cell. Server time can create a new version again when a request is replayed.
     *
     * @param family the family name
     * @param qualifier the qualifier bytes, possibly empty
     * @param timestampMicros nonnegative microseconds, or -1 to request server time
     * @param value the value bytes, possibly empty
     * @return the mutation
     */
    public static ConditionalMutation setCell(
            String family, ByteString qualifier, long timestampMicros, ByteString value) {
        Preconditions.checkArgument(
                timestampMicros >= -1, "timestampMicros must be nonnegative or -1 for server time");
        return new ConditionalMutation(
                Kind.SET_CELL,
                ConditionalFilter.checkedFamily(family),
                checkedQualifier(qualifier),
                Preconditions.checkNotNull(value, "value must not be null"),
                timestampMicros,
                null,
                null,
                null);
    }

    /**
     * Deletes all versions of one column.
     *
     * @param family the family name
     * @param qualifier the qualifier bytes
     * @return the mutation
     */
    public static ConditionalMutation deleteCells(String family, ByteString qualifier) {
        return deleteCells(family, qualifier, null, null);
    }

    /**
     * Deletes versions in a half-open microsecond range. A null bound is unbounded.
     *
     * @param family the family name
     * @param qualifier the qualifier bytes
     * @param startInclusive the nonnegative inclusive start, or null
     * @param endExclusive the positive exclusive end, or null
     * @return the mutation
     */
    public static ConditionalMutation deleteCells(
            String family,
            ByteString qualifier,
            @Nullable Long startInclusive,
            @Nullable Long endExclusive) {
        Preconditions.checkArgument(
                startInclusive == null || startInclusive >= 0,
                "startInclusive must be nonnegative");
        Preconditions.checkArgument(
                endExclusive == null || endExclusive > 0, "endExclusive must be positive");
        Preconditions.checkArgument(
                startInclusive == null || endExclusive == null || startInclusive < endExclusive,
                "endExclusive must exceed startInclusive");
        return new ConditionalMutation(
                Kind.DELETE_CELLS,
                ConditionalFilter.checkedFamily(family),
                checkedQualifier(qualifier),
                null,
                0,
                startInclusive,
                endExclusive,
                null);
    }

    /**
     * Deletes every cell of one family from this row.
     *
     * @param family the family name
     * @return the mutation
     */
    public static ConditionalMutation deleteFamily(String family) {
        return new ConditionalMutation(
                Kind.DELETE_FAMILY,
                ConditionalFilter.checkedFamily(family),
                null,
                null,
                0,
                null,
                null,
                null);
    }

    /**
     * Deletes every cell of the row.
     *
     * @return the mutation
     */
    public static ConditionalMutation deleteRow() {
        return new ConditionalMutation(Kind.DELETE_ROW, null, null, null, 0, null, null, null);
    }

    /**
     * Adds an input to a compatible aggregate cell.
     *
     * @param family the aggregate family
     * @param qualifier the qualifier bytes
     * @param timestampMicros the concrete nonnegative timestamp, including zero
     * @param input the typed input
     * @return the mutation
     */
    public static ConditionalMutation addToCell(
            String family, ByteString qualifier, long timestampMicros, AggregateValue input) {
        return aggregate(Kind.ADD_TO_CELL, family, qualifier, timestampMicros, input);
    }

    /**
     * Merges a state into a compatible aggregate cell.
     *
     * @param family the aggregate family
     * @param qualifier the qualifier bytes
     * @param timestampMicros the concrete nonnegative timestamp, including zero
     * @param state the typed state
     * @return the mutation
     */
    public static ConditionalMutation mergeToCell(
            String family, ByteString qualifier, long timestampMicros, AggregateValue state) {
        return aggregate(Kind.MERGE_TO_CELL, family, qualifier, timestampMicros, state);
    }

    private static ConditionalMutation aggregate(
            Kind kind, String family, ByteString qualifier, long timestamp, AggregateValue value) {
        Preconditions.checkArgument(
                timestamp >= 0, "timestampMicros must be nonnegative for aggregate cells");
        return new ConditionalMutation(
                kind,
                ConditionalFilter.checkedFamily(family),
                checkedQualifier(qualifier),
                null,
                timestamp,
                null,
                null,
                Preconditions.checkNotNull(value, "aggregate value must not be null"));
    }

    private static ByteString checkedQualifier(ByteString qualifier) {
        return Preconditions.checkNotNull(qualifier, "qualifier must not be null");
    }
}
