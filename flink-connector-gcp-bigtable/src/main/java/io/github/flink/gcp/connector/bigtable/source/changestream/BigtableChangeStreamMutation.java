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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * One complete logical row mutation returned by Bigtable Change Streams.
 *
 * <p>Instances are immutable and compare by value. There is deliberately <b>no {@code
 * toString}</b>: a mutation's row key and cell values are the row's own data, and a value type
 * whose {@code toString} prints user data is one accidental log line away from putting that data
 * where it does not belong.
 *
 * <p>A redacting {@code toString} rendering only the metadata and the row key and entry list
 * <em>sizes</em> was considered and declined. Redaction is a property of one implementation rather
 * than an invariant: no test can pin the absence of user data in a free-form string, and every
 * later widening of it — printing the entries to debug something — is individually plausible.
 * Omitting it leaves nothing to erode. A caller that wants to render a mutation chooses what to
 * print, through the accessors. Spanner's {@code DataChangeRecord} is governed by the same
 * reasoning.
 */
@PublicEvolving
@TypeInfo(BigtableChangeStreamMutationTypeInfoFactory.class)
public final class BigtableChangeStreamMutation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ByteString rowKey;
    private final MutationType type;
    private final String sourceClusterId;
    private final Instant commitTime;
    private final int tieBreaker;
    private final String token;
    private final Instant estimatedLowWatermarkTime;
    private final List<Entry> entries;

    /**
     * Creates a mutation from its metadata and ordered entries. The connector builds instances from
     * the service stream; user code normally only reads them, but a test may construct its own.
     *
     * @param rowKey the key of the mutated row
     * @param type what produced the mutation
     * @param sourceClusterId the id of the cluster the user write was applied on, or an empty
     *     string for a garbage-collection mutation
     * @param commitTime the timestamp at which Bigtable applied the mutation
     * @param tieBreaker the conflict-resolution rank among writes sharing the commit timestamp
     * @param token the encoded position at which the partition's stream resumes after this mutation
     * @param estimatedLowWatermarkTime the service's delivery-progress estimate for the partition
     * @param entries the ordered entries; copied, and must not be or contain null
     */
    public BigtableChangeStreamMutation(
            ByteString rowKey,
            MutationType type,
            String sourceClusterId,
            Instant commitTime,
            int tieBreaker,
            String token,
            Instant estimatedLowWatermarkTime,
            List<Entry> entries) {
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        this.type = Preconditions.checkNotNull(type, "type must not be null");
        this.sourceClusterId =
                Preconditions.checkNotNull(sourceClusterId, "sourceClusterId must not be null");
        this.commitTime = Preconditions.checkNotNull(commitTime, "commitTime must not be null");
        this.tieBreaker = tieBreaker;
        this.token = Preconditions.checkNotNull(token, "token must not be null");
        this.estimatedLowWatermarkTime =
                Preconditions.checkNotNull(
                        estimatedLowWatermarkTime, "estimatedLowWatermarkTime must not be null");
        Preconditions.checkNotNull(entries, "entries must not be null");
        Preconditions.checkArgument(!entries.contains(null), "entries must not contain null");
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** Returns the key of the row every entry in this mutation applies to. */
    public ByteString getRowKey() {
        return rowKey;
    }

    /**
     * Returns what produced this mutation: a user write, or the table's garbage-collection policy.
     */
    public MutationType getType() {
        return type;
    }

    /**
     * Returns the id of the cluster the user write was applied on, or an empty string for a
     * garbage-collection mutation, which no cluster originates.
     */
    public String getSourceClusterId() {
        return sourceClusterId;
    }

    /** Returns the timestamp at which Bigtable applied the mutation on the server. */
    public Instant getCommitTime() {
        return commitTime;
    }

    /**
     * Returns the rank Bigtable uses to resolve conflicting writes: when the same cell is written
     * on different clusters at the same commit timestamp, the write with the larger tie-breaker
     * wins the eventually consistent state.
     */
    public int getTieBreaker() {
        return tieBreaker;
    }

    /**
     * Returns the encoded position at which the partition's stream resumes reading after this
     * mutation. Flink checkpoints govern where a restored job actually resumes — a delivered
     * mutation's token is not yet durable — so this is exposed for applications that track stream
     * positions themselves, and such an application must expect a replay of mutations it has
     * already seen.
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns the service's estimate of a commit time below which the partition has usually
     * delivered every record. Bigtable explicitly permits a later record below an earlier estimate,
     * so this is delivery-progress information, not an ordering guarantee, and the source
     * deliberately does not turn it into a Flink watermark.
     */
    public Instant getEstimatedLowWatermarkTime() {
        return estimatedLowWatermarkTime;
    }

    /**
     * Returns the ordered entries of this atomic row mutation, as an unmodifiable list. In a
     * mutation the source delivered, it is empty only when entry filtering removed every entry the
     * service reported and the source is configured to still deliver such mutations.
     */
    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BigtableChangeStreamMutation)) {
            return false;
        }
        BigtableChangeStreamMutation that = (BigtableChangeStreamMutation) other;
        return tieBreaker == that.tieBreaker
                && type == that.type
                && rowKey.equals(that.rowKey)
                && sourceClusterId.equals(that.sourceClusterId)
                && commitTime.equals(that.commitTime)
                && token.equals(that.token)
                && estimatedLowWatermarkTime.equals(that.estimatedLowWatermarkTime)
                && entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rowKey,
                type,
                sourceClusterId,
                commitTime,
                tieBreaker,
                token,
                estimatedLowWatermarkTime,
                entries);
    }

    /** What produced a mutation: a user write, or the table's garbage-collection policy. */
    @PublicEvolving
    public enum MutationType {

        /** A user-initiated write. */
        USER,

        /** A mutation the table's garbage-collection policy applied. */
        GARBAGE_COLLECTION
    }

    /**
     * One ordered mutation entry.
     *
     * <p>The constructor is private, so the subtypes below are the complete set. Branch on {@link
     * Entry#getKind()} rather than on the concrete type; the connector's own dispatch goes through
     * a package-private visitor instead, so that a subtype added here cannot compile until every
     * handler states what it does with it.
     */
    @PublicEvolving
    public abstract static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;

        private Entry() {}

        /** Returns the name of the column family this entry applies to. */
        public abstract String getFamilyName();

        /** Returns which of the entry subtypes this is. */
        public abstract EntryKind getKind();

        abstract <R, A> R accept(
                ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException;
    }

    /** Which subtype an {@link Entry} is. */
    @PublicEvolving
    public enum EntryKind {

        /** A {@link SetCellEntry}. */
        SET_CELL,

        /** A {@link DeleteCellsEntry}. */
        DELETE_CELLS,

        /** A {@link DeleteFamilyEntry}. */
        DELETE_FAMILY,

        /** An {@link AddToCellEntry}. */
        ADD_TO_CELL,

        /** A {@link MergeToCellEntry}. */
        MERGE_TO_CELL
    }

    /** Writes one cell version. */
    @PublicEvolving
    public static final class SetCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final ByteString qualifier;
        private final long timestampMicros;
        private final ByteString value;

        /**
         * Creates a set-cell entry.
         *
         * @param familyName the column family written to
         * @param qualifier the qualifier of the written column; may be empty
         * @param timestampMicros the version timestamp of the written cell, in microseconds since
         *     the epoch
         * @param value the bytes written into the cell
         */
        public SetCellEntry(
                String familyName, ByteString qualifier, long timestampMicros, ByteString value) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
            this.timestampMicros = timestampMicros;
            this.value = Preconditions.checkNotNull(value, "value must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
        }

        /** Returns the qualifier of the written column; may be empty. */
        public ByteString getQualifier() {
            return qualifier;
        }

        /** Returns the version timestamp of the written cell, in microseconds since the epoch. */
        public long getTimestampMicros() {
            return timestampMicros;
        }

        /** Returns the bytes written into the cell. */
        public ByteString getValue() {
            return value;
        }

        @Override
        public EntryKind getKind() {
            return EntryKind.SET_CELL;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof SetCellEntry)) {
                return false;
            }
            SetCellEntry that = (SetCellEntry) other;
            return timestampMicros == that.timestampMicros
                    && familyName.equals(that.familyName)
                    && qualifier.equals(that.qualifier)
                    && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(familyName, qualifier, timestampMicros, value);
        }
    }

    /** Deletes cell versions in one timestamp range. */
    @PublicEvolving
    public static final class DeleteCellsEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final ByteString qualifier;
        private final TimestampRange timestampRange;

        /**
         * Creates a delete-cells entry.
         *
         * @param familyName the column family deleted from
         * @param qualifier the qualifier of the deleted column; may be empty
         * @param timestampRange the timestamp range whose cell versions were deleted
         */
        public DeleteCellsEntry(
                String familyName, ByteString qualifier, TimestampRange timestampRange) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
            this.timestampRange =
                    Preconditions.checkNotNull(timestampRange, "timestampRange must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
        }

        /** Returns the qualifier of the deleted column; may be empty. */
        public ByteString getQualifier() {
            return qualifier;
        }

        /** Returns the timestamp range whose cell versions were deleted. */
        public TimestampRange getTimestampRange() {
            return timestampRange;
        }

        @Override
        public EntryKind getKind() {
            return EntryKind.DELETE_CELLS;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DeleteCellsEntry)) {
                return false;
            }
            DeleteCellsEntry that = (DeleteCellsEntry) other;
            return familyName.equals(that.familyName)
                    && qualifier.equals(that.qualifier)
                    && timestampRange.equals(that.timestampRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(familyName, qualifier, timestampRange);
        }
    }

    /** Deletes every cell in one family. */
    @PublicEvolving
    public static final class DeleteFamilyEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;

        /**
         * Creates a delete-family entry.
         *
         * @param familyName the column family whose cells were all deleted
         */
        public DeleteFamilyEntry(String familyName) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
        }

        @Override
        public EntryKind getKind() {
            return EntryKind.DELETE_FAMILY;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof DeleteFamilyEntry
                    && familyName.equals(((DeleteFamilyEntry) other).familyName);
        }

        @Override
        public int hashCode() {
            return familyName.hashCode();
        }
    }

    /** Adds an aggregate input to one cell. */
    @PublicEvolving
    public static final class AddToCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final Value qualifier;
        private final Value timestamp;
        private final Value input;

        /**
         * Creates an add-to-cell entry.
         *
         * @param familyName the aggregate column family added to
         * @param qualifier the qualifier of the target column, which the service documents as a
         *     {@link RawValue}
         * @param timestamp the version timestamp of the target cell, which the service documents as
         *     a {@link RawTimestamp}
         * @param input the input accumulated into the cell, typed by the family's input type
         */
        public AddToCellEntry(String familyName, Value qualifier, Value timestamp, Value input) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
            this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp must not be null");
            this.input = Preconditions.checkNotNull(input, "input must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
        }

        /**
         * Returns the qualifier of the target column. The service documents it as a {@link
         * RawValue}; branch on {@link Value#getType()} rather than assuming the subtype.
         */
        public Value getQualifier() {
            return qualifier;
        }

        /**
         * Returns the version timestamp of the target cell. The service documents it as a {@link
         * RawTimestamp}; branch on {@link Value#getType()} rather than assuming the subtype.
         */
        public Value getTimestamp() {
            return timestamp;
        }

        /** Returns the input accumulated into the cell, typed by the family's input type. */
        public Value getInput() {
            return input;
        }

        @Override
        public EntryKind getKind() {
            return EntryKind.ADD_TO_CELL;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof AddToCellEntry)) {
                return false;
            }
            AddToCellEntry that = (AddToCellEntry) other;
            return familyName.equals(that.familyName)
                    && qualifier.equals(that.qualifier)
                    && timestamp.equals(that.timestamp)
                    && input.equals(that.input);
        }

        @Override
        public int hashCode() {
            return Objects.hash(familyName, qualifier, timestamp, input);
        }
    }

    /** Merges an aggregate input into one cell. */
    @PublicEvolving
    public static final class MergeToCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final Value qualifier;
        private final Value timestamp;
        private final Value input;

        /**
         * Creates a merge-to-cell entry.
         *
         * @param familyName the aggregate column family merged into
         * @param qualifier the qualifier of the target column, which the service documents as a
         *     {@link RawValue}
         * @param timestamp the version timestamp of the target cell, which the service documents as
         *     a {@link RawTimestamp}
         * @param input the accumulator state merged into the cell, typed by the family's state type
         */
        public MergeToCellEntry(String familyName, Value qualifier, Value timestamp, Value input) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
            this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
            this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp must not be null");
            this.input = Preconditions.checkNotNull(input, "input must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
        }

        /**
         * Returns the qualifier of the target column. The service documents it as a {@link
         * RawValue}; branch on {@link Value#getType()} rather than assuming the subtype.
         */
        public Value getQualifier() {
            return qualifier;
        }

        /**
         * Returns the version timestamp of the target cell. The service documents it as a {@link
         * RawTimestamp}; branch on {@link Value#getType()} rather than assuming the subtype.
         */
        public Value getTimestamp() {
            return timestamp;
        }

        /** Returns the accumulator state merged into the cell, typed by the family's state type. */
        public Value getInput() {
            return input;
        }

        @Override
        public EntryKind getKind() {
            return EntryKind.MERGE_TO_CELL;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof MergeToCellEntry)) {
                return false;
            }
            MergeToCellEntry that = (MergeToCellEntry) other;
            return familyName.equals(that.familyName)
                    && qualifier.equals(that.qualifier)
                    && timestamp.equals(that.timestamp)
                    && input.equals(that.input);
        }

        @Override
        public int hashCode() {
            return Objects.hash(familyName, qualifier, timestamp, input);
        }
    }

    /**
     * One typed value used by an aggregate entry.
     *
     * <p>The constructor is private, so the subtypes below are the complete set. Branch on {@link
     * Value#getType()} rather than on the concrete type, for the reason {@link Entry} gives.
     */
    @PublicEvolving
    public abstract static class Value implements Serializable {
        private static final long serialVersionUID = 1L;

        private Value() {}

        /** Returns which of the value subtypes this is. */
        public abstract ValueType getType();

        abstract <R, A> R accept(
                ChangeStreamMutationDispatcher.ValueVisitor<R, A> visitor, A argument)
                throws IOException;
    }

    /** Which subtype a {@link Value} is. */
    @PublicEvolving
    public enum ValueType {

        /** A {@link RawValue}. */
        RAW_VALUE,

        /** A {@link RawTimestamp}. */
        RAW_TIMESTAMP,

        /** An {@link Int64Value}. */
        INT64
    }

    /** Arbitrary bytes in an aggregate entry. */
    @PublicEvolving
    public static final class RawValue extends Value {
        private static final long serialVersionUID = 1L;

        private final ByteString value;

        /**
         * Creates a raw-bytes value.
         *
         * @param value the raw bytes, with no type information attached
         */
        public RawValue(ByteString value) {
            this.value = Preconditions.checkNotNull(value, "value must not be null");
        }

        @Override
        public ValueType getType() {
            return ValueType.RAW_VALUE;
        }

        /** Returns the raw bytes, with no type information attached. */
        public ByteString getValue() {
            return value;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.ValueVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof RawValue && value.equals(((RawValue) other).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    /** A raw microsecond timestamp in an aggregate entry. */
    @PublicEvolving
    public static final class RawTimestamp extends Value {
        private static final long serialVersionUID = 1L;

        private final long value;

        /**
         * Creates a raw-timestamp value.
         *
         * @param value the cell timestamp, in microseconds since the epoch
         */
        public RawTimestamp(long value) {
            this.value = value;
        }

        @Override
        public ValueType getType() {
            return ValueType.RAW_TIMESTAMP;
        }

        /** Returns the cell timestamp, in microseconds since the epoch. */
        public long getValue() {
            return value;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.ValueVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof RawTimestamp && value == ((RawTimestamp) other).value;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }
    }

    /** A signed 64-bit integer in an aggregate entry. */
    @PublicEvolving
    public static final class Int64Value extends Value {
        private static final long serialVersionUID = 1L;

        private final long value;

        /**
         * Creates a 64-bit integer value.
         *
         * @param value the integer the aggregate entry carries, as an {@code int64}-typed input or
         *     accumulator state
         */
        public Int64Value(long value) {
            this.value = value;
        }

        @Override
        public ValueType getType() {
            return ValueType.INT64;
        }

        /**
         * Returns the integer the aggregate entry carries, as an {@code int64}-typed input or
         * accumulator state.
         */
        public long getValue() {
            return value;
        }

        @Override
        <R, A> R accept(ChangeStreamMutationDispatcher.ValueVisitor<R, A> visitor, A argument)
                throws IOException {
            return visitor.visit(this, argument);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Int64Value && value == ((Int64Value) other).value;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }
    }

    /** One timestamp-range boundary. */
    @PublicEvolving
    public static final class TimestampBound implements Serializable {
        private static final long serialVersionUID = 1L;

        private final BoundType type;
        private final long timestampMicros;

        private TimestampBound(BoundType type, long timestampMicros) {
            this.type = Preconditions.checkNotNull(type, "type must not be null");
            this.timestampMicros = timestampMicros;
        }

        /**
         * Returns the bound that leaves its end of the range unconstrained.
         *
         * @return the bound
         */
        public static TimestampBound unbounded() {
            return new TimestampBound(BoundType.UNBOUNDED, 0L);
        }

        /**
         * Returns an exclusive bound at the given timestamp.
         *
         * @param timestampMicros the excluded boundary timestamp, in microseconds since the epoch
         * @return the bound
         */
        public static TimestampBound open(long timestampMicros) {
            return new TimestampBound(BoundType.OPEN, timestampMicros);
        }

        /**
         * Returns an inclusive bound at the given timestamp.
         *
         * @param timestampMicros the included boundary timestamp, in microseconds since the epoch
         * @return the bound
         */
        public static TimestampBound closed(long timestampMicros) {
            return new TimestampBound(BoundType.CLOSED, timestampMicros);
        }

        /** Returns how this bound constrains its end of the range. */
        public BoundType getType() {
            return type;
        }

        /**
         * Returns the boundary timestamp in microseconds since the epoch, or empty when the bound
         * is unbounded.
         */
        public OptionalLong getTimestampMicros() {
            return type == BoundType.UNBOUNDED
                    ? OptionalLong.empty()
                    : OptionalLong.of(timestampMicros);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TimestampBound
                    && type == ((TimestampBound) other).type
                    && timestampMicros == ((TimestampBound) other).timestampMicros;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, timestampMicros);
        }
    }

    /**
     * How a {@link TimestampBound} constrains its end of the range: exclusive, inclusive, or not at
     * all.
     */
    @PublicEvolving
    public enum BoundType {

        /** The boundary timestamp is excluded from the range. */
        OPEN,

        /** The boundary timestamp is included in the range. */
        CLOSED,

        /** This end of the range is unconstrained, and the bound carries no timestamp. */
        UNBOUNDED
    }

    /** Timestamp range affected by a cell deletion. */
    @PublicEvolving
    public static final class TimestampRange implements Serializable {
        private static final long serialVersionUID = 1L;

        private final TimestampBound start;
        private final TimestampBound end;

        /**
         * Creates a timestamp range. Either bound may be {@link TimestampBound#unbounded()}, and
         * the constructor does not validate the order of the two.
         *
         * @param start the lower bound
         * @param end the upper bound
         */
        public TimestampRange(TimestampBound start, TimestampBound end) {
            this.start = Preconditions.checkNotNull(start, "start must not be null");
            this.end = Preconditions.checkNotNull(end, "end must not be null");
        }

        /** Returns the lower bound of the range, possibly {@link TimestampBound#unbounded()}. */
        public TimestampBound getStart() {
            return start;
        }

        /** Returns the upper bound of the range, possibly {@link TimestampBound#unbounded()}. */
        public TimestampBound getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TimestampRange
                    && start.equals(((TimestampRange) other).start)
                    && end.equals(((TimestampRange) other).end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }
    }
}
