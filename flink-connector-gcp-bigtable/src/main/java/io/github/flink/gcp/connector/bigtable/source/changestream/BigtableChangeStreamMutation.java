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

import org.apache.flink.annotation.Public;
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
@Public
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

    public ByteString getRowKey() {
        return rowKey;
    }

    public MutationType getType() {
        return type;
    }

    public String getSourceClusterId() {
        return sourceClusterId;
    }

    public Instant getCommitTime() {
        return commitTime;
    }

    public int getTieBreaker() {
        return tieBreaker;
    }

    public String getToken() {
        return token;
    }

    public Instant getEstimatedLowWatermarkTime() {
        return estimatedLowWatermarkTime;
    }

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

    @Public
    public enum MutationType {
        USER,
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
    @Public
    public abstract static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;

        private Entry() {}

        public abstract String getFamilyName();

        /** Which of the entry subtypes this is. */
        public abstract EntryKind getKind();

        abstract <R, A> R accept(
                ChangeStreamMutationDispatcher.EntryVisitor<R, A> visitor, A argument)
                throws IOException;
    }

    /** Which subtype an {@link Entry} is. */
    @Public
    public enum EntryKind {
        SET_CELL,
        DELETE_CELLS,
        DELETE_FAMILY,
        ADD_TO_CELL,
        MERGE_TO_CELL
    }

    /** Writes one cell version. */
    @Public
    public static final class SetCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final ByteString qualifier;
        private final long timestampMicros;
        private final ByteString value;

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

        public ByteString getQualifier() {
            return qualifier;
        }

        public long getTimestampMicros() {
            return timestampMicros;
        }

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
    @Public
    public static final class DeleteCellsEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final ByteString qualifier;
        private final TimestampRange timestampRange;

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

        public ByteString getQualifier() {
            return qualifier;
        }

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
    @Public
    public static final class DeleteFamilyEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;

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
    @Public
    public static final class AddToCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final Value qualifier;
        private final Value timestamp;
        private final Value input;

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

        public Value getQualifier() {
            return qualifier;
        }

        public Value getTimestamp() {
            return timestamp;
        }

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
    @Public
    public static final class MergeToCellEntry extends Entry {
        private static final long serialVersionUID = 1L;

        private final String familyName;
        private final Value qualifier;
        private final Value timestamp;
        private final Value input;

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

        public Value getQualifier() {
            return qualifier;
        }

        public Value getTimestamp() {
            return timestamp;
        }

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
    @Public
    public abstract static class Value implements Serializable {
        private static final long serialVersionUID = 1L;

        private Value() {}

        public abstract ValueType getType();

        abstract <R, A> R accept(
                ChangeStreamMutationDispatcher.ValueVisitor<R, A> visitor, A argument)
                throws IOException;
    }

    @Public
    public enum ValueType {
        RAW_VALUE,
        RAW_TIMESTAMP,
        INT64
    }

    /** Arbitrary bytes in an aggregate entry. */
    @Public
    public static final class RawValue extends Value {
        private static final long serialVersionUID = 1L;

        private final ByteString value;

        public RawValue(ByteString value) {
            this.value = Preconditions.checkNotNull(value, "value must not be null");
        }

        @Override
        public ValueType getType() {
            return ValueType.RAW_VALUE;
        }

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
    @Public
    public static final class RawTimestamp extends Value {
        private static final long serialVersionUID = 1L;

        private final long value;

        public RawTimestamp(long value) {
            this.value = value;
        }

        @Override
        public ValueType getType() {
            return ValueType.RAW_TIMESTAMP;
        }

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
    @Public
    public static final class Int64Value extends Value {
        private static final long serialVersionUID = 1L;

        private final long value;

        public Int64Value(long value) {
            this.value = value;
        }

        @Override
        public ValueType getType() {
            return ValueType.INT64;
        }

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
    @Public
    public static final class TimestampBound implements Serializable {
        private static final long serialVersionUID = 1L;

        private final BoundType type;
        private final long timestampMicros;

        private TimestampBound(BoundType type, long timestampMicros) {
            this.type = Preconditions.checkNotNull(type, "type must not be null");
            this.timestampMicros = timestampMicros;
        }

        public static TimestampBound unbounded() {
            return new TimestampBound(BoundType.UNBOUNDED, 0L);
        }

        public static TimestampBound open(long timestampMicros) {
            return new TimestampBound(BoundType.OPEN, timestampMicros);
        }

        public static TimestampBound closed(long timestampMicros) {
            return new TimestampBound(BoundType.CLOSED, timestampMicros);
        }

        public BoundType getType() {
            return type;
        }

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

    @Public
    public enum BoundType {
        OPEN,
        CLOSED,
        UNBOUNDED
    }

    /** Timestamp range affected by a cell deletion. */
    @Public
    public static final class TimestampRange implements Serializable {
        private static final long serialVersionUID = 1L;

        private final TimestampBound start;
        private final TimestampBound end;

        public TimestampRange(TimestampBound start, TimestampBound end) {
            this.start = Preconditions.checkNotNull(start, "start must not be null");
            this.end = Preconditions.checkNotNull(end, "end must not be null");
        }

        public TimestampBound getStart() {
            return start;
        }

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
