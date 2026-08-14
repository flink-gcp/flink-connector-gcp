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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** One complete logical row mutation returned by Bigtable Change Streams. */
@PublicEvolving
@TypeInfo(ChangeStreamMutationTypeInfoFactory.class)
public final class ChangeStreamMutation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ByteString rowKey;
    private final MutationType type;
    private final String sourceClusterId;
    private final Instant commitTime;
    private final int tieBreaker;
    private final String token;
    private final Instant estimatedLowWatermarkTime;
    private final List<Entry> entries;

    public ChangeStreamMutation(
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

    @PublicEvolving
    public enum MutationType {
        USER,
        GARBAGE_COLLECTION
    }

    /** One ordered mutation entry. */
    @PublicEvolving
    public abstract static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;

        private Entry() {}

        public abstract String getFamilyName();
    }

    /** Writes one cell version. */
    @PublicEvolving
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

        public DeleteFamilyEntry(String familyName) {
            this.familyName = Preconditions.checkNotNull(familyName, "familyName must not be null");
        }

        @Override
        public String getFamilyName() {
            return familyName;
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

    /** One typed value used by an aggregate entry. */
    @PublicEvolving
    public abstract static class Value implements Serializable {
        private static final long serialVersionUID = 1L;

        private Value() {}

        public abstract ValueType getType();
    }

    @PublicEvolving
    public enum ValueType {
        RAW_VALUE,
        RAW_TIMESTAMP,
        INT64
    }

    /** Arbitrary bytes in an aggregate entry. */
    @PublicEvolving
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

    @PublicEvolving
    public enum BoundType {
        OPEN,
        CLOSED,
        UNBOUNDED
    }

    /** Timestamp range affected by a cell deletion. */
    @PublicEvolving
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
