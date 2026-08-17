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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One data-change record returned by a Spanner Change Streams read function.
 *
 * <p>Built through {@link #builder()}, and never positionally: the record carries thirteen values,
 * among them two {@code long}s that count the same kind of thing and two {@code boolean}s, so a
 * constructor taking them in order would accept a transposed pair without complaint. Every value is
 * required — {@link Builder#build()} names the ones that were not supplied rather than defaulting
 * them, so a forgotten count cannot arrive as {@code 0}.
 *
 * <p>{@link #toBuilder()} is the way to derive a record from another, which is what the connector's
 * own column projection does: it changes {@code columnTypes} and {@code mods} and keeps the other
 * eleven values.
 *
 * <p>Instances are immutable and compare by value. There is deliberately <b>no {@code
 * toString}</b>: a record's mods hold the row's own data as JSON, and a value type whose {@code
 * toString} prints user data is one accidental log line away from putting that data where it does
 * not belong.
 */
@Public
@TypeInfo(DataChangeRecordTypeInfoFactory.class)
public final class DataChangeRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Instant commitTimestamp;
    private final String recordSequence;
    private final String serverTransactionId;
    private final boolean lastRecordInTransactionInPartition;
    private final String tableName;
    private final List<ColumnType> columnTypes;
    private final List<Mod> mods;
    private final ModType modType;
    private final ValueCaptureType valueCaptureType;
    private final long numberOfRecordsInTransaction;
    private final long numberOfPartitionsInTransaction;
    private final String transactionTag;
    private final boolean systemTransaction;

    private DataChangeRecord(Builder builder) {
        this.commitTimestamp = builder.commitTimestamp;
        this.recordSequence = builder.recordSequence;
        this.serverTransactionId = builder.serverTransactionId;
        this.lastRecordInTransactionInPartition = builder.lastRecordInTransactionInPartition;
        this.tableName = builder.tableName;
        // Taken, not copied: the two setters are the only way to reach these fields and each one
        // binds a fresh copy, so a builder reused or re-set after build() cannot reach a record's
        // list. Copying again here would cost the projection path — which calls toBuilder() per
        // filtered record — two more allocations for nothing.
        this.columnTypes = builder.columnTypes;
        this.mods = builder.mods;
        this.modType = builder.modType;
        this.valueCaptureType = builder.valueCaptureType;
        this.numberOfRecordsInTransaction = builder.numberOfRecordsInTransaction;
        this.numberOfPartitionsInTransaction = builder.numberOfPartitionsInTransaction;
        this.transactionTag = builder.transactionTag;
        this.systemTransaction = builder.systemTransaction;
    }

    /**
     * Creates a builder with nothing set.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder carrying every value of this record, for deriving one record from another.
     *
     * @return a builder holding this record's values
     */
    public Builder toBuilder() {
        return new Builder()
                .commitTimestamp(commitTimestamp)
                .recordSequence(recordSequence)
                .serverTransactionId(serverTransactionId)
                .lastRecordInTransactionInPartition(lastRecordInTransactionInPartition)
                .tableName(tableName)
                .columnTypes(columnTypes)
                .mods(mods)
                .modType(modType)
                .valueCaptureType(valueCaptureType)
                .numberOfRecordsInTransaction(numberOfRecordsInTransaction)
                .numberOfPartitionsInTransaction(numberOfPartitionsInTransaction)
                .transactionTag(transactionTag)
                .systemTransaction(systemTransaction);
    }

    public Instant getCommitTimestamp() {
        return commitTimestamp;
    }

    public String getRecordSequence() {
        return recordSequence;
    }

    public String getServerTransactionId() {
        return serverTransactionId;
    }

    public boolean isLastRecordInTransactionInPartition() {
        return lastRecordInTransactionInPartition;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnType> getColumnTypes() {
        return Collections.unmodifiableList(columnTypes);
    }

    public List<Mod> getMods() {
        return Collections.unmodifiableList(mods);
    }

    public ModType getModType() {
        return modType;
    }

    public ValueCaptureType getValueCaptureType() {
        return valueCaptureType;
    }

    public long getNumberOfRecordsInTransaction() {
        return numberOfRecordsInTransaction;
    }

    public long getNumberOfPartitionsInTransaction() {
        return numberOfPartitionsInTransaction;
    }

    public String getTransactionTag() {
        return transactionTag;
    }

    public boolean isSystemTransaction() {
        return systemTransaction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataChangeRecord)) {
            return false;
        }
        DataChangeRecord that = (DataChangeRecord) o;
        return lastRecordInTransactionInPartition == that.lastRecordInTransactionInPartition
                && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
                && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
                && systemTransaction == that.systemTransaction
                && commitTimestamp.equals(that.commitTimestamp)
                && recordSequence.equals(that.recordSequence)
                && serverTransactionId.equals(that.serverTransactionId)
                && tableName.equals(that.tableName)
                && columnTypes.equals(that.columnTypes)
                && mods.equals(that.mods)
                && modType == that.modType
                && valueCaptureType == that.valueCaptureType
                && transactionTag.equals(that.transactionTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                commitTimestamp,
                recordSequence,
                serverTransactionId,
                lastRecordInTransactionInPartition,
                tableName,
                columnTypes,
                mods,
                modType,
                valueCaptureType,
                numberOfRecordsInTransaction,
                numberOfPartitionsInTransaction,
                transactionTag,
                systemTransaction);
    }

    /**
     * Builder for {@link DataChangeRecord}. Every value is required; {@link #build()} names the
     * ones that are missing.
     */
    @Public
    public static final class Builder {

        @Nullable private Instant commitTimestamp;
        @Nullable private String recordSequence;
        @Nullable private String serverTransactionId;
        private boolean lastRecordInTransactionInPartition;
        private boolean lastRecordInTransactionInPartitionSet;
        @Nullable private String tableName;
        @Nullable private List<ColumnType> columnTypes;
        @Nullable private List<Mod> mods;
        @Nullable private ModType modType;
        @Nullable private ValueCaptureType valueCaptureType;
        private long numberOfRecordsInTransaction = -1;
        private long numberOfPartitionsInTransaction = -1;
        private boolean systemTransaction;
        private boolean systemTransactionSet;
        @Nullable private String transactionTag;

        private Builder() {}

        /**
         * Sets the timestamp at which Spanner committed the change.
         *
         * @param commitTimestamp the commit timestamp
         * @return this builder
         */
        public Builder commitTimestamp(Instant commitTimestamp) {
            this.commitTimestamp =
                    Preconditions.checkNotNull(commitTimestamp, "commitTimestamp must not be null");
            return this;
        }

        /**
         * Sets the record's sequence within its partition, commit timestamp and transaction.
         *
         * @param recordSequence the record sequence
         * @return this builder
         */
        public Builder recordSequence(String recordSequence) {
            this.recordSequence =
                    Preconditions.checkNotNull(recordSequence, "recordSequence must not be null");
            return this;
        }

        /**
         * Sets the transaction identifier Spanner assigned.
         *
         * @param serverTransactionId the server transaction id
         * @return this builder
         */
        public Builder serverTransactionId(String serverTransactionId) {
            this.serverTransactionId =
                    Preconditions.checkNotNull(
                            serverTransactionId, "serverTransactionId must not be null");
            return this;
        }

        /**
         * Sets whether this is the transaction's final record in the originating partition.
         *
         * @param lastRecordInTransactionInPartition whether this is the last record
         * @return this builder
         */
        public Builder lastRecordInTransactionInPartition(
                boolean lastRecordInTransactionInPartition) {
            this.lastRecordInTransactionInPartition = lastRecordInTransactionInPartition;
            this.lastRecordInTransactionInPartitionSet = true;
            return this;
        }

        /**
         * Sets the table the change applies to, as Spanner reported it.
         *
         * @param tableName the table name
         * @return this builder
         */
        public Builder tableName(String tableName) {
            this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null");
            return this;
        }

        /**
         * Sets the watched columns and their type descriptors.
         *
         * @param columnTypes the column descriptors, none of them null
         * @return this builder
         */
        public Builder columnTypes(List<ColumnType> columnTypes) {
            Preconditions.checkNotNull(columnTypes, "columnTypes must not be null");
            Preconditions.checkArgument(
                    !columnTypes.contains(null), "columnTypes must not contain null");
            this.columnTypes = new ArrayList<>(columnTypes);
            return this;
        }

        /**
         * Sets the row modifications this record reports.
         *
         * @param mods the modifications, none of them null
         * @return this builder
         */
        public Builder mods(List<Mod> mods) {
            Preconditions.checkNotNull(mods, "mods must not be null");
            Preconditions.checkArgument(!mods.contains(null), "mods must not contain null");
            this.mods = new ArrayList<>(mods);
            return this;
        }

        /**
         * Sets the operation the record represents.
         *
         * @param modType the modification type
         * @return this builder
         */
        public Builder modType(ModType modType) {
            this.modType = Preconditions.checkNotNull(modType, "modType must not be null");
            return this;
        }

        /**
         * Sets the value-capture policy that was active when the change was recorded.
         *
         * @param valueCaptureType the value-capture type
         * @return this builder
         */
        public Builder valueCaptureType(ValueCaptureType valueCaptureType) {
            this.valueCaptureType =
                    Preconditions.checkNotNull(
                            valueCaptureType, "valueCaptureType must not be null");
            return this;
        }

        /**
         * Sets how many data-change records the originating transaction produced.
         *
         * @param numberOfRecordsInTransaction the record count, not negative
         * @return this builder
         */
        public Builder numberOfRecordsInTransaction(long numberOfRecordsInTransaction) {
            Preconditions.checkArgument(
                    numberOfRecordsInTransaction >= 0,
                    "numberOfRecordsInTransaction must not be negative");
            this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
            return this;
        }

        /**
         * Sets how many change-stream partitions contained the originating transaction.
         *
         * @param numberOfPartitionsInTransaction the partition count, not negative
         * @return this builder
         */
        public Builder numberOfPartitionsInTransaction(long numberOfPartitionsInTransaction) {
            Preconditions.checkArgument(
                    numberOfPartitionsInTransaction >= 0,
                    "numberOfPartitionsInTransaction must not be negative");
            this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
            return this;
        }

        /**
         * Sets the transaction tag, which is an empty string when Spanner supplied none.
         *
         * @param transactionTag the transaction tag
         * @return this builder
         */
        public Builder transactionTag(String transactionTag) {
            this.transactionTag =
                    Preconditions.checkNotNull(transactionTag, "transactionTag must not be null");
            return this;
        }

        /**
         * Sets whether Spanner identifies the transaction as a system transaction.
         *
         * @param systemTransaction whether the transaction is a system one
         * @return this builder
         */
        public Builder systemTransaction(boolean systemTransaction) {
            this.systemTransaction = systemTransaction;
            this.systemTransactionSet = true;
            return this;
        }

        /**
         * Builds the record.
         *
         * <p>The two counts and the two flags are tracked as set-or-not rather than left at a
         * default, because a default is indistinguishable from a value Spanner reported: a count
         * silently defaulting to {@code 0} would describe a transaction that produced no records.
         *
         * @return the record
         * @throws IllegalStateException naming every value that was not supplied
         */
        public DataChangeRecord build() {
            List<String> missing = new ArrayList<>();
            require(missing, commitTimestamp, "commitTimestamp");
            require(missing, recordSequence, "recordSequence");
            require(missing, serverTransactionId, "serverTransactionId");
            require(missing, tableName, "tableName");
            require(missing, columnTypes, "columnTypes");
            require(missing, mods, "mods");
            require(missing, modType, "modType");
            require(missing, valueCaptureType, "valueCaptureType");
            require(missing, transactionTag, "transactionTag");
            if (!lastRecordInTransactionInPartitionSet) {
                missing.add("lastRecordInTransactionInPartition");
            }
            if (!systemTransactionSet) {
                missing.add("systemTransaction");
            }
            if (numberOfRecordsInTransaction < 0) {
                missing.add("numberOfRecordsInTransaction");
            }
            if (numberOfPartitionsInTransaction < 0) {
                missing.add("numberOfPartitionsInTransaction");
            }
            Preconditions.checkState(
                    missing.isEmpty(),
                    "A DataChangeRecord needs every value; these were not set: %s",
                    String.join(", ", missing));
            return new DataChangeRecord(this);
        }

        private static void require(List<String> missing, @Nullable Object value, String name) {
            if (value == null) {
                missing.add(name);
            }
        }
    }

    /** Per-record description of one watched column. */
    @Public
    public static final class ColumnType implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final String typeDescriptorJson;
        private final boolean primaryKey;
        private final long ordinalPosition;

        public ColumnType(
                String name, String typeDescriptorJson, boolean primaryKey, long ordinalPosition) {
            this.name = Preconditions.checkNotNull(name, "name must not be null");
            this.typeDescriptorJson =
                    SpannerChangeStreamJsonNormalizer.normalizeObject(
                            Preconditions.checkNotNull(
                                    typeDescriptorJson, "typeDescriptorJson must not be null"),
                            "typeDescriptorJson");
            this.primaryKey = primaryKey;
            Preconditions.checkArgument(ordinalPosition > 0, "ordinalPosition must be positive");
            this.ordinalPosition = ordinalPosition;
        }

        public String getName() {
            return name;
        }

        /**
         * Returns the complete type descriptor supplied with this record as normalized JSON.
         *
         * <p>The descriptor, rather than the current client library's type enum, is authoritative.
         * Nested array descriptors, annotations, {@code TOKENLIST}, and future codes therefore
         * survive deserialization and Java serialization unchanged.
         */
        public String getTypeDescriptorJson() {
            return typeDescriptorJson;
        }

        /** Returns the descriptor's string {@code code}, or empty if it has none. */
        public Optional<String> getTypeCode() {
            JsonObject descriptor = JsonParser.parseString(typeDescriptorJson).getAsJsonObject();
            JsonElement code = descriptor.get("code");
            return code != null && code.isJsonPrimitive() && code.getAsJsonPrimitive().isString()
                    ? Optional.of(code.getAsString())
                    : Optional.empty();
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public long getOrdinalPosition() {
            return ordinalPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnType)) {
                return false;
            }
            ColumnType that = (ColumnType) o;
            return primaryKey == that.primaryKey
                    && ordinalPosition == that.ordinalPosition
                    && name.equals(that.name)
                    && typeDescriptorJson.equals(that.typeDescriptorJson);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, typeDescriptorJson, primaryKey, ordinalPosition);
        }
    }
}
