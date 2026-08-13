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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.util.Preconditions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One data-change record returned by a Spanner Change Streams read function. */
@PublicEvolving
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

    public DataChangeRecord(
            Instant commitTimestamp,
            String recordSequence,
            String serverTransactionId,
            boolean lastRecordInTransactionInPartition,
            String tableName,
            List<ColumnType> columnTypes,
            List<Mod> mods,
            ModType modType,
            ValueCaptureType valueCaptureType,
            long numberOfRecordsInTransaction,
            long numberOfPartitionsInTransaction,
            String transactionTag,
            boolean systemTransaction) {
        this.commitTimestamp =
                Preconditions.checkNotNull(commitTimestamp, "commitTimestamp must not be null");
        this.recordSequence =
                Preconditions.checkNotNull(recordSequence, "recordSequence must not be null");
        this.serverTransactionId =
                Preconditions.checkNotNull(
                        serverTransactionId, "serverTransactionId must not be null");
        this.lastRecordInTransactionInPartition = lastRecordInTransactionInPartition;
        this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null");
        Preconditions.checkNotNull(columnTypes, "columnTypes must not be null");
        Preconditions.checkArgument(
                !columnTypes.contains(null), "columnTypes must not contain null");
        this.columnTypes = new ArrayList<>(columnTypes);
        Preconditions.checkNotNull(mods, "mods must not be null");
        Preconditions.checkArgument(!mods.contains(null), "mods must not contain null");
        this.mods = new ArrayList<>(mods);
        this.modType = Preconditions.checkNotNull(modType, "modType must not be null");
        this.valueCaptureType =
                Preconditions.checkNotNull(valueCaptureType, "valueCaptureType must not be null");
        Preconditions.checkArgument(
                numberOfRecordsInTransaction >= 0,
                "numberOfRecordsInTransaction must not be negative");
        Preconditions.checkArgument(
                numberOfPartitionsInTransaction >= 0,
                "numberOfPartitionsInTransaction must not be negative");
        this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
        this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
        this.transactionTag =
                Preconditions.checkNotNull(transactionTag, "transactionTag must not be null");
        this.systemTransaction = systemTransaction;
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

    /** Per-record description of one watched column. */
    @PublicEvolving
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
