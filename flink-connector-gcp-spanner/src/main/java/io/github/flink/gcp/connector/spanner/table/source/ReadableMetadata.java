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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Stable scalar fields a Spanner change-stream table can expose as metadata columns. */
@Internal
enum ReadableMetadata {

    /** The timestamp at which Spanner committed the data change, at nanosecond precision. */
    COMMIT_TIMESTAMP(
            "commit-timestamp",
            DataTypes.TIMESTAMP_LTZ(9).notNull(),
            (record, modNumber) -> TimestampData.fromInstant(record.getCommitTimestamp())),

    /** The record's sequence within its partition, commit timestamp, and transaction. */
    SEQUENCE(
            "sequence",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getRecordSequence())),

    /** The transaction identifier assigned by Spanner. */
    SERVER_TRANSACTION_ID(
            "server-transaction-id",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getServerTransactionId())),

    /** Whether this is the transaction's final record in the originating partition. */
    IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION(
            "is-last-record-in-transaction-in-partition",
            DataTypes.BOOLEAN().notNull(),
            (record, modNumber) -> record.isLastRecordInTransactionInPartition()),

    /** The native API table name reported by Spanner. */
    TABLE(
            "table",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getTableName())),

    /** The Spanner modification type, such as {@code INSERT}, {@code UPDATE}, or {@code DELETE}. */
    MOD_TYPE(
            "mod-type",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getModType().name())),

    /** The value-capture type configured on the change stream. */
    VALUE_CAPTURE_TYPE(
            "value-capture-type",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getValueCaptureType().name())),

    /** The number of data-change records in the originating transaction. */
    NUMBER_OF_RECORDS_IN_TRANSACTION(
            "number-of-records-in-transaction",
            DataTypes.BIGINT().notNull(),
            (record, modNumber) -> record.getNumberOfRecordsInTransaction()),

    /** The number of change-stream partitions containing the originating transaction. */
    NUMBER_OF_PARTITIONS_IN_TRANSACTION(
            "number-of-partitions-in-transaction",
            DataTypes.BIGINT().notNull(),
            (record, modNumber) -> record.getNumberOfPartitionsInTransaction()),

    /** The transaction tag, preserved as an empty string when none was supplied. */
    TRANSACTION_TAG(
            "transaction-tag",
            DataTypes.STRING().notNull(),
            (record, modNumber) -> StringData.fromString(record.getTransactionTag())),

    /** Whether Spanner identifies the transaction as a system transaction. */
    SYSTEM_TRANSACTION(
            "system-transaction",
            DataTypes.BOOLEAN().notNull(),
            (record, modNumber) -> record.isSystemTransaction()),

    /** The zero-based position of the mod in the original Spanner data-change record. */
    MOD_NUMBER("mod-number", DataTypes.INT().notNull(), (record, modNumber) -> modNumber);

    /** Reads one metadata value from a data-change record and its selected mod. */
    @FunctionalInterface
    interface MetadataConverter extends Serializable {

        Object read(DataChangeRecord record, int modNumber);
    }

    private final String key;
    private final DataType dataType;
    private final MetadataConverter converter;

    ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    MetadataConverter getConverter() {
        return converter;
    }

    /** Returns all readable metadata in the stable order presented to the planner. */
    static Map<String, DataType> listAll() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        for (ReadableMetadata value : values()) {
            metadata.put(value.key, value.dataType);
        }
        return Collections.unmodifiableMap(metadata);
    }

    /** Returns the metadata declaration with the given key. */
    static ReadableMetadata of(String key) {
        for (ReadableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException(
                "Unknown Spanner change-stream readable metadata key '" + key + "'.");
    }
}
