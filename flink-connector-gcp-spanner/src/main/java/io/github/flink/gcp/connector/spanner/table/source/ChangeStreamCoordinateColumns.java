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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The physical columns a change-stream table fills with a record's ordering coordinates, and the
 * watched columns that remain.
 *
 * <p>The coordinates are physical columns rather than metadata columns on purpose. Reading a
 * metadata column of an upsert source makes the Flink planner keep a {@code ChangelogNormalize}
 * operator, which completes a key-only delete from the last row it holds, so the delete carries the
 * coordinates of the change it deletes. Flink 2.x drops that operator when the query reads only
 * physical columns, and a delete then reaches the sink with its own coordinates (#1960).
 *
 * <p>The watched columns are what the record is validated against and converted from, so the
 * factory builds the table's {@code SpannerTableSchemaConverter} over {@link #getDataType()} and
 * this class widens each converted row back to the declared one.
 */
@Internal
public final class ChangeStreamCoordinateColumns implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType dataType;
    private final int[] dataPrimaryKeyIndexes;
    private final int physicalFieldCount;
    private final int[] dataPhysicalIndexes;
    private final RowData.FieldGetter[] dataGetters;
    private final int commitTimestampIndex;
    private final int sequenceIndex;
    private final int modNumberIndex;
    private final LinkedHashMap<String, String> optionKeysByColumn;

    private ChangeStreamCoordinateColumns(
            RowType dataType,
            int[] dataPrimaryKeyIndexes,
            int physicalFieldCount,
            int[] dataPhysicalIndexes,
            int commitTimestampIndex,
            int sequenceIndex,
            int modNumberIndex,
            LinkedHashMap<String, String> optionKeysByColumn) {
        this.dataType = dataType;
        this.dataPrimaryKeyIndexes = dataPrimaryKeyIndexes;
        this.physicalFieldCount = physicalFieldCount;
        this.dataPhysicalIndexes = dataPhysicalIndexes;
        this.dataGetters = new RowData.FieldGetter[dataType.getFieldCount()];
        for (int index = 0; index < dataGetters.length; index++) {
            // Nullable getters: a key-only delete leaves a NOT NULL watched column null, and
            // Flink 1.20's getter for a NOT NULL type reads it without a null check.
            dataGetters[index] =
                    RowData.createFieldGetter(dataType.getTypeAt(index).copy(true), index);
        }
        this.commitTimestampIndex = commitTimestampIndex;
        this.sequenceIndex = sequenceIndex;
        this.modNumberIndex = modNumberIndex;
        this.optionKeysByColumn = optionKeysByColumn;
    }

    /**
     * Resolves the columns the three options name, or returns {@code null} when none is set.
     *
     * @param physicalType the table's physical columns
     * @param primaryKeyIndexes the declared primary key's positions in {@code physicalType}
     * @param commitTimestampColumn the column for the commit timestamp, or {@code null}
     * @param sequenceColumn the column for the record sequence, or {@code null}
     * @param modNumberColumn the column for the mod number, or {@code null}
     * @return the resolved columns, or {@code null} when no option names one
     * @throws ValidationException naming the option when a column is undeclared, part of the
     *     primary key, or of a type that cannot hold the coordinate
     */
    @Nullable
    public static ChangeStreamCoordinateColumns resolve(
            RowType physicalType,
            int[] primaryKeyIndexes,
            @Nullable String commitTimestampColumn,
            @Nullable String sequenceColumn,
            @Nullable String modNumberColumn) {
        if (commitTimestampColumn == null && sequenceColumn == null && modNumberColumn == null) {
            return null;
        }
        int commitTimestampIndex =
                index(
                        physicalType,
                        primaryKeyIndexes,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_COMMIT_TIMESTAMP_COLUMN.key(),
                        commitTimestampColumn,
                        "TIMESTAMP_LTZ(9)",
                        type ->
                                type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                                        && ((LocalZonedTimestampType) type).getPrecision() == 9);
        int sequenceIndex =
                index(
                        physicalType,
                        primaryKeyIndexes,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_SEQUENCE_COLUMN.key(),
                        sequenceColumn,
                        "STRING",
                        type ->
                                type.getTypeRoot() == LogicalTypeRoot.VARCHAR
                                        && ((VarCharType) type).getLength()
                                                == VarCharType.MAX_LENGTH);
        int modNumberIndex =
                index(
                        physicalType,
                        primaryKeyIndexes,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_MOD_NUMBER_COLUMN.key(),
                        modNumberColumn,
                        "INT",
                        type -> type.getTypeRoot() == LogicalTypeRoot.INTEGER);
        // Two options cannot name one column: TIMESTAMP_LTZ(9), STRING and INT exclude each other.

        List<RowType.RowField> dataFields = new ArrayList<>();
        List<Integer> dataPhysicalIndexes = new ArrayList<>();
        int[] dataPositions = new int[physicalType.getFieldCount()];
        for (int index = 0; index < physicalType.getFieldCount(); index++) {
            if (index == commitTimestampIndex
                    || index == sequenceIndex
                    || index == modNumberIndex) {
                dataPositions[index] = -1;
                continue;
            }
            dataPositions[index] = dataFields.size();
            dataFields.add(physicalType.getFields().get(index));
            dataPhysicalIndexes.add(index);
        }
        int[] dataPrimaryKeyIndexes =
                Arrays.stream(primaryKeyIndexes).map(index -> dataPositions[index]).toArray();
        LinkedHashMap<String, String> optionKeysByColumn = new LinkedHashMap<>();
        if (commitTimestampColumn != null) {
            optionKeysByColumn.put(
                    commitTimestampColumn,
                    SpannerConnectorOptions.SCAN_CHANGE_STREAM_COMMIT_TIMESTAMP_COLUMN.key());
        }
        if (sequenceColumn != null) {
            optionKeysByColumn.put(
                    sequenceColumn,
                    SpannerConnectorOptions.SCAN_CHANGE_STREAM_SEQUENCE_COLUMN.key());
        }
        if (modNumberColumn != null) {
            optionKeysByColumn.put(
                    modNumberColumn,
                    SpannerConnectorOptions.SCAN_CHANGE_STREAM_MOD_NUMBER_COLUMN.key());
        }
        return new ChangeStreamCoordinateColumns(
                new RowType(physicalType.isNullable(), dataFields),
                dataPrimaryKeyIndexes,
                physicalType.getFieldCount(),
                dataPhysicalIndexes.stream().mapToInt(Integer::intValue).toArray(),
                commitTimestampIndex,
                sequenceIndex,
                modNumberIndex,
                optionKeysByColumn);
    }

    private static int index(
            RowType physicalType,
            int[] primaryKeyIndexes,
            String optionKey,
            @Nullable String column,
            String typeName,
            Predicate<LogicalType> accepts) {
        if (column == null) {
            return -1;
        }
        int index = physicalType.getFieldNames().indexOf(column);
        if (index < 0) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' names column '%s', which the table does not declare as"
                                    + " a physical column.",
                            optionKey, column));
        }
        for (int keyIndex : primaryKeyIndexes) {
            if (keyIndex == index) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' names the primary-key column '%s'; name a non-key"
                                        + " physical column.",
                                optionKey, column));
            }
        }
        LogicalType type = physicalType.getTypeAt(index);
        if (!accepts.test(type)) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' names column '%s' of type %s; declare it %s.",
                            optionKey, column, type.asSummaryString(), typeName));
        }
        return index;
    }

    /** Returns the watched columns: the physical columns without the coordinate columns. */
    public RowType getDataType() {
        return dataType;
    }

    /**
     * Returns the option that names a column, or {@code null} when no coordinate option names it.
     *
     * @param column a top-level physical column name
     */
    @Nullable
    public String optionKeyFor(String column) {
        return optionKeysByColumn.get(column);
    }

    /** Returns the names of the coordinate columns, none of which a record may carry. */
    Set<String> getColumnNames() {
        return Collections.unmodifiableSet(optionKeysByColumn.keySet());
    }

    /** Returns the declared primary key's positions in {@link #getDataType()}. */
    public int[] getDataPrimaryKeyIndexes() {
        return dataPrimaryKeyIndexes.clone();
    }

    /**
     * Widens a row converted over the watched columns to the declared physical row, filling the
     * coordinate columns from the record and the mod.
     */
    RowData widen(RowData data, DataChangeRecord record, int modNumber) {
        GenericRowData physical = new GenericRowData(data.getRowKind(), physicalFieldCount);
        for (int index = 0; index < dataGetters.length; index++) {
            physical.setField(dataPhysicalIndexes[index], dataGetters[index].getFieldOrNull(data));
        }
        // Filled on a delete as on an upsert: these are the delete's own coordinates, which is
        // what lets a downstream sink order it against the change it deletes.
        if (commitTimestampIndex >= 0) {
            physical.setField(
                    commitTimestampIndex,
                    ReadableMetadata.COMMIT_TIMESTAMP.getConverter().read(record, modNumber));
        }
        if (sequenceIndex >= 0) {
            physical.setField(
                    sequenceIndex,
                    ReadableMetadata.SEQUENCE.getConverter().read(record, modNumber));
        }
        if (modNumberIndex >= 0) {
            physical.setField(
                    modNumberIndex,
                    ReadableMetadata.MOD_NUMBER.getConverter().read(record, modNumber));
        }
        return physical;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ChangeStreamCoordinateColumns)) {
            return false;
        }
        ChangeStreamCoordinateColumns that = (ChangeStreamCoordinateColumns) other;
        return physicalFieldCount == that.physicalFieldCount
                && commitTimestampIndex == that.commitTimestampIndex
                && sequenceIndex == that.sequenceIndex
                && modNumberIndex == that.modNumberIndex
                && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                dataType, physicalFieldCount, commitTimestampIndex, sequenceIndex, modNumberIndex);
    }
}
