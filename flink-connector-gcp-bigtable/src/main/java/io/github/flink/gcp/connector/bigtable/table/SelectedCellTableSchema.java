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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * The physical primary key, the value-format payload and the commit columns of a selected-cell
 * table.
 *
 * <p>The commit columns are physical rather than metadata columns on purpose. Reading a metadata
 * column of an upsert source makes the Flink planner keep a {@code ChangelogNormalize} operator,
 * which completes a key-only delete from the last row it holds, so the delete carries the commit
 * timestamp of the change it deletes. Flink 2.x drops that operator when the query reads only
 * physical columns, and a delete then reaches the sink with its own commit timestamp and tie
 * breaker (#1960).
 */
@Internal
public final class SelectedCellTableSchema {

    private final DataType physicalDataType;
    private final DataType payloadDataType;
    private final int primaryKeyIndex;
    private final LogicalType primaryKeyType;
    private final int[] payloadPhysicalIndexes;
    private final int commitTimestampIndex;
    private final int tieBreakerIndex;

    private SelectedCellTableSchema(
            DataType physicalDataType,
            DataType payloadDataType,
            int primaryKeyIndex,
            LogicalType primaryKeyType,
            int[] payloadPhysicalIndexes,
            int commitTimestampIndex,
            int tieBreakerIndex) {
        this.physicalDataType = physicalDataType;
        this.payloadDataType = payloadDataType;
        this.primaryKeyIndex = primaryKeyIndex;
        this.primaryKeyType = primaryKeyType;
        this.payloadPhysicalIndexes = payloadPhysicalIndexes;
        this.commitTimestampIndex = commitTimestampIndex;
        this.tieBreakerIndex = tieBreakerIndex;
    }

    /**
     * Derives the format payload by removing the one declared primary-key field and the named
     * commit columns.
     *
     * @param commitTimestampColumn the physical column the source fills with the mutation's commit
     *     timestamp, or {@code null} for none
     * @param tieBreakerColumn the physical column the source fills with the mutation's tie breaker,
     *     or {@code null} for none
     */
    public static SelectedCellTableSchema of(
            DataType physicalDataType,
            int[] primaryKeyIndexes,
            @Nullable String commitTimestampColumn,
            @Nullable String tieBreakerColumn) {
        Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        if (primaryKeyIndexes.length != 1) {
            throw new ValidationException(
                    "A 'bigtable' selected-cell Change Streams source requires exactly one"
                            + " physical PRIMARY KEY column so the mutation row key identifies"
                            + " each upsert and delete.");
        }

        RowType physicalType = (RowType) physicalDataType.getLogicalType();
        int primaryKeyIndex = primaryKeyIndexes[0];
        String primaryKeyName = physicalType.getFieldNames().get(primaryKeyIndex);
        LogicalType primaryKeyType = physicalType.getTypeAt(primaryKeyIndex);
        CellValueCodec.checkSupported(primaryKeyName, primaryKeyType);

        int commitTimestampIndex =
                commitColumnIndex(
                        physicalType,
                        primaryKeyIndex,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_COMMIT_TIMESTAMP_COLUMN.key(),
                        commitTimestampColumn,
                        "TIMESTAMP_LTZ(9)",
                        type ->
                                type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                                        && ((LocalZonedTimestampType) type).getPrecision() == 9);
        int tieBreakerIndex =
                commitColumnIndex(
                        physicalType,
                        primaryKeyIndex,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_TIE_BREAKER_COLUMN.key(),
                        tieBreakerColumn,
                        "INT",
                        type -> type.getTypeRoot() == LogicalTypeRoot.INTEGER);
        // The two never resolve to one column: no type is both TIMESTAMP_LTZ(9) and INT.

        List<DataTypes.Field> payloadFields = new ArrayList<>();
        List<Integer> payloadIndexes = new ArrayList<>();
        for (int index = 0; index < physicalType.getFieldCount(); index++) {
            if (index != primaryKeyIndex
                    && index != commitTimestampIndex
                    && index != tieBreakerIndex) {
                payloadFields.add(
                        DataTypes.FIELD(
                                physicalType.getFieldNames().get(index),
                                physicalDataType.getChildren().get(index)));
                payloadIndexes.add(index);
            }
        }
        if (payloadFields.isEmpty()) {
            throw new ValidationException(
                    "A 'bigtable' selected-cell Change Streams source needs at least one"
                            + " non-key physical column for 'value.format' to decode, besides"
                            + " the commit-timestamp and tie-breaker columns.");
        }

        return new SelectedCellTableSchema(
                physicalDataType,
                DataTypes.ROW(payloadFields.toArray(new DataTypes.Field[0])),
                primaryKeyIndex,
                primaryKeyType,
                payloadIndexes.stream().mapToInt(Integer::intValue).toArray(),
                commitTimestampIndex,
                tieBreakerIndex);
    }

    /**
     * Resolves a commit column named by an option, or returns {@code -1} when the option is unset.
     * The column is a name this connector resolves against the DDL, so a name the DDL does not
     * declare, the primary key, or a column of another type fails here under the option key rather
     * than as a format or planner error that names neither.
     */
    private static int commitColumnIndex(
            RowType physicalType,
            int primaryKeyIndex,
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
        if (index == primaryKeyIndex) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' names the primary-key column '%s'; name a non-key"
                                    + " physical column.",
                            optionKey, column));
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

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }

    public DataType getPayloadDataType() {
        return payloadDataType;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    public LogicalType getPrimaryKeyType() {
        return primaryKeyType;
    }

    /** Returns the physical position of each payload field, in payload order. */
    public int[] getPayloadPhysicalIndexes() {
        return payloadPhysicalIndexes.clone();
    }

    /** Returns the physical position of the commit-timestamp column, or {@code -1} for none. */
    public int getCommitTimestampIndex() {
        return commitTimestampIndex;
    }

    /** Returns the physical position of the tie-breaker column, or {@code -1} for none. */
    public int getTieBreakerIndex() {
        return tieBreakerIndex;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SelectedCellTableSchema)) {
            return false;
        }
        SelectedCellTableSchema that = (SelectedCellTableSchema) other;
        return primaryKeyIndex == that.primaryKeyIndex
                && commitTimestampIndex == that.commitTimestampIndex
                && tieBreakerIndex == that.tieBreakerIndex
                && physicalDataType.equals(that.physicalDataType)
                && payloadDataType.equals(that.payloadDataType)
                && primaryKeyType.equals(that.primaryKeyType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalDataType,
                payloadDataType,
                primaryKeyIndex,
                primaryKeyType,
                commitTimestampIndex,
                tieBreakerIndex);
    }
}
