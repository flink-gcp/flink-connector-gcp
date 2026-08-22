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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The physical primary key and value-format payload of a selected-cell table. */
@Internal
public final class SelectedCellTableSchema {

    private final DataType physicalDataType;
    private final DataType payloadDataType;
    private final int primaryKeyIndex;
    private final LogicalType primaryKeyType;

    private SelectedCellTableSchema(
            DataType physicalDataType,
            DataType payloadDataType,
            int primaryKeyIndex,
            LogicalType primaryKeyType) {
        this.physicalDataType = physicalDataType;
        this.payloadDataType = payloadDataType;
        this.primaryKeyIndex = primaryKeyIndex;
        this.primaryKeyType = primaryKeyType;
    }

    /** Derives the format payload by removing the one declared primary-key field. */
    public static SelectedCellTableSchema of(DataType physicalDataType, int[] primaryKeyIndexes) {
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

        List<DataTypes.Field> payloadFields = new ArrayList<>();
        for (int index = 0; index < physicalType.getFieldCount(); index++) {
            if (index != primaryKeyIndex) {
                payloadFields.add(
                        DataTypes.FIELD(
                                physicalType.getFieldNames().get(index),
                                physicalDataType.getChildren().get(index)));
            }
        }
        if (payloadFields.isEmpty()) {
            throw new ValidationException(
                    "A 'bigtable' selected-cell Change Streams source needs at least one"
                            + " non-key physical column for 'value.format' to decode.");
        }

        return new SelectedCellTableSchema(
                physicalDataType,
                DataTypes.ROW(payloadFields.toArray(new DataTypes.Field[0])),
                primaryKeyIndex,
                primaryKeyType);
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
                && physicalDataType.equals(that.physicalDataType)
                && payloadDataType.equals(that.payloadDataType)
                && primaryKeyType.equals(that.primaryKeyType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalDataType, payloadDataType, primaryKeyIndex, primaryKeyType);
    }
}
