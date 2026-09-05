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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataReadModifyWriteSerializationSchemaTest {
    @Test
    void skipsNullCellsAndFamiliesAndRetainsRowKeyEncodingAcrossSerialization() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.APPEND);
        schema = InstantiationUtil.clone(schema, getClass().getClassLoader());
        ReadModifyWriteRequest request =
                schema.serialize(
                        GenericRowData.of(
                                GenericRowData.of(null, StringData.fromString("value")), 7L, null),
                        null);
        assertThat(request.getRules()).hasSize(1);
        assertThat(request.getRowKey())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 7}));
    }

    @Test
    void nullKeysAllNullInputsEmptyAppendsAndNonInsertRowsFail() {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.APPEND);
        assertThatThrownBy(() -> schema.serialize(GenericRowData.of(null, null, null), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("row-key column 'k' is null");
        assertThatThrownBy(() -> schema.serialize(GenericRowData.of(null, 1L, null), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("at least one nonnull cell");
        assertThatThrownBy(
                        () ->
                                schema.serialize(
                                        GenericRowData.of(GenericRowData.of(null, null), 1L, null),
                                        null))
                .hasMessageContaining("at least one nonnull cell");
        assertThatThrownBy(
                        () ->
                                schema.serialize(
                                        GenericRowData.of(
                                                GenericRowData.of(StringData.fromString(""), null),
                                                1L,
                                                null),
                                        null))
                .hasMessageContaining("cf.a")
                .hasMessageContaining("empty");
        GenericRowData update =
                GenericRowData.of(GenericRowData.of(StringData.fromString("v"), null), 1L, null);
        update.setRowKind(RowKind.UPDATE_AFTER);
        assertThatThrownBy(() -> schema.serialize(update, null))
                .hasMessageContaining("INSERT-only");
    }

    @Test
    void zeroAndNegativeIncrementsAreNotSkipped() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.INCREMENT);
        ReadModifyWriteRequest request =
                schema.serialize(GenericRowData.of(GenericRowData.of(0L, -1L), 1L, null), null);
        assertThat(request.getRules()).hasSize(2);
    }

    private static RowDataReadModifyWriteSerializationSchema schema(WriteMode mode) {
        org.apache.flink.table.types.DataType type =
                mode == WriteMode.APPEND ? DataTypes.STRING() : DataTypes.BIGINT();
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "cf",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a", type),
                                                        DataTypes.FIELD("b", type))),
                                        DataTypes.FIELD("k", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "other", DataTypes.ROW(DataTypes.FIELD("v", type))))
                                .getLogicalType();
        return new RowDataReadModifyWriteSerializationSchema(BigtableTableSchema.of(row), mode);
    }
}
