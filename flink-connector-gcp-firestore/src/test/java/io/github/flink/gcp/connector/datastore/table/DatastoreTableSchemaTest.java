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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreTableSchemaTest {

    private static RowType row(DataTypes.Field... fields) {
        return (RowType) DataTypes.ROW(fields).getLogicalType();
    }

    private static DatastoreTableSchema of(RowType row, int... key) {
        return DatastoreTableSchema.of(row, key, List.of());
    }

    @Test
    void everyMappedTypeIsAccepted() {
        RowType row =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("s", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.CHAR(3)),
                        DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD("d", DataTypes.DOUBLE()),
                        DataTypes.FIELD("bytes", DataTypes.BYTES()),
                        DataTypes.FIELD("fixed", DataTypes.BINARY(4)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD("x", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "items",
                                                DataTypes.ARRAY(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "y",
                                                                        DataTypes.STRING())))))));

        DatastoreTableSchema schema = of(row, 0);

        assertThat(schema.hasPrimaryKey()).isTrue();
        assertThat(schema.getKeyIndex()).isZero();
        assertThat(schema.isKeyId()).isFalse();
        assertThat(schema.getRowType()).isEqualTo(row);
    }

    @Test
    void aBigintKeyIsTheKeysNumericId() {
        DatastoreTableSchema schema =
                of(
                        row(
                                DataTypes.FIELD("s", DataTypes.STRING()),
                                DataTypes.FIELD("id", DataTypes.BIGINT().notNull())),
                        1);

        assertThat(schema.getKeyIndex()).isEqualTo(1);
        assertThat(schema.isKeyId()).isTrue();
    }

    @Test
    void aTableWithoutAKeyHasNone() {
        DatastoreTableSchema schema = of(row(DataTypes.FIELD("s", DataTypes.STRING())));

        assertThat(schema.hasPrimaryKey()).isFalse();
        assertThat(schema.getKeyIndex()).isEqualTo(-1);
        assertThat(schema.isKeyId()).isFalse();
    }

    @Test
    void aKeyOfAnotherTypeOrOfTwoColumnsIsRefused() {
        RowType intKey =
                row(
                        DataTypes.FIELD("id", DataTypes.INT().notNull()),
                        DataTypes.FIELD("s", DataTypes.STRING()));
        RowType twoColumns =
                row(
                        DataTypes.FIELD("a", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("b", DataTypes.STRING().notNull()));

        assertThatThrownBy(() -> of(intKey, 0))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must be STRING or BIGINT, but it is INT NOT NULL");
        assertThatThrownBy(() -> of(twoColumns, 0, 1))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("this one has 2 columns");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "INT",
                "FLOAT",
                "DECIMAL(10, 2)",
                "DATE",
                "TIMESTAMP(3)",
                "MAP<STRING, INT>"
            })
    void aTypeWithoutADatastoreFormIsRefused(String type) {
        DataType dataType =
                TypeConversions.fromLogicalToDataType(
                        LogicalTypeParser.parse(
                                type, DatastoreTableSchemaTest.class.getClassLoader()));

        assertThatThrownBy(() -> of(row(DataTypes.FIELD("v", dataType))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Field 'v' has the type");
        assertThatThrownBy(
                        () ->
                                of(
                                        row(
                                                DataTypes.FIELD(
                                                        "r",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD("v", dataType))))))
                .hasMessageContaining("Field 'r.v' has the type");
    }

    @Test
    void anArraysElementsAreCheckedLikeAColumn() {
        assertThatThrownBy(() -> of(row(DataTypes.FIELD("v", DataTypes.ARRAY(DataTypes.INT())))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Field 'v' has the type INT");
        assertThatThrownBy(
                        () ->
                                of(
                                        row(
                                                DataTypes.FIELD(
                                                        "items",
                                                        DataTypes.ARRAY(
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "__x__",
                                                                                DataTypes
                                                                                        .STRING())))))))
                .hasMessageContaining("Field 'items.__x__' has a name Datastore reserves");
    }

    @Test
    void anArrayOfArraysIsRefused() {
        assertThatThrownBy(
                        () ->
                                of(
                                        row(
                                                DataTypes.FIELD(
                                                        "v",
                                                        DataTypes.ARRAY(
                                                                DataTypes.ARRAY(
                                                                        DataTypes.STRING()))))))
                .hasMessageContaining("does not store an array directly in an array");
    }

    @Test
    void aReservedPropertyNameIsRefusedAtAnyDepth() {
        assertThatThrownBy(() -> of(row(DataTypes.FIELD("__key__", DataTypes.STRING()))))
                .hasMessageContaining("Field '__key__' has a name Datastore reserves");
        assertThatThrownBy(
                        () ->
                                of(
                                        row(
                                                DataTypes.FIELD(
                                                        "r",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "__x__",
                                                                        DataTypes.STRING()))))))
                .hasMessageContaining("Field 'r.__x__' has a name Datastore reserves");
        // Only both ends around at least one character reserve a name: the emulator stores the
        // kind '____'. A reserved key column is never a property.
        assertThat(of(row(DataTypes.FIELD("__x", DataTypes.STRING())))).isNotNull();
        assertThat(of(row(DataTypes.FIELD("____", DataTypes.STRING())))).isNotNull();
        assertThatThrownBy(() -> of(row(DataTypes.FIELD("_____", DataTypes.STRING()))))
                .hasMessageContaining("Field '_____' has a name Datastore reserves");
        assertThat(
                        of(
                                row(
                                        DataTypes.FIELD("__key__", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("s", DataTypes.STRING())),
                                0))
                .isNotNull();
    }

    @Test
    void aPropertyNameLongerThanDatastoreStoresIsRefused() {
        assertThatThrownBy(() -> of(row(DataTypes.FIELD("n".repeat(1501), DataTypes.STRING()))))
                .hasMessageContaining("longer than 1500 bytes");
        assertThat(of(row(DataTypes.FIELD("n".repeat(1500), DataTypes.STRING())))).isNotNull();
        // Bytes, not characters: 500 three-byte characters are 1,500 bytes.
        assertThatThrownBy(() -> of(row(DataTypes.FIELD("あ".repeat(501), DataTypes.STRING()))))
                .hasMessageContaining("longer than 1500 bytes");
    }

    @Test
    void theUnindexedColumnsAreTopLevelProperties() {
        RowType row =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("body", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "r", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.STRING()))));

        DatastoreTableSchema schema = DatastoreTableSchema.of(row, new int[] {0}, List.of("body"));

        assertThat(schema.isUnindexed("body")).isTrue();
        assertThat(schema.isUnindexed("r")).isFalse();
        assertThatThrownBy(() -> DatastoreTableSchema.of(row, new int[] {0}, List.of("id")))
                .hasMessageContaining("names the PRIMARY KEY column 'id'");
        assertThatThrownBy(() -> DatastoreTableSchema.of(row, new int[] {0}, List.of("r.x")))
                .hasMessageContaining("names columns the table does not declare: 'r.x'");
        // 'body;;r' parses to an empty element, which the message must still show.
        assertThatThrownBy(
                        () -> DatastoreTableSchema.of(row, new int[] {0}, List.of("body", "", "r")))
                .hasMessageContaining("names columns the table does not declare: ''");
    }

    @Test
    void equalSchemasAreEqual() {
        RowType row = row(DataTypes.FIELD("s", DataTypes.STRING()));

        assertThat(DatastoreTableSchema.of(row, new int[0], List.of("s")))
                .isEqualTo(DatastoreTableSchema.of(row, new int[0], List.of("s")))
                .hasSameHashCodeAs(DatastoreTableSchema.of(row, new int[0], List.of("s")))
                .isNotEqualTo(DatastoreTableSchema.of(row, new int[0], List.of()));
    }
}
