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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for reading the HBase-compatible DDL model out of a table's physical columns. */
class BigtableTableSchemaTest {

    private static RowType rowType(DataTypes.Field... fields) {
        return (RowType) DataTypes.ROW(fields).getLogicalType();
    }

    @Test
    void theAtomicColumnIsTheRowKeyAndEveryRowColumnIsAFamily() {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        rowType(
                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                DataTypes.FIELD(
                                        "cf1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("q1", DataTypes.STRING()),
                                                DataTypes.FIELD("q2", DataTypes.BIGINT()))),
                                DataTypes.FIELD(
                                        "cf2",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("metric", DataTypes.DOUBLE())))));

        assertThat(schema.getRowKeyIndex()).isZero();
        assertThat(schema.getRowKeyName()).isEqualTo("rowkey");
        assertThat(schema.getRowKeyType()).isEqualTo(DataTypes.STRING().getLogicalType());
        assertThat(schema.getFamilies())
                .extracting(BigtableTableSchema.Family::getName)
                .containsExactly("cf1", "cf2");
        assertThat(schema.getFamilies().get(0).getIndex()).isEqualTo(1);
        assertThat(schema.getFamilies().get(1).getIndex()).isEqualTo(2);
        assertThat(schema.getFamilies().get(0).getQualifiers())
                .extracting(BigtableTableSchema.Qualifier::getName)
                .containsExactly("q1", "q2");
        assertThat(schema.getFamilies().get(0).getQualifiers().get(1).getType())
                .isEqualTo(DataTypes.BIGINT().getLogicalType());
    }

    @Test
    void theRowKeyMayBeAnyColumn() {
        // Its position is read, not assumed: nothing requires it to be written first, and a
        // projection's indexes refer to the DDL order rather than to a normalised one.
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        rowType(
                                DataTypes.FIELD(
                                        "cf1",
                                        DataTypes.ROW(DataTypes.FIELD("q1", DataTypes.STRING()))),
                                DataTypes.FIELD("k", DataTypes.BIGINT())));

        assertThat(schema.getRowKeyIndex()).isEqualTo(1);
        assertThat(schema.getRowKeyName()).isEqualTo("k");
        assertThat(schema.getFamilies().get(0).getIndex()).isZero();
    }

    @Test
    void aTableMayDeclareNoFamilyAtAll() {
        // Legal as a model — a row-key-only table is readable. The sink rejects it separately,
        // because a mutation with no cell is not a write.
        BigtableTableSchema schema =
                BigtableTableSchema.of(rowType(DataTypes.FIELD("rowkey", DataTypes.STRING())));

        assertThat(schema.getFamilies()).isEmpty();
    }

    @Test
    void aSecondAtomicColumnIsRejectedNamingBoth() {
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD("stray", DataTypes.INT()))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'rowkey'")
                .hasMessageContaining("'stray'")
                .hasMessageContaining("exactly one atomic column");
    }

    @Test
    void aTableWithNoAtomicColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q1",
                                                                        DataTypes.STRING()))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("needs one atomic column to be its row key");
    }

    @Test
    void aNestedRowInsideAFamilyIsRejected() {
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "nested",
                                                                        DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        "x",
                                                                                        DataTypes
                                                                                                .INT()))))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'cf1.nested' is a ROW")
                .hasMessageContaining("Nested rows have no encoding here");
    }

    @Test
    void aFamilyNameWithAColonIsRejected() {
        // Not a general identifier check: a colon survives RE2 escaping, so a projection over such
        // a family would fail at read time instead of here.
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "ns:cf",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q1",
                                                                        DataTypes.STRING()))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'ns:cf' contains ':'");
    }

    @Test
    void anUnencodableQualifierIsRejectedByItsFullName() {
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "tags",
                                                                        DataTypes.ARRAY(
                                                                                DataTypes
                                                                                        .STRING())))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'cf1.tags'")
                .hasMessageContaining("no Bigtable cell encoding");
    }

    @Test
    void anUnencodableRowKeyIsRejectedToo() {
        // The row key goes through the same check as a cell, which is what keeps a MAP or an ARRAY
        // from being read as "the atomic column" merely because it is not a ROW.
        assertThatThrownBy(
                        () ->
                                BigtableTableSchema.of(
                                        rowType(
                                                DataTypes.FIELD(
                                                        "rowkey",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.INT())))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'rowkey'")
                .hasMessageContaining("no Bigtable cell encoding");
    }

    @Test
    void twoSchemasOfTheSameColumnsAreEqual() {
        RowType columns =
                rowType(
                        DataTypes.FIELD("rowkey", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("q1", DataTypes.STRING()))));

        assertThat(BigtableTableSchema.of(columns))
                .isEqualTo(BigtableTableSchema.of(columns))
                .hasSameHashCodeAs(BigtableTableSchema.of(columns));
    }
}
