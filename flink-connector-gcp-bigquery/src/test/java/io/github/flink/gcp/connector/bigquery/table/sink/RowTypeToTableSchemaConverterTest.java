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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowTypeToTableSchemaConverter}, pinning every row of the type mapping. */
class RowTypeToTableSchemaConverterTest {

    private static RowType rowOf(DataType type) {
        return (RowType) DataTypes.ROW(DataTypes.FIELD("v", type)).getLogicalType();
    }

    private static TableSchema convert(DataType type) {
        return RowTypeToTableSchemaConverter.convert(rowOf(type), RowDataSchemaOptions.defaults());
    }

    private static TableFieldSchema first(TableSchema schema) {
        return schema.getFields(0);
    }

    static Stream<Arguments> theTypeMapping() {
        return Stream.of(
                Arguments.of(DataTypes.CHAR(3), TableFieldSchema.Type.STRING),
                Arguments.of(DataTypes.VARCHAR(3), TableFieldSchema.Type.STRING),
                Arguments.of(DataTypes.STRING(), TableFieldSchema.Type.STRING),
                Arguments.of(DataTypes.BOOLEAN(), TableFieldSchema.Type.BOOL),
                Arguments.of(DataTypes.BINARY(4), TableFieldSchema.Type.BYTES),
                Arguments.of(DataTypes.VARBINARY(4), TableFieldSchema.Type.BYTES),
                Arguments.of(DataTypes.BYTES(), TableFieldSchema.Type.BYTES),
                Arguments.of(DataTypes.TINYINT(), TableFieldSchema.Type.INT64),
                Arguments.of(DataTypes.SMALLINT(), TableFieldSchema.Type.INT64),
                Arguments.of(DataTypes.INT(), TableFieldSchema.Type.INT64),
                Arguments.of(DataTypes.BIGINT(), TableFieldSchema.Type.INT64),
                Arguments.of(DataTypes.FLOAT(), TableFieldSchema.Type.DOUBLE),
                Arguments.of(DataTypes.DOUBLE(), TableFieldSchema.Type.DOUBLE),
                Arguments.of(DataTypes.DATE(), TableFieldSchema.Type.DATE),
                Arguments.of(DataTypes.TIME(3), TableFieldSchema.Type.TIME),
                Arguments.of(DataTypes.TIMESTAMP(6), TableFieldSchema.Type.DATETIME),
                Arguments.of(DataTypes.TIMESTAMP_LTZ(6), TableFieldSchema.Type.TIMESTAMP));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("theTypeMapping")
    void mapsEveryScalarType(DataType type, TableFieldSchema.Type expected) {
        assertThat(first(convert(type)).getType()).isEqualTo(expected);
    }

    @Test
    void aWallClockTimestampIsCivilAndAnInstantIsAnInstant() {
        // The deliberate deviation from the Dataproc connector, which maps these the other way
        // round — storing a wall-clock value as an instant and an instant as a wall-clock value.
        assertThat(first(convert(DataTypes.TIMESTAMP(6))).getType())
                .isEqualTo(TableFieldSchema.Type.DATETIME);
        assertThat(first(convert(DataTypes.TIMESTAMP_LTZ(6))).getType())
                .isEqualTo(TableFieldSchema.Type.TIMESTAMP);
    }

    @Test
    void decimalsSplitOnIntegerDigitsRatherThanTotalPrecision() {
        assertThat(first(convert(DataTypes.DECIMAL(38, 9))).getType())
                .isEqualTo(TableFieldSchema.Type.NUMERIC);
        // One more integer digit than NUMERIC holds, though the precision is unchanged.
        assertThat(first(convert(DataTypes.DECIMAL(38, 8))).getType())
                .isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        // Scale alone can push it over too.
        assertThat(first(convert(DataTypes.DECIMAL(20, 10))).getType())
                .isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
    }

    @Test
    void decimalsCarryTheirPrecisionAndScale() {
        TableFieldSchema field = first(convert(DataTypes.DECIMAL(20, 4)));
        assertThat(field.getPrecision()).isEqualTo(20);
        assertThat(field.getScale()).isEqualTo(4);
    }

    @Test
    void everyDecimalFlinkCanExpressFitsBigNumeric() {
        // Measured rather than assumed: Flink caps DECIMAL precision at 38, and BIGNUMERIC holds 38
        // integer digits and 38 fractional ones, so the "otherwise rejected" row of the mapping
        // table cannot fire from SQL at all. The check in the converter stays as the invariant it
        // shares with the Avro path — this pins that no SQL decimal reaches it.
        assertThat(first(convert(DataTypes.DECIMAL(38, 38))).getType())
                .isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(first(convert(DataTypes.DECIMAL(38, 0))).getType())
                .isEqualTo(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(first(convert(DataTypes.DECIMAL(1, 0))).getType())
                .isEqualTo(TableFieldSchema.Type.NUMERIC);
    }

    @Test
    void everyColumnIsNullableByDefault() {
        TableSchema schema =
                RowTypeToTableSchemaConverter.convert(
                        (RowType)
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.STRING().notNull()))
                                        .getLogicalType(),
                        RowDataSchemaOptions.defaults());
        assertThat(first(schema).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void notNullDerivesRequiredOnlyUnderTheOptIn() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("required", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("optional", DataTypes.STRING()))
                                .getLogicalType();
        TableSchema schema =
                RowTypeToTableSchemaConverter.convert(
                        rowType,
                        RowDataSchemaOptions.builder().deriveRequiredColumns(true).build());
        assertThat(schema.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(schema.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void anArrayIsRepeatedAndItsElementsMustNotBeNullable() {
        TableFieldSchema field = first(convert(DataTypes.ARRAY(DataTypes.STRING().notNull())));
        assertThat(field.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRING);

        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.STRING())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nullable elements");
    }

    @Test
    void arraysDoNotNest() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.STRING().notNull())
                                                        .notNull())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not nest");
    }

    @Test
    void aMapBecomesARepeatedKeyValueStruct() {
        TableFieldSchema field =
                first(
                        convert(
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(), DataTypes.INT().notNull())));
        assertThat(field.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(field.getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("key", "value");
        assertThat(field.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.INT64);
    }

    @Test
    void aMultisetBecomesTheSameShapeWithAnIntCount() {
        TableFieldSchema field = first(convert(DataTypes.MULTISET(DataTypes.STRING().notNull())));
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(field.getFields(1).getName()).isEqualTo("value");
        assertThat(field.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.INT64);
    }

    @Test
    void aRowBecomesAStructRecursively() {
        TableFieldSchema field =
                first(
                        convert(
                                DataTypes.ROW(
                                        DataTypes.FIELD("inner", DataTypes.INT()),
                                        DataTypes.FIELD(
                                                "deeper",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "leaf", DataTypes.STRING()))))));
        assertThat(field.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(field.getFields(1).getFields(0).getName()).isEqualTo("leaf");
    }

    @Test
    void columnsDifferingOnlyByCaseAreRejected() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING()),
                                        DataTypes.FIELD("ID", DataTypes.STRING()))
                                .getLogicalType();
        assertThatThrownBy(
                        () ->
                                RowTypeToTableSchemaConverter.convert(
                                        rowType, RowDataSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ only by case");
    }

    @Test
    void timePastMillisecondsIsRejectedBecauseARowCannotCarryIt() {
        assertThatThrownBy(() -> convert(DataTypes.TIME(6)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TIME(3)");
    }

    @Test
    void timestampPastMicrosecondsIsRejected() {
        assertThatThrownBy(() -> convert(DataTypes.TIMESTAMP(9)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("microseconds");
        assertThatThrownBy(() -> convert(DataTypes.TIMESTAMP_LTZ(9)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("microseconds");
    }

    @Test
    void theTypesBigQueryCannotHoldAreRejectedAtJobStart() {
        assertThatThrownBy(() -> convert(DataTypes.INTERVAL(DataTypes.DAY())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
        assertThatThrownBy(() -> convert(DataTypes.NULL()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
    }

    @Test
    void aMarkedStringBecomesJsonOrGeography() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("doc", DataTypes.STRING()),
                                        DataTypes.FIELD("where", DataTypes.STRING()))
                                .getLogicalType();
        TableSchema schema =
                RowTypeToTableSchemaConverter.convert(
                        rowType,
                        RowDataSchemaOptions.builder()
                                .jsonFieldPaths(Collections.singletonList("doc"))
                                .geographyFieldPaths(Collections.singletonList("where"))
                                .build());
        assertThat(schema.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(schema.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
    }

    @Test
    void aMarkedRowBecomesAJsonColumnRatherThanAStruct() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "doc",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a", DataTypes.INT()))))
                                .getLogicalType();
        TableSchema schema =
                RowTypeToTableSchemaConverter.convert(
                        rowType,
                        RowDataSchemaOptions.builder()
                                .jsonFieldPaths(Collections.singletonList("doc"))
                                .build());
        assertThat(schema.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(schema.getFields(0).getFieldsList()).isEmpty();
    }

    @Test
    void aGeographyMarkerOnARowIsRejected() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "where",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a", DataTypes.INT()))))
                                .getLogicalType();
        assertThatThrownBy(
                        () ->
                                RowTypeToTableSchemaConverter.convert(
                                        rowType,
                                        RowDataSchemaOptions.builder()
                                                .geographyFieldPaths(
                                                        Collections.singletonList("where"))
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GEOGRAPHY mapping requires");
    }

    @Test
    void aMarkerMatchingNoColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                RowTypeToTableSchemaConverter.convert(
                                        rowOf(DataTypes.STRING()),
                                        RowDataSchemaOptions.builder()
                                                .jsonFieldPaths(
                                                        Collections.singletonList("nowhere"))
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("matching no column");
    }

    @Test
    void aColumnClaimedByBothMarkersIsRejected() {
        assertThatThrownBy(
                        () ->
                                RowTypeToTableSchemaConverter.convert(
                                        rowOf(DataTypes.STRING()),
                                        RowDataSchemaOptions.builder()
                                                .jsonFieldPaths(Collections.singletonList("v"))
                                                .geographyFieldPaths(Collections.singletonList("v"))
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both a JSON and a GEOGRAPHY");
    }

    @Test
    void aNestedColumnIsMarkedByItsDottedPath() {
        RowType rowType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "event",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "body", DataTypes.STRING()))))
                                .getLogicalType();
        TableSchema schema =
                RowTypeToTableSchemaConverter.convert(
                        rowType,
                        RowDataSchemaOptions.builder()
                                .jsonFieldPaths(Arrays.asList("event.body"))
                                .build());
        assertThat(schema.getFields(0).getFields(0).getType())
                .isEqualTo(TableFieldSchema.Type.JSON);
    }

    @Test
    void aTableWithNoColumnsIsRejected() {
        assertThatThrownBy(
                        () ->
                                RowTypeToTableSchemaConverter.convert(
                                        (RowType) DataTypes.ROW().getLogicalType(),
                                        RowDataSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no columns");
    }
}
