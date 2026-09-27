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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldElementType;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Every row of the BigQuery-to-Flink mapping, and every refusal naming its column. */
class BigQuerySchemaToFlinkConverterTest {

    static Stream<Arguments> scalarTypes() {
        return Stream.of(
                Arguments.of(StandardSQLTypeName.BOOL, DataTypes.BOOLEAN()),
                Arguments.of(StandardSQLTypeName.INT64, DataTypes.BIGINT()),
                Arguments.of(StandardSQLTypeName.FLOAT64, DataTypes.DOUBLE()),
                Arguments.of(StandardSQLTypeName.NUMERIC, DataTypes.DECIMAL(38, 9)),
                Arguments.of(StandardSQLTypeName.STRING, DataTypes.STRING()),
                Arguments.of(StandardSQLTypeName.JSON, DataTypes.STRING()),
                Arguments.of(StandardSQLTypeName.GEOGRAPHY, DataTypes.STRING()),
                Arguments.of(StandardSQLTypeName.BYTES, DataTypes.BYTES()),
                Arguments.of(StandardSQLTypeName.DATE, DataTypes.DATE()),
                Arguments.of(StandardSQLTypeName.TIME, DataTypes.TIME(3)),
                Arguments.of(StandardSQLTypeName.DATETIME, DataTypes.TIMESTAMP(6)),
                Arguments.of(StandardSQLTypeName.TIMESTAMP, DataTypes.TIMESTAMP_LTZ(6)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scalarTypes")
    void mapsEachScalarTypeToTheWidestFlinkTypeTheConnectorReads(
            StandardSQLTypeName type, DataType expected) {
        assertThat(convert(Field.of("c", type))).isEqualTo(expected);
    }

    @Test
    void requiredIsNotNullAndNullableIsNullable() {
        assertThat(convert(field("c", StandardSQLTypeName.STRING, Field.Mode.REQUIRED)))
                .isEqualTo(DataTypes.STRING().notNull());
        assertThat(convert(field("c", StandardSQLTypeName.STRING, Field.Mode.NULLABLE)))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void repeatedIsANullableArrayOfNotNullElements() {
        // The sink rejects a nullable element declaration, and BigQuery stores no null element.
        assertThat(convert(field("c", StandardSQLTypeName.INT64, Field.Mode.REPEATED)))
                .isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT().notNull()));
    }

    @Test
    void structIsARowRecursivelyAndKeepsFieldDescriptions() {
        Field inner =
                Field.newBuilder("inner", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .setDescription("the inner one")
                        .build();
        Field nested =
                Field.newBuilder(
                                "nested",
                                StandardSQLTypeName.STRUCT,
                                Field.of("tags", StandardSQLTypeName.STRING).toBuilder()
                                        .setMode(Field.Mode.REPEATED)
                                        .build())
                        .build();
        Field struct =
                Field.newBuilder("s", StandardSQLTypeName.STRUCT, inner, nested)
                        .setMode(Field.Mode.REPEATED)
                        .build();

        DataType nestedRow =
                DataTypes.ROW(
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING().notNull())));
        DataType element =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "inner", DataTypes.STRING().notNull(), "the inner one"),
                                DataTypes.FIELD("nested", nestedRow))
                        .notNull();
        assertThat(convert(struct)).isEqualTo(DataTypes.ARRAY(element));
    }

    static Stream<Arguments> rangeTypes() {
        return Stream.of(
                Arguments.of("DATE", DataTypes.DATE()),
                Arguments.of("DATETIME", DataTypes.TIMESTAMP(6)),
                Arguments.of("TIMESTAMP", DataTypes.TIMESTAMP_LTZ(6)));
    }

    @ParameterizedTest(name = "RANGE<{0}>")
    @MethodSource("rangeTypes")
    void rangeIsARowOfNullableStartAndEnd(String elementType, DataType endpoint) {
        Field range =
                Field.newBuilder("r", StandardSQLTypeName.RANGE)
                        .setRangeElementType(
                                FieldElementType.newBuilder().setType(elementType).build())
                        .build();

        assertThat(convert(range))
                .isEqualTo(
                        DataTypes.ROW(
                                DataTypes.FIELD("start", endpoint),
                                DataTypes.FIELD("end", endpoint)));
    }

    @Test
    void parameterizedDecimalsKeepTheirPrecisionAndScale() {
        assertThat(convert(decimal(StandardSQLTypeName.NUMERIC, 10L, 2L)))
                .isEqualTo(DataTypes.DECIMAL(10, 2));
        // A precision without a scale means scale 0, as BigQuery defines NUMERIC(P).
        assertThat(convert(decimal(StandardSQLTypeName.NUMERIC, 12L, null)))
                .isEqualTo(DataTypes.DECIMAL(12, 0));
        assertThat(convert(decimal(StandardSQLTypeName.BIGNUMERIC, 38L, 20L)))
                .isEqualTo(DataTypes.DECIMAL(38, 20));
    }

    @Test
    void refusesAnUnparameterizedBignumericNamingTheColumn() {
        assertThatThrownBy(() -> convert(Field.of("amount", StandardSQLTypeName.BIGNUMERIC)))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("Column 'amount'")
                .hasMessageContaining("BIGNUMERIC")
                .hasMessageContaining("76 digits");
    }

    @Test
    void refusesABignumericWiderThanFlinkDecimal() {
        assertThatThrownBy(() -> convert(decimal(StandardSQLTypeName.BIGNUMERIC, 39L, 0L)))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("BIGNUMERIC(39)")
                .hasMessageContaining("at most 38 digits");
    }

    @Test
    void refusesIntervalNamingTheNestedColumnPath() {
        Field struct =
                Field.newBuilder(
                                "outer",
                                StandardSQLTypeName.STRUCT,
                                Field.of("wait", StandardSQLTypeName.INTERVAL))
                        .build();

        assertThatThrownBy(() -> convert(struct))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("Column 'outer.wait' has BigQuery type INTERVAL");
    }

    @Test
    void refusesATypeTheClientHasNoStandardNameForNamingTheColumn() {
        // The client keeps an unknown type name, such as FOREIGN, without a standard equivalent.
        Field foreign = Field.of("f", LegacySQLTypeName.valueOf("FOREIGN"));

        assertThatThrownBy(() -> convert(foreign))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("Column 'f' has BigQuery type FOREIGN");
    }

    @Test
    void refusesATimestampFinerThanMicroseconds() {
        Field picos =
                Field.newBuilder("t", StandardSQLTypeName.TIMESTAMP)
                        .setTimestampPrecision(12L)
                        .build();
        Field micros =
                Field.newBuilder("t", StandardSQLTypeName.TIMESTAMP)
                        .setTimestampPrecision(6L)
                        .build();

        assertThatThrownBy(() -> convert(picos))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("TIMESTAMP(12)");
        assertThat(convert(micros)).isEqualTo(DataTypes.TIMESTAMP_LTZ(6));
    }

    @Test
    void aPrimaryKeyMatchesColumnsIgnoringCaseAndKeepsTheConstraintsOrder() {
        FieldList fields =
                FieldList.of(
                        Field.of("Region", StandardSQLTypeName.STRING),
                        Field.of("id", StandardSQLTypeName.STRING));

        Schema schema =
                BigQuerySchemaToFlinkConverter.toSchema(fields, Arrays.asList("ID", "region"));

        assertThat(schema.getPrimaryKey())
                .hasValueSatisfying(
                        key -> assertThat(key.getColumnNames()).containsExactly("id", "Region"));
    }

    @Test
    void theSchemaCarriesColumnCommentsAndAPrimaryKeyWhoseColumnsBecomeNotNull() {
        FieldList fields =
                FieldList.of(
                        Field.newBuilder("id", StandardSQLTypeName.STRING)
                                .setDescription("the key")
                                .build(),
                        Field.of("amount", StandardSQLTypeName.INT64));

        Schema schema = BigQuerySchemaToFlinkConverter.toSchema(fields, Arrays.asList("id"));

        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("id", "amount");
        Schema.UnresolvedPhysicalColumn id =
                (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(0);
        assertThat(id.getDataType()).isEqualTo(DataTypes.STRING().notNull());
        assertThat(id.getComment()).contains("the key");
        assertThat(schema.getPrimaryKey())
                .hasValueSatisfying(key -> assertThat(key.getColumnNames()).containsExactly("id"));
    }

    @Test
    void aTableWithoutAPrimaryKeyHasNone() {
        Schema schema =
                BigQuerySchemaToFlinkConverter.toSchema(
                        FieldList.of(Field.of("id", StandardSQLTypeName.STRING)),
                        Collections.emptyList());

        assertThat(schema.getPrimaryKey()).isEmpty();
    }

    @Test
    void refusesAPrimaryKeyNamingNoTopLevelColumn() {
        assertThatThrownBy(
                        () ->
                                BigQuerySchemaToFlinkConverter.toSchema(
                                        FieldList.of(Field.of("id", StandardSQLTypeName.STRING)),
                                        List.of("missing")))
                .isInstanceOf(BigQuerySchemaToFlinkConverter.UnsupportedColumnException.class)
                .hasMessageContaining("[missing]");
    }

    private static DataType convert(Field field) {
        return BigQuerySchemaToFlinkConverter.toDataType(field, field.getName());
    }

    private static Field field(String name, StandardSQLTypeName type, Field.Mode mode) {
        return Field.newBuilder(name, type).setMode(mode).build();
    }

    private static Field decimal(StandardSQLTypeName type, Long precision, Long scale) {
        return Field.newBuilder("d", type).setPrecision(precision).setScale(scale).build();
    }
}
