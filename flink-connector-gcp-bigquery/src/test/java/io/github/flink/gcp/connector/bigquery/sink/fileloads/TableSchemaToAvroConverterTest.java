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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableSchemaToAvroConverter}. */
class TableSchemaToAvroConverterTest {

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static Schema convertSingle(TableFieldSchema field) {
        Schema schema =
                TableSchemaToAvroConverter.convert(
                        TableSchema.newBuilder().addFields(field).build());
        return schema.getFields().get(0).schema();
    }

    private static Schema requiredSchemaOf(TableFieldSchema.Type type) {
        return convertSingle(field("f", type, TableFieldSchema.Mode.REQUIRED));
    }

    @Test
    void mapsScalarTypes() {
        assertThat(requiredSchemaOf(TableFieldSchema.Type.STRING).getType())
                .isEqualTo(Schema.Type.STRING);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.JSON).getType())
                .isEqualTo(Schema.Type.STRING);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.GEOGRAPHY).getType())
                .isEqualTo(Schema.Type.STRING);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.BYTES).getType())
                .isEqualTo(Schema.Type.BYTES);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.INT64).getType())
                .isEqualTo(Schema.Type.LONG);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.DOUBLE).getType())
                .isEqualTo(Schema.Type.DOUBLE);
        assertThat(requiredSchemaOf(TableFieldSchema.Type.BOOL).getType())
                .isEqualTo(Schema.Type.BOOLEAN);
    }

    @Test
    void mapsTemporalTypesToLogicalTypes() {
        Schema timestamp = requiredSchemaOf(TableFieldSchema.Type.TIMESTAMP);
        assertThat(timestamp.getType()).isEqualTo(Schema.Type.LONG);
        assertThat(timestamp.getLogicalType()).isEqualTo(LogicalTypes.timestampMicros());

        Schema date = requiredSchemaOf(TableFieldSchema.Type.DATE);
        assertThat(date.getType()).isEqualTo(Schema.Type.INT);
        assertThat(date.getLogicalType()).isEqualTo(LogicalTypes.date());

        Schema time = requiredSchemaOf(TableFieldSchema.Type.TIME);
        assertThat(time.getType()).isEqualTo(Schema.Type.LONG);
        assertThat(time.getLogicalType()).isEqualTo(LogicalTypes.timeMicros());

        // Avro has no timezone-less datetime logical type BigQuery loads accept everywhere;
        // DATETIME travels as a canonical civil-time string.
        Schema datetime = requiredSchemaOf(TableFieldSchema.Type.DATETIME);
        assertThat(datetime.getType()).isEqualTo(Schema.Type.STRING);
        assertThat(datetime.getLogicalType()).isNull();
    }

    @Test
    void mapsDecimalTypes() {
        Schema numeric = requiredSchemaOf(TableFieldSchema.Type.NUMERIC);
        assertThat(numeric.getType()).isEqualTo(Schema.Type.BYTES);
        assertThat(numeric.getLogicalType()).isEqualTo(LogicalTypes.decimal(38, 9));

        Schema bignumeric = requiredSchemaOf(TableFieldSchema.Type.BIGNUMERIC);
        assertThat(bignumeric.getLogicalType()).isEqualTo(LogicalTypes.decimal(77, 38));
    }

    @Test
    void keepsParameterizedDecimalPrecisionAndScale() {
        Schema parameterized =
                convertSingle(
                        TableFieldSchema.newBuilder()
                                .setName("f")
                                .setType(TableFieldSchema.Type.NUMERIC)
                                .setMode(TableFieldSchema.Mode.REQUIRED)
                                .setPrecision(10)
                                .setScale(2)
                                .build());
        assertThat(parameterized.getLogicalType()).isEqualTo(LogicalTypes.decimal(10, 2));
    }

    @Test
    void nullableBecomesUnionWithNullDefault() {
        Schema schema =
                TableSchemaToAvroConverter.convert(
                        TableSchema.newBuilder()
                                .addFields(
                                        field(
                                                "f",
                                                TableFieldSchema.Type.INT64,
                                                TableFieldSchema.Mode.NULLABLE))
                                .build());

        Schema.Field avroField = schema.getFields().get(0);
        assertThat(avroField.schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(avroField.schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
        assertThat(avroField.hasDefaultValue()).isTrue();
    }

    @Test
    void repeatedBecomesArrayOfNonNullItems() {
        Schema schema =
                convertSingle(
                        field("f", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REPEATED));

        assertThat(schema.getType()).isEqualTo(Schema.Type.ARRAY);
        assertThat(schema.getElementType().getType()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void structBecomesNestedRecord() {
        TableFieldSchema struct =
                TableFieldSchema.newBuilder()
                        .setName("outer")
                        .setType(TableFieldSchema.Type.STRUCT)
                        .setMode(TableFieldSchema.Mode.REQUIRED)
                        .addFields(
                                field(
                                        "inner",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("nested")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.REPEATED)
                                        .addFields(
                                                field(
                                                        "leaf",
                                                        TableFieldSchema.Type.STRING,
                                                        TableFieldSchema.Mode.REQUIRED)))
                        .build();

        Schema schema = convertSingle(struct);
        assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(schema.getName()).isEqualTo("outer");
        assertThat(schema.getField("inner")).isNotNull();

        Schema nested = schema.getField("nested").schema();
        assertThat(nested.getType()).isEqualTo(Schema.Type.ARRAY);
        assertThat(nested.getElementType().getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(nested.getElementType().getField("leaf").schema().getType())
                .isEqualTo(Schema.Type.STRING);
    }

    @Test
    void sameNestedRecordNamesInDifferentBranchesDoNotCollide() {
        TableFieldSchema.Builder leaf =
                TableFieldSchema.newBuilder()
                        .setName("child")
                        .setType(TableFieldSchema.Type.STRUCT)
                        .setMode(TableFieldSchema.Mode.REQUIRED)
                        .addFields(
                                field(
                                        "v",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.REQUIRED));
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("a")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.REQUIRED)
                                        .addFields(leaf))
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("b")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.REQUIRED)
                                        .addFields(leaf))
                        .build();

        Schema avro = TableSchemaToAvroConverter.convert(schema);
        assertThat(avro.getField("a").schema().getField("child").schema().getFullName())
                .isNotEqualTo(avro.getField("b").schema().getField("child").schema().getFullName());
    }

    @Test
    void rejectsFlexibleColumnNames() {
        assertThatThrownBy(
                        () ->
                                convertSingle(
                                        field(
                                                "1st_field",
                                                TableFieldSchema.Type.STRING,
                                                TableFieldSchema.Mode.REQUIRED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flexible column names");
    }

    @Test
    void rejectsUnsupportedTypes() {
        assertThatThrownBy(() -> requiredSchemaOf(TableFieldSchema.Type.INTERVAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FILE_LOADS");
        assertThatThrownBy(() -> requiredSchemaOf(TableFieldSchema.Type.RANGE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FILE_LOADS");
    }
}
