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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StorageSchemaConverter}. */
class StorageSchemaConverterTest {

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    @Test
    void convertsScalarTypesAndModes() {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "s",
                                        TableFieldSchema.Type.STRING,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "i",
                                        TableFieldSchema.Type.INT64,
                                        TableFieldSchema.Mode.REQUIRED))
                        .addFields(
                                field(
                                        "d",
                                        TableFieldSchema.Type.DOUBLE,
                                        TableFieldSchema.Mode.REPEATED))
                        .addFields(
                                field(
                                        "b",
                                        TableFieldSchema.Type.BOOL,
                                        TableFieldSchema.Mode.MODE_UNSPECIFIED))
                        .addFields(
                                field(
                                        "ts",
                                        TableFieldSchema.Type.TIMESTAMP,
                                        TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                field(
                                        "j",
                                        TableFieldSchema.Type.JSON,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();

        Schema converted = StorageSchemaConverter.toBigQuerySchema(schema);

        assertThat(converted.getFields()).hasSize(6);
        assertThat(converted.getFields().get("s").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.STRING);
        assertThat(converted.getFields().get("s").getMode()).isEqualTo(Field.Mode.NULLABLE);
        assertThat(converted.getFields().get("i").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.INT64);
        assertThat(converted.getFields().get("i").getMode()).isEqualTo(Field.Mode.REQUIRED);
        assertThat(converted.getFields().get("d").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.FLOAT64);
        assertThat(converted.getFields().get("d").getMode()).isEqualTo(Field.Mode.REPEATED);
        assertThat(converted.getFields().get("b").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.BOOL);
        assertThat(converted.getFields().get("b").getMode()).isEqualTo(Field.Mode.NULLABLE);
        assertThat(converted.getFields().get("ts").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.TIMESTAMP);
        assertThat(converted.getFields().get("j").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.JSON);
    }

    @Test
    void convertsNestedStructs() {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("outer")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.REPEATED)
                                        .addFields(
                                                field(
                                                        "inner",
                                                        TableFieldSchema.Type.INT64,
                                                        TableFieldSchema.Mode.NULLABLE))
                                        .build())
                        .build();

        Schema converted = StorageSchemaConverter.toBigQuerySchema(schema);

        Field outer = converted.getFields().get("outer");
        assertThat(outer.getType().getStandardType()).isEqualTo(StandardSQLTypeName.STRUCT);
        assertThat(outer.getMode()).isEqualTo(Field.Mode.REPEATED);
        assertThat(outer.getSubFields().get("inner").getType().getStandardType())
                .isEqualTo(StandardSQLTypeName.INT64);
    }

    @Test
    void carriesFieldMetadata() {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("s")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE)
                                        .setDescription("a description")
                                        .setMaxLength(42)
                                        .setDefaultValueExpression("'x'")
                                        .build())
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("n")
                                        .setType(TableFieldSchema.Type.NUMERIC)
                                        .setMode(TableFieldSchema.Mode.NULLABLE)
                                        .setPrecision(10)
                                        .setScale(2)
                                        .build())
                        .build();

        Schema converted = StorageSchemaConverter.toBigQuerySchema(schema);

        Field s = converted.getFields().get("s");
        assertThat(s.getDescription()).isEqualTo("a description");
        assertThat(s.getMaxLength()).isEqualTo(42);
        assertThat(s.getDefaultValueExpression()).isEqualTo("'x'");
        Field n = converted.getFields().get("n");
        assertThat(n.getType().getStandardType()).isEqualTo(StandardSQLTypeName.NUMERIC);
        assertThat(n.getPrecision()).isEqualTo(10);
        assertThat(n.getScale()).isEqualTo(2);
    }

    @Test
    void convertsRangeWithElementType() {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                                "r",
                                                TableFieldSchema.Type.RANGE,
                                                TableFieldSchema.Mode.NULLABLE)
                                        .toBuilder()
                                        .setRangeElementType(
                                                TableFieldSchema.FieldElementType.newBuilder()
                                                        .setType(TableFieldSchema.Type.DATE))
                                        .build())
                        .build();

        Schema converted = StorageSchemaConverter.toBigQuerySchema(schema);

        Field r = converted.getFields().get("r");
        assertThat(r.getType().getStandardType()).isEqualTo(StandardSQLTypeName.RANGE);
        assertThat(r.getRangeElementType().getType()).isEqualTo("DATE");
    }

    @Test
    void rejectsUnsupportedTypes() {
        TableSchema schema =
                TableSchema.newBuilder()
                        .addFields(
                                field(
                                        "u",
                                        TableFieldSchema.Type.TYPE_UNSPECIFIED,
                                        TableFieldSchema.Mode.NULLABLE))
                        .build();

        assertThatThrownBy(() -> StorageSchemaConverter.toBigQuerySchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TYPE_UNSPECIFIED");
    }
}
