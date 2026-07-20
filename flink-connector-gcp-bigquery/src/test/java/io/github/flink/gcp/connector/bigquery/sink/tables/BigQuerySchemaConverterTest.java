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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQuerySchemaConverter}. */
class BigQuerySchemaConverterTest {

    @Test
    void convertsScalarTypesAndModes() {
        Schema schema =
                Schema.of(
                        Field.newBuilder("name", StandardSQLTypeName.STRING)
                                .setMode(Field.Mode.REQUIRED)
                                .build(),
                        Field.newBuilder("count", StandardSQLTypeName.INT64)
                                .setMode(Field.Mode.NULLABLE)
                                .build(),
                        Field.newBuilder("tags", StandardSQLTypeName.STRING)
                                .setMode(Field.Mode.REPEATED)
                                .build(),
                        Field.newBuilder("ratio", StandardSQLTypeName.FLOAT64).build());

        TableSchema converted = BigQuerySchemaConverter.toStorageSchema(schema);

        assertThat(converted.getFields(0).getName()).isEqualTo("name");
        assertThat(converted.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(converted.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(converted.getFields(1).getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(converted.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(converted.getFields(2).getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(converted.getFields(3).getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        // The REST API may omit the mode; it defaults to NULLABLE.
        assertThat(converted.getFields(3).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void convertsStructsRecursively() {
        Schema schema =
                Schema.of(
                        Field.newBuilder(
                                        "address",
                                        StandardSQLTypeName.STRUCT,
                                        FieldList.of(
                                                Field.newBuilder("city", StandardSQLTypeName.STRING)
                                                        .setMode(Field.Mode.REQUIRED)
                                                        .build()))
                                .setMode(Field.Mode.NULLABLE)
                                .build());

        TableSchema converted = BigQuerySchemaConverter.toStorageSchema(schema);

        TableFieldSchema struct = converted.getFields(0);
        assertThat(struct.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(struct.getFields(0).getName()).isEqualTo("city");
        assertThat(struct.getFields(0).getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(struct.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void convertsLosslessAttributes() {
        Schema schema =
                Schema.of(
                        Field.newBuilder("name", StandardSQLTypeName.STRING)
                                .setDescription("the name")
                                .setMaxLength(64L)
                                .setDefaultValueExpression("'unknown'")
                                .build(),
                        Field.newBuilder("amount", StandardSQLTypeName.NUMERIC)
                                .setPrecision(38L)
                                .setScale(9L)
                                .build());

        TableSchema converted = BigQuerySchemaConverter.toStorageSchema(schema);

        assertThat(converted.getFields(0).getDescription()).isEqualTo("the name");
        assertThat(converted.getFields(0).getMaxLength()).isEqualTo(64);
        assertThat(converted.getFields(0).getDefaultValueExpression()).isEqualTo("'unknown'");
        assertThat(converted.getFields(1).getPrecision()).isEqualTo(38);
        assertThat(converted.getFields(1).getScale()).isEqualTo(9);
    }

    @Test
    void roundTripsThroughTheStorageToRestConverter() {
        TableSchema original =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("name")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.REQUIRED)
                                        .setDescription("the name"))
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("address")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.REPEATED)
                                        .addFields(
                                                TableFieldSchema.newBuilder()
                                                        .setName("city")
                                                        .setType(TableFieldSchema.Type.STRING)
                                                        .setMode(TableFieldSchema.Mode.NULLABLE)))
                        .build();

        TableSchema roundTripped =
                BigQuerySchemaConverter.toStorageSchema(
                        StorageSchemaConverter.toBigQuerySchema(original));

        assertThat(roundTripped).isEqualTo(original);
    }
}
