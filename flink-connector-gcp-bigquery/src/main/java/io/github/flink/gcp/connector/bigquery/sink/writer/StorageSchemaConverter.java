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

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldElementType;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Storage API {@link TableSchema} (the schema form of the serializer SPI) into the REST
 * client {@link Schema} needed to create tables.
 *
 * <p>(The reverse of the client library's {@code BqToBqStorageSchemaConverter}; the client library
 * offers no converter in this direction.)
 */
@Internal
public final class StorageSchemaConverter {

    private StorageSchemaConverter() {}

    /**
     * Converts the given Storage API table schema to a REST client schema.
     *
     * @param schema the Storage API schema
     * @return the equivalent REST client schema
     */
    public static Schema toBigQuerySchema(TableSchema schema) {
        List<Field> fields = new ArrayList<>(schema.getFieldsCount());
        for (TableFieldSchema field : schema.getFieldsList()) {
            fields.add(toBigQueryField(field));
        }
        return Schema.of(fields);
    }

    /**
     * Converts a single Storage API field to a REST client field.
     *
     * @param field the Storage API field
     * @return the equivalent REST client field
     */
    static Field toBigQueryField(TableFieldSchema field) {
        Field.Builder builder;
        if (field.getType() == TableFieldSchema.Type.STRUCT) {
            List<Field> subFields = new ArrayList<>(field.getFieldsCount());
            for (TableFieldSchema subField : field.getFieldsList()) {
                subFields.add(toBigQueryField(subField));
            }
            builder =
                    Field.newBuilder(
                            field.getName(), StandardSQLTypeName.STRUCT, FieldList.of(subFields));
        } else {
            builder = Field.newBuilder(field.getName(), convertType(field));
        }
        builder.setMode(convertMode(field));
        if (!field.getDescription().isEmpty()) {
            builder.setDescription(field.getDescription());
        }
        if (field.getMaxLength() > 0) {
            builder.setMaxLength(field.getMaxLength());
        }
        if (field.getPrecision() > 0) {
            builder.setPrecision(field.getPrecision());
        }
        if (field.getScale() > 0) {
            builder.setScale(field.getScale());
        }
        if (!field.getDefaultValueExpression().isEmpty()) {
            builder.setDefaultValueExpression(field.getDefaultValueExpression());
        }
        if (field.hasRangeElementType()) {
            builder.setRangeElementType(
                    FieldElementType.newBuilder()
                            .setType(field.getRangeElementType().getType().name())
                            .build());
        }
        return builder.build();
    }

    private static StandardSQLTypeName convertType(TableFieldSchema field) {
        switch (field.getType()) {
            case STRING:
                return StandardSQLTypeName.STRING;
            case INT64:
                return StandardSQLTypeName.INT64;
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case BOOL:
                return StandardSQLTypeName.BOOL;
            case BYTES:
                return StandardSQLTypeName.BYTES;
            case TIMESTAMP:
                return StandardSQLTypeName.TIMESTAMP;
            case DATE:
                return StandardSQLTypeName.DATE;
            case TIME:
                return StandardSQLTypeName.TIME;
            case DATETIME:
                return StandardSQLTypeName.DATETIME;
            case GEOGRAPHY:
                return StandardSQLTypeName.GEOGRAPHY;
            case NUMERIC:
                return StandardSQLTypeName.NUMERIC;
            case BIGNUMERIC:
                return StandardSQLTypeName.BIGNUMERIC;
            case INTERVAL:
                return StandardSQLTypeName.INTERVAL;
            case JSON:
                return StandardSQLTypeName.JSON;
            case RANGE:
                return StandardSQLTypeName.RANGE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Storage API field type "
                                + field.getType()
                                + " for auto-created table field "
                                + field.getName());
        }
    }

    private static Field.Mode convertMode(TableFieldSchema field) {
        switch (field.getMode()) {
            case REQUIRED:
                return Field.Mode.REQUIRED;
            case REPEATED:
                return Field.Mode.REPEATED;
            default:
                return Field.Mode.NULLABLE;
        }
    }
}
