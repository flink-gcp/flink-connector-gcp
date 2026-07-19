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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;

/**
 * Converts a REST client {@link Schema} (as read from live tables) into the Storage API {@link
 * TableSchema} form the sink's schema handling operates on.
 *
 * <p>(The inverse of {@link StorageSchemaConverter}; same direction as the client library's {@code
 * BqToBqStorageSchemaConverter}, but implemented independently so lossless attributes — max length,
 * precision, scale, description, default value expression — survive the round trip.)
 */
@Internal
public final class BigQuerySchemaConverter {

    private BigQuerySchemaConverter() {}

    /**
     * Converts the given REST client schema to a Storage API table schema.
     *
     * @param schema the REST client schema
     * @return the equivalent Storage API schema
     */
    public static TableSchema toStorageSchema(Schema schema) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        for (Field field : schema.getFields()) {
            builder.addFields(convertField(field));
        }
        return builder.build();
    }

    private static TableFieldSchema convertField(Field field) {
        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder()
                        .setName(field.getName())
                        .setType(convertType(field))
                        .setMode(convertMode(field));
        if (field.getType() == LegacySQLTypeName.RECORD) {
            for (Field subField : field.getSubFields()) {
                builder.addFields(convertField(subField));
            }
        }
        if (field.getDescription() != null) {
            builder.setDescription(field.getDescription());
        }
        if (field.getMaxLength() != null) {
            builder.setMaxLength(field.getMaxLength());
        }
        if (field.getPrecision() != null) {
            builder.setPrecision(field.getPrecision());
        }
        if (field.getScale() != null) {
            builder.setScale(field.getScale());
        }
        if (field.getDefaultValueExpression() != null) {
            builder.setDefaultValueExpression(field.getDefaultValueExpression());
        }
        if (field.getRangeElementType() != null) {
            builder.setRangeElementType(
                    TableFieldSchema.FieldElementType.newBuilder()
                            .setType(
                                    TableFieldSchema.Type.valueOf(
                                            field.getRangeElementType().getType()))
                            .build());
        }
        return builder.build();
    }

    private static TableFieldSchema.Type convertType(Field field) {
        StandardSQLTypeName type = field.getType().getStandardType();
        switch (type) {
            case STRING:
                return TableFieldSchema.Type.STRING;
            case INT64:
                return TableFieldSchema.Type.INT64;
            case FLOAT64:
                return TableFieldSchema.Type.DOUBLE;
            case BOOL:
                return TableFieldSchema.Type.BOOL;
            case BYTES:
                return TableFieldSchema.Type.BYTES;
            case TIMESTAMP:
                return TableFieldSchema.Type.TIMESTAMP;
            case DATE:
                return TableFieldSchema.Type.DATE;
            case TIME:
                return TableFieldSchema.Type.TIME;
            case DATETIME:
                return TableFieldSchema.Type.DATETIME;
            case GEOGRAPHY:
                return TableFieldSchema.Type.GEOGRAPHY;
            case NUMERIC:
                return TableFieldSchema.Type.NUMERIC;
            case BIGNUMERIC:
                return TableFieldSchema.Type.BIGNUMERIC;
            case INTERVAL:
                return TableFieldSchema.Type.INTERVAL;
            case JSON:
                return TableFieldSchema.Type.JSON;
            case RANGE:
                return TableFieldSchema.Type.RANGE;
            case STRUCT:
                return TableFieldSchema.Type.STRUCT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported BigQuery field type "
                                + type
                                + " for table field "
                                + field.getName());
        }
    }

    private static TableFieldSchema.Mode convertMode(Field field) {
        if (field.getMode() == null) {
            // The REST API may omit the mode; it defaults to NULLABLE.
            return TableFieldSchema.Mode.NULLABLE;
        }
        switch (field.getMode()) {
            case REQUIRED:
                return TableFieldSchema.Mode.REQUIRED;
            case REPEATED:
                return TableFieldSchema.Mode.REPEATED;
            default:
                return TableFieldSchema.Mode.NULLABLE;
        }
    }
}
