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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Converts one Spanner result row to Flink's internal row representation. */
@Internal
final class StructToRowDataConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<SpannerTableSchemaConverter.Column> columns;

    StructToRowDataConverter(SpannerTableSchemaConverter schema, @Nullable int[] projectedFields) {
        columns = new ArrayList<>();
        if (projectedFields == null) {
            columns.addAll(schema.getColumns());
        } else {
            for (int field : projectedFields) {
                columns.add(schema.getColumns().get(field));
            }
        }
    }

    RowData convert(Struct struct) {
        GenericRowData row = new GenericRowData(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            SpannerTableSchemaConverter.Column column = columns.get(i);
            Value value = struct.getValue(column.getName());
            row.setField(i, convertValue(value, column.getLogicalType(), column.getSpannerType()));
        }
        return row;
    }

    @Nullable
    private static Object convertValue(Value value, LogicalType logicalType, Type spannerType) {
        if (value.isNull()) {
            return null;
        }
        switch (spannerType.getCode()) {
            case BOOL:
                return value.getBool();
            case INT64:
            case ENUM:
                return value.getInt64();
            case FLOAT32:
                return value.getFloat32();
            case FLOAT64:
                return value.getFloat64();
            case NUMERIC:
            case PG_NUMERIC:
                DecimalType decimal = (DecimalType) logicalType;
                return DecimalData.fromBigDecimal(
                        value.getNumeric(), decimal.getPrecision(), decimal.getScale());
            case STRING:
                return StringData.fromString(value.getString());
            case JSON:
                return StringData.fromString(value.getJson());
            case PG_JSONB:
                return StringData.fromString(value.getPgJsonb());
            case BYTES:
            case PROTO:
                return value.getBytes().toByteArray();
            case DATE:
                Date date = value.getDate();
                return (int)
                        LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth())
                                .toEpochDay();
            case TIMESTAMP:
                Timestamp timestamp = value.getTimestamp();
                return TimestampData.fromEpochMillis(
                        timestamp.getSeconds() * 1000L + timestamp.getNanos() / 1_000_000,
                        timestamp.getNanos() % 1_000_000);
            case ARRAY:
                return new GenericArrayData(
                        convertArray(value, logicalType, spannerType.getArrayElementType()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner result type: " + spannerType);
        }
    }

    private static Object[] convertArray(Value value, LogicalType logicalType, Type elementType) {
        List<?> values;
        switch (elementType.getCode()) {
            case BOOL:
                values = value.getBoolArray();
                break;
            case INT64:
            case ENUM:
                values = value.getInt64Array();
                break;
            case FLOAT32:
                values = value.getFloat32Array();
                break;
            case FLOAT64:
                values = value.getFloat64Array();
                break;
            case NUMERIC:
            case PG_NUMERIC:
                values = value.getNumericArray();
                break;
            case STRING:
                values = value.getStringArray();
                break;
            case JSON:
                values = value.getJsonArray();
                break;
            case PG_JSONB:
                values = value.getPgJsonbArray();
                break;
            case BYTES:
            case PROTO:
                values = value.getBytesArray();
                break;
            case DATE:
                values = value.getDateArray();
                break;
            case TIMESTAMP:
                values = value.getTimestampArray();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner array type: " + elementType);
        }
        LogicalType elementLogicalType =
                ((org.apache.flink.table.types.logical.ArrayType) logicalType).getElementType();
        Object[] converted = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object item = values.get(i);
            converted[i] =
                    item == null ? null : convertArrayItem(item, elementLogicalType, elementType);
        }
        return converted;
    }

    private static Object convertArrayItem(Object item, LogicalType logicalType, Type type) {
        switch (type.getCode()) {
            case NUMERIC:
            case PG_NUMERIC:
                DecimalType decimal = (DecimalType) logicalType;
                return DecimalData.fromBigDecimal(
                        (java.math.BigDecimal) item, decimal.getPrecision(), decimal.getScale());
            case STRING:
            case JSON:
            case PG_JSONB:
                return StringData.fromString((String) item);
            case BYTES:
            case PROTO:
                return ((ByteArray) item).toByteArray();
            case DATE:
                Date date = (Date) item;
                return (int)
                        LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth())
                                .toEpochDay();
            case TIMESTAMP:
                Timestamp timestamp = (Timestamp) item;
                return TimestampData.fromEpochMillis(
                        timestamp.getSeconds() * 1000L + timestamp.getNanos() / 1_000_000,
                        timestamp.getNanos() % 1_000_000);
            default:
                return item;
        }
    }
}
