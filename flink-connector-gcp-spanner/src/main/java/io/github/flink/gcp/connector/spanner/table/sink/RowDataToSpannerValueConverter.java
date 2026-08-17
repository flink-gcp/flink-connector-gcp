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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.table.UuidStringParser;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Converts Flink internal values to typed Spanner values. */
@Internal
final class RowDataToSpannerValueConverter {

    private RowDataToSpannerValueConverter() {}

    static Value convert(
            RowData row, int index, LogicalType logicalType, Type spannerType, String columnName) {
        Object value = RowData.createFieldGetter(logicalType, index).getFieldOrNull(row);
        return convertValue(value, logicalType, spannerType, columnName);
    }

    static Object keyPart(
            RowData row, int index, LogicalType logicalType, Type spannerType, String columnName) {
        Object value = RowData.createFieldGetter(logicalType, index).getFieldOrNull(row);
        if (value == null) {
            return null;
        }
        switch (spannerType.getCode()) {
            case BOOL:
                return value;
            case INT64:
            case ENUM:
                return (Long) value;
            case FLOAT32:
                return (Float) value;
            case FLOAT64:
                return (Double) value;
            case NUMERIC:
                DecimalType decimal = (DecimalType) logicalType;
                return ((DecimalData) value).toBigDecimal();
            case STRING:
            case JSON:
            case PG_JSONB:
                return value.toString();
            case UUID:
                return UuidStringParser.parse(value.toString(), columnName);
            case BYTES:
            case PROTO:
                return ByteArray.copyFrom((byte[]) value);
            case DATE:
                return toDate((Integer) value);
            case TIMESTAMP:
                return toTimestamp((TimestampData) value);
            default:
                throw new IllegalArgumentException(
                        "Spanner type " + spannerType + " cannot be a primary-key part.");
        }
    }

    private static Value convertValue(
            @Nullable Object value, LogicalType logicalType, Type spannerType, String columnName) {
        if (spannerType.getCode() == Type.Code.ARRAY) {
            return arrayValue((ArrayData) value, (ArrayType) logicalType, spannerType, columnName);
        }
        switch (spannerType.getCode()) {
            case BOOL:
                return Value.bool((Boolean) value);
            case INT64:
                return Value.int64((Long) value);
            case ENUM:
                return Value.protoEnum((Long) value, spannerType.getProtoTypeFqn());
            case FLOAT32:
                return Value.float32((Float) value);
            case FLOAT64:
                return Value.float64((Double) value);
            case NUMERIC:
                return Value.numeric(value == null ? null : ((DecimalData) value).toBigDecimal());
            case PG_NUMERIC:
                return Value.pgNumeric(
                        value == null
                                ? null
                                : ((DecimalData) value).toBigDecimal().toPlainString());
            case STRING:
                return Value.string(value == null ? null : value.toString());
            case JSON:
                return Value.json(value == null ? null : value.toString());
            case PG_JSONB:
                return Value.pgJsonb(value == null ? null : value.toString());
            case UUID:
                return Value.uuid(
                        value == null
                                ? null
                                : UuidStringParser.parse(value.toString(), columnName));
            case BYTES:
                return Value.bytes(value == null ? null : ByteArray.copyFrom((byte[]) value));
            case PROTO:
                return Value.protoMessage(
                        value == null ? null : ByteArray.copyFrom((byte[]) value),
                        spannerType.getProtoTypeFqn());
            case DATE:
                return Value.date(value == null ? null : toDate((Integer) value));
            case TIMESTAMP:
                return Value.timestamp(value == null ? null : toTimestamp((TimestampData) value));
            case STRUCT:
                return structValue((RowData) value, (RowType) logicalType, spannerType);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner table value type " + spannerType + ".");
        }
    }

    private static Value structValue(@Nullable RowData row, RowType rowType, Type spannerType) {
        if (row == null) {
            return Value.struct(spannerType, null);
        }
        Struct.Builder builder = Struct.newBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            builder.set(rowType.getFieldNames().get(i))
                    .to(
                            convert(
                                    row,
                                    i,
                                    rowType.getTypeAt(i),
                                    spannerType.getStructFields().get(i).getType(),
                                    rowType.getFieldNames().get(i)));
        }
        return Value.struct(builder.build());
    }

    private static Value arrayValue(
            @Nullable ArrayData array, ArrayType arrayType, Type spannerType, String columnName) {
        Type elementType = spannerType.getArrayElementType();
        if (array == null) {
            return nullArray(elementType);
        }
        LogicalType logicalElement = arrayType.getElementType();
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(logicalElement);
        List<Object> values = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            Object element = getter.getElementOrNull(array, i);
            values.add(
                    toArrayElement(
                            element, logicalElement, elementType, columnName + "[" + i + "]"));
        }
        return populatedArray(elementType, values);
    }

    @Nullable
    private static Object toArrayElement(
            @Nullable Object value, LogicalType logicalType, Type spannerType, String columnName) {
        if (value == null) {
            return null;
        }
        switch (spannerType.getCode()) {
            case BOOL:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                return value;
            case NUMERIC:
                return ((DecimalData) value).toBigDecimal();
            case PG_NUMERIC:
                return ((DecimalData) value).toBigDecimal().toPlainString();
            case STRING:
            case JSON:
            case PG_JSONB:
                return ((StringData) value).toString();
            case UUID:
                return UuidStringParser.parse(((StringData) value).toString(), columnName);
            case BYTES:
            case PROTO:
                return ByteArray.copyFrom((byte[]) value);
            case ENUM:
                return value;
            case DATE:
                return toDate((Integer) value);
            case TIMESTAMP:
                return toTimestamp((TimestampData) value);
            case STRUCT:
                return structValue((RowData) value, (RowType) logicalType, spannerType).getStruct();
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner array element type " + spannerType + ".");
        }
    }

    @SuppressWarnings("unchecked")
    private static Value populatedArray(Type elementType, List<Object> values) {
        switch (elementType.getCode()) {
            case BOOL:
                return Value.boolArray((List<Boolean>) (List<?>) values);
            case INT64:
                return Value.int64Array((List<Long>) (List<?>) values);
            case ENUM:
                return Value.protoEnumArray(
                        (List<Long>) (List<?>) values, elementType.getProtoTypeFqn());
            case FLOAT32:
                return Value.float32Array((List<Float>) (List<?>) values);
            case FLOAT64:
                return Value.float64Array((List<Double>) (List<?>) values);
            case NUMERIC:
                return Value.numericArray((List<java.math.BigDecimal>) (List<?>) values);
            case PG_NUMERIC:
                return Value.pgNumericArray((List<String>) (List<?>) values);
            case STRING:
                return Value.stringArray((List<String>) (List<?>) values);
            case JSON:
                return Value.jsonArray((List<String>) (List<?>) values);
            case PG_JSONB:
                return Value.pgJsonbArray((List<String>) (List<?>) values);
            case UUID:
                return Value.uuidArray((List<UUID>) (List<?>) values);
            case BYTES:
                return Value.bytesArray((List<ByteArray>) (List<?>) values);
            case PROTO:
                return Value.protoMessageArray(
                        (List<ByteArray>) (List<?>) values, elementType.getProtoTypeFqn());
            case DATE:
                return Value.dateArray((List<Date>) (List<?>) values);
            case TIMESTAMP:
                return Value.timestampArray((List<Timestamp>) (List<?>) values);
            case STRUCT:
                return Value.structArray(elementType, (List<Struct>) (List<?>) values);
            default:
                throw new IllegalArgumentException(
                        "Unsupported Spanner array element type " + elementType + ".");
        }
    }

    private static Value nullArray(Type elementType) {
        return populatedArray(elementType, null);
    }

    private static Date toDate(int epochDay) {
        LocalDate date = LocalDate.ofEpochDay(epochDay);
        return Date.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
    }

    private static Timestamp toTimestamp(TimestampData value) {
        Instant instant = value.toInstant();
        return Timestamp.ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano());
    }
}
