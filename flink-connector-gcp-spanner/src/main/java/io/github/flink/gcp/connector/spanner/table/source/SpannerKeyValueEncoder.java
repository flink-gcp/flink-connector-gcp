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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Type;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import io.github.flink.gcp.connector.spanner.table.UuidStringParser;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;

/** Converts Flink row and literal values into native Spanner key parts. */
final class SpannerKeyValueEncoder {

    private SpannerKeyValueEncoder() {}

    static Object rowValue(RowData row, int position, SpannerTableSchemaConverter.Column column) {
        Object value =
                RowData.createFieldGetter(column.getLogicalType(), position).getFieldOrNull(row);
        return internalValue(value, column.getSpannerType(), column.getName());
    }

    static Optional<Object> literalValue(
            SpannerTableSchemaConverter.Column column, ValueLiteralExpression literal) {
        if (literal.isNull()) {
            return Optional.empty();
        }
        try {
            switch (column.getSpannerType().getCode()) {
                case BOOL:
                    return value(literal, Boolean.class);
                case INT64:
                    return literal.getValueAs(BigDecimal.class)
                            .map(BigDecimal::longValueExact)
                            .map(value -> (Object) value);
                case FLOAT64:
                    return value(literal, Double.class);
                case NUMERIC:
                    return value(literal, BigDecimal.class);
                case STRING:
                    return value(literal, String.class);
                case UUID:
                    return literal.getValueAs(String.class)
                            .map(value -> UuidStringParser.parse(value, column.getName()))
                            .map(value -> (Object) value);
                case BYTES:
                    return literal.getValueAs(byte[].class)
                            .map(ByteArray::copyFrom)
                            .map(value -> (Object) value);
                case DATE:
                    return literal.getValueAs(LocalDate.class)
                            .map(
                                    value ->
                                            Date.fromYearMonthDay(
                                                    value.getYear(),
                                                    value.getMonthValue(),
                                                    value.getDayOfMonth()))
                            .map(value -> (Object) value);
                case TIMESTAMP:
                    return literal.getValueAs(Instant.class)
                            .map(
                                    value ->
                                            Timestamp.ofTimeSecondsAndNanos(
                                                    value.getEpochSecond(), value.getNano()))
                            .map(value -> (Object) value);
                default:
                    return Optional.empty();
            }
        } catch (ArithmeticException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static Object internalValue(Object value, Type spannerType, String columnName) {
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
            case STRING:
                return value.toString();
            case UUID:
                return UuidStringParser.parse(value.toString(), columnName);
            case BYTES:
                return ByteArray.copyFrom((byte[]) value);
            case DATE:
                LocalDate date = LocalDate.ofEpochDay((Integer) value);
                return Date.fromYearMonthDay(
                        date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            case TIMESTAMP:
                Instant instant = ((TimestampData) value).toInstant();
                return Timestamp.ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano());
            default:
                throw new IllegalArgumentException(
                        "Spanner type " + spannerType + " cannot be a key part.");
        }
    }

    private static <T> Optional<Object> value(ValueLiteralExpression literal, Class<T> type) {
        return literal.getValueAs(type).map(value -> (Object) value);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static int compare(Object left, Object right) {
        if (left instanceof Double && right instanceof Double) {
            double leftDouble = (Double) left;
            double rightDouble = (Double) right;
            if (leftDouble == 0.0d && rightDouble == 0.0d) {
                return 0;
            }
            return Double.compare(leftDouble, rightDouble);
        }
        if (left instanceof ByteArray && right instanceof ByteArray) {
            byte[] leftBytes = ((ByteArray) left).toByteArray();
            byte[] rightBytes = ((ByteArray) right).toByteArray();
            int length = Math.min(leftBytes.length, rightBytes.length);
            for (int i = 0; i < length; i++) {
                int comparison =
                        Integer.compare(
                                Byte.toUnsignedInt(leftBytes[i]),
                                Byte.toUnsignedInt(rightBytes[i]));
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(leftBytes.length, rightBytes.length);
        }
        if (left instanceof String && right instanceof String) {
            return compareStrings((String) left, (String) right);
        }
        return ((Comparable) left).compareTo(right);
    }

    private static int compareStrings(String left, String right) {
        int leftOffset = 0;
        int rightOffset = 0;
        while (leftOffset < left.length() && rightOffset < right.length()) {
            int leftCodePoint = left.codePointAt(leftOffset);
            int rightCodePoint = right.codePointAt(rightOffset);
            int comparison = Integer.compare(leftCodePoint, rightCodePoint);
            if (comparison != 0) {
                return comparison;
            }
            leftOffset += Character.charCount(leftCodePoint);
            rightOffset += Character.charCount(rightCodePoint);
        }
        return Integer.compare(left.length() - leftOffset, right.length() - rightOffset);
    }
}
