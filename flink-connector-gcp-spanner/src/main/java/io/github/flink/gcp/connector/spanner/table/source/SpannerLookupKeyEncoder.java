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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Type;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;

/** Encodes a planner lookup-key row in declared Spanner primary-key order. */
final class SpannerLookupKeyEncoder implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SpannerTableSchemaConverter schema;
    private final int[] keyPositions;

    SpannerLookupKeyEncoder(SpannerTableSchemaConverter schema, int[] keyPositions) {
        this.schema = schema;
        this.keyPositions = keyPositions;
    }

    Key encode(RowData row) {
        Key.Builder key = Key.newBuilder();
        int[] primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            SpannerTableSchemaConverter.Column column =
                    schema.getColumns().get(primaryKeyIndexes[i]);
            Object value =
                    RowData.createFieldGetter(column.getLogicalType(), keyPositions[i])
                            .getFieldOrNull(row);
            key.appendObject(convert(value, column.getLogicalType(), column.getSpannerType()));
        }
        return key.build();
    }

    private static Object convert(Object value, LogicalType logicalType, Type spannerType) {
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
                        "Spanner type " + spannerType + " cannot be a primary-key part.");
        }
    }
}
