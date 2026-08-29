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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

import java.util.List;

/** Estimates decoded logical content bytes without trying to model JVM object overhead. */
@Internal
final class StructSizeEstimator {

    /** What field content of a type this estimator does not know is assumed to cost. */
    static final long UNKNOWN_VALUE_BYTES = 64;

    private StructSizeEstimator() {}

    /** Returns the saturated sum of the row's non-null field content sizes. */
    static long estimate(Struct row) {
        long bytes = 0;
        for (int index = 0; index < row.getColumnCount(); index++) {
            bytes = saturatedAdd(bytes, estimate(row, index));
        }
        return bytes;
    }

    private static long estimate(Struct row, int columnIndex) {
        if (row.isNull(columnIndex)) {
            return 0;
        }
        Type type = row.getColumnType(columnIndex);
        Type.Code code = type.getCode();
        return code == Type.Code.ARRAY
                ? estimateArray(row, columnIndex, type.getArrayElementType().getCode())
                : estimateScalar(row, columnIndex, code);
    }

    static long estimateScalar(Struct row, int columnIndex, Type.Code code) {
        switch (code) {
            case BOOL:
                return 1;
            case INT64:
            case PG_OID:
            case FLOAT64:
            case ENUM:
                return 8;
            case FLOAT32:
                return 4;
            case NUMERIC:
                return 16;
            case PG_NUMERIC:
            case STRING:
                return utf8Length(row.getString(columnIndex));
            case JSON:
                return utf8Length(row.getJson(columnIndex));
            case PG_JSONB:
                return utf8Length(row.getPgJsonb(columnIndex));
            case PROTO:
            case BYTES:
                return row.getBytes(columnIndex).length();
            case TIMESTAMP:
                return 12;
            case DATE:
                return 12;
            case UUID:
            case INTERVAL:
                return 16;
            case STRUCT:
                return estimate(row.getStruct(columnIndex));
            case ARRAY:
            case UNRECOGNIZED:
            default:
                return UNKNOWN_VALUE_BYTES;
        }
    }

    static long estimateArray(Struct row, int columnIndex, Type.Code elementCode) {
        switch (elementCode) {
            case BOOL:
                return fixedArraySize(row.getBooleanList(columnIndex), 1);
            case INT64:
            case PG_OID:
            case ENUM:
                return fixedArraySize(row.getLongList(columnIndex), 8);
            case FLOAT32:
                return fixedArraySize(row.getFloatList(columnIndex), 4);
            case FLOAT64:
                return fixedArraySize(row.getDoubleList(columnIndex), 8);
            case NUMERIC:
                return fixedArraySize(row.getBigDecimalList(columnIndex), 16);
            case PG_NUMERIC:
            case STRING:
                return stringArraySize(row.getStringList(columnIndex));
            case JSON:
                return stringArraySize(row.getJsonList(columnIndex));
            case PG_JSONB:
                return stringArraySize(row.getPgJsonbList(columnIndex));
            case PROTO:
            case BYTES:
                return bytesArraySize(row.getBytesList(columnIndex));
            case TIMESTAMP:
                return fixedArraySize(row.getTimestampList(columnIndex), 12);
            case DATE:
                return fixedArraySize(row.getDateList(columnIndex), 12);
            case UUID:
                return fixedArraySize(row.getUuidList(columnIndex), 16);
            case INTERVAL:
                return fixedArraySize(row.getIntervalList(columnIndex), 16);
            case STRUCT:
                long bytes = 0;
                for (Struct element : row.getStructList(columnIndex)) {
                    if (element != null) {
                        bytes = saturatedAdd(bytes, estimate(element));
                    }
                }
                return bytes;
            case ARRAY:
            case UNRECOGNIZED:
            default:
                return UNKNOWN_VALUE_BYTES;
        }
    }

    private static long fixedArraySize(List<?> values, int bytesPerValue) {
        long bytes = 0;
        for (Object value : values) {
            if (value != null) {
                bytes = saturatedAdd(bytes, bytesPerValue);
            }
        }
        return bytes;
    }

    private static long stringArraySize(List<String> values) {
        long bytes = 0;
        for (String value : values) {
            if (value != null) {
                bytes = saturatedAdd(bytes, utf8Length(value));
            }
        }
        return bytes;
    }

    private static long bytesArraySize(List<ByteArray> values) {
        long bytes = 0;
        for (ByteArray value : values) {
            if (value != null) {
                bytes = saturatedAdd(bytes, value.length());
            }
        }
        return bytes;
    }

    private static long utf8Length(String value) {
        long bytes = 0;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character <= 0x7f) {
                bytes = saturatedAdd(bytes, 1);
            } else if (character <= 0x7ff) {
                bytes = saturatedAdd(bytes, 2);
            } else if (Character.isHighSurrogate(character)
                    && index + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(index + 1))) {
                bytes = saturatedAdd(bytes, 4);
                index++;
            } else {
                bytes = saturatedAdd(bytes, 3);
            }
        }
        return bytes;
    }

    static long saturatedAdd(long left, long right) {
        return right > Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
    }
}
