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
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.table.ChangeStreamChangelogMode;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Converts one Spanner data-change record into an atomic batch of Flink changelog rows. */
@Internal
final class DataChangeRecordToRowDataConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SpannerTableSchemaConverter schema;
    private final SpannerTableName table;
    private final ChangeStreamChangelogMode changelogMode;
    private final StructToRowDataConverter rowConverter;
    private final boolean[] primaryKeyFields;

    DataChangeRecordToRowDataConverter(
            SpannerTableSchemaConverter schema,
            SpannerTableName table,
            ChangeStreamChangelogMode changelogMode) {
        this.schema = schema;
        this.table = table;
        this.changelogMode = changelogMode;
        this.rowConverter = new StructToRowDataConverter(schema, null);
        this.primaryKeyFields = new boolean[schema.getColumns().size()];
        for (int index : schema.getPrimaryKeyIndexes()) {
            primaryKeyFields[index] = true;
        }
    }

    List<ConvertedRow> convert(DataChangeRecord record) throws IOException {
        int modIndex = -1;
        try {
            if (!table.matchesNativeApiName(record.getTableName())) {
                return Collections.emptyList();
            }
            validateCaptureType(record);
            validateSchema(record);
            List<ConvertedRow> rows = new ArrayList<>();
            for (int index = 0; index < record.getMods().size(); index++) {
                modIndex = index;
                appendRows(record, record.getMods().get(index), index, rows);
            }
            return rows;
        } catch (RuntimeException ignored) {
            throw failure(record, modIndex);
        }
    }

    private void validateCaptureType(DataChangeRecord record) {
        ValueCaptureType capture = record.getValueCaptureType();
        if (changelogMode == ChangeStreamChangelogMode.FULL) {
            require(
                    capture == ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                    "full changelog mode requires NEW_ROW_AND_OLD_VALUES value capture");
        } else {
            require(
                    capture == ValueCaptureType.NEW_ROW
                            || capture == ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                    "upsert changelog mode requires NEW_ROW or NEW_ROW_AND_OLD_VALUES value capture");
        }
    }

    private void validateSchema(DataChangeRecord record) {
        Map<String, DataChangeRecord.ColumnType> byName = new HashMap<>();
        for (DataChangeRecord.ColumnType column : record.getColumnTypes()) {
            require(byName.put(column.getName(), column) == null, "duplicate watched column");
        }
        for (SpannerTableSchemaConverter.Column expected : schema.getColumns()) {
            DataChangeRecord.ColumnType actual = byName.get(expected.getName());
            require(actual != null, "the record omits a declared table column");
            validateType(actual.getTypeDescriptorJson(), expected.getSpannerType());
        }
        if (changelogMode == ChangeStreamChangelogMode.UPSERT) {
            Set<String> actualKey = new HashSet<>();
            for (DataChangeRecord.ColumnType column : record.getColumnTypes()) {
                if (column.isPrimaryKey()) {
                    actualKey.add(column.getName());
                }
            }
            Set<String> expectedKey = new HashSet<>();
            for (int index : schema.getPrimaryKeyIndexes()) {
                expectedKey.add(schema.getColumns().get(index).getName());
            }
            require(actualKey.equals(expectedKey), "record primary key differs from the table DDL");
        }
    }

    private static void validateType(String descriptorJson, Type expected) {
        JsonObject descriptor = JsonParser.parseString(descriptorJson).getAsJsonObject();
        String code = stringMember(descriptor, "code");
        if (expected.getCode() == Type.Code.ARRAY) {
            require("ARRAY".equals(code), "record column type differs from the table DDL");
            JsonElement element = descriptor.get("array_element_type");
            require(
                    element != null && element.isJsonObject(),
                    "record ARRAY type has no element type");
            validateType(element.toString(), expected.getArrayElementType());
            return;
        }
        String annotation = optionalStringMember(descriptor, "type_annotation");
        String expectedCode = expected.getCode().name();
        boolean matches = expectedCode.equals(code);
        if (expected.getCode() == Type.Code.PG_NUMERIC) {
            matches = matches || ("NUMERIC".equals(code) && "PG_NUMERIC".equals(annotation));
        } else if (expected.getCode() == Type.Code.PG_JSONB) {
            matches = matches || ("JSON".equals(code) && "PG_JSONB".equals(annotation));
        }
        require(matches, "record column type differs from the table DDL");
        if (expected.getCode() == Type.Code.PROTO || expected.getCode() == Type.Code.ENUM) {
            require(
                    expected.getProtoTypeFqn().equals(stringMember(descriptor, "proto_type_fqn")),
                    "record named type differs from the table DDL");
        }
    }

    private void appendRows(
            DataChangeRecord record, Mod mod, int modNumber, List<ConvertedRow> rows) {
        JsonObject keys = object(mod.getKeysJson(), "keys");
        if (changelogMode == ChangeStreamChangelogMode.UPSERT) {
            if (record.getModType() == ModType.DELETE) {
                rows.add(new ConvertedRow(row(keys, null, false, RowKind.DELETE), modNumber));
            } else {
                rows.add(
                        new ConvertedRow(
                                row(
                                        keys,
                                        object(
                                                required(
                                                        mod.getNewValuesJson().orElse(null),
                                                        "new_values"),
                                                "new_values"),
                                        true,
                                        record.getModType() == ModType.INSERT
                                                ? RowKind.INSERT
                                                : RowKind.UPDATE_AFTER),
                                modNumber));
            }
            return;
        }
        switch (record.getModType()) {
            case INSERT:
                rows.add(
                        new ConvertedRow(
                                row(
                                        keys,
                                        object(
                                                required(
                                                        mod.getNewValuesJson().orElse(null),
                                                        "new_values"),
                                                "new_values"),
                                        true,
                                        RowKind.INSERT),
                                modNumber));
                return;
            case UPDATE:
                JsonObject afterValues =
                        object(
                                required(mod.getNewValuesJson().orElse(null), "new_values"),
                                "new_values");
                JsonObject beforeValues = afterValues.deepCopy();
                JsonObject oldValues =
                        object(
                                required(mod.getOldValuesJson().orElse(null), "old_values"),
                                "old_values");
                for (Map.Entry<String, JsonElement> old : oldValues.entrySet()) {
                    beforeValues.add(old.getKey(), old.getValue());
                }
                rows.add(
                        new ConvertedRow(
                                row(keys, beforeValues, true, RowKind.UPDATE_BEFORE), modNumber));
                rows.add(
                        new ConvertedRow(
                                row(keys, afterValues, true, RowKind.UPDATE_AFTER), modNumber));
                return;
            case DELETE:
                rows.add(
                        new ConvertedRow(
                                row(
                                        keys,
                                        object(
                                                required(
                                                        mod.getOldValuesJson().orElse(null),
                                                        "old_values"),
                                                "old_values"),
                                        true,
                                        RowKind.DELETE),
                                modNumber));
                return;
            default:
                throw new IllegalStateException("Unhandled modification type.");
        }
    }

    /** One staged changelog row and the position of the mod that produced it. */
    static final class ConvertedRow {
        private final RowData row;
        private final int modNumber;

        private ConvertedRow(RowData row, int modNumber) {
            this.row = row;
            this.modNumber = modNumber;
        }

        RowData getRow() {
            return row;
        }

        int getModNumber() {
            return modNumber;
        }
    }

    private RowData row(
            JsonObject keys, @Nullable JsonObject values, boolean requireComplete, RowKind kind) {
        Struct.Builder builder = Struct.newBuilder();
        for (SpannerTableSchemaConverter.Column column : schema.getColumns()) {
            JsonElement encoded = keys.get(column.getName());
            boolean key = primaryKeyFields[column.getIndex()];
            if (encoded == null && values != null) {
                encoded = values.get(column.getName());
            }
            if (encoded == null) {
                require(!key && !requireComplete, "a required row value is absent");
                encoded = com.google.gson.JsonNull.INSTANCE;
            }
            builder.set(column.getName()).to(value(encoded, column.getSpannerType()));
        }
        RowData row = rowConverter.convert(builder.build());
        row.setRowKind(kind);
        return row;
    }

    private static Value value(JsonElement json, Type type) {
        if (type.getCode() == Type.Code.ARRAY) {
            return arrayValue(json, type.getArrayElementType());
        }
        if (json.isJsonNull()) {
            return nullValue(type);
        }
        switch (type.getCode()) {
            case BOOL:
                return Value.bool(json.getAsBoolean());
            case INT64:
                return Value.int64(Long.parseLong(json.getAsString()));
            case FLOAT32:
                return Value.float32(Float.parseFloat(json.getAsString()));
            case FLOAT64:
                return Value.float64(Double.parseDouble(json.getAsString()));
            case NUMERIC:
                return Value.numeric(new BigDecimal(json.getAsString()));
            case PG_NUMERIC:
                return Value.pgNumeric(json.getAsString());
            case STRING:
                return Value.string(json.getAsString());
            case JSON:
                return Value.json(json.getAsString());
            case PG_JSONB:
                return Value.pgJsonb(json.getAsString());
            case UUID:
                return Value.uuid(UUID.fromString(json.getAsString()));
            case BYTES:
                return Value.bytesFromBase64(json.getAsString());
            case PROTO:
                return Value.protoMessage(
                        ByteArray.fromBase64(json.getAsString()), type.getProtoTypeFqn());
            case ENUM:
                return Value.protoEnum(Long.parseLong(json.getAsString()), type.getProtoTypeFqn());
            case TIMESTAMP:
                return Value.timestamp(Timestamp.parseTimestamp(json.getAsString()));
            case DATE:
                return Value.date(Date.parseDate(json.getAsString()));
            default:
                throw new IllegalArgumentException("Unsupported change-stream value type.");
        }
    }

    private static Value arrayValue(JsonElement json, Type elementType) {
        if (json.isJsonNull()) {
            return nullArrayValue(elementType);
        }
        JsonArray array = json.getAsJsonArray();
        switch (elementType.getCode()) {
            case BOOL:
                return Value.boolArray(
                        list(array, item -> item.isJsonNull() ? null : item.getAsBoolean()));
            case INT64:
                return Value.int64Array(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Long.parseLong(item.getAsString())));
            case FLOAT32:
                return Value.float32Array(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Float.parseFloat(item.getAsString())));
            case FLOAT64:
                return Value.float64Array(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Double.parseDouble(item.getAsString())));
            case NUMERIC:
                return Value.numericArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : new BigDecimal(item.getAsString())));
            case PG_NUMERIC:
                return Value.pgNumericArray(
                        list(array, item -> item.isJsonNull() ? null : item.getAsString()));
            case STRING:
                return Value.stringArray(
                        list(array, item -> item.isJsonNull() ? null : item.getAsString()));
            case JSON:
                return Value.jsonArray(
                        list(array, item -> item.isJsonNull() ? null : item.getAsString()));
            case PG_JSONB:
                return Value.pgJsonbArray(
                        list(array, item -> item.isJsonNull() ? null : item.getAsString()));
            case UUID:
                return Value.uuidArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : UUID.fromString(item.getAsString())));
            case BYTES:
                return Value.bytesArrayFromBase64(
                        list(array, item -> item.isJsonNull() ? null : item.getAsString()));
            case PROTO:
                return Value.protoMessageArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : ByteArray.fromBase64(item.getAsString())),
                        elementType.getProtoTypeFqn());
            case ENUM:
                return Value.protoEnumArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Long.parseLong(item.getAsString())),
                        elementType.getProtoTypeFqn());
            case TIMESTAMP:
                return Value.timestampArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Timestamp.parseTimestamp(item.getAsString())));
            case DATE:
                return Value.dateArray(
                        list(
                                array,
                                item ->
                                        item.isJsonNull()
                                                ? null
                                                : Date.parseDate(item.getAsString())));
            default:
                throw new IllegalArgumentException("Unsupported change-stream ARRAY element type.");
        }
    }

    private static Value nullValue(Type type) {
        switch (type.getCode()) {
            case BOOL:
                return Value.bool((Boolean) null);
            case INT64:
                return Value.int64((Long) null);
            case FLOAT32:
                return Value.float32((Float) null);
            case FLOAT64:
                return Value.float64((Double) null);
            case NUMERIC:
                return Value.numeric(null);
            case PG_NUMERIC:
                return Value.pgNumeric(null);
            case STRING:
                return Value.string(null);
            case JSON:
                return Value.json(null);
            case PG_JSONB:
                return Value.pgJsonb(null);
            case UUID:
                return Value.uuid(null);
            case BYTES:
                return Value.bytes(null);
            case PROTO:
                return Value.protoMessage((ByteArray) null, type.getProtoTypeFqn());
            case ENUM:
                return Value.protoEnum((Long) null, type.getProtoTypeFqn());
            case TIMESTAMP:
                return Value.timestamp(null);
            case DATE:
                return Value.date(null);
            default:
                throw new IllegalArgumentException("Unsupported null value type.");
        }
    }

    private static Value nullArrayValue(Type type) {
        switch (type.getCode()) {
            case BOOL:
                return Value.boolArray((Iterable<Boolean>) null);
            case INT64:
                return Value.int64Array((Iterable<Long>) null);
            case FLOAT32:
                return Value.float32Array((Iterable<Float>) null);
            case FLOAT64:
                return Value.float64Array((Iterable<Double>) null);
            case NUMERIC:
                return Value.numericArray(null);
            case PG_NUMERIC:
                return Value.pgNumericArray(null);
            case STRING:
                return Value.stringArray(null);
            case JSON:
                return Value.jsonArray(null);
            case PG_JSONB:
                return Value.pgJsonbArray(null);
            case UUID:
                return Value.uuidArray(null);
            case BYTES:
                return Value.bytesArray(null);
            case PROTO:
                return Value.protoMessageArray((Iterable<ByteArray>) null, type.getProtoTypeFqn());
            case ENUM:
                return Value.protoEnumArray((Iterable<Long>) null, type.getProtoTypeFqn());
            case TIMESTAMP:
                return Value.timestampArray(null);
            case DATE:
                return Value.dateArray(null);
            default:
                throw new IllegalArgumentException("Unsupported null ARRAY element type.");
        }
    }

    private static <T> List<T> list(JsonArray array, JsonDecoder<T> decoder) {
        List<T> values = new ArrayList<>(array.size());
        for (JsonElement item : array) {
            values.add(decoder.decode(item));
        }
        return values;
    }

    private static JsonObject object(String json, String member) {
        JsonElement value = JsonParser.parseString(json);
        require(value.isJsonObject(), member + " must be a JSON object");
        return value.getAsJsonObject();
    }

    private static String required(@Nullable String value, String member) {
        require(value != null, member + " is absent");
        return value;
    }

    private static String stringMember(JsonObject object, String member) {
        String value = optionalStringMember(object, member);
        require(value != null, "type descriptor has no " + member);
        return value;
    }

    @Nullable
    private static String optionalStringMember(JsonObject object, String member) {
        JsonElement value = object.get(member);
        return value != null && value.isJsonPrimitive() ? value.getAsString() : null;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    static IOException failure(DataChangeRecord record, int modIndex) {
        return new IOException(
                "Could not convert Spanner change-stream record"
                        + " (table="
                        + record.getTableName()
                        + ", commitTimestamp="
                        + record.getCommitTimestamp()
                        + ", transaction="
                        + record.getServerTransactionId()
                        + ", sequence="
                        + record.getRecordSequence()
                        + ", modIndex="
                        + modIndex
                        + ").");
    }

    @FunctionalInterface
    private interface JsonDecoder<T> {
        T decode(JsonElement value);
    }
}
