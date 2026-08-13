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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@code RowData} into the protobuf row the Storage Write API appends, following the
 * table schema {@link RowTypeToTableSchemaConverter} derived from the same {@code RowType}.
 *
 * <p>The conversion is a plan built once from the (row type, table schema, descriptor) triple and
 * then applied per record, which is {@code AvroRowConverter}'s shape.
 *
 * <p><b>What a {@code *Plan} is.</b> The word is this module's, shared with {@code
 * AvroRowConverter} and {@code ProtoRowConverter}: a plan is not a converter for a type but one
 * step of a tree mirroring the schema, resolved at construction so that per-record work is a flat
 * walk over it. Four of them divide the job:
 *
 * <ul>
 *   <li>{@link RecordPlan} — one <em>message</em>: a descriptor and one field plan per column, in
 *       order
 *   <li>{@link FieldPlan} — one <em>column</em>: the {@code FieldGetter} that reads it off the row,
 *       and whether it is written as a singular value, a repeated one or a map
 *   <li>{@link ValuePlan} — one <em>type</em>: the encoding {@link Kind}, a decimal's scale, and
 *       either a nested record plan or a JSON renderer. This is the piece that reads as "the
 *       converter for this column type", and the only one reached from more than one place
 *   <li>{@link MapPlan} — a map's <em>entries</em>: a value plan and an element getter for the key
 *       and for the value
 * </ul>
 *
 * <p>The value plan is separate here where the sibling converters fold it into their field plan,
 * because an array element and a map's key and value have no field to be read from — Avro reaches
 * the same place with a sentinel field position instead.
 *
 * <p>Two properties of the plan are load-bearing:
 *
 * <ul>
 *   <li><b>Descriptor fields are paired by position, never by name.</b> {@code
 *       BQTableSchemaToProtoDescriptor} lowercases column names with the <em>default</em> locale,
 *       so under {@code tr_TR} a column named {@code ID} becomes the proto field {@code ıd} that no
 *       {@code Locale.ROOT} key matches. Position is exact because the descriptor is always derived
 *       from the table schema this connector just produced — and here the row type is that schema's
 *       source, so all three orders agree.
 *   <li><b>The kind switch is exhaustive over the column types the schema converter can emit.</b> A
 *       missing case would not fail a schema test: the column derives correctly and then throws on
 *       the first record, inside the writers' failure handler. Its {@code default} is that guard.
 * </ul>
 *
 * <p>Values are read through {@code RowData.FieldGetter}s and copied rather than aliased — a {@code
 * StringData} may be backed by a buffer the runtime reuses when object reuse is on.
 */
@Internal
final class RowDataToProtoConverter {

    /** One encoding per BigQuery column type, not one per Java type. */
    private enum Kind {
        STRING,
        JSON_DOCUMENT,
        BYTES,
        INT64,
        DOUBLE,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        NUMERIC,
        BIGNUMERIC,
        STRUCT
    }

    private final RecordPlan plan;

    /**
     * Builds the converter.
     *
     * @param rowType the physical columns of the SQL table
     * @param tableSchema the schema derived from them
     * @param rowDescriptor the descriptor derived from that schema
     */
    public RowDataToProtoConverter(
            RowType rowType, TableSchema tableSchema, Descriptors.Descriptor rowDescriptor) {
        this.plan = buildRecordPlan(rowType, tableSchema.getFieldsList(), rowDescriptor, "");
    }

    /** Builds a root-row converter that reads only the selected physical column indexes. */
    RowDataToProtoConverter(
            RowType rowType,
            TableSchema tableSchema,
            Descriptors.Descriptor rowDescriptor,
            int[] selectedIndexes) {
        this.plan =
                buildSelectedRecordPlan(
                        rowType, tableSchema.getFieldsList(), rowDescriptor, selectedIndexes);
    }

    /**
     * Converts one row.
     *
     * @param row the row
     * @return the protobuf row
     * @throws IOException if a value cannot be represented in its destination column
     */
    public DynamicMessage convert(RowData row) throws IOException {
        return plan.convert(row);
    }

    /** Converts a row without requiring descriptor fields excluded from this converter's plan. */
    DynamicMessage convertPartial(RowData row) throws IOException {
        return plan.convertPartial(row);
    }

    private static RecordPlan buildSelectedRecordPlan(
            RowType rowType,
            List<TableFieldSchema> fields,
            Descriptors.Descriptor descriptor,
            int[] selectedIndexes) {
        Preconditions.checkState(
                descriptor.getFields().size() == fields.size(),
                "The row descriptor has %s fields where the root schema has %s",
                descriptor.getFields().size(),
                fields.size());
        boolean[] selected = new boolean[fields.size()];
        List<FieldPlan> plans = new ArrayList<>();
        for (int index : selectedIndexes) {
            Preconditions.checkArgument(
                    index >= 0 && index < fields.size(),
                    "Selected physical column index %s is outside a row with %s columns",
                    index,
                    fields.size());
            Preconditions.checkArgument(
                    !selected[index], "Selected physical column index %s is repeated", index);
            selected[index] = true;
            RowType.RowField rowField = rowType.getFields().get(index);
            plans.add(
                    buildFieldPlan(
                            rowField.getType(),
                            fields.get(index),
                            descriptor.getFields().get(index),
                            rowField.getName(),
                            RowData.createFieldGetter(rowField.getType(), index)));
        }
        return new RecordPlan(descriptor, plans);
    }

    private static RecordPlan buildRecordPlan(
            RowType rowType,
            List<TableFieldSchema> fields,
            Descriptors.Descriptor descriptor,
            String parentPath) {
        Preconditions.checkState(
                descriptor.getFields().size() == fields.size(),
                "The row descriptor has %s fields where the schema at %s has %s",
                descriptor.getFields().size(),
                parentPath.isEmpty() ? "<root>" : parentPath,
                fields.size());
        List<FieldPlan> plans = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            TableFieldSchema field = fields.get(i);
            Descriptors.FieldDescriptor protoField = descriptor.getFields().get(i);
            RowType.RowField rowField = rowType.getFields().get(i);
            String path =
                    parentPath.isEmpty()
                            ? rowField.getName()
                            : parentPath + "." + rowField.getName();
            plans.add(
                    buildFieldPlan(
                            rowField.getType(),
                            field,
                            protoField,
                            path,
                            RowData.createFieldGetter(rowField.getType(), i)));
        }
        return new RecordPlan(descriptor, plans);
    }

    private static FieldPlan buildFieldPlan(
            LogicalType type,
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            String path,
            RowData.FieldGetter getter) {
        boolean repeated = field.getMode() == TableFieldSchema.Mode.REPEATED;
        LogicalTypeRoot root = type.getTypeRoot();

        if (repeated && (root == LogicalTypeRoot.MAP || root == LogicalTypeRoot.MULTISET)) {
            return FieldPlan.map(protoField, getter, mapPlan(type, field, protoField, path), path);
        }
        if (repeated && root == LogicalTypeRoot.ARRAY) {
            LogicalType element = ((ArrayType) type).getElementType();
            ValuePlan value = valuePlan(element, field, protoField, path);
            return FieldPlan.array(
                    protoField, getter, ArrayData.createElementGetter(element), value, path);
        }
        // A marked ROW column is JSON, so it is singular even though its type is a row; every other
        // repeated shape was handled above.
        return FieldPlan.singular(
                protoField, getter, valuePlan(type, field, protoField, path), path);
    }

    private static MapPlan mapPlan(
            LogicalType type,
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            String path) {
        LogicalType keyType;
        LogicalType valueType;
        if (type.getTypeRoot() == LogicalTypeRoot.MULTISET) {
            keyType = ((MultisetType) type).getElementType();
            valueType = new IntType(false);
        } else {
            keyType = ((MapType) type).getKeyType();
            valueType = ((MapType) type).getValueType();
        }
        Descriptors.Descriptor entry = protoField.getMessageType();
        // "key" and "value" by name: BQTableSchemaToProtoDescriptor emits these two lowercase
        // already, so there is no locale hazard to dodge here.
        Descriptors.FieldDescriptor keyField = entry.findFieldByName("key");
        Descriptors.FieldDescriptor valueField = entry.findFieldByName("value");
        TableFieldSchema keySchema = field.getFields(0);
        TableFieldSchema valueSchema = field.getFields(1);
        return new MapPlan(
                entry,
                keyField,
                valuePlan(keyType, keySchema, keyField, path + ".key"),
                ArrayData.createElementGetter(keyType),
                valueField,
                valuePlan(valueType, valueSchema, valueField, path + ".value"),
                ArrayData.createElementGetter(valueType),
                path);
    }

    private static ValuePlan valuePlan(
            LogicalType type,
            TableFieldSchema field,
            Descriptors.FieldDescriptor protoField,
            String path) {
        switch (field.getType()) {
            case STRING:
            case GEOGRAPHY:
                return new ValuePlan(Kind.STRING, 0, null, null, path);
            case JSON:
                // A marked STRING is already JSON text and goes through verbatim, exactly as on
                // every other write path; a marked ROW is rendered here.
                if (type.getTypeRoot() == LogicalTypeRoot.ROW) {
                    return new ValuePlan(
                            Kind.JSON_DOCUMENT, 0, null, new RowDataJsonRenderer(type, path), path);
                }
                return new ValuePlan(Kind.STRING, 0, null, null, path);
            case BYTES:
                return new ValuePlan(Kind.BYTES, 0, null, null, path);
            case INT64:
                return new ValuePlan(Kind.INT64, 0, null, null, path);
            case DOUBLE:
                return new ValuePlan(Kind.DOUBLE, 0, null, null, path);
            case BOOL:
                return new ValuePlan(Kind.BOOL, 0, null, null, path);
            case DATE:
                return new ValuePlan(Kind.DATE, 0, null, null, path);
            case TIME:
                return new ValuePlan(Kind.TIME, 0, null, null, path);
            case DATETIME:
                return new ValuePlan(Kind.DATETIME, 0, null, null, path);
            case TIMESTAMP:
                return new ValuePlan(Kind.TIMESTAMP, 0, null, null, path);
            case NUMERIC:
                return new ValuePlan(
                        Kind.NUMERIC, ((DecimalType) type).getScale(), null, null, path);
            case BIGNUMERIC:
                return new ValuePlan(
                        Kind.BIGNUMERIC, ((DecimalType) type).getScale(), null, null, path);
            case STRUCT:
                return new ValuePlan(
                        Kind.STRUCT,
                        0,
                        buildRecordPlan(
                                (RowType) type,
                                field.getFieldsList(),
                                protoField.getMessageType(),
                                path),
                        null,
                        path);
            default:
                throw new IllegalArgumentException(
                        "Column "
                                + path
                                + " has type "
                                + field.getType()
                                + ", which the RowData serializer does not produce");
        }
    }

    /** One message: its descriptor and one field plan per column, in order. */
    private static final class RecordPlan {

        private final Descriptors.Descriptor descriptor;
        private final List<FieldPlan> fields;

        RecordPlan(Descriptors.Descriptor descriptor, List<FieldPlan> fields) {
            this.descriptor = descriptor;
            this.fields = fields;
        }

        DynamicMessage convert(RowData row) throws IOException {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            for (FieldPlan field : fields) {
                field.apply(row, builder);
            }
            return builder.build();
        }

        DynamicMessage convertPartial(RowData row) throws IOException {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            for (FieldPlan field : fields) {
                field.apply(row, builder);
            }
            return builder.buildPartial();
        }
    }

    /** One column: how its value is read off a row and written into the message. */
    private static final class FieldPlan {

        private final Descriptors.FieldDescriptor protoField;
        private final RowData.FieldGetter getter;
        private final ValuePlan value;
        private final ArrayData.ElementGetter elementGetter;
        private final MapPlan map;
        private final String path;

        private FieldPlan(
                Descriptors.FieldDescriptor protoField,
                RowData.FieldGetter getter,
                ValuePlan value,
                ArrayData.ElementGetter elementGetter,
                MapPlan map,
                String path) {
            this.protoField = protoField;
            this.getter = getter;
            this.value = value;
            this.elementGetter = elementGetter;
            this.map = map;
            this.path = path;
        }

        static FieldPlan singular(
                Descriptors.FieldDescriptor protoField,
                RowData.FieldGetter getter,
                ValuePlan value,
                String path) {
            return new FieldPlan(protoField, getter, value, null, null, path);
        }

        static FieldPlan array(
                Descriptors.FieldDescriptor protoField,
                RowData.FieldGetter getter,
                ArrayData.ElementGetter elementGetter,
                ValuePlan value,
                String path) {
            return new FieldPlan(protoField, getter, value, elementGetter, null, path);
        }

        static FieldPlan map(
                Descriptors.FieldDescriptor protoField,
                RowData.FieldGetter getter,
                MapPlan map,
                String path) {
            return new FieldPlan(protoField, getter, null, null, map, path);
        }

        void apply(RowData row, DynamicMessage.Builder target) throws IOException {
            Object raw = getter.getFieldOrNull(row);
            if (map != null) {
                if (raw != null) {
                    map.apply((MapData) raw, protoField, target);
                }
                return;
            }
            if (elementGetter != null) {
                if (raw != null) {
                    ArrayData array = (ArrayData) raw;
                    for (int i = 0; i < array.size(); i++) {
                        Object element = elementGetter.getElementOrNull(array, i);
                        if (element == null) {
                            throw new IOException(
                                    "Column "
                                            + path
                                            + " is a BigQuery REPEATED column, which cannot hold a"
                                            + " null element");
                        }
                        target.addRepeatedField(protoField, value.convert(element));
                    }
                }
                return;
            }
            if (raw == null) {
                if (protoField.isRequired()) {
                    throw new IOException(
                            "Column "
                                    + path
                                    + " is a BigQuery REQUIRED column but the row carries no value"
                                    + " for it");
                }
                return;
            }
            target.setField(protoField, value.convert(raw));
        }
    }

    /** A map's entries: how they become the repeated {@code STRUCT<key, value>} messages. */
    private static final class MapPlan {

        private final Descriptors.Descriptor entryDescriptor;
        private final Descriptors.FieldDescriptor keyField;
        private final ValuePlan keyPlan;
        private final ArrayData.ElementGetter keyGetter;
        private final Descriptors.FieldDescriptor valueField;
        private final ValuePlan valuePlan;
        private final ArrayData.ElementGetter valueGetter;
        private final String path;

        MapPlan(
                Descriptors.Descriptor entryDescriptor,
                Descriptors.FieldDescriptor keyField,
                ValuePlan keyPlan,
                ArrayData.ElementGetter keyGetter,
                Descriptors.FieldDescriptor valueField,
                ValuePlan valuePlan,
                ArrayData.ElementGetter valueGetter,
                String path) {
            this.entryDescriptor = entryDescriptor;
            this.keyField = keyField;
            this.keyPlan = keyPlan;
            this.keyGetter = keyGetter;
            this.valueField = valueField;
            this.valuePlan = valuePlan;
            this.valueGetter = valueGetter;
            this.path = path;
        }

        void apply(
                MapData map, Descriptors.FieldDescriptor protoField, DynamicMessage.Builder target)
                throws IOException {
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            for (int i = 0; i < map.size(); i++) {
                Object key = keyGetter.getElementOrNull(keys, i);
                if (key == null) {
                    throw new IOException(
                            "Column " + path + " has a null key, which BigQuery cannot store");
                }
                DynamicMessage.Builder entry = DynamicMessage.newBuilder(entryDescriptor);
                entry.setField(keyField, keyPlan.convert(key));
                Object value = valueGetter.getElementOrNull(values, i);
                if (value != null) {
                    entry.setField(valueField, valuePlan.convert(value));
                } else if (valueField.isRequired()) {
                    throw new IOException(
                            "Column " + path + " has a null value under a REQUIRED value column");
                }
                target.addRepeatedField(protoField, entry.build());
            }
        }
    }

    /** One type: how a value of it becomes its protobuf form. */
    private static final class ValuePlan {

        private final Kind kind;
        private final int decimalScale;
        private final RecordPlan nested;
        private final RowDataJsonRenderer renderer;
        private final String path;

        ValuePlan(
                Kind kind,
                int decimalScale,
                RecordPlan nested,
                RowDataJsonRenderer renderer,
                String path) {
            this.kind = kind;
            this.decimalScale = decimalScale;
            this.nested = nested;
            this.renderer = renderer;
            this.path = path;
        }

        Object convert(Object value) throws IOException {
            try {
                switch (kind) {
                    case STRING:
                        // Copied rather than aliased: a StringData may be backed by a buffer the
                        // runtime reuses for the next record.
                        return value.toString();
                    case JSON_DOCUMENT:
                        return renderer.render(value);
                    case BYTES:
                        return ByteString.copyFrom((byte[]) value);
                    case INT64:
                        return ((Number) value).longValue();
                    case DOUBLE:
                        return ((Number) value).doubleValue();
                    case BOOL:
                        return value;
                    case DATE:
                        return value;
                    case TIME:
                        return CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(
                                LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1_000_000L));
                    case DATETIME:
                        return CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(
                                ((TimestampData) value).toLocalDateTime());
                    case TIMESTAMP:
                        return toEpochMicros((TimestampData) value);
                    case NUMERIC:
                        return BigDecimalByteStringEncoder.encodeToNumericByteString(
                                ((DecimalData) value)
                                        .toBigDecimal()
                                        .setScale(decimalScale, RoundingMode.UNNECESSARY));
                    case BIGNUMERIC:
                        return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                                ((DecimalData) value)
                                        .toBigDecimal()
                                        .setScale(decimalScale, RoundingMode.UNNECESSARY));
                    case STRUCT:
                        return nested.convert((RowData) value);
                    default:
                        // An AssertionError rather than an exception the catch below would turn
                        // into a row failure: an unhandled kind is this class disagreeing with
                        // itself, not a value the destination cannot hold.
                        throw new AssertionError("Unhandled kind: " + kind);
                }
            } catch (ArithmeticException
                    | IllegalArgumentException
                    | IllegalStateException
                    | DateTimeException e) {
                throw new IOException(
                        "Value of column "
                                + path
                                + " cannot be represented in the destination column: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * An instant as epoch microseconds, which is what a BigQuery {@code TIMESTAMP} column takes.
     */
    private static long toEpochMicros(TimestampData value) {
        return Math.addExact(
                Math.multiplyExact(value.getMillisecond(), 1_000L),
                value.getNanoOfMillisecond() / 1_000L);
    }
}
