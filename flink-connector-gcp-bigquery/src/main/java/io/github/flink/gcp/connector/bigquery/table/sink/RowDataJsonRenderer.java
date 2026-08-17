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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Renders a column marked by {@code sink.json-field-paths} as the JSON text a BigQuery {@code JSON}
 * column takes on the wire.
 *
 * <p>A marked {@code STRING} column needs none of this — its value is already JSON text and is
 * passed through unvalidated, as on every other write path. This exists for a marked {@code ROW},
 * which the schema converter stops recursing into: the column is a {@code JSON} column, so the row
 * has to become an object rather than a {@code STRUCT}.
 *
 * <p>The renderer is a tree built once from the column's {@code LogicalType}, so a type it cannot
 * render is rejected while the job graph is built rather than per record — the rule the whole
 * module follows, since the per-record path runs inside the writers' failure handler where one
 * misconfiguration would look like a poison record.
 *
 * <p>What the values become: a string is escaped per RFC 8259, {@code BYTES} is base64 (there being
 * no bytes in JSON), {@code DECIMAL} is an unquoted number at its declared scale, and the temporal
 * types are ISO-8601 strings — {@code DATE} as {@code 2026-08-06}, {@code TIME} as {@code
 * 12:34:56.789}, {@code TIMESTAMP} as a local date-time and {@code TIMESTAMP_LTZ} as an instant
 * with its {@code Z}. A {@code MAP} becomes an object and so needs string keys; a {@code MULTISET}
 * has no JSON form at all and is rejected.
 */
@Internal
final class RowDataJsonRenderer implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Renders one value of a known type into the buffer. */
    @FunctionalInterface
    private interface Renderer extends Serializable {
        void render(Object value, StringBuilder out);
    }

    private final LogicalType type;
    private final String path;

    /**
     * The renderer tree, rebuilt on deserialization rather than serialized with the job graph.
     *
     * <p>Each node is a lambda, and a serialized lambda's identity is its {@code SerializedLambda}
     * synthetic-method name. Lambdas sharing an enclosing declaration and a descriptor share one
     * name hash and differ only by a trailing index — measured, eleven of this class's do — so
     * supporting one more {@code LogicalTypeRoot} in {@link #build(LogicalType, String)} — which is
     * ordered the way {@link LogicalTypeRoot} declares its constants, so a new root lands in the
     * middle — renumbers every later node. A job graph restored against such a build then binds a
     * node to a *different* type's renderer without any error, which is why the tree is derived
     * from the type rather than carried in the bytes. {@code GenericRecordToRowDataConverter} keeps
     * the read path's converters the same way.
     */
    private transient Renderer renderer;

    /**
     * Builds a renderer for a marked column.
     *
     * @param type the column's type
     * @param path the column's dotted path, for error messages
     * @throws IllegalArgumentException if the column, or anything nested in it, has no JSON form
     */
    RowDataJsonRenderer(LogicalType type, String path) {
        this.type = type;
        this.path = path;
        initializeRenderer();
    }

    private void initializeRenderer() {
        this.renderer = build(type, path);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        initializeRenderer();
    }

    /**
     * Renders a value of the column's type.
     *
     * @param value the value, never {@code null} (a null column is written as unset instead)
     * @return the JSON text
     */
    String render(Object value) {
        StringBuilder out = new StringBuilder();
        renderer.render(value, out);
        return out.toString();
    }

    private static Renderer build(LogicalType type, String path) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (value, out) -> escape(value.toString(), out);
            case BOOLEAN:
                return (value, out) -> out.append(((Boolean) value).booleanValue());
            case TINYINT:
                return (value, out) -> out.append(((Byte) value).byteValue());
            case SMALLINT:
                return (value, out) -> out.append(((Short) value).shortValue());
            case INTEGER:
                return (value, out) -> out.append(((Integer) value).intValue());
            case BIGINT:
                return (value, out) -> out.append(((Long) value).longValue());
            case FLOAT:
                return (value, out) -> appendFinite(((Float) value).doubleValue(), out, path);
            case DOUBLE:
                return (value, out) -> appendFinite(((Double) value).doubleValue(), out, path);
            case DECIMAL:
                int scale = ((DecimalType) type).getScale();
                return (value, out) ->
                        out.append(
                                ((DecimalData) value)
                                        .toBigDecimal()
                                        .setScale(scale, RoundingMode.UNNECESSARY)
                                        .toPlainString());
            case BINARY:
            case VARBINARY:
                return (value, out) ->
                        escape(Base64.getEncoder().encodeToString((byte[]) value), out);
            case DATE:
                return (value, out) ->
                        escape(LocalDate.ofEpochDay(((Integer) value).longValue()).toString(), out);
            case TIME_WITHOUT_TIME_ZONE:
                return (value, out) ->
                        escape(
                                LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1_000_000L)
                                        .format(DateTimeFormatter.ISO_LOCAL_TIME),
                                out);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (value, out) ->
                        escape(((TimestampData) value).toLocalDateTime().toString(), out);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (value, out) -> escape(((TimestampData) value).toInstant().toString(), out);
            case ARRAY:
                return arrayRenderer((ArrayType) type, path);
            case MAP:
                return mapRenderer((MapType) type, path);
            case ROW:
                return rowRenderer((RowType) type, path);
            default:
                throw new IllegalArgumentException(
                        "Column "
                                + path
                                + " is "
                                + type
                                + ", which has no JSON form; it cannot be part of a column marked"
                                + " by sink.json-field-paths");
        }
    }

    private static Renderer arrayRenderer(ArrayType type, String path) {
        LogicalType elementType = type.getElementType();
        Renderer item = build(elementType, path + "[]");
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(elementType);
        return (value, out) -> {
            ArrayData array = (ArrayData) value;
            out.append('[');
            for (int i = 0; i < array.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                Object elementValue = getter.getElementOrNull(array, i);
                if (elementValue == null) {
                    out.append("null");
                } else {
                    item.render(elementValue, out);
                }
            }
            out.append(']');
        };
    }

    private static Renderer mapRenderer(MapType type, String path) {
        LogicalTypeRoot keyRoot = type.getKeyType().getTypeRoot();
        if (keyRoot != LogicalTypeRoot.CHAR && keyRoot != LogicalTypeRoot.VARCHAR) {
            throw new IllegalArgumentException(
                    "Column "
                            + path
                            + " is a map keyed by "
                            + type.getKeyType()
                            + ", but a JSON object's keys are strings");
        }
        LogicalType valueType = type.getValueType();
        Renderer valueRenderer = build(valueType, path + ".value");
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(type.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return (value, out) -> {
            MapData map = (MapData) value;
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            out.append('{');
            for (int i = 0; i < map.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                Object key = keyGetter.getElementOrNull(keys, i);
                if (key == null) {
                    throw new IllegalStateException(
                            "Column " + path + " has a null key, which a JSON object cannot hold");
                }
                escape(key.toString(), out);
                out.append(':');
                Object mapValue = valueGetter.getElementOrNull(values, i);
                if (mapValue == null) {
                    out.append("null");
                } else {
                    valueRenderer.render(mapValue, out);
                }
            }
            out.append('}');
        };
    }

    private static Renderer rowRenderer(RowType type, String path) {
        List<String> names = new ArrayList<>();
        List<Renderer> renderers = new ArrayList<>();
        List<RowData.FieldGetter> getters = new ArrayList<>();
        for (int i = 0; i < type.getFieldCount(); i++) {
            RowType.RowField field = type.getFields().get(i);
            String childPath = path.isEmpty() ? field.getName() : path + "." + field.getName();
            names.add(field.getName());
            renderers.add(build(field.getType(), childPath));
            getters.add(RowData.createFieldGetter(field.getType(), i));
        }
        return (value, out) -> {
            RowData row = (RowData) value;
            out.append('{');
            for (int i = 0; i < names.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                escape(names.get(i), out);
                out.append(':');
                Object field = getters.get(i).getFieldOrNull(row);
                if (field == null) {
                    out.append("null");
                } else {
                    renderers.get(i).render(field, out);
                }
            }
            out.append('}');
        };
    }

    /**
     * JSON has no NaN and no infinity, so a value that is neither finite nor representable fails
     * its own row rather than producing a document BigQuery rejects with a message about the whole
     * column.
     */
    private static void appendFinite(double value, StringBuilder out, String path) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalStateException(
                    "Column " + path + " is " + value + ", which JSON cannot represent");
        }
        out.append(value);
    }

    /** Appends the JSON string form of the value, escaped per RFC 8259. */
    private static void escape(String value, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                    break;
            }
        }
        out.append('"');
    }
}
