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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The physical DDL schema and its corresponding native Spanner types. */
@Internal
public final class SpannerTableSchemaConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Column> columns;
    private final int[] primaryKeyIndexes;

    private SpannerTableSchemaConverter(List<Column> columns, int[] primaryKeyIndexes) {
        this.columns = Collections.unmodifiableList(columns);
        this.primaryKeyIndexes = primaryKeyIndexes;
    }

    /** Parses and validates one physical table schema. */
    public static SpannerTableSchemaConverter of(
            RowType rowType,
            int[] primaryKeyIndexes,
            Dialect dialect,
            List<String> jsonPaths,
            Map<String, String> protoTypes,
            Map<String, String> enumTypes) {
        return of(
                rowType,
                primaryKeyIndexes,
                dialect,
                jsonPaths,
                Collections.emptyList(),
                protoTypes,
                enumTypes);
    }

    /** Parses and validates one physical table schema, including native UUID markers. */
    public static SpannerTableSchemaConverter of(
            RowType rowType,
            int[] primaryKeyIndexes,
            Dialect dialect,
            List<String> jsonPaths,
            List<String> uuidPaths,
            Map<String, String> protoTypes,
            Map<String, String> enumTypes) {
        Markers markers = new Markers(jsonPaths, uuidPaths, protoTypes, enumTypes);
        List<Column> columns = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String name = rowType.getFieldNames().get(i);
            LogicalType logicalType = rowType.getTypeAt(i);
            columns.add(
                    new Column(
                            name,
                            i,
                            logicalType,
                            toSpannerType(name, logicalType, dialect, markers)));
        }
        markers.checkAllConsumed();
        checkPrimaryKey(primaryKeyIndexes, columns);
        return new SpannerTableSchemaConverter(
                columns, Arrays.copyOf(primaryKeyIndexes, primaryKeyIndexes.length));
    }

    private static Type toSpannerType(
            String path, LogicalType logicalType, Dialect dialect, Markers markers) {
        String proto = markers.proto(path);
        String enumType = markers.enumType(path);
        boolean json = markers.json(path);
        boolean uuid = markers.uuid(path);
        int specialCount =
                (proto != null ? 1 : 0)
                        + (enumType != null ? 1 : 0)
                        + (json ? 1 : 0)
                        + (uuid ? 1 : 0);
        if (specialCount > 1) {
            throw new ValidationException(
                    "Field path '" + path + "' has more than one special Spanner type marker.");
        }
        if (dialect == Dialect.POSTGRESQL && (proto != null || enumType != null)) {
            throw new ValidationException(
                    "Field path '"
                            + path
                            + "' uses a PROTO or ENUM marker, but those named types require the GOOGLE_STANDARD_SQL dialect.");
        }
        if (proto != null) {
            return markedScalarOrArray(path, logicalType, Type.proto(proto), "PROTO", "BYTES");
        }
        if (enumType != null) {
            return markedScalarOrArray(
                    path, logicalType, Type.protoEnum(enumType), "ENUM", "BIGINT");
        }
        if (json) {
            Type jsonType = dialect == Dialect.POSTGRESQL ? Type.pgJsonb() : Type.json();
            return markedScalarOrArray(path, logicalType, jsonType, "JSON", "STRING");
        }
        if (uuid) {
            return markedScalarOrArray(path, logicalType, Type.uuid(), "UUID", "STRING");
        }

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return Type.bool();
            case BIGINT:
                return Type.int64();
            case FLOAT:
                return Type.float32();
            case DOUBLE:
                return Type.float64();
            case DECIMAL:
                DecimalType decimal = (DecimalType) logicalType;
                if (dialect == Dialect.GOOGLE_STANDARD_SQL
                        && (decimal.getPrecision() != 38 || decimal.getScale() != 9)) {
                    throw unsupported(
                            path,
                            logicalType,
                            "The connector supports only DECIMAL(38, 9) for GoogleSQL NUMERIC.");
                }
                return dialect == Dialect.POSTGRESQL ? Type.pgNumeric() : Type.numeric();
            case CHAR:
            case VARCHAR:
                return Type.string();
            case BINARY:
            case VARBINARY:
                return Type.bytes();
            case DATE:
                return Type.date();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int precision = ((LocalZonedTimestampType) logicalType).getPrecision();
                if (precision > 9) {
                    throw unsupported(
                            path,
                            logicalType,
                            "Spanner TIMESTAMP supports at most nanosecond precision.");
                }
                return Type.timestamp();
            case ARRAY:
                if (((ArrayType) logicalType)
                        .getElementType()
                        .is(org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY)) {
                    throw unsupported(
                            path, logicalType, "Spanner ARRAY elements cannot be another ARRAY.");
                }
                return Type.array(
                        toSpannerType(
                                path + "[]",
                                ((ArrayType) logicalType).getElementType(),
                                dialect,
                                markers));
            default:
                throw unsupported(
                        path, logicalType, "The type has no lossless Spanner table mapping.");
        }
    }

    private static Type markedScalarOrArray(
            String path,
            LogicalType logicalType,
            Type markedType,
            String spannerName,
            String flinkName) {
        LogicalType candidate = logicalType;
        boolean array =
                candidate.getTypeRoot()
                        == org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
        if (array) {
            candidate = ((ArrayType) candidate).getElementType();
        }
        boolean compatible;
        if ("BYTES".equals(flinkName)) {
            compatible =
                    candidate.isAnyOf(
                            org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY,
                            org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY);
        } else if ("BIGINT".equals(flinkName)) {
            compatible = candidate.is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT);
        } else {
            compatible =
                    candidate.isAnyOf(
                            org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR,
                            org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR);
        }
        if (!compatible) {
            throw unsupported(
                    path,
                    logicalType,
                    spannerName
                            + " must be declared as "
                            + flinkName
                            + (array ? " elements." : "."));
        }
        return array ? Type.array(markedType) : markedType;
    }

    private static ValidationException unsupported(String path, LogicalType type, String reason) {
        return new ValidationException(
                "Column '"
                        + path
                        + "' has unsupported type "
                        + type.asSummaryString()
                        + ". "
                        + reason);
    }

    private static void checkPrimaryKey(int[] indexes, List<Column> columns) {
        Set<Integer> seen = new HashSet<>();
        for (int index : indexes) {
            if (index < 0 || index >= columns.size() || !seen.add(index)) {
                throw new ValidationException(
                        "The declared PRIMARY KEY contains an invalid physical column index.");
            }
            Type.Code code = columns.get(index).spannerType.getCode();
            if (!(code == Type.Code.BOOL
                    || code == Type.Code.INT64
                    || code == Type.Code.FLOAT64
                    || code == Type.Code.NUMERIC
                    || code == Type.Code.STRING
                    || code == Type.Code.BYTES
                    || code == Type.Code.TIMESTAMP
                    || code == Type.Code.DATE
                    || code == Type.Code.UUID)) {
                throw new ValidationException(
                        "PRIMARY KEY column '"
                                + columns.get(index).name
                                + "' maps to "
                                + code
                                + ", which cannot be a Spanner key part.");
            }
        }
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int[] getPrimaryKeyIndexes() {
        return Arrays.copyOf(primaryKeyIndexes, primaryKeyIndexes.length);
    }

    public boolean hasPrimaryKey() {
        return primaryKeyIndexes.length > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpannerTableSchemaConverter that = (SpannerTableSchemaConverter) o;
        return columns.equals(that.columns)
                && Arrays.equals(primaryKeyIndexes, that.primaryKeyIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, Arrays.hashCode(primaryKeyIndexes));
    }

    /** One physical column. */
    @Internal
    public static final class Column implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final int index;
        private final LogicalType logicalType;
        private final Type spannerType;

        private Column(String name, int index, LogicalType logicalType, Type spannerType) {
            this.name = name;
            this.index = index;
            this.logicalType = logicalType;
            this.spannerType = spannerType;
        }

        public String getName() {
            return name;
        }

        public int getIndex() {
            return index;
        }

        public LogicalType getLogicalType() {
            return logicalType;
        }

        public Type getSpannerType() {
            return spannerType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Column column = (Column) o;
            return index == column.index
                    && name.equals(column.name)
                    && logicalType.equals(column.logicalType)
                    && spannerType.equals(column.spannerType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, index, logicalType, spannerType);
        }
    }

    private static final class Markers {
        private final Set<String> json;
        private final Set<String> uuid;
        private final Map<String, String> proto;
        private final Map<String, String> enumTypes;
        private final Set<String> consumed = new LinkedHashSet<>();

        private Markers(
                List<String> json,
                List<String> uuid,
                Map<String, String> proto,
                Map<String, String> enumTypes) {
            this.json = new LinkedHashSet<>(json);
            this.uuid = new LinkedHashSet<>(uuid);
            this.proto = new LinkedHashMap<>(proto);
            this.enumTypes = new LinkedHashMap<>(enumTypes);
        }

        private boolean json(String path) {
            if (json.contains(path)) {
                consumed.add(path);
            }
            return json.contains(path);
        }

        private String proto(String path) {
            if (proto.containsKey(path)) {
                consumed.add(path);
            }
            return proto.get(path);
        }

        private boolean uuid(String path) {
            if (uuid.contains(path)) {
                consumed.add(path);
            }
            return uuid.contains(path);
        }

        private String enumType(String path) {
            if (enumTypes.containsKey(path)) {
                consumed.add(path);
            }
            return enumTypes.get(path);
        }

        private void checkAllConsumed() {
            Set<String> declared = new LinkedHashSet<>(json);
            declared.addAll(uuid);
            declared.addAll(proto.keySet());
            declared.addAll(enumTypes.keySet());
            declared.removeAll(consumed);
            if (!declared.isEmpty()) {
                throw new ValidationException(
                        "Special Spanner type markers name unknown field paths: " + declared + ".");
            }
        }
    }
}
