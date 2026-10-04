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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.ColumnMetadata;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.NamedTypeKind;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The inverse of the connector page's type mapping: a Spanner table's {@code INFORMATION_SCHEMA}
 * columns as a Flink schema, plus the marker options for the columns whose Flink type alone does
 * not name their Spanner type.
 *
 * <p>Each Spanner type maps to the one Flink type a hand-written table would declare for it
 * (docs/adr/0168): JSON, UUID, PROTO and ENUM columns map to their carrier types and are marked,
 * since the connector cannot tell them from the carrier otherwise (docs/adr/0096). Hidden columns
 * are left out, as Spanner's own {@code SELECT *} leaves them out, and so is a generated column
 * that is not stored, which Spanner's read API refuses to return; a stored generated column is kept
 * and marked, so reads return it and writes leave it out. Primary-key columns are {@code NOT NULL},
 * which the planner requires of a key; a generated non-key column is nullable whatever Spanner
 * says, because a statement does not write it and the planner would otherwise refuse the row.
 */
@Internal
final class SpannerSchemaToFlinkConverter {

    private SpannerSchemaToFlinkConverter() {}

    /** A converted table: its Flink schema and the marker options its columns need. */
    static final class Converted {
        private final Schema schema;
        private final Map<String, String> markerOptions;

        private Converted(Schema schema, Map<String, String> markerOptions) {
            this.schema = schema;
            this.markerOptions = markerOptions;
        }

        Schema schema() {
            return schema;
        }

        Map<String, String> markerOptions() {
            return markerOptions;
        }
    }

    /** A column whose Spanner type the connector has no Flink type for. */
    static final class UnsupportedColumnException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        UnsupportedColumnException(String message) {
            super(message);
        }
    }

    /**
     * Converts a table's columns.
     *
     * @param columns the columns in ordinal order
     * @param dialect the database's dialect
     * @param protoBundleTypes the database's PROTO and ENUM types by name, asked for only when a
     *     column has one
     * @return the schema and marker options
     * @throws UnsupportedColumnException naming the first column the mapping does not cover
     */
    static Converted convert(
            List<ColumnMetadata> columns,
            Dialect dialect,
            Supplier<Map<String, NamedTypeKind>> protoBundleTypes) {
        Schema.Builder schema = Schema.newBuilder();
        List<String> json = new ArrayList<>();
        List<String> uuid = new ArrayList<>();
        List<String> generated = new ArrayList<>();
        Map<String, String> protos = new LinkedHashMap<>();
        Map<String, String> enums = new LinkedHashMap<>();
        List<ColumnMetadata> key = new ArrayList<>();
        Map<String, NamedTypeKind> kinds = null;

        for (ColumnMetadata column : columns) {
            boolean keyColumn = column.keyPosition() != null;
            if (!keyColumn && (column.hidden() || (column.generated() && !column.stored()))) {
                // Neither is part of the readable row: Spanner's SELECT * leaves a hidden column
                // out, and its read API refuses a generated column that is not stored.
                continue;
            }
            SpannerTypeSpelling type = SpannerTypeSpelling.parse(column.spannerType(), dialect);
            DataType element;
            switch (type.kind()) {
                case JSON:
                    json.add(column.name());
                    element = DataTypes.STRING();
                    break;
                case UUID:
                    uuid.add(column.name());
                    element = DataTypes.STRING();
                    break;
                case PROTO:
                    protos.put(column.name(), type.typeName());
                    element = DataTypes.BYTES();
                    break;
                case ENUM:
                    enums.put(column.name(), type.typeName());
                    element = DataTypes.BIGINT();
                    break;
                case NAMED:
                    if (kinds == null) {
                        kinds = protoBundleTypes.get();
                    }
                    NamedTypeKind kind = kinds.get(type.typeName());
                    if (kind == null) {
                        throw unsupported(
                                column,
                                "the database's proto bundle declares no message or enum of that"
                                        + " name");
                    }
                    if (kind == NamedTypeKind.PROTO) {
                        protos.put(column.name(), type.typeName());
                        element = DataTypes.BYTES();
                    } else {
                        enums.put(column.name(), type.typeName());
                        element = DataTypes.BIGINT();
                    }
                    break;
                case UNSUPPORTED:
                    throw unsupported(column, "the connector's type mapping has no row for it");
                default:
                    element = scalar(type.kind());
            }
            DataType dataType = type.isArray() ? DataTypes.ARRAY(element) : element;
            if (keyColumn || (!column.nullable() && !column.generated())) {
                dataType = dataType.notNull();
            }
            if (column.generated()) {
                generated.add(column.name());
            }
            if (keyColumn) {
                key.add(column);
            }
            schema.column(column.name(), dataType);
        }
        if (!key.isEmpty()) {
            key.sort(Comparator.comparing(ColumnMetadata::keyPosition));
            List<String> keyNames = new ArrayList<>(key.size());
            for (ColumnMetadata column : key) {
                keyNames.add(column.name());
            }
            schema.primaryKey(keyNames);
        }

        Map<String, String> options = new LinkedHashMap<>();
        putList(options, SpannerConnectorOptions.JSON_FIELD_PATHS.key(), json);
        putList(options, SpannerConnectorOptions.UUID_FIELD_PATHS.key(), uuid);
        putList(options, SpannerConnectorOptions.GENERATED_COLUMNS.key(), generated);
        putMap(options, SpannerConnectorOptions.PROTO_TYPE_NAMES.key(), protos);
        putMap(options, SpannerConnectorOptions.ENUM_TYPE_NAMES.key(), enums);
        return new Converted(schema.build(), options);
    }

    private static DataType scalar(SpannerTypeSpelling.Kind kind) {
        switch (kind) {
            case BOOL:
                return DataTypes.BOOLEAN();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT32:
                return DataTypes.FLOAT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case NUMERIC:
                // GoogleSQL NUMERIC is exactly this; PostgreSQL numeric has no fixed precision,
                // and a value outside it fails the read rather than rounding (docs/adr/0135).
                return DataTypes.DECIMAL(38, 9);
            case STRING:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case DATE:
                return DataTypes.DATE();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP_LTZ(9);
            default:
                throw new IllegalStateException("Not a scalar kind: " + kind);
        }
    }

    private static UnsupportedColumnException unsupported(ColumnMetadata column, String reason) {
        return new UnsupportedColumnException(
                "Column '"
                        + column.name()
                        + "' has Spanner type "
                        + column.spannerType()
                        + ", which has no Flink type the connector reads or writes: "
                        + reason
                        + ".");
    }

    private static void putList(Map<String, String> options, String key, List<String> names) {
        if (!names.isEmpty()) {
            options.put(key, SpannerMarkerValues.list(names));
        }
    }

    private static void putMap(
            Map<String, String> options, String key, Map<String, String> entries) {
        if (!entries.isEmpty()) {
            options.put(key, SpannerMarkerValues.map(entries));
        }
    }
}
