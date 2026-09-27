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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Maps a BigQuery REST table schema to the Flink schema the {@code bigquery} connector reads and
 * writes it as — the inverse of the connector page's type mapping.
 *
 * <p>Each BigQuery type has exactly one Flink answer, the widest the connector accepts for it:
 * {@code INT64} is {@code BIGINT} although a {@code TINYINT} declaration writes it too. A type with
 * no Flink type the connector can carry fails the whole table, naming the column by its dotted
 * path; a column is never dropped, because a schema missing one reads as a table that lacks it.
 *
 * <p>{@code TIME} is {@code TIME(3)}, the finest precision the source converts. Flink 1.20 and 2.2
 * plan it as {@code TIME(0)} while the connector still receives {@code TIME(3)}; see the connector
 * page's {@code TIME} note.
 */
@Internal
final class BigQuerySchemaToFlinkConverter {

    /** Flink's {@code DECIMAL} precision ceiling. */
    static final int MAX_DECIMAL_PRECISION = 38;

    /** The precision BigQuery's {@code TIME} is read at; see the class comment. */
    static final int TIME_PRECISION = 3;

    /** The precision BigQuery's {@code DATETIME} and {@code TIMESTAMP} carry: microseconds. */
    static final int TIMESTAMP_PRECISION = 6;

    /** An unparameterized {@code NUMERIC}'s precision. */
    private static final int NUMERIC_PRECISION = 38;

    /** An unparameterized {@code NUMERIC}'s scale. */
    private static final int NUMERIC_SCALE = 9;

    private BigQuerySchemaToFlinkConverter() {}

    /**
     * Builds the Flink schema of a BigQuery table.
     *
     * @param fields the table's top-level fields
     * @param primaryKey the table's primary-key columns, empty for none; each becomes {@code NOT
     *     NULL}, as a Flink primary key requires and as a {@code PRIMARY KEY} in Flink DDL implies
     * @return the schema
     * @throws UnsupportedColumnException if a column has no Flink type the connector carries, or a
     *     primary-key column is not a top-level column
     */
    static Schema toSchema(FieldList fields, List<String> primaryKey) {
        // BigQuery column names are case-insensitive, so a key column is matched ignoring case and
        // named as the schema spells it.
        Map<String, String> keyColumns = new LinkedHashMap<>();
        for (String column : primaryKey) {
            keyColumns.put(column.toLowerCase(Locale.ROOT), column);
        }
        List<String> keyNames = new ArrayList<>();
        Schema.Builder schema = Schema.newBuilder();
        for (Field field : fields) {
            DataType type = toDataType(field, field.getName());
            if (keyColumns.remove(field.getName().toLowerCase(Locale.ROOT)) != null) {
                type = type.notNull();
                keyNames.add(field.getName());
            }
            schema.column(field.getName(), type);
            if (field.getDescription() != null) {
                schema.withComment(field.getDescription());
            }
        }
        if (!keyColumns.isEmpty()) {
            throw new UnsupportedColumnException(
                    "Primary-key columns "
                            + sorted(keyColumns.values())
                            + " are not top-level columns.");
        }
        if (!keyNames.isEmpty()) {
            // In the key's declared order, which is the constraint's, not the schema's.
            List<String> ordered = new ArrayList<>();
            for (String column : primaryKey) {
                for (String name : keyNames) {
                    if (name.equalsIgnoreCase(column)) {
                        ordered.add(name);
                    }
                }
            }
            schema.primaryKey(ordered);
        }
        return schema.build();
    }

    /**
     * Maps one field, applying its mode.
     *
     * <p>{@code REPEATED} becomes an {@code ARRAY} of {@code NOT NULL} elements, since BigQuery
     * stores no null element and the sink rejects a nullable element declaration. The array itself
     * stays nullable: BigQuery reads a missing repeated value back as empty, and a nullable
     * declaration is what a hand-written {@code CREATE TABLE} would carry.
     */
    static DataType toDataType(Field field, String path) {
        DataType type = baseType(field, path);
        Field.Mode mode = field.getMode() == null ? Field.Mode.NULLABLE : field.getMode();
        switch (mode) {
            case REPEATED:
                return DataTypes.ARRAY(type.notNull());
            case REQUIRED:
                return type.notNull();
            default:
                return type;
        }
    }

    private static DataType baseType(Field field, String path) {
        StandardSQLTypeName type = field.getType().getStandardType();
        if (type == null) {
            // The client keeps a type name it does not know — FOREIGN, or one BigQuery adds later
            // — without a standard equivalent.
            throw unsupported(path, field.getType().name(), "the connector maps no such type");
        }
        switch (type) {
            case BOOL:
                return DataTypes.BOOLEAN();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT64:
                return DataTypes.DOUBLE();
            case NUMERIC:
                return field.getPrecision() == null
                        ? DataTypes.DECIMAL(NUMERIC_PRECISION, NUMERIC_SCALE)
                        : decimal(field, path);
            case BIGNUMERIC:
                if (field.getPrecision() == null) {
                    throw unsupported(
                            path,
                            "BIGNUMERIC",
                            "without a declared precision it holds up to 76 digits, and Flink"
                                    + " DECIMAL at most "
                                    + MAX_DECIMAL_PRECISION);
                }
                return decimal(field, path);
            case STRING:
            case JSON:
            case GEOGRAPHY:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME(TIME_PRECISION);
            case DATETIME:
                return DataTypes.TIMESTAMP(TIMESTAMP_PRECISION);
            case TIMESTAMP:
                Long precision = field.getTimestampPrecision();
                if (precision != null && precision > TIMESTAMP_PRECISION) {
                    throw unsupported(
                            path,
                            "TIMESTAMP(" + precision + ")",
                            "the connector reads timestamps to microseconds");
                }
                return DataTypes.TIMESTAMP_LTZ(TIMESTAMP_PRECISION);
            case STRUCT:
                return row(field.getSubFields(), path);
            case RANGE:
                return range(field, path);
            case INTERVAL:
                throw unsupported(
                        path,
                        "INTERVAL",
                        "Flink splits intervals into year-month and day-time types, and neither"
                                + " holds BigQuery's months, days and microseconds together");
            default:
                throw unsupported(path, type.name(), "the connector maps no such type");
        }
    }

    /** A parameterized decimal; a precision without a scale has scale 0, as BigQuery defines. */
    private static DataType decimal(Field field, String path) {
        long precision = field.getPrecision();
        Long scale = field.getScale();
        if (precision > MAX_DECIMAL_PRECISION) {
            throw unsupported(
                    path,
                    field.getType().getStandardType().name() + "(" + precision + ")",
                    "Flink DECIMAL holds at most " + MAX_DECIMAL_PRECISION + " digits");
        }
        return DataTypes.DECIMAL((int) precision, scale == null ? 0 : scale.intValue());
    }

    private static DataType row(@Nullable FieldList subFields, String path) {
        List<DataTypes.Field> fields = new ArrayList<>();
        if (subFields != null) {
            for (Field subField : subFields) {
                DataType type = toDataType(subField, path + "." + subField.getName());
                fields.add(
                        subField.getDescription() == null
                                ? DataTypes.FIELD(subField.getName(), type)
                                : DataTypes.FIELD(
                                        subField.getName(), type, subField.getDescription()));
            }
        }
        return DataTypes.ROW(fields);
    }

    /** {@code RANGE<T>}, as the Storage Read API returns it: a record of nullable endpoints. */
    private static DataType range(Field field, String path) {
        String elementType =
                field.getRangeElementType() == null ? null : field.getRangeElementType().getType();
        DataType endpoint;
        if ("DATE".equals(elementType)) {
            endpoint = DataTypes.DATE();
        } else if ("DATETIME".equals(elementType)) {
            endpoint = DataTypes.TIMESTAMP(TIMESTAMP_PRECISION);
        } else if ("TIMESTAMP".equals(elementType)) {
            endpoint = DataTypes.TIMESTAMP_LTZ(TIMESTAMP_PRECISION);
        } else {
            throw unsupported(
                    path,
                    "RANGE<" + elementType + ">",
                    "the connector reads RANGE of DATE, DATETIME or TIMESTAMP only");
        }
        return DataTypes.ROW(DataTypes.FIELD("start", endpoint), DataTypes.FIELD("end", endpoint));
    }

    private static UnsupportedColumnException unsupported(
            String path, String bigQueryType, String reason) {
        return new UnsupportedColumnException(
                "Column '"
                        + path
                        + "' has BigQuery type "
                        + bigQueryType
                        + ", which has no Flink type the connector reads or writes: "
                        + reason
                        + ".");
    }

    private static List<String> sorted(Collection<String> names) {
        List<String> list = new ArrayList<>(names);
        list.sort(null);
        return list;
    }

    /** A column the catalog cannot give a Flink type, or a primary key it cannot place. */
    static final class UnsupportedColumnException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        UnsupportedColumnException(String message) {
            super(message);
        }
    }
}
