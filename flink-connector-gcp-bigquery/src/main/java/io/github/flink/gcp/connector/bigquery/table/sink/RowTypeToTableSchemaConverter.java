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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Derives a BigQuery table schema from the {@code RowType} of a SQL table's physical columns.
 *
 * <p>Every mapping decision here is {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroToTableSchemaConverter
 * AvroToTableSchemaConverter}'s, restated over Flink's type system rather than re-derived: the
 * {@code NUMERIC}/{@code BIGNUMERIC} split on <em>integer</em> digits, {@code MAP} to a repeated
 * {@code STRUCT<key, value>}, case-collision rejection, and {@code NULLABLE} as the default mode
 * with {@code sink.derive-required-columns} as the only way to a {@code REQUIRED} column. The
 * protobuf mapping is normative for every serializer in this module, and this is one more front end
 * onto it.
 *
 * <p>Two rows deviate from what a reader might expect, both deliberately:
 *
 * <ul>
 *   <li>{@code TIMESTAMP} is a wall-clock type and maps to BigQuery {@code DATETIME}; {@code
 *       TIMESTAMP_LTZ} is an instant and maps to {@code TIMESTAMP}. The Dataproc connector maps
 *       them the other way round, which stores a wall-clock value as an instant and an instant as a
 *       wall-clock value.
 *   <li>{@code TIME} is rejected above precision 3, not 6. BigQuery's {@code TIME} holds
 *       microseconds, but {@link org.apache.flink.table.data.RowData} carries a time of day as an
 *       {@code int} of <em>milliseconds</em>, so a column declared {@code TIME(6)} could only ever
 *       be filled to millisecond precision — and a schema claiming more than the values can carry
 *       is worse than a rejection.
 * </ul>
 */
@Internal
public final class RowTypeToTableSchemaConverter {

    /** BigQuery {@code NUMERIC} holds at most this many digits after the point. */
    private static final int NUMERIC_MAX_SCALE = 9;

    /** BigQuery {@code NUMERIC} holds at most this many digits before it. */
    private static final int NUMERIC_MAX_INTEGER_DIGITS = 29;

    /** BigQuery {@code BIGNUMERIC} holds at most this many digits after the point. */
    private static final int BIGNUMERIC_MAX_SCALE = 38;

    /** BigQuery {@code BIGNUMERIC} holds at most this many digits before it. */
    private static final int BIGNUMERIC_MAX_INTEGER_DIGITS = 38;

    /** The precision a {@code RowData} time of day can carry, being milliseconds of the day. */
    private static final int MAX_TIME_PRECISION = 3;

    /** The precision BigQuery's {@code TIMESTAMP} and {@code DATETIME} carry. */
    private static final int MAX_TIMESTAMP_PRECISION = 6;

    private RowTypeToTableSchemaConverter() {}

    /**
     * Converts the given row type to a BigQuery table schema.
     *
     * @param rowType the physical columns of the SQL table
     * @param options the schema mapping options
     * @return the derived table schema
     * @throws IllegalArgumentException if a column has no BigQuery equivalent, or if a marked path
     *     matches no column
     */
    public static TableSchema convert(RowType rowType, RowDataSchemaOptions options) {
        Preconditions.checkNotNull(rowType, "rowType must not be null");
        Preconditions.checkNotNull(options, "options must not be null");
        Preconditions.checkArgument(
                rowType.getFieldCount() > 0,
                "The table has no columns, which BigQuery cannot represent");

        Set<String> matchedMarkedPaths = new HashSet<>();
        TableSchema.Builder builder = TableSchema.newBuilder();
        checkCaseCollisions(rowType, "");
        for (RowType.RowField field : rowType.getFields()) {
            builder.addFields(convertField(field, "", options, matchedMarkedPaths));
        }

        Set<String> unmatched = new HashSet<>(options.getJsonFieldPaths());
        unmatched.addAll(options.getGeographyFieldPaths());
        unmatched.removeAll(matchedMarkedPaths);
        Preconditions.checkArgument(
                unmatched.isEmpty(),
                "JSON or GEOGRAPHY field paths matching no column of the table: %s",
                unmatched);
        return builder.build();
    }

    private static TableFieldSchema convertField(
            RowType.RowField field,
            String parentPath,
            RowDataSchemaOptions options,
            Set<String> matchedMarkedPaths) {
        String path = parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
        return convertValue(field.getName(), field.getType(), path, options, matchedMarkedPaths);
    }

    private static TableFieldSchema convertValue(
            String name,
            LogicalType type,
            String path,
            RowDataSchemaOptions options,
            Set<String> matchedMarkedPaths) {
        TableFieldSchema.Builder builder = TableFieldSchema.newBuilder().setName(name);
        // Resolved once here and handed to whichever branch runs, so the both-markers rejection
        // inside it fires exactly once per column — the shape both sibling converters use.
        TableFieldSchema.Type marked = markedType(options, path, matchedMarkedPaths);

        switch (type.getTypeRoot()) {
            case ARRAY:
                // REPEATED whether or not the array column is nullable: BigQuery has no NULL array,
                // so a null one lands as an empty one — the same loss the Avro path documents.
                builder.setMode(TableFieldSchema.Mode.REPEATED);
                applyType(
                        builder,
                        elementOf((ArrayType) type, path),
                        path,
                        marked,
                        options,
                        matchedMarkedPaths);
                break;
            case MAP:
            case MULTISET:
                // Checked here rather than left to the unmatched-path rejection: a map's value does
                // reach applyType, only under path + ".value", so a path naming the map itself
                // would otherwise be reported as matching no column at all.
                Preconditions.checkArgument(
                        marked == null,
                        "%s mapping requires a (possibly repeated or nullable) string column, but"
                                + " %s is a map",
                        marked,
                        path);
                builder.setMode(TableFieldSchema.Mode.REPEATED);
                applyMapEntry(builder, type, path, options, matchedMarkedPaths);
                break;
            default:
                builder.setMode(modeOf(type.isNullable(), options, marked != null));
                applyType(builder, type, path, marked, options, matchedMarkedPaths);
                break;
        }
        return builder.build();
    }

    /**
     * Returns the mode of a non-collection column: {@code NULLABLE} unless {@code
     * sink.derive-required-columns} is set, and then {@code REQUIRED} for a column declared {@code
     * NOT NULL}.
     *
     * <p>The polarity is the repository-wide one (#124/#145) and is not a per-format choice: {@code
     * REQUIRED} is the mode BigQuery cannot walk back, so it is opted into rather than inferred
     * from a {@code NOT NULL} that a user may have written for the planner's benefit. A marked
     * column is never {@code REQUIRED}, matching both sibling converters.
     */
    private static TableFieldSchema.Mode modeOf(
            boolean nullable, RowDataSchemaOptions options, boolean markedColumn) {
        if (!options.isDeriveRequiredColumns() || nullable || markedColumn) {
            return TableFieldSchema.Mode.NULLABLE;
        }
        return TableFieldSchema.Mode.REQUIRED;
    }

    /** Returns the element type of an array, rejecting the shapes BigQuery cannot repeat. */
    private static LogicalType elementOf(ArrayType array, String path) {
        LogicalType element = array.getElementType();
        Preconditions.checkArgument(
                !element.isNullable(),
                "Array %s has nullable elements, which a BigQuery REPEATED column cannot hold."
                        + " Declare the element type NOT NULL",
                path);
        Preconditions.checkArgument(
                element.getTypeRoot() != LogicalTypeRoot.ARRAY
                        && element.getTypeRoot() != LogicalTypeRoot.MAP
                        && element.getTypeRoot() != LogicalTypeRoot.MULTISET,
                "Array %s has %s elements; BigQuery REPEATED columns do not nest",
                path,
                element.getTypeRoot());
        return element;
    }

    /**
     * Maps a {@code MAP} or {@code MULTISET} to the {@code STRUCT<key, value>} BigQuery repeats —
     * the column layout the protobuf and Avro paths already produce, so one logical shape reaches
     * one table shape whatever wrote it. A {@code MULTISET<T>} is a {@code MAP<T, INT>} in {@code
     * RowData}, and is mapped as one.
     */
    private static void applyMapEntry(
            TableFieldSchema.Builder builder,
            LogicalType type,
            String path,
            RowDataSchemaOptions options,
            Set<String> matchedMarkedPaths) {
        LogicalType keyType;
        LogicalType valueType;
        if (type.getTypeRoot() == LogicalTypeRoot.MULTISET) {
            keyType = ((MultisetType) type).getElementType();
            valueType = new IntType(false);
        } else {
            keyType = ((MapType) type).getKeyType();
            valueType = ((MapType) type).getValueType();
        }
        TableFieldSchema.Builder key = TableFieldSchema.newBuilder().setName("key");
        key.setMode(modeOf(false, options, false));
        applyType(key, keyType, path + ".key", null, options, matchedMarkedPaths);

        TableFieldSchema.Builder value = TableFieldSchema.newBuilder().setName("value");
        value.setMode(modeOf(valueType.isNullable(), options, false));
        applyType(
                value,
                valueType,
                path + ".value",
                markedType(options, path + ".value", matchedMarkedPaths),
                options,
                matchedMarkedPaths);

        builder.setType(TableFieldSchema.Type.STRUCT)
                .addFields(key.build())
                .addFields(value.build());
    }

    private static void applyType(
            TableFieldSchema.Builder builder,
            LogicalType type,
            String path,
            TableFieldSchema.Type marked,
            RowDataSchemaOptions options,
            Set<String> matchedMarkedPaths) {
        if (marked != null) {
            Preconditions.checkArgument(
                    isMarkable(type, marked),
                    "%s mapping requires a (possibly repeated or nullable) %s column, but %s is %s",
                    marked,
                    marked == TableFieldSchema.Type.JSON ? "string or row" : "string",
                    path,
                    type);
            // A marked column is a string on the wire whichever it is, so the recursion stops
            // here — a marked ROW is rendered as JSON text rather than expanded into a STRUCT.
            builder.setType(marked);
            return;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                builder.setType(TableFieldSchema.Type.STRING);
                break;
            case BOOLEAN:
                builder.setType(TableFieldSchema.Type.BOOL);
                break;
            case BINARY:
            case VARBINARY:
                builder.setType(TableFieldSchema.Type.BYTES);
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                builder.setType(TableFieldSchema.Type.INT64);
                break;
            case FLOAT:
            case DOUBLE:
                builder.setType(TableFieldSchema.Type.DOUBLE);
                break;
            case DECIMAL:
                applyDecimal(builder, (DecimalType) type, path);
                break;
            case DATE:
                builder.setType(TableFieldSchema.Type.DATE);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                Preconditions.checkArgument(
                        ((TimeType) type).getPrecision() <= MAX_TIME_PRECISION,
                        "Column %s is %s, but a row carries a time of day as milliseconds, so"
                                + " nothing past TIME(%s) could ever be written",
                        path,
                        type,
                        MAX_TIME_PRECISION);
                builder.setType(TableFieldSchema.Type.TIME);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                checkTimestampPrecision(((TimestampType) type).getPrecision(), type, path);
                // A wall-clock value becomes a civil one: DATETIME, not TIMESTAMP.
                builder.setType(TableFieldSchema.Type.DATETIME);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                checkTimestampPrecision(
                        ((LocalZonedTimestampType) type).getPrecision(), type, path);
                // An instant stays an instant.
                builder.setType(TableFieldSchema.Type.TIMESTAMP);
                break;
            case ROW:
                builder.setType(TableFieldSchema.Type.STRUCT);
                RowType nested = (RowType) type;
                checkCaseCollisions(nested, path);
                for (RowType.RowField field : nested.getFields()) {
                    builder.addFields(convertField(field, path, options, matchedMarkedPaths));
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Column " + path + " is " + type + ", which has no BigQuery equivalent");
        }
    }

    private static void checkTimestampPrecision(int precision, LogicalType type, String path) {
        Preconditions.checkArgument(
                precision <= MAX_TIMESTAMP_PRECISION,
                "Column %s is %s, but BigQuery stores microseconds, so nothing past precision %s"
                        + " could be kept",
                path,
                type,
                MAX_TIMESTAMP_PRECISION);
    }

    private static void applyDecimal(
            TableFieldSchema.Builder builder, DecimalType type, String path) {
        int precision = type.getPrecision();
        int scale = type.getScale();
        int integerDigits = precision - scale;
        if (scale <= NUMERIC_MAX_SCALE && integerDigits <= NUMERIC_MAX_INTEGER_DIGITS) {
            builder.setType(TableFieldSchema.Type.NUMERIC);
        } else {
            Preconditions.checkArgument(
                    scale <= BIGNUMERIC_MAX_SCALE && integerDigits <= BIGNUMERIC_MAX_INTEGER_DIGITS,
                    "DECIMAL(%s, %s) of column %s exceeds BigQuery BIGNUMERIC, which holds at most"
                            + " %s integer digits and at most %s fractional ones",
                    precision,
                    scale,
                    path,
                    BIGNUMERIC_MAX_INTEGER_DIGITS,
                    BIGNUMERIC_MAX_SCALE);
            builder.setType(TableFieldSchema.Type.BIGNUMERIC);
        }
        // Carried so the FILE_LOADS staging converter can read the declared scale back.
        builder.setPrecision(precision).setScale(scale);
    }

    /**
     * Returns the marking of the given path, recording that it matched. A column claimed by both
     * markers is rejected here, the one place both are visible.
     */
    private static TableFieldSchema.Type markedType(
            RowDataSchemaOptions options, String path, Set<String> matchedMarkedPaths) {
        boolean json = options.getJsonFieldPaths().contains(path);
        boolean geography = options.getGeographyFieldPaths().contains(path);
        Preconditions.checkArgument(
                !(json && geography),
                "Column %s is marked as both a JSON and a GEOGRAPHY column",
                path);
        if (!json && !geography) {
            return null;
        }
        matchedMarkedPaths.add(path);
        return json ? TableFieldSchema.Type.JSON : TableFieldSchema.Type.GEOGRAPHY;
    }

    /**
     * A JSON column may be a string or a row — a row is rendered as a JSON object — while a
     * geography column must be a string, since no structured value means a geometry to BigQuery.
     * The same narrowing the protobuf marker makes.
     */
    private static boolean isMarkable(LogicalType type, TableFieldSchema.Type marked) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return true;
            case ROW:
                return marked == TableFieldSchema.Type.JSON;
            default:
                return false;
        }
    }

    /**
     * Rejects columns of one row that differ only by case: {@code BQTableSchemaToProtoDescriptor}
     * lowercases every column name, so BigQuery could not tell them apart.
     */
    private static void checkCaseCollisions(RowType rowType, String path) {
        Set<String> seen = new HashSet<>();
        for (RowType.RowField field : rowType.getFields()) {
            Preconditions.checkArgument(
                    seen.add(field.getName().toLowerCase(Locale.ROOT)),
                    "Columns of %s differ only by case (%s), which the BigQuery Storage API cannot"
                            + " distinguish",
                    path.isEmpty() ? "the table" : path,
                    field.getName());
        }
    }
}
