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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Derives a BigQuery {@link TableSchema} from an Avro {@link Schema}.
 *
 * <p>This is the inverse of {@link
 * io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.TableSchemaToAvroConverter}, which
 * maps the same table schema back to the Avro form staged files are written with, extended to the
 * Avro constructs that converter never emits (enums, fixed, maps, uuid, millisecond-precision
 * temporal types).
 *
 * <p>Type mapping:
 *
 * <ul>
 *   <li>{@code string} → {@code STRING} ({@code uuid} included), {@code enum} → {@code STRING} (the
 *       symbol name), {@code bytes}/{@code fixed} → {@code BYTES}
 *   <li>{@code int}/{@code long} → {@code INT64}, {@code float}/{@code double} → {@code DOUBLE},
 *       {@code boolean} → {@code BOOL}
 *   <li>{@code date} → {@code DATE}; {@code time-millis}/{@code time-micros} → {@code TIME}; {@code
 *       timestamp-millis}/{@code timestamp-micros} → {@code TIMESTAMP}; {@code
 *       local-timestamp-millis}/{@code local-timestamp-micros} → {@code DATETIME}
 *   <li>{@code decimal(p, s)} → {@code NUMERIC} when it fits ({@code s ≤ 9}, {@code p - s ≤ 29}),
 *       else {@code BIGNUMERIC}; the precision and scale are carried onto the field so a FILE_LOADS
 *       round trip through {@code TableSchemaToAvroConverter} keeps them
 *   <li>{@code record} → {@code STRUCT}, recursively; {@code map<string, V>} → {@code REPEATED
 *       STRUCT<key, value>}, matching the shape proto maps already get
 *   <li>{@code string} fields selected by {@link AvroSchemaOptions#isJsonField} → {@code JSON} (the
 *       value is taken to be JSON text already and is not validated)
 *   <li>modes: arrays and maps → {@code REPEATED}; everything else → {@code NULLABLE}, unless
 *       {@link AvroSchemaOptions#isDeriveRequiredColumns} is set, and then a field that is not a
 *       {@code ["null", T]} union → {@code REQUIRED}. A {@code JSON} column stays {@code NULLABLE}
 *       either way, as does the synthesized map {@code key} column unless the option is set
 * </ul>
 *
 * <p>Rejected as configuration errors, because writing something plausible instead would be worse
 * than failing at job start: unions with more than one non-null branch (BigQuery has no union
 * type), a bare {@code null} field, arrays of nullable elements and arrays of arrays or maps
 * (BigQuery {@code REPEATED} fields hold no NULLs and do not nest), recursive record types, sibling
 * fields whose names differ only by case (the Storage API lowercases descriptor field names), a
 * decimal wider than {@code BIGNUMERIC}, and the logical types BigQuery cannot store without
 * silently losing information ({@code timestamp-nanos}, {@code local-timestamp-nanos}, {@code
 * duration}, {@code big-decimal}, and {@code uuid} on a {@code fixed}).
 */
@Internal
public final class AvroToTableSchemaConverter {

    /**
     * BigQuery constrains a parameterized decimal by its <em>integer</em> digits, not by its total
     * precision: {@code NUMERIC(P, S)} requires {@code S <= 9} and {@code P - S <= 29}, {@code
     * BIGNUMERIC(P, S)} requires {@code S <= 38} and {@code P - S <= 38}. Testing {@code P} against
     * the type maximum instead would derive, say, {@code NUMERIC(35, 2)} — which BigQuery rejects
     * at table creation, and whose large values the Storage Write API rejects row by row.
     */
    private static final int NUMERIC_MAX_SCALE = 9;

    private static final int NUMERIC_MAX_INTEGER_DIGITS = 29;

    private static final int BIGNUMERIC_MAX_SCALE = 38;

    private static final int BIGNUMERIC_MAX_INTEGER_DIGITS = 38;

    private AvroToTableSchemaConverter() {}

    /**
     * Converts the given Avro schema to a BigQuery table schema.
     *
     * @param avroSchema the root Avro record schema
     * @param options schema mapping options
     * @return the derived table schema
     */
    public static TableSchema convert(Schema avroSchema, AvroSchemaOptions options) {
        Preconditions.checkNotNull(avroSchema, "avroSchema must not be null");
        Preconditions.checkNotNull(options, "options must not be null");
        Preconditions.checkArgument(
                avroSchema.getType() == Schema.Type.RECORD,
                "The root Avro schema must be a record, but was %s",
                avroSchema.getType());

        Set<String> ancestors = new HashSet<>();
        ancestors.add(avroSchema.getFullName());
        Set<String> matchedJsonPaths = new HashSet<>();
        TableSchema.Builder builder = TableSchema.newBuilder();
        checkCaseCollisions(avroSchema, "");
        for (Schema.Field field : avroSchema.getFields()) {
            builder.addFields(convertField(field, "", options, ancestors, matchedJsonPaths));
        }

        Set<String> unmatched = new HashSet<>(options.getJsonFieldPaths());
        unmatched.removeAll(matchedJsonPaths);
        Preconditions.checkArgument(
                unmatched.isEmpty(),
                "JSON field paths matching no field of %s: %s",
                avroSchema.getFullName(),
                unmatched);
        return builder.build();
    }

    private static TableFieldSchema convertField(
            Schema.Field field,
            String parentPath,
            AvroSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        String path = parentPath.isEmpty() ? field.name() : parentPath + "." + field.name();
        return convertValue(
                field.name(), field.schema(), path, options, ancestors, matchedJsonPaths);
    }

    private static TableFieldSchema convertValue(
            String name,
            Schema schema,
            String path,
            AvroSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        boolean nullable = false;
        Schema base = schema;
        if (schema.getType() == Schema.Type.UNION) {
            nullable = hasNullBranch(schema);
            base = nonNullBranch(schema, path);
        }

        TableFieldSchema.Builder builder = TableFieldSchema.newBuilder().setName(name);
        switch (base.getType()) {
            case ARRAY:
                // A collection is REPEATED whether or not the union around it admitted null: a
                // BigQuery REPEATED field is simply empty, there being no NULL array.
                builder.setMode(TableFieldSchema.Mode.REPEATED);
                applyType(
                        builder, elementOf(base, path), path, options, ancestors, matchedJsonPaths);
                break;
            case MAP:
                // Checked here rather than left to the unmatched-path rejection: a map field does
                // reach applyType, only under path + ".value", so a path naming the map itself
                // would otherwise be reported as matching no field at all.
                Preconditions.checkArgument(
                        !options.isJsonField(path),
                        "JSON mapping requires a (possibly repeated or nullable) string field, but"
                                + " %s is a map",
                        path);
                builder.setMode(TableFieldSchema.Mode.REPEATED);
                applyMapEntry(builder, base, path, options, ancestors, matchedJsonPaths);
                break;
            default:
                builder.setMode(modeOf(nullable, options, options.isJsonField(path)));
                applyType(builder, base, path, options, ancestors, matchedJsonPaths);
                break;
        }
        return builder.build();
    }

    /**
     * Returns the mode of a non-collection column: {@code NULLABLE} unless {@link
     * AvroSchemaOptions.Builder#deriveRequiredColumns()} is set, and then {@code REQUIRED} for a
     * field the Avro schema does not admit null for.
     *
     * <p>Shared by the field path and the map-key path so the two cannot drift. Collections do not
     * come here at all — a BigQuery {@code REPEATED} column cannot be {@code NULLABLE}, so {@code
     * ARRAY} and {@code MAP} are {@code REPEATED} whatever the option says.
     *
     * <p>A singular {@code JSON} column is never {@code REQUIRED}, matching {@link
     * io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoToTableSchemaConverter
     * ProtoToTableSchemaConverter} — the two options share a name, so they must not diverge on
     * which columns they constrain. That side needs the rule to avoid poisoning every record which
     * omits the field; here it is a plainer matter of not making a column mandatory that BigQuery
     * cannot relax afterwards, when an empty string is a row-level error in a {@code JSON} column
     * either way.
     *
     * @param nullable whether the Avro schema admits null for this field
     * @param options the schema mapping options
     * @param jsonColumn whether this column is mapped to {@code JSON}
     * @return the BigQuery mode
     */
    private static TableFieldSchema.Mode modeOf(
            boolean nullable, AvroSchemaOptions options, boolean jsonColumn) {
        if (!options.isDeriveRequiredColumns() || nullable || jsonColumn) {
            return TableFieldSchema.Mode.NULLABLE;
        }
        return TableFieldSchema.Mode.REQUIRED;
    }

    /** Returns the element type of an array, rejecting the shapes BigQuery cannot repeat. */
    private static Schema elementOf(Schema array, String path) {
        Schema element = array.getElementType();
        if (element.getType() == Schema.Type.UNION) {
            Preconditions.checkArgument(
                    !hasNullBranch(element),
                    "Array %s has nullable elements, which a BigQuery REPEATED field cannot hold",
                    path);
            element = nonNullBranch(element, path);
        }
        Preconditions.checkArgument(
                element.getType() != Schema.Type.ARRAY && element.getType() != Schema.Type.MAP,
                "Array %s has %s elements; BigQuery REPEATED fields do not nest",
                path,
                element.getType());
        return element;
    }

    /**
     * Maps an Avro map to the {@code STRUCT<key, value>} BigQuery repeats — the column layout a
     * proto map already gets from {@link
     * io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoToTableSchemaConverter
     * ProtoToTableSchemaConverter}, so both serializers produce one table shape for the same
     * logical data. Avro map keys are always strings and an entry always has one, so the key column
     * is where {@link AvroSchemaOptions#isDeriveRequiredColumns} has the most obvious effect:
     * {@code REQUIRED} under it, {@code NULLABLE} by default like every other column. The proto
     * path converges here — a proto3 map entry's key has no presence either.
     */
    private static void applyMapEntry(
            TableFieldSchema.Builder builder,
            Schema map,
            String path,
            AvroSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        builder.setType(TableFieldSchema.Type.STRUCT)
                .addFields(
                        TableFieldSchema.newBuilder()
                                .setName("key")
                                .setType(TableFieldSchema.Type.STRING)
                                // An Avro map key is never a union and never a JSON column, so it
                                // is never nullable of its own accord — the option alone decides.
                                .setMode(modeOf(false, options, false))
                                .build())
                .addFields(
                        convertValue(
                                "value",
                                map.getValueType(),
                                path + ".value",
                                options,
                                ancestors,
                                matchedJsonPaths));
    }

    private static void applyType(
            TableFieldSchema.Builder builder,
            Schema schema,
            String path,
            AvroSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        if (options.isJsonField(path)) {
            Preconditions.checkArgument(
                    schema.getType() == Schema.Type.STRING,
                    "JSON mapping requires a (possibly repeated or nullable) string field, but %s is"
                            + " %s",
                    path,
                    schema.getType());
            matchedJsonPaths.add(path);
            builder.setType(TableFieldSchema.Type.JSON);
            return;
        }

        LogicalType logicalType = schema.getLogicalType();
        checkSupportedLogicalType(logicalType, schema, path);
        switch (schema.getType()) {
            case STRING:
            case ENUM:
                builder.setType(TableFieldSchema.Type.STRING);
                break;
            case BYTES:
            case FIXED:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    applyDecimal(builder, (LogicalTypes.Decimal) logicalType, path);
                } else {
                    builder.setType(TableFieldSchema.Type.BYTES);
                }
                break;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    builder.setType(TableFieldSchema.Type.DATE);
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    builder.setType(TableFieldSchema.Type.TIME);
                } else {
                    builder.setType(TableFieldSchema.Type.INT64);
                }
                break;
            case LONG:
                builder.setType(longType(logicalType));
                break;
            case FLOAT:
            case DOUBLE:
                builder.setType(TableFieldSchema.Type.DOUBLE);
                break;
            case BOOLEAN:
                builder.setType(TableFieldSchema.Type.BOOL);
                break;
            case RECORD:
                applyRecord(builder, schema, path, options, ancestors, matchedJsonPaths);
                break;
            default:
                throw new IllegalArgumentException(
                        "Avro type "
                                + schema.getType()
                                + " of field "
                                + path
                                + " has no BigQuery equivalent");
        }
    }

    private static TableFieldSchema.Type longType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.TimestampMillis
                || logicalType instanceof LogicalTypes.TimestampMicros) {
            return TableFieldSchema.Type.TIMESTAMP;
        }
        if (logicalType instanceof LogicalTypes.TimeMicros) {
            return TableFieldSchema.Type.TIME;
        }
        if (logicalType instanceof LogicalTypes.LocalTimestampMillis
                || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
            return TableFieldSchema.Type.DATETIME;
        }
        return TableFieldSchema.Type.INT64;
    }

    private static void applyDecimal(
            TableFieldSchema.Builder builder, LogicalTypes.Decimal decimal, String path) {
        int precision = decimal.getPrecision();
        int scale = decimal.getScale();
        int integerDigits = precision - scale;
        if (scale <= NUMERIC_MAX_SCALE && integerDigits <= NUMERIC_MAX_INTEGER_DIGITS) {
            builder.setType(TableFieldSchema.Type.NUMERIC);
        } else {
            Preconditions.checkArgument(
                    scale <= BIGNUMERIC_MAX_SCALE && integerDigits <= BIGNUMERIC_MAX_INTEGER_DIGITS,
                    "decimal(%s, %s) of field %s exceeds BigQuery BIGNUMERIC, which holds at most"
                            + " %s integer digits and at most %s fractional ones",
                    precision,
                    scale,
                    path,
                    BIGNUMERIC_MAX_INTEGER_DIGITS,
                    BIGNUMERIC_MAX_SCALE);
            builder.setType(TableFieldSchema.Type.BIGNUMERIC);
        }
        // Carried so that a FILE_LOADS round trip through TableSchemaToAvroConverter, which reads
        // them back, stages values at the scale they were declared with.
        builder.setPrecision(precision).setScale(scale);
    }

    private static void applyRecord(
            TableFieldSchema.Builder builder,
            Schema record,
            String path,
            AvroSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        Preconditions.checkArgument(
                !ancestors.contains(record.getFullName()),
                "Recursive record types are not supported by BigQuery: %s (field %s)",
                record.getFullName(),
                path);
        ancestors.add(record.getFullName());
        builder.setType(TableFieldSchema.Type.STRUCT);
        checkCaseCollisions(record, path);
        for (Schema.Field field : record.getFields()) {
            builder.addFields(convertField(field, path, options, ancestors, matchedJsonPaths));
        }
        ancestors.remove(record.getFullName());
    }

    /**
     * Rejects the logical types BigQuery cannot store faithfully. Falling back to the base type
     * would silently drop nanosecond precision or write an Avro-internal encoding into a BYTES
     * column, neither of which a user would find until they read the data back.
     */
    private static void checkSupportedLogicalType(
            LogicalType logicalType, Schema schema, String path) {
        if (logicalType == null) {
            return;
        }
        Preconditions.checkArgument(
                !(logicalType instanceof LogicalTypes.TimestampNanos
                        || logicalType instanceof LogicalTypes.LocalTimestampNanos),
                "Logical type %s of field %s cannot be stored by BigQuery, whose TIMESTAMP and"
                        + " DATETIME are microsecond-precision; use a microsecond logical type",
                logicalType.getName(),
                path);
        Preconditions.checkArgument(
                !(logicalType instanceof LogicalTypes.Duration),
                "Logical type %s of field %s has no BigQuery equivalent the Storage Write API can"
                        + " carry",
                logicalType.getName(),
                path);
        Preconditions.checkArgument(
                !(logicalType instanceof LogicalTypes.BigDecimal),
                "Logical type %s of field %s carries no precision or scale; declare the field as"
                        + " decimal(precision, scale) instead",
                logicalType.getName(),
                path);
        Preconditions.checkArgument(
                !(logicalType instanceof LogicalTypes.Uuid
                        && schema.getType() == Schema.Type.FIXED),
                "Logical type %s of field %s is only supported on a string field",
                logicalType.getName(),
                path);
    }

    private static boolean hasNullBranch(Schema union) {
        for (Schema branch : union.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the single non-null branch of a union. BigQuery has no union type, so {@code ["null",
     * T]} (in either order) and the degenerate {@code [T]} are the only forms that map.
     *
     * <p>Shared with {@link AvroRowConverter} rather than copied: a laxer second version there —
     * taking the first non-null branch — would only be safe because this one already rejected the
     * multi-branch case, and nothing would state that coupling.
     */
    static Schema nonNullBranch(Schema union, String path) {
        Schema found = null;
        for (Schema branch : union.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                continue;
            }
            Preconditions.checkArgument(
                    found == null,
                    "Union %s of field %s has more than one non-null branch, which BigQuery cannot"
                            + " represent",
                    union,
                    path);
            found = branch;
        }
        Preconditions.checkArgument(
                found != null, "Union %s of field %s has no non-null branch", union, path);
        return found;
    }

    private static void checkCaseCollisions(Schema record, String path) {
        Set<String> seen = new HashSet<>();
        List<Schema.Field> fields = record.getFields();
        for (Schema.Field field : fields) {
            Preconditions.checkArgument(
                    seen.add(field.name().toLowerCase(Locale.ROOT)),
                    "Fields of %s differ only by case (%s), which the BigQuery Storage API cannot"
                            + " distinguish (at %s)",
                    record.getFullName(),
                    field.name(),
                    path.isEmpty() ? "<root>" : path);
        }
    }
}
