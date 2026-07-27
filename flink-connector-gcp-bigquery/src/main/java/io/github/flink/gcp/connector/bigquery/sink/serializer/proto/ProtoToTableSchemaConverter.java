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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Derives a BigQuery {@link TableSchema} from a protobuf {@link Descriptors.Descriptor}.
 *
 * <p>(Named distinctly from the client library's {@code
 * com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter}, which converts descriptors to Storage
 * API {@code ProtoSchema} messages.)
 *
 * <p>Type mapping:
 *
 * <ul>
 *   <li>proto integer types → {@code INT64}: int32/sint32/sfixed32/int64/sint64/sfixed64 map
 *       losslessly, uint32/fixed32 are converted unsigned; uint64/fixed64 values above {@code
 *       Long.MAX_VALUE} are not representable and are rejected at conversion time
 *   <li>float/double → {@code DOUBLE}, bool → {@code BOOL}, string → {@code STRING}, bytes → {@code
 *       BYTES}
 *   <li>enum → {@code STRING} (the enum value name; unknown enum numbers surface as protobuf's
 *       {@code UNKNOWN_ENUM_VALUE_*} placeholder names)
 *   <li>{@code google.protobuf.Timestamp} → {@code TIMESTAMP} (microsecond precision; anything
 *       finer is truncated)
 *   <li>{@code google.protobuf.Duration} → {@code INT64} microseconds (likewise truncated; a
 *       duration outside protobuf's valid range is a row-level failure)
 *   <li>{@code google.protobuf.FieldMask} → {@code STRING}, its paths joined by commas exactly as
 *       declared — not lowerCamelCased, as protobuf's canonical JSON form would
 *   <li>the nine {@code google.protobuf.*Value} wrappers → the wrapped scalar's type. A wrapper
 *       exists precisely so that "unset" is distinguishable from {@code 0} or {@code ""}, and that
 *       distinction reaches the column: being a message field it has presence, so it stays {@code
 *       NULLABLE} even when required columns are derived
 *   <li>{@code google.protobuf.Struct}, {@code Value} and {@code ListValue} → {@code JSON}, with no
 *       configuration. They carry arbitrary JSON and are mutually recursive, so this is the only
 *       shape a BigQuery schema can represent them in at all
 *   <li>{@code google.protobuf.Any} → {@code STRUCT<type_url, value>}, deliberately not unpacked:
 *       the payload cannot be expanded without the descriptor its type URL names
 *   <li>message → {@code STRUCT}, recursively; map fields → {@code REPEATED STRUCT<key, value>}
 *   <li>message and string fields selected by {@link ProtoSchemaOptions#isJsonField} → {@code JSON}
 *       (a message is not expanded into a {@code STRUCT}; a string is taken to be JSON text
 *       already). This selection wins over every well-known-type mapping above, so a marked {@code
 *       Timestamp} or wrapper field is printed as canonical protobuf JSON rather than flattened
 * </ul>
 *
 * <p>Column modes: repeated fields (including maps) are {@code REPEATED}, and everything else is
 * {@code NULLABLE} — unless {@link ProtoSchemaOptions.Builder#deriveRequiredColumns()} is set,
 * which derives {@code REQUIRED} for a field that {@linkplain
 * Descriptors.FieldDescriptor#isRequired is declared required} or that {@linkplain
 * Descriptors.FieldDescriptor#hasPresence has no presence}. Note that a plain proto3 scalar has no
 * presence either way: unset values materialize as protobuf defaults (0, empty string, first enum
 * value), never as NULL.
 *
 * <p>Recursive message types are rejected (BigQuery schemas cannot represent them), as are sibling
 * fields whose names differ only by case (the Storage API lowercases descriptor field names) and
 * messages with no fields at all, {@code google.protobuf.Empty} among them (a BigQuery {@code
 * STRUCT} must have at least one column). Configured JSON field paths that match no field are
 * rejected; a configured JSON field option number that matches no field is not, since a message
 * need not have JSON columns.
 */
@Internal
public final class ProtoToTableSchemaConverter {

    private ProtoToTableSchemaConverter() {}

    /**
     * Returns whether the given field may be mapped to a {@code JSON} column: a non-map message
     * field, whose canonical protobuf JSON is written, or a string field, whose value is already
     * JSON text and is written through verbatim. Map fields are excluded because a proto map has no
     * meaningful JSON column form here — its BigQuery shape is {@code REPEATED STRUCT<key, value>}.
     */
    private static boolean isJsonMappable(Descriptors.FieldDescriptor field) {
        switch (field.getJavaType()) {
            case MESSAGE:
                return !field.isMapField();
            case STRING:
                return true;
            default:
                return false;
        }
    }

    /**
     * Converts the given descriptor to a BigQuery table schema.
     *
     * @param descriptor the root message descriptor
     * @param options schema mapping options
     * @return the derived table schema
     */
    public static TableSchema convert(
            Descriptors.Descriptor descriptor, ProtoSchemaOptions options) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        Set<String> ancestors = new HashSet<>();
        ancestors.add(descriptor.getFullName());
        Set<String> matchedJsonPaths = new HashSet<>();
        checkCaseCollisions(descriptor, "");
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            builder.addFields(convertField(field, "", options, ancestors, matchedJsonPaths));
        }
        Set<String> unmatched = new HashSet<>(options.getJsonFieldPaths());
        unmatched.removeAll(matchedJsonPaths);
        Preconditions.checkArgument(
                unmatched.isEmpty(),
                "JSON field paths matching no field of %s: %s",
                descriptor.getFullName(),
                unmatched);
        return builder.build();
    }

    private static TableFieldSchema convertField(
            Descriptors.FieldDescriptor field,
            String parentPath,
            ProtoSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths) {
        String path = parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
        // Asked once and reused: this is the single JSON decision point, it walks the descriptor's
        // file-dependency graph for every configured option, and it decides the mode as well as
        // the type.
        //
        // Struct/Value/ListValue join it here, and that placement does three things with no second
        // branch: modeOf's "a singular JSON column is never REQUIRED" rule covers them as written;
        // the recursion guard is never reached, which is the whole point, since they are mutually
        // recursive; and an explicitly configured JSON marking keeps winning over every
        // well-known-type mapping, because this branch returns before the switch below ever asks
        // what the message type is.
        ProtoWellKnownType wellKnown = ProtoWellKnownType.of(field);
        boolean jsonColumn = options.isJsonField(field, path) || wellKnown.isJsonMapped();
        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder()
                        .setName(field.getName())
                        .setMode(modeOf(field, options, jsonColumn));

        if (jsonColumn) {
            Preconditions.checkArgument(
                    isJsonMappable(field),
                    "JSON mapping requires a (possibly repeated) message or string field, but %s is"
                            + " %s",
                    path,
                    field.isMapField() ? "a map field" : field.getJavaType().toString());
            matchedJsonPaths.add(path);
            return builder.setType(TableFieldSchema.Type.JSON).build();
        }

        // The well-known types that become one flat column are settled here rather than inside
        // message expansion, so the classification is consulted once and every branch that
        // consumes it sits within a few lines of it. Reaching expansion at all therefore means an
        // ordinary message: no case for JSON is possible below, because the branch above already
        // returned for it.
        switch (wellKnown) {
            case TIMESTAMP:
                return builder.setType(TableFieldSchema.Type.TIMESTAMP).build();
            case DURATION:
                return builder.setType(TableFieldSchema.Type.INT64).build();
            case FIELD_MASK:
                return builder.setType(TableFieldSchema.Type.STRING).build();
            case WRAPPER:
                // The wrapped scalar decides the column type, through the same function a bare
                // scalar of that type goes through, so the two cannot drift.
                return builder.setType(
                                scalarType(field.getMessageType().findFieldByName("value"), path))
                        .build();
            case NONE:
            default:
                break;
        }

        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            expandMessageField(field, path, options, ancestors, matchedJsonPaths, builder);
        } else {
            builder.setType(scalarType(field, path));
        }
        return builder.build();
    }

    /**
     * Returns the BigQuery type of a non-message field.
     *
     * <p>Shared with the wrapper mapping, which asks it about the wrapper's {@code value} sub-field
     * — so {@code Int64Value} and a bare {@code int64} cannot map to different column types.
     *
     * @param field a field that is not a message, or the {@code value} field of a wrapper
     * @param path the dotted path used in the error message
     * @return the BigQuery column type
     */
    private static TableFieldSchema.Type scalarType(
            Descriptors.FieldDescriptor field, String path) {
        switch (field.getJavaType()) {
            case INT:
            case LONG:
                return TableFieldSchema.Type.INT64;
            case FLOAT:
            case DOUBLE:
                return TableFieldSchema.Type.DOUBLE;
            case BOOLEAN:
                return TableFieldSchema.Type.BOOL;
            case STRING:
            case ENUM:
                return TableFieldSchema.Type.STRING;
            case BYTE_STRING:
                return TableFieldSchema.Type.BYTES;
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type " + field.getType() + " for field " + path);
        }
    }

    /**
     * Returns the BigQuery mode of the given field.
     *
     * <p>{@code repeated} is tested first and unconditionally, so a repeated JSON-mapped field
     * stays {@code REPEATED JSON} and a map stays {@code REPEATED STRUCT} — a BigQuery {@code
     * REPEATED} column cannot be {@code NULLABLE} anyway.
     *
     * <p>A singular {@code JSON} column is never {@code REQUIRED}. The rule is stated about JSON
     * rather than about presence because the alternative would poison whole streams: {@link
     * ProtoRowConverter} deliberately leaves a JSON-mapped string <em>without presence</em> unset
     * when it is empty (an empty string is not valid JSON), and "without presence" is exactly the
     * condition that would make the column {@code REQUIRED} — a required target field left unset
     * fails {@code build()} for every record that legitimately omits it. The broader rule also
     * covers a proto2 {@code required} JSON field, where {@code REQUIRED} would in fact be correct;
     * one clause is worth more than fidelity in a combination that exotic.
     *
     * <p>Well-known types need no clause here. A wrapper, a {@code Duration} and a {@code
     * FieldMask} are message fields, so they have presence and stay {@code NULLABLE} — which for a
     * wrapper is the whole point of the type. The one exception is deliberate: a proto2 {@code
     * required} wrapper derives {@code REQUIRED}, and it is mandatory, so that is faithful.
     */
    private static TableFieldSchema.Mode modeOf(
            Descriptors.FieldDescriptor field, ProtoSchemaOptions options, boolean jsonColumn) {
        if (field.isRepeated()) {
            return TableFieldSchema.Mode.REPEATED;
        }
        if (!options.isDeriveRequiredColumns() || jsonColumn) {
            return TableFieldSchema.Mode.NULLABLE;
        }
        // isRequired() as well as presence: a proto2 required field has presence and is mandatory
        // all the same, so testing presence alone would map the one unambiguous case to NULLABLE.
        return field.isRequired() || !field.hasPresence()
                ? TableFieldSchema.Mode.REQUIRED
                : TableFieldSchema.Mode.NULLABLE;
    }

    /**
     * Expands an ordinary message field into a {@code STRUCT}, recursively.
     *
     * <p>Only ever reached for a message that is <em>not</em> a recognised well-known type: {@link
     * #convertField} settles those before calling this, so none of the guards below can fire on
     * one. That ordering is deliberate — a well-known type is never expanded, so it can neither
     * recurse nor collide by case, and checking here instead would reject a message carrying two
     * {@code Timestamp}s on one path.
     */
    private static void expandMessageField(
            Descriptors.FieldDescriptor field,
            String path,
            ProtoSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths,
            TableFieldSchema.Builder builder) {
        Descriptors.Descriptor messageType = field.getMessageType();
        // A sibling of the recursion rejection below, and stated about columns rather than about
        // google.protobuf.Empty, because any zero-field message reaches it. Measured: the BigQuery
        // client library rejects such a column itself ("The RECORD field must have at least one
        // sub-field"), before a request is ever sent, so the table cannot be created whatever the
        // service would say — and that message names no field.
        Preconditions.checkArgument(
                !messageType.getFields().isEmpty(),
                "BigQuery cannot represent a STRUCT with no columns, but %s has no fields (field"
                        + " %s)",
                messageType.getFullName(),
                path);
        Preconditions.checkArgument(
                !ancestors.contains(messageType.getFullName()),
                "Recursive message types are not supported by BigQuery: %s (field %s)",
                messageType.getFullName(),
                path);
        ancestors.add(messageType.getFullName());
        builder.setType(TableFieldSchema.Type.STRUCT);
        checkCaseCollisions(messageType, path);
        for (Descriptors.FieldDescriptor sub : messageType.getFields()) {
            builder.addFields(convertField(sub, path, options, ancestors, matchedJsonPaths));
        }
        ancestors.remove(messageType.getFullName());
    }

    private static void checkCaseCollisions(Descriptors.Descriptor descriptor, String path) {
        Set<String> seen = new HashSet<>();
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            Preconditions.checkArgument(
                    seen.add(field.getName().toLowerCase(Locale.ROOT)),
                    "Fields of %s differ only by case (%s), which the BigQuery Storage API cannot"
                            + " distinguish (at %s)",
                    descriptor.getFullName(),
                    field.getName(),
                    path.isEmpty() ? "<root>" : path);
        }
    }
}
