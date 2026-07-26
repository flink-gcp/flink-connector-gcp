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
import com.google.protobuf.Timestamp;

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
 *   <li>{@code google.protobuf.Timestamp} → {@code TIMESTAMP} (microsecond precision)
 *   <li>message → {@code STRUCT}, recursively; map fields → {@code REPEATED STRUCT<key, value>}
 *   <li>message and string fields selected by {@link ProtoSchemaOptions#isJsonField} → {@code JSON}
 *       (a message is not expanded into a {@code STRUCT}; a string is taken to be JSON text
 *       already)
 * </ul>
 *
 * <p>Column modes: repeated fields (including maps) are {@code REPEATED}, and everything else is
 * {@code NULLABLE} — unless {@link ProtoSchemaOptions.Builder#deriveRequiredFromSchema()} is set,
 * which derives {@code REQUIRED} for a field that {@linkplain
 * Descriptors.FieldDescriptor#isRequired is declared required} or that {@linkplain
 * Descriptors.FieldDescriptor#hasPresence has no presence}. Note that a plain proto3 scalar has no
 * presence either way: unset values materialize as protobuf defaults (0, empty string, first enum
 * value), never as NULL.
 *
 * <p>Recursive message types are rejected (BigQuery schemas cannot represent them), as are sibling
 * fields whose names differ only by case (the Storage API lowercases descriptor field names).
 * Configured JSON field paths that match no field are rejected; a configured JSON field option
 * number that matches no field is not, since a message need not have JSON columns.
 */
@Internal
public final class ProtoToTableSchemaConverter {

    private ProtoToTableSchemaConverter() {}

    /**
     * Returns whether the given message field is a {@code google.protobuf.Timestamp}, mapped to a
     * BigQuery {@code TIMESTAMP} column. Shared with the row conversion planner so schema and value
     * conversion cannot disagree.
     *
     * @param field a message-typed field
     * @return whether the field maps to TIMESTAMP
     */
    static boolean isTimestampMessage(Descriptors.FieldDescriptor field) {
        return Timestamp.getDescriptor().getFullName().equals(field.getMessageType().getFullName());
    }

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
        boolean jsonColumn = options.isJsonField(field, path);
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

        switch (field.getJavaType()) {
            case INT:
            case LONG:
                builder.setType(TableFieldSchema.Type.INT64);
                break;
            case FLOAT:
            case DOUBLE:
                builder.setType(TableFieldSchema.Type.DOUBLE);
                break;
            case BOOLEAN:
                builder.setType(TableFieldSchema.Type.BOOL);
                break;
            case STRING:
            case ENUM:
                builder.setType(TableFieldSchema.Type.STRING);
                break;
            case BYTE_STRING:
                builder.setType(TableFieldSchema.Type.BYTES);
                break;
            case MESSAGE:
                convertMessageField(field, path, options, ancestors, matchedJsonPaths, builder);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type " + field.getType() + " for field " + path);
        }
        return builder.build();
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
     */
    private static TableFieldSchema.Mode modeOf(
            Descriptors.FieldDescriptor field, ProtoSchemaOptions options, boolean jsonColumn) {
        if (field.isRepeated()) {
            return TableFieldSchema.Mode.REPEATED;
        }
        if (!options.isDeriveRequiredFromSchema() || jsonColumn) {
            return TableFieldSchema.Mode.NULLABLE;
        }
        // isRequired() as well as presence: a proto2 required field has presence and is mandatory
        // all the same, so testing presence alone would map the one unambiguous case to NULLABLE.
        return field.isRequired() || !field.hasPresence()
                ? TableFieldSchema.Mode.REQUIRED
                : TableFieldSchema.Mode.NULLABLE;
    }

    private static void convertMessageField(
            Descriptors.FieldDescriptor field,
            String path,
            ProtoSchemaOptions options,
            Set<String> ancestors,
            Set<String> matchedJsonPaths,
            TableFieldSchema.Builder builder) {
        Descriptors.Descriptor messageType = field.getMessageType();
        if (isTimestampMessage(field)) {
            builder.setType(TableFieldSchema.Type.TIMESTAMP);
            return;
        }
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
