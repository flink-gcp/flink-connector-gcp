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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Derives the protobuf row descriptor a BigQuery table schema is written through. */
@Internal
public final class RowDescriptors {

    private RowDescriptors() {}

    /**
     * Returns the BigQuery-storage compatible row descriptor of the given table schema.
     *
     * <p>The descriptor does not depend on the JVM's default locale. The client library lower-cases
     * each column name with that locale, and a Turkish one lower-cases the column {@code userId} to
     * {@code userıd}; that is not a protobuf-compatible name, so the library names the field with a
     * placeholder whose {@code column_name} is {@code userıd}, which BigQuery rejects as a field
     * the table does not have. The names therefore reach the library already lower-cased under
     * {@link Locale#ROOT}, which no locale changes again. The fields are then the ones the library
     * derives under an English locale; only a nested message type's name, which the library takes
     * from the column name as handed over, is lower-case too.
     *
     * <p>A schema in which two {@code STRUCT} or {@code RANGE} columns would get one protobuf
     * message type name with different fields is refused with an {@link IllegalStateException}
     * naming both columns. The library names a nested type after its column's path joined with
     * {@code __}, so the column {@code a__b} and the column {@code b} inside {@code a} both get
     * {@code root__a__b}. Two types with one name make the library resolve a field to either of
     * them, so the descriptor itself can give a column the other's fields, and the writer schema
     * the Storage Write API receives keeps only one of them. Types whose fields are identical, as
     * the library makes them for columns with identical field lists, are harmless and accepted.
     *
     * <p>Everything the client library refuses to map is still reported as it reports it: a {@code
     * RANGE} column with no element type, for instance, is an {@link IllegalArgumentException}
     * thrown from the call below and passed through unwrapped. The checked {@link
     * com.google.protobuf.Descriptors.DescriptorValidationException DescriptorValidationException}
     * is what remains: a schema <em>handed to</em> this connector reaches it, since the generated
     * descriptor's field names are lower-cased and two columns differing only in case collide
     * there, while a schema this connector derives is checked for exactly that first — each schema
     * converter rejects such names under {@link Locale#ROOT}, the same lower-casing applied here.
     * It is wrapped rather than declared because a schema is not a record: each serializer derives
     * once in its constructor, where this fails on the client, and a checked exception would oblige
     * the paths that derive again on a task manager to treat a configuration error as a bad row.
     * Neither failure depends on the default locale, so a schema the client accepts is accepted on
     * every task manager too.
     *
     * @param tableSchema the destination table schema
     * @param schemaDescription how the schema is named in the failure message
     * @return the row descriptor
     */
    public static Descriptors.Descriptor derive(TableSchema tableSchema, String schemaDescription) {
        Descriptors.Descriptor descriptor;
        try {
            descriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            lowerCased(tableSchema));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive a BigQuery-storage compatible descriptor for "
                            + schemaDescription,
                    e);
        }
        checkMessageTypeNames(descriptor, tableSchema, schemaDescription);
        return descriptor;
    }

    /**
     * Refuses two message types with one name and different fields, among every type the library
     * built: the files the root type depends on, transitively, not only the types its fields
     * resolve to, since a field resolves to only one of two types sharing a name.
     */
    private static void checkMessageTypeNames(
            Descriptors.Descriptor descriptor, TableSchema tableSchema, String schemaDescription) {
        Map<String, DescriptorProtos.DescriptorProto> layouts = new HashMap<>();
        Set<Descriptors.FileDescriptor> visited =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Descriptors.FileDescriptor> files = new ArrayDeque<>();
        files.push(descriptor.getFile());
        while (!files.isEmpty()) {
            Descriptors.FileDescriptor file = files.pop();
            if (!visited.add(file)) {
                continue;
            }
            for (Descriptors.Descriptor type : file.getMessageTypes()) {
                DescriptorProtos.DescriptorProto layout = type.toProto();
                DescriptorProtos.DescriptorProto other =
                        layouts.putIfAbsent(type.getFullName(), layout);
                if (other != null && !other.equals(layout)) {
                    Map<String, List<String>> paths = new HashMap<>();
                    typePaths(tableSchema.getFieldsList(), descriptor.getName(), "", paths);
                    throw new IllegalStateException(
                            String.format(
                                    "Failed to derive a BigQuery-storage compatible descriptor"
                                            + " for %s: the columns %s would share the protobuf"
                                            + " message type %s with different fields; rename"
                                            + " one of them",
                                    schemaDescription,
                                    String.join(
                                            " and ",
                                            paths.getOrDefault(
                                                    type.getFullName(),
                                                    Collections.singletonList("<unknown>"))),
                                    type.getFullName()));
                }
            }
            files.addAll(file.getDependencies());
        }
    }

    /**
     * Collects the paths of the {@code STRUCT} and {@code RANGE} columns by the message type name
     * the library gives them: the parent type's name, {@code __}, and the column's field name.
     */
    private static void typePaths(
            List<TableFieldSchema> columns,
            String parentType,
            String parentPath,
            Map<String, List<String>> paths) {
        for (TableFieldSchema column : columns) {
            if (column.getType() != TableFieldSchema.Type.STRUCT
                    && column.getType() != TableFieldSchema.Type.RANGE) {
                continue;
            }
            String type = parentType + "__" + fieldName(column.getName().toLowerCase(Locale.ROOT));
            String path =
                    parentPath.isEmpty() ? column.getName() : parentPath + "." + column.getName();
            paths.computeIfAbsent(type, unused -> new ArrayList<>()).add(path);
            typePaths(column.getFieldsList(), type, path, paths);
        }
    }

    /**
     * Returns the name the client library gives the field for a lower-cased name, and resolves a
     * lower-cased JSON key by: the name itself, or its {@code col_} placeholder where the name is
     * not protobuf-compatible.
     *
     * @param lowerCaseName a lower-cased column name or JSON key
     * @return the protobuf field name
     */
    public static String fieldName(String lowerCaseName) {
        return BigQuerySchemaUtil.isProtoCompatible(lowerCaseName)
                ? lowerCaseName
                : BigQuerySchemaUtil.generatePlaceholderFieldName(lowerCaseName);
    }

    /**
     * Returns the table schema with every column name lower-cased under {@link Locale#ROOT}, at
     * every depth, and nothing else changed.
     *
     * <p>{@link #derive(TableSchema, String)} hands the client library this form. A caller that
     * hands the library the schema beside that descriptor passes this form too, because the library
     * checks the two against each other by lower-casing the schema's names with the default locale.
     *
     * @param tableSchema the table schema
     * @return the schema with lower-cased column names
     */
    public static TableSchema lowerCased(TableSchema tableSchema) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        for (TableFieldSchema field : tableSchema.getFieldsList()) {
            builder.addFields(lowerCased(field));
        }
        return builder.build();
    }

    private static TableFieldSchema lowerCased(TableFieldSchema field) {
        TableFieldSchema.Builder builder =
                field.toBuilder().setName(field.getName().toLowerCase(Locale.ROOT)).clearFields();
        for (TableFieldSchema subField : field.getFieldsList()) {
            builder.addFields(lowerCased(subField));
        }
        return builder.build();
    }
}
