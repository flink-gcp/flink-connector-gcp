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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;

import java.util.HashSet;
import java.util.Set;

/**
 * Derives a BigQuery {@link TableSchema} from a protobuf {@link Descriptors.Descriptor}.
 *
 * <p>Type mapping:
 *
 * <ul>
 *   <li>proto integer types (int32/int64/uint32/sint/fixed and uint64) → {@code INT64}. Note that
 *       uint64 values above {@code Long.MAX_VALUE} are not representable.
 *   <li>float/double → {@code DOUBLE}, bool → {@code BOOL}, string → {@code STRING}, bytes → {@code
 *       BYTES}
 *   <li>enum → {@code STRING} (the enum value name)
 *   <li>{@code google.protobuf.Timestamp} → {@code TIMESTAMP} (microsecond precision)
 *   <li>message → {@code STRUCT}, recursively; map fields → {@code REPEATED STRUCT<key, value>}
 *   <li>fields configured in {@link ProtoSchemaOptions#getJsonFieldPaths()} → {@code JSON}
 *   <li>repeated fields → {@code REPEATED} mode, everything else → {@code NULLABLE}
 * </ul>
 *
 * <p>Recursive message types are rejected: BigQuery schemas cannot represent them.
 */
@Internal
public final class ProtoSchemaConverter {

    private static final String TIMESTAMP_FULL_NAME = "google.protobuf.Timestamp";

    private ProtoSchemaConverter() {}

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
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            builder.addFields(convertField(field, "", options, ancestors));
        }
        return builder.build();
    }

    private static TableFieldSchema convertField(
            Descriptors.FieldDescriptor field,
            String parentPath,
            ProtoSchemaOptions options,
            Set<String> ancestors) {
        String path = parentPath.isEmpty() ? field.getName() : parentPath + "." + field.getName();
        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder()
                        .setName(field.getName())
                        .setMode(
                                field.isRepeated()
                                        ? TableFieldSchema.Mode.REPEATED
                                        : TableFieldSchema.Mode.NULLABLE);

        if (options.getJsonFieldPaths().contains(path)) {
            Preconditions.checkArgument(
                    field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                            && !field.isMapField(),
                    "JSON mapping requires a (possibly repeated) message field: %s",
                    path);
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
                convertMessageField(field, path, options, ancestors, builder);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type " + field.getType() + " for field " + path);
        }
        return builder.build();
    }

    private static void convertMessageField(
            Descriptors.FieldDescriptor field,
            String path,
            ProtoSchemaOptions options,
            Set<String> ancestors,
            TableFieldSchema.Builder builder) {
        Descriptors.Descriptor messageType = field.getMessageType();
        if (TIMESTAMP_FULL_NAME.equals(messageType.getFullName())) {
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
        for (Descriptors.FieldDescriptor sub : messageType.getFields()) {
            builder.addFields(convertField(sub, path, options, ancestors));
        }
        ancestors.remove(messageType.getFullName());
    }
}
