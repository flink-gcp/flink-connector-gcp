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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Rewrites protobuf messages into {@link DynamicMessage}s conforming to a BigQuery-storage
 * compatible target descriptor (as produced by {@code BQTableSchemaToProtoDescriptor} from the
 * schema derived by {@link ProtoSchemaConverter}).
 *
 * <p>Value conversions mirror the schema mapping: {@code google.protobuf.Timestamp} fields are
 * flattened to epoch microseconds, enum values become their names, JSON-mapped message fields are
 * printed as canonical protobuf JSON, nested and repeated fields (including maps) are converted
 * recursively.
 *
 * <p>Instances hold non-serializable descriptors and must be re-created after deserialization (see
 * {@link ProtoMessageSerializer}).
 */
@Internal
public final class ProtoRowConverter {

    private static final String TIMESTAMP_FULL_NAME = "google.protobuf.Timestamp";

    private final Descriptors.Descriptor targetDescriptor;
    private final ProtoSchemaOptions options;
    private final JsonFormat.Printer jsonPrinter =
            JsonFormat.printer().omittingInsignificantWhitespace();

    /**
     * Creates a converter towards the given target descriptor.
     *
     * @param targetDescriptor the BigQuery-storage compatible row descriptor
     * @param options the schema mapping options used to derive the target descriptor
     */
    public ProtoRowConverter(Descriptors.Descriptor targetDescriptor, ProtoSchemaOptions options) {
        this.targetDescriptor =
                Preconditions.checkNotNull(targetDescriptor, "targetDescriptor must not be null");
        this.options = Preconditions.checkNotNull(options, "options must not be null");
    }

    /**
     * Converts a source message into a row message of the target descriptor.
     *
     * @param source the source protobuf message
     * @return the converted row
     */
    public DynamicMessage convert(MessageOrBuilder source) {
        return convertMessage(source, targetDescriptor, "");
    }

    private DynamicMessage convertMessage(
            MessageOrBuilder source, Descriptors.Descriptor target, String parentPath) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(target);
        Map<String, Descriptors.FieldDescriptor> sourceFields = fieldsByLowerName(source);
        for (Descriptors.FieldDescriptor targetField : target.getFields()) {
            Descriptors.FieldDescriptor sourceField =
                    sourceFields.get(targetField.getName().toLowerCase(Locale.ROOT));
            if (sourceField == null) {
                continue;
            }
            String path =
                    parentPath.isEmpty()
                            ? sourceField.getName()
                            : parentPath + "." + sourceField.getName();
            if (sourceField.isRepeated()) {
                int count = source.getRepeatedFieldCount(sourceField);
                for (int i = 0; i < count; i++) {
                    builder.addRepeatedField(
                            targetField,
                            convertValue(
                                    source.getRepeatedField(sourceField, i),
                                    sourceField,
                                    targetField,
                                    path));
                }
            } else {
                if (sourceField.hasPresence() && !source.hasField(sourceField)) {
                    continue;
                }
                builder.setField(
                        targetField,
                        convertValue(source.getField(sourceField), sourceField, targetField, path));
            }
        }
        return builder.build();
    }

    private Object convertValue(
            Object value,
            Descriptors.FieldDescriptor sourceField,
            Descriptors.FieldDescriptor targetField,
            String path) {
        if (options.getJsonFieldPaths().contains(path)) {
            return printJson((MessageOrBuilder) value, path);
        }
        switch (sourceField.getJavaType()) {
            case INT:
                return ((Integer) value).longValue();
            case FLOAT:
                return ((Float) value).doubleValue();
            case LONG:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
                return value;
            case ENUM:
                return ((Descriptors.EnumValueDescriptor) value).getName();
            case MESSAGE:
                MessageOrBuilder message = (MessageOrBuilder) value;
                if (TIMESTAMP_FULL_NAME.equals(message.getDescriptorForType().getFullName())) {
                    return toEpochMicros(message);
                }
                Preconditions.checkArgument(
                        targetField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE,
                        "Target field for %s is not a struct",
                        path);
                return convertMessage(message, targetField.getMessageType(), path);
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type "
                                + sourceField.getType()
                                + " for field "
                                + path);
        }
    }

    private String printJson(MessageOrBuilder message, String path) {
        try {
            return jsonPrinter.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to serialize field " + path + " to JSON", e);
        }
    }

    private static long toEpochMicros(MessageOrBuilder timestamp) {
        Descriptors.Descriptor descriptor = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(descriptor.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(descriptor.findFieldByName("nanos"));
        return seconds * 1_000_000L + nanos / 1_000L;
    }

    private static Map<String, Descriptors.FieldDescriptor> fieldsByLowerName(
            MessageOrBuilder message) {
        Map<String, Descriptors.FieldDescriptor> byName = new HashMap<>();
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
            byName.put(field.getName().toLowerCase(Locale.ROOT), field);
        }
        return byName;
    }
}
