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

import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites protobuf messages into {@link DynamicMessage}s conforming to a BigQuery-storage
 * compatible target descriptor (as produced by {@code BQTableSchemaToProtoDescriptor} from the
 * schema derived by {@link ProtoToTableSchemaConverter}).
 *
 * <p>All descriptor-dependent decisions — source/target field pairing (via {@link
 * BigQuerySchemaUtil#getFieldName}, which tracks the library's field-naming contract), value
 * conversion kinds, JSON selection, timestamp sub-field descriptors — are resolved once into a
 * conversion plan at construction time; per-record conversion is a flat loop over the plan with no
 * lookups, string building or name comparisons.
 *
 * <p>Value conversions mirror the schema mapping: {@code google.protobuf.Timestamp} fields are
 * flattened to validated epoch microseconds ({@link Timestamps#toMicros}), uint32/fixed32 are
 * widened unsigned, uint64/fixed64 values above {@code Long.MAX_VALUE} are rejected, enum values
 * become their names, JSON-mapped message fields are printed as canonical protobuf JSON while
 * JSON-mapped string fields are written through verbatim (a {@code JSON} column is carried as a
 * string, and the value is taken to be JSON text already — the connector does not validate it;
 * BigQuery rejects malformed JSON as a row-level error, with the sole exception of the empty string
 * on a field without presence, which is left unset rather than written), and nested/repeated fields
 * (including maps) are converted recursively.
 *
 * <p>Instances hold non-serializable descriptors and must be re-created after deserialization (see
 * {@link ProtoMessageSerializer}).
 */
@Internal
public final class ProtoRowConverter {

    private static final JsonFormat.Printer JSON_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();

    private final MessagePlan plan;

    /**
     * Creates a converter from the given source descriptor towards the given target descriptor.
     *
     * @param sourceDescriptor the descriptor of the source messages
     * @param targetDescriptor the BigQuery-storage compatible row descriptor
     * @param options the schema mapping options used to derive the target descriptor
     */
    public ProtoRowConverter(
            Descriptors.Descriptor sourceDescriptor,
            Descriptors.Descriptor targetDescriptor,
            ProtoSchemaOptions options) {
        Preconditions.checkNotNull(sourceDescriptor, "sourceDescriptor must not be null");
        Preconditions.checkNotNull(targetDescriptor, "targetDescriptor must not be null");
        Preconditions.checkNotNull(options, "options must not be null");
        this.plan = buildMessagePlan(sourceDescriptor, targetDescriptor, options, "");
    }

    /**
     * Converts a source message into a row message of the target descriptor.
     *
     * @param source the source protobuf message
     * @return the converted row
     * @throws IOException if a JSON-mapped field cannot be printed
     */
    public DynamicMessage convert(MessageOrBuilder source) throws IOException {
        return plan.convert(source);
    }

    /** How a source value is converted to its target representation. */
    private enum Kind {
        IDENTITY,
        INT_TO_LONG,
        UINT32_TO_LONG,
        UINT64_CHECKED,
        FLOAT_TO_DOUBLE,
        ENUM_NAME,
        TIMESTAMP_MICROS,
        JSON,
        JSON_STRING,
        STRUCT
    }

    private static MessagePlan buildMessagePlan(
            Descriptors.Descriptor source,
            Descriptors.Descriptor target,
            ProtoSchemaOptions options,
            String parentPath) {
        List<FieldPlan> fieldPlans = new ArrayList<>(target.getFields().size());
        for (Descriptors.FieldDescriptor targetField : target.getFields()) {
            String sourceName = BigQuerySchemaUtil.getFieldName(targetField);
            Descriptors.FieldDescriptor sourceField = source.findFieldByName(sourceName);
            Preconditions.checkState(
                    sourceField != null,
                    "No source field named %s in %s for target field %s",
                    sourceName,
                    source.getFullName(),
                    targetField.getName());
            String path =
                    parentPath.isEmpty()
                            ? sourceField.getName()
                            : parentPath + "." + sourceField.getName();
            fieldPlans.add(buildFieldPlan(sourceField, targetField, options, path));
        }
        return new MessagePlan(target, fieldPlans.toArray(new FieldPlan[0]));
    }

    private static FieldPlan buildFieldPlan(
            Descriptors.FieldDescriptor sourceField,
            Descriptors.FieldDescriptor targetField,
            ProtoSchemaOptions options,
            String path) {
        // A JSON-mapped string already holds JSON text and its target field is a proto string, so
        // it needs no value conversion — only the empty-value handling that JSON_STRING carries.
        // Which fields may be JSON-mapped at all is validated once, in ProtoToTableSchemaConverter,
        // which always runs first to produce the target descriptor.
        if (options.isJsonField(sourceField, path)) {
            Kind kind =
                    sourceField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                            ? Kind.JSON
                            : Kind.JSON_STRING;
            return new FieldPlan(sourceField, targetField, kind, path, null, null, null);
        }
        switch (sourceField.getJavaType()) {
            case INT:
                return new FieldPlan(
                        sourceField,
                        targetField,
                        isUnsigned(sourceField) ? Kind.UINT32_TO_LONG : Kind.INT_TO_LONG,
                        path,
                        null,
                        null,
                        null);
            case LONG:
                return new FieldPlan(
                        sourceField,
                        targetField,
                        isUnsigned(sourceField) ? Kind.UINT64_CHECKED : Kind.IDENTITY,
                        path,
                        null,
                        null,
                        null);
            case FLOAT:
                return new FieldPlan(
                        sourceField, targetField, Kind.FLOAT_TO_DOUBLE, path, null, null, null);
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
                return new FieldPlan(
                        sourceField, targetField, Kind.IDENTITY, path, null, null, null);
            case ENUM:
                return new FieldPlan(
                        sourceField, targetField, Kind.ENUM_NAME, path, null, null, null);
            case MESSAGE:
                if (ProtoToTableSchemaConverter.isTimestampMessage(sourceField)) {
                    Descriptors.Descriptor timestampType = sourceField.getMessageType();
                    return new FieldPlan(
                            sourceField,
                            targetField,
                            Kind.TIMESTAMP_MICROS,
                            path,
                            null,
                            timestampType.findFieldByName("seconds"),
                            timestampType.findFieldByName("nanos"));
                }
                return new FieldPlan(
                        sourceField,
                        targetField,
                        Kind.STRUCT,
                        path,
                        buildMessagePlan(
                                sourceField.getMessageType(),
                                targetField.getMessageType(),
                                options,
                                path),
                        null,
                        null);
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type "
                                + sourceField.getType()
                                + " for field "
                                + path);
        }
    }

    private static boolean isUnsigned(Descriptors.FieldDescriptor field) {
        switch (field.getType()) {
            case UINT32:
            case FIXED32:
            case UINT64:
            case FIXED64:
                return true;
            default:
                return false;
        }
    }

    private static final class MessagePlan {
        private final Descriptors.Descriptor target;
        private final FieldPlan[] fields;

        MessagePlan(Descriptors.Descriptor target, FieldPlan[] fields) {
            this.target = target;
            this.fields = fields;
        }

        DynamicMessage convert(MessageOrBuilder source) throws IOException {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(target);
            for (FieldPlan field : fields) {
                if (field.sourceField.isRepeated()) {
                    int count = source.getRepeatedFieldCount(field.sourceField);
                    for (int i = 0; i < count; i++) {
                        builder.addRepeatedField(
                                field.targetField,
                                field.convertValue(source.getRepeatedField(field.sourceField, i)));
                    }
                } else {
                    if (field.sourceField.hasPresence() && !source.hasField(field.sourceField)) {
                        continue;
                    }
                    Object value = source.getField(field.sourceField);
                    if (field.omitsEmptyJsonString(value)) {
                        continue;
                    }
                    builder.setField(field.targetField, field.convertValue(value));
                }
            }
            return builder.build();
        }
    }

    private static final class FieldPlan {
        private final Descriptors.FieldDescriptor sourceField;
        private final Descriptors.FieldDescriptor targetField;
        private final Kind kind;
        private final String path;
        private final MessagePlan nested;
        private final Descriptors.FieldDescriptor timestampSeconds;
        private final Descriptors.FieldDescriptor timestampNanos;

        FieldPlan(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                Kind kind,
                String path,
                MessagePlan nested,
                Descriptors.FieldDescriptor timestampSeconds,
                Descriptors.FieldDescriptor timestampNanos) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.kind = kind;
            this.path = path;
            this.nested = nested;
            this.timestampSeconds = timestampSeconds;
            this.timestampNanos = timestampNanos;
        }

        /**
         * Returns whether this singular value must be left unset rather than written.
         *
         * <p>A plain proto3 scalar has no presence, so an unset JSON-mapped string arrives here as
         * {@code ""} — and the BigQuery row descriptor's JSON field <em>does</em> have presence, so
         * writing it would put an explicit empty string in the column. The empty string is not
         * valid JSON, so that could only ever come back as a row-level error, which for a field
         * most records legitimately leave unset would mean failing on most records. Leaving the
         * column unset (NULL) is the only outcome that can succeed.
         *
         * <p>Deliberately limited to fields without presence: where the source can say "unset", an
         * explicit {@code ""} is the user's own statement and is passed through unchanged. Repeated
         * elements are explicit for the same reason.
         */
        boolean omitsEmptyJsonString(Object value) {
            return kind == Kind.JSON_STRING
                    && !sourceField.hasPresence()
                    && ((String) value).isEmpty();
        }

        Object convertValue(Object value) throws IOException {
            switch (kind) {
                case IDENTITY:
                case JSON_STRING:
                    return value;
                case INT_TO_LONG:
                    return ((Integer) value).longValue();
                case UINT32_TO_LONG:
                    return Integer.toUnsignedLong((Integer) value);
                case UINT64_CHECKED:
                    long unsigned = (Long) value;
                    if (unsigned < 0) {
                        throw new IllegalArgumentException(
                                "uint64 value "
                                        + Long.toUnsignedString(unsigned)
                                        + " of field "
                                        + path
                                        + " exceeds Long.MAX_VALUE and cannot be represented as"
                                        + " INT64");
                    }
                    return unsigned;
                case FLOAT_TO_DOUBLE:
                    return ((Float) value).doubleValue();
                case ENUM_NAME:
                    return ((Descriptors.EnumValueDescriptor) value).getName();
                case TIMESTAMP_MICROS:
                    return toEpochMicros(value);
                case JSON:
                    return JSON_PRINTER.print((MessageOrBuilder) value);
                case STRUCT:
                    return nested.convert((MessageOrBuilder) value);
                default:
                    throw new IllegalStateException("Unknown conversion kind: " + kind);
            }
        }

        private long toEpochMicros(Object value) {
            if (value instanceof Timestamp) {
                return Timestamps.toMicros((Timestamp) value);
            }
            MessageOrBuilder message = (MessageOrBuilder) value;
            return Timestamps.toMicros(
                    Timestamp.newBuilder()
                            .setSeconds((Long) message.getField(timestampSeconds))
                            .setNanos((Integer) message.getField(timestampNanos))
                            .build());
        }
    }
}
