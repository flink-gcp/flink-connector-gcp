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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FieldMask;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.FieldMaskUtil;
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
 * <p>All descriptor-dependent decisions — source/target field pairing, value conversion kinds, JSON
 * selection, well-known-type sub-field descriptors — are resolved once into a conversion plan at
 * construction time; per-record conversion is a flat loop over the plan with no lookups, string
 * building or name comparisons.
 *
 * <p>Fields are paired by position, never by name. The target descriptor holds one field per source
 * field, in the same order, but its field names are lower-cased: a field {@code hasNext} is the
 * target field {@code hasnext}. A name lookup would also have to repeat how they were lower-cased,
 * which {@code RowDescriptors.derive} does under {@code Locale.ROOT} while the client library alone
 * uses the default locale, giving the column name {@code userıd} for {@code userId} under {@code
 * tr_TR}; position holds either way. The column keeps the name as written, and BigQuery matches
 * column names case-insensitively.
 *
 * <p>Value conversions mirror the schema mapping: {@code google.protobuf.Timestamp} fields are
 * flattened to validated epoch microseconds ({@link Timestamps#toMicros}, an invalid timestamp
 * being a row-level failure) and {@code Duration} fields to microseconds ({@link
 * Durations#toMicros}, an invalid duration being a row-level failure), a {@code FieldMask} becomes
 * its comma-joined paths ({@link FieldMaskUtil#toString}), a {@code *Value} wrapper is unwrapped
 * and then converted exactly as the scalar it holds, {@code Struct}/{@code Value}/{@code ListValue}
 * are printed as canonical protobuf JSON like any other JSON-mapped message, uint32/fixed32 are
 * widened unsigned (or sign-extended under {@link
 * ProtoSchemaOptions#isUnsignedIntegersAsSigned()}), uint64/fixed64 keep their 64 bits as the
 * signed {@code long} protobuf-java returns (so a value at or above 2<sup>63</sup> is written as a
 * negative {@code INT64}; BigQuery has no unsigned 64-bit type), enum values become their names,
 * JSON-mapped message fields are printed as canonical protobuf JSON while marked string fields —
 * {@code JSON} and {@code GEOGRAPHY} alike — are written through verbatim (both columns are carried
 * as strings, and the value is taken to be JSON text or a geometry literal already; the connector
 * does not validate either, and BigQuery rejects a malformed one as a row-level error, with the
 * sole exception of the empty string on a field without presence, which is left unset rather than
 * written), and nested/repeated fields (including maps) are converted recursively.
 *
 * <p>A well-known type that {@link ProtoSchemaOptions#getWellKnownTypesAsMessages} lists takes the
 * ordinary message path instead: its sub-fields are converted as any message's are, so a {@code
 * Duration} is written as its raw {@code seconds} and {@code nanos} and a wrapper holding {@code 0}
 * as {@code {value: 0}}.
 *
 * <p>Instances hold non-serializable descriptors and must be re-created after deserialization (see
 * {@link ProtoMessageSerializationSchema}).
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
        FLOAT_TO_DOUBLE,
        ENUM_NAME,
        TIMESTAMP_MICROS,
        DURATION_MICROS,
        FIELD_MASK_STRING,
        /** Unwraps a {@code google.protobuf.*Value}, then applies the scalar kind beneath it. */
        WRAPPER,
        JSON,
        STRUCT
    }

    private static MessagePlan buildMessagePlan(
            Descriptors.Descriptor source,
            Descriptors.Descriptor target,
            ProtoSchemaOptions options,
            String parentPath) {
        // Paired by position, never by name; the class Javadoc says why.
        List<Descriptors.FieldDescriptor> sourceFields = source.getFields();
        List<Descriptors.FieldDescriptor> targetFields = target.getFields();
        String at = parentPath.isEmpty() ? "<root>" : parentPath;
        Preconditions.checkState(
                sourceFields.size() == targetFields.size(),
                "The row descriptor has %s fields where %s at %s has %s",
                targetFields.size(),
                source.getFullName(),
                at,
                sourceFields.size());
        List<FieldPlan> fieldPlans = new ArrayList<>(targetFields.size());
        for (int i = 0; i < targetFields.size(); i++) {
            Descriptors.FieldDescriptor sourceField = sourceFields.get(i);
            Descriptors.FieldDescriptor targetField = targetFields.get(i);
            // A guard on the pairing, not its key. equalsIgnoreCase folds each character without a
            // locale, so the dotless i of a tr_TR lower-casing still equals the I it came from.
            String targetName = BigQuerySchemaUtil.getFieldName(targetField);
            Preconditions.checkState(
                    targetName.equalsIgnoreCase(sourceField.getName()),
                    "Row descriptor field %s does not match field %s of %s at %s",
                    targetName,
                    sourceField.getName(),
                    source.getFullName(),
                    at);
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
        // A marked string already holds the text the column wants and its target field is a proto
        // string, so it converts as IDENTITY; only the empty-value rule is its own, and that is
        // decided here rather than per record. Which fields may be marked at all is validated once,
        // in ProtoToTableSchemaConverter, which always runs first to produce the target descriptor.
        // Its markedType is called here rather than recomputed: the target field of a marked column
        // is a string, so a plan that disagreed would ask a string field for its message type and
        // throw at construction.
        if (ProtoToTableSchemaConverter.markedType(sourceField, path, options) != null) {
            if (sourceField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                // A marked message can only be a JSON column: the geography marking accepts nothing
                // but a string, and that converter rejected anything else before we got here.
                return FieldPlan.json(sourceField, targetField, path);
            }
            return FieldPlan.verbatimString(
                    sourceField, targetField, path, !sourceField.hasPresence());
        }
        if (sourceField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return FieldPlan.scalar(
                    sourceField, targetField, scalarKind(sourceField, options, path), path);
        }
        Descriptors.Descriptor messageType = sourceField.getMessageType();
        switch (ProtoWellKnownType.of(sourceField, options)) {
            case TIMESTAMP:
                return FieldPlan.timestamp(
                        sourceField,
                        targetField,
                        path,
                        messageType.findFieldByName("seconds"),
                        messageType.findFieldByName("nanos"));
            case DURATION:
                return FieldPlan.duration(
                        sourceField,
                        targetField,
                        path,
                        messageType.findFieldByName("seconds"),
                        messageType.findFieldByName("nanos"));
            case FIELD_MASK:
                return FieldPlan.fieldMask(
                        sourceField, targetField, path, messageType.findFieldByName("paths"));
            case WRAPPER:
                // The inner kind comes from the same function a bare scalar goes through, so a
                // UInt32Value is converted by exactly the code that converts a bare uint32.
                Descriptors.FieldDescriptor valueField = messageType.findFieldByName("value");
                return FieldPlan.wrapper(
                        sourceField,
                        targetField,
                        path,
                        scalarKind(valueField, options, path),
                        valueField);
            case NONE:
            default:
                // JSON cannot arrive here: the branch above returns for it, in this same method.
                return FieldPlan.struct(
                        sourceField,
                        targetField,
                        path,
                        buildMessagePlan(messageType, targetField.getMessageType(), options, path));
        }
    }

    /**
     * Returns the conversion kind of a non-message field.
     *
     * @param field a field that is not a message
     * @param options the schema mapping options, which decide how a uint32 is read
     * @param path the dotted path used in the error message
     * @return the conversion kind
     */
    private static Kind scalarKind(
            Descriptors.FieldDescriptor field, ProtoSchemaOptions options, String path) {
        switch (field.getJavaType()) {
            case INT:
                // Under unsignedIntegersAsSigned a uint32/fixed32 takes the int32 path: the int
                // protobuf-java returns, sign-extended, as Avro's ProtobufData writes it (#2201).
                return isUnsigned32(field) && !options.isUnsignedIntegersAsSigned()
                        ? Kind.UINT32_TO_LONG
                        : Kind.INT_TO_LONG;
            case FLOAT:
                return Kind.FLOAT_TO_DOUBLE;
            case ENUM:
                return Kind.ENUM_NAME;
            // uint64/fixed64 among LONG: protobuf-java already holds them as a long with the
            // same 64 bits, the one INT64 value that loses nothing (#2200).
            case LONG:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
                return Kind.IDENTITY;
            default:
                throw new IllegalArgumentException(
                        "Unsupported protobuf type " + field.getType() + " for field " + path);
        }
    }

    private static boolean isUnsigned32(Descriptors.FieldDescriptor field) {
        switch (field.getType()) {
            case UINT32:
            case FIXED32:
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
                    if (field.omits(value)) {
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

        /**
         * The {@code seconds} and {@code nanos} of a {@code Timestamp} or a {@code Duration} — one
         * pair of slots, because the two well-known types have the same two-field shape.
         */
        private final Descriptors.FieldDescriptor secondsField;

        private final Descriptors.FieldDescriptor nanosField;

        /**
         * The one sub-field a value is read from: a wrapper's {@code value}, a {@code FieldMask}'s
         * {@code paths}. Both are "the message's only field", which is why one slot carries them.
         */
        private final Descriptors.FieldDescriptor valueField;

        /** The scalar kind applied once a wrapper is unwrapped; never a message kind. */
        private final Kind innerKind;

        /**
         * Whether an empty value must be left unset rather than written, decided once here so the
         * per-record path stays a flat switch.
         *
         * <p>Only ever true for a marked string without presence. A plain proto3 scalar has no
         * presence, so an unset one arrives as {@code ""} — and the BigQuery row descriptor's field
         * for a marked column <em>does</em> have presence, so writing it would put an explicit
         * empty string in the column. The empty string is neither valid JSON nor a valid geometry,
         * so that could only come back as a row-level error, which for a field most records
         * legitimately leave unset means failing on most records. Leaving the column unset (NULL)
         * is the only outcome that can succeed.
         *
         * <p>Deliberately limited to fields without presence: where the source can say "unset", an
         * explicit {@code ""} is the user's own statement and is passed through unchanged. Repeated
         * elements are explicit for the same reason.
         */
        private final boolean omitEmptyString;

        private FieldPlan(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                Kind kind,
                String path,
                MessagePlan nested,
                Descriptors.FieldDescriptor secondsField,
                Descriptors.FieldDescriptor nanosField,
                Descriptors.FieldDescriptor valueField,
                Kind innerKind,
                boolean omitEmptyString) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.kind = kind;
            this.path = path;
            this.nested = nested;
            this.secondsField = secondsField;
            this.nanosField = nanosField;
            this.valueField = valueField;
            this.innerKind = innerKind;
            this.omitEmptyString = omitEmptyString;
        }

        /** A field converted by a scalar {@link Kind}, with nothing else to carry. */
        static FieldPlan scalar(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                Kind kind,
                String path) {
            return new FieldPlan(
                    sourceField, targetField, kind, path, null, null, null, null, null, false);
        }

        /** A JSON-mapped message field, printed as canonical protobuf JSON. */
        static FieldPlan json(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path) {
            return new FieldPlan(
                    sourceField, targetField, Kind.JSON, path, null, null, null, null, null, false);
        }

        /** A marked string field — {@code JSON} or {@code GEOGRAPHY} — written through verbatim. */
        static FieldPlan verbatimString(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                boolean omitEmptyString) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.IDENTITY,
                    path,
                    null,
                    null,
                    null,
                    null,
                    null,
                    omitEmptyString);
        }

        /** A nested message field, converted by its own plan. */
        static FieldPlan struct(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                MessagePlan nested) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.STRUCT,
                    path,
                    nested,
                    null,
                    null,
                    null,
                    null,
                    false);
        }

        /** A {@code google.protobuf.Timestamp} field, flattened to epoch microseconds. */
        static FieldPlan timestamp(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                Descriptors.FieldDescriptor seconds,
                Descriptors.FieldDescriptor nanos) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.TIMESTAMP_MICROS,
                    path,
                    null,
                    seconds,
                    nanos,
                    null,
                    null,
                    false);
        }

        /** A {@code google.protobuf.Duration} field, flattened to microseconds. */
        static FieldPlan duration(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                Descriptors.FieldDescriptor seconds,
                Descriptors.FieldDescriptor nanos) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.DURATION_MICROS,
                    path,
                    null,
                    seconds,
                    nanos,
                    null,
                    null,
                    false);
        }

        /** A {@code google.protobuf.FieldMask} field, flattened to its comma-joined paths. */
        static FieldPlan fieldMask(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                Descriptors.FieldDescriptor paths) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.FIELD_MASK_STRING,
                    path,
                    null,
                    null,
                    null,
                    paths,
                    null,
                    false);
        }

        /** A {@code google.protobuf.*Value} wrapper, unwrapped then converted as its scalar. */
        static FieldPlan wrapper(
                Descriptors.FieldDescriptor sourceField,
                Descriptors.FieldDescriptor targetField,
                String path,
                Kind innerKind,
                Descriptors.FieldDescriptor valueField) {
            return new FieldPlan(
                    sourceField,
                    targetField,
                    Kind.WRAPPER,
                    path,
                    null,
                    null,
                    null,
                    valueField,
                    innerKind,
                    false);
        }

        boolean omits(Object value) {
            return omitEmptyString && ((String) value).isEmpty();
        }

        Object convertValue(Object value) throws IOException {
            switch (kind) {
                case TIMESTAMP_MICROS:
                    return toEpochMicros(value);
                case DURATION_MICROS:
                    return toDurationMicros(value);
                case FIELD_MASK_STRING:
                    return toFieldMaskString(value);
                case WRAPPER:
                    // A wrapper adds exactly one thing to a bare scalar: presence, which the
                    // caller has already acted on by the time we get here. Unwrap, then run the
                    // conversion the scalar itself would have run.
                    return convertScalar(
                            innerKind, ((MessageOrBuilder) value).getField(valueField));
                case JSON:
                    return JSON_PRINTER.print((MessageOrBuilder) value);
                case STRUCT:
                    return nested.convert((MessageOrBuilder) value);
                default:
                    return convertScalar(kind, value);
            }
        }

        /** The scalar half, reached directly and through a wrapper. */
        private Object convertScalar(Kind scalarKind, Object value) {
            switch (scalarKind) {
                case IDENTITY:
                    return value;
                case INT_TO_LONG:
                    return ((Integer) value).longValue();
                case UINT32_TO_LONG:
                    return Integer.toUnsignedLong((Integer) value);
                case FLOAT_TO_DOUBLE:
                    return ((Float) value).doubleValue();
                case ENUM_NAME:
                    return ((Descriptors.EnumValueDescriptor) value).getName();
                default:
                    throw new IllegalStateException("Unknown conversion kind: " + scalarKind);
            }
        }

        private long toEpochMicros(Object value) {
            Timestamp timestamp;
            if (value instanceof Timestamp) {
                timestamp = (Timestamp) value;
            } else {
                MessageOrBuilder message = (MessageOrBuilder) value;
                timestamp =
                        Timestamp.newBuilder()
                                .setSeconds((Long) message.getField(secondsField))
                                .setNanos((Integer) message.getField(nanosField))
                                .build();
            }
            try {
                return Timestamps.toMicros(timestamp);
            } catch (IllegalArgumentException e) {
                // Rewrapped for the same reason as a Duration's: protobuf's message names no field.
                // Protobuf's seconds bounds coincide with BigQuery's TIMESTAMP range.
                throw new IllegalArgumentException(
                        "google.protobuf.Timestamp value of field "
                                + path
                                + " (seconds="
                                + timestamp.getSeconds()
                                + ", nanos="
                                + timestamp.getNanos()
                                + ") is not a valid Timestamp: its seconds must lie between"
                                + " 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z, which is also"
                                + " BigQuery's TIMESTAMP range, and its nanos between 0 and"
                                + " 999,999,999",
                        e);
            }
        }

        private long toDurationMicros(Object value) {
            Duration duration;
            if (value instanceof Duration) {
                duration = (Duration) value;
            } else {
                MessageOrBuilder message = (MessageOrBuilder) value;
                duration =
                        Duration.newBuilder()
                                .setSeconds((Long) message.getField(secondsField))
                                .setNanos((Integer) message.getField(nanosField))
                                .build();
            }
            try {
                return Durations.toMicros(duration);
            } catch (IllegalArgumentException e) {
                // Row-level — one malformed record must not fail the job. Rewrapped because
                // protobuf's own message names no field, and a message with several Duration
                // columns would give a FailedRow nobody can act on.
                throw new IllegalArgumentException(
                        "google.protobuf.Duration value of field "
                                + path
                                + " (seconds="
                                + duration.getSeconds()
                                + ", nanos="
                                + duration.getNanos()
                                + ") is not a valid Duration: its seconds must lie between"
                                + " -315,576,000,000 and 315,576,000,000 (about 10,000 years),"
                                + " its nanos between -999,999,999 and 999,999,999, and the two"
                                + " must not have opposite signs",
                        e);
            }
        }

        private String toFieldMaskString(Object value) {
            if (value instanceof FieldMask) {
                return FieldMaskUtil.toString((FieldMask) value);
            }
            MessageOrBuilder message = (MessageOrBuilder) value;
            @SuppressWarnings("unchecked")
            List<String> paths = (List<String>) message.getField(valueField);
            return FieldMaskUtil.toString(FieldMask.newBuilder().addAllPaths(paths).build());
        }
    }
}
