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

import com.google.protobuf.BoolValue;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.Value;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Classifies a message field by the protobuf well-known type it carries. This is the single
 * decision point consulted by both {@link ProtoToTableSchemaConverter} and {@link
 * ProtoRowConverter}, so schema derivation and value conversion cannot disagree about what a field
 * means — and adding a constant here forces a compile-visible decision in both.
 *
 * <p><em>Well-known types</em> is protobuf's own term, not one coined here: it names the messages
 * shipped in {@code google/protobuf/*.proto} and documented under <a
 * href="https://protobuf.dev/reference/protobuf/google.protobuf/">Protocol Buffers Well-Known
 * Types</a>. The grouping below follows that reference too — wrappers, the temporal pair ({@code
 * Timestamp}, {@code Duration}) and the structural trio ({@code Struct}, {@code ListValue}, {@code
 * Value}) are Google's categories, so a reader who knows protobuf already knows what this enum
 * partitions. Names are taken from the vendor wherever the vendor has one, as {@code sink.storage}
 * is spelled after {@code com.google.cloud.bigquery.storage.v1}.
 *
 * <p>A classification by <em>behaviour</em> rather than by type: the nine wrapper types share one
 * constant because everything either converter needs about a wrapper — its BigQuery column type,
 * its value conversion — is derived from the wrapper's {@code value} sub-field by the very code
 * that maps a bare scalar of that type, so a wrapper and the scalar it wraps can never drift apart.
 *
 * <p>{@code google.protobuf.Any} is absent on purpose. It stays a {@code STRUCT<type_url, value>},
 * which is faithful: the payload cannot be expanded without the descriptor its type URL names, and
 * the connector has no way to obtain one.
 */
enum ProtoWellKnownType {

    /** Not a message field, or not a recognised well-known type. */
    NONE,

    /** {@code google.protobuf.Timestamp} to {@code TIMESTAMP}, epoch microseconds. */
    TIMESTAMP,

    /** {@code google.protobuf.Duration} to {@code INT64} microseconds. */
    DURATION,

    /** {@code google.protobuf.FieldMask} to {@code STRING}, its paths joined by commas. */
    FIELD_MASK,

    /** One of the nine {@code google.protobuf.*Value} wrappers, to the wrapped scalar's type. */
    WRAPPER,

    /**
     * {@code google.protobuf.Struct}, {@code Value} or {@code ListValue}, to a {@code JSON} column.
     */
    JSON;

    private static final Map<String, ProtoWellKnownType> BY_FULL_NAME = byFullName();

    /**
     * Returns the well-known type carried by the given field, or {@link #NONE} for a field that is
     * not a message or whose message type is not recognised. Safe to call on any field.
     *
     * @param field any field descriptor
     * @return the recognised well-known type, or {@link #NONE}
     */
    static ProtoWellKnownType of(Descriptors.FieldDescriptor field) {
        if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return NONE;
        }
        Descriptors.Descriptor messageType = field.getMessageType();
        ProtoWellKnownType candidate = BY_FULL_NAME.getOrDefault(messageType.getFullName(), NONE);
        return candidate.hasExpectedShape(messageType) ? candidate : NONE;
    }

    /**
     * Returns whether the given message really has the sub-fields this type's conversions read.
     *
     * <p>The name alone is not proof. {@code package google.protobuf; message Duration { int64
     * millis = 1; }} is legal protobuf that any descriptor pool can carry — nothing reserves the
     * package — and before this check it derived an {@code INT64} column and then threw {@code
     * NullPointerException: ... because "field" is null} on <em>every record</em>, from inside the
     * writers' {@code FailedRowHandler} catch, where a log-and-drop policy would discard the whole
     * stream. Measured, not supposed.
     *
     * <p>Answering {@link #NONE} rather than throwing is deliberate: such a message is expanded as
     * the ordinary {@code STRUCT} its fields describe, which is exactly what its author declared.
     * There is nothing to reject — only a name that turned out not to mean what it usually does.
     *
     * <p>{@link #JSON} needs no entry: no sub-field of a {@code Struct}, {@code Value} or {@code
     * ListValue} is ever dereferenced here, since {@code JsonFormat} prints them whole.
     */
    private boolean hasExpectedShape(Descriptors.Descriptor messageType) {
        switch (this) {
            case TIMESTAMP:
            case DURATION:
                return isSingular(messageType, "seconds", Descriptors.FieldDescriptor.JavaType.LONG)
                        && isSingular(
                                messageType, "nanos", Descriptors.FieldDescriptor.JavaType.INT);
            case FIELD_MASK:
                Descriptors.FieldDescriptor paths = messageType.findFieldByName("paths");
                return paths != null
                        && paths.isRepeated()
                        && paths.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING;
            case WRAPPER:
                // Any scalar will do: scalarType/scalarKind map whatever it is, and a message-typed
                // `value` is the one thing they cannot.
                Descriptors.FieldDescriptor value = messageType.findFieldByName("value");
                return value != null
                        && !value.isRepeated()
                        && value.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE;
            case JSON:
            case NONE:
            default:
                return true;
        }
    }

    private static boolean isSingular(
            Descriptors.Descriptor messageType,
            String name,
            Descriptors.FieldDescriptor.JavaType javaType) {
        Descriptors.FieldDescriptor field = messageType.findFieldByName(name);
        return field != null && !field.isRepeated() && field.getJavaType() == javaType;
    }

    /**
     * Returns whether this type is mapped to a {@code JSON} column without being configured as one.
     */
    boolean isJsonMapped() {
        return this == JSON;
    }

    /**
     * Builds the lookup table from the generated descriptors, so a name cannot be mistyped.
     *
     * <p>Keyed on the full name rather than on descriptor identity, because a descriptor built from
     * a serialized {@code FileDescriptorSet} carries its <em>own</em> copy of {@code
     * wrappers.proto} and friends, whose {@code Descriptor} instances differ from the generated
     * ones.
     */
    private static Map<String, ProtoWellKnownType> byFullName() {
        Map<String, ProtoWellKnownType> map = new HashMap<>();
        map.put(Timestamp.getDescriptor().getFullName(), TIMESTAMP);
        map.put(Duration.getDescriptor().getFullName(), DURATION);
        map.put(FieldMask.getDescriptor().getFullName(), FIELD_MASK);
        map.put(Struct.getDescriptor().getFullName(), JSON);
        map.put(Value.getDescriptor().getFullName(), JSON);
        map.put(ListValue.getDescriptor().getFullName(), JSON);
        for (Descriptors.Descriptor wrapper :
                new Descriptors.Descriptor[] {
                    Int32Value.getDescriptor(),
                    UInt32Value.getDescriptor(),
                    Int64Value.getDescriptor(),
                    UInt64Value.getDescriptor(),
                    FloatValue.getDescriptor(),
                    DoubleValue.getDescriptor(),
                    BoolValue.getDescriptor(),
                    StringValue.getDescriptor(),
                    BytesValue.getDescriptor()
                }) {
            map.put(wrapper.getFullName(), WRAPPER);
        }
        return Collections.unmodifiableMap(map);
    }
}
