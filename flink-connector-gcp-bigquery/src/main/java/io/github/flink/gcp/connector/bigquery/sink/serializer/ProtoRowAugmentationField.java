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
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;

/** One validated field consumed by the generic protobuf row-augmentation engine. */
@Internal
public final class ProtoRowAugmentationField<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Whether the field also belongs to the physical BigQuery table schema. */
    enum SchemaOwnership {
        PHYSICAL,
        WRITE_ONLY
    }

    /**
     * Write-only fields accepted by BigQuery's documented Storage Write API contract.
     *
     * <p>An enum rather than an arbitrary name keeps ordinary extra fields physical: the Storage
     * Write API rejects fields absent from the destination schema, while CDC explicitly admits
     * these two pseudocolumns.
     */
    public enum WriteOnlyField {
        CDC_CHANGE_TYPE("_change_type"),
        CDC_SEQUENCE_NUMBER("_change_sequence_number");

        private final String fieldName;

        WriteOnlyField(String fieldName) {
            this.fieldName = fieldName;
        }
    }

    /** Serializable provider whose validation may report a row-level {@link IOException}. */
    @FunctionalInterface
    public interface ValueProvider<T> extends Serializable {
        @Nullable
        Object getValue(T element) throws IOException;
    }

    private final FieldDescriptorProto descriptorField;
    @Nullable private final TableFieldSchema tableField;
    private final SchemaOwnership schemaOwnership;
    private final AdditionalFieldNullPolicy nullPolicy;
    private final ValueProvider<? super T> valueProvider;
    private final String providerFailureMessage;
    private final String nullValueMessage;

    private ProtoRowAugmentationField(
            FieldDescriptorProto descriptorField,
            @Nullable TableFieldSchema tableField,
            SchemaOwnership schemaOwnership,
            AdditionalFieldNullPolicy nullPolicy,
            ValueProvider<? super T> valueProvider,
            String providerFailureMessage,
            String nullValueMessage) {
        this.descriptorField = descriptorField;
        this.tableField = tableField;
        this.schemaOwnership = schemaOwnership;
        this.nullPolicy = nullPolicy;
        this.valueProvider = valueProvider;
        this.providerFailureMessage = providerFailureMessage;
        this.nullValueMessage = nullValueMessage;
    }

    /** Converts one public additional physical field into the internal representation. */
    public static <T> ProtoRowAugmentationField<T> physical(AdditionalField<? super T> field) {
        String name = field.getName();
        TableFieldSchema.Type tableType = tableType(field.getType());
        TableFieldSchema.Mode tableMode =
                field.getNullPolicy() == AdditionalFieldNullPolicy.REQUIRED
                        ? TableFieldSchema.Mode.REQUIRED
                        : TableFieldSchema.Mode.NULLABLE;
        TableFieldSchema tableField =
                TableFieldSchema.newBuilder()
                        .setName(name)
                        .setType(tableType)
                        .setMode(tableMode)
                        .build();
        return new ProtoRowAugmentationField<>(
                descriptorField(tableField),
                tableField,
                SchemaOwnership.PHYSICAL,
                field.getNullPolicy(),
                element ->
                        encode(field.getType(), field.getValueProvider().getValue(element), name),
                "The provider for additional field " + name + " failed",
                "The provider for required additional field " + name + " returned null");
    }

    /** Creates one of BigQuery's explicitly supported write-only fields. */
    public static <T> ProtoRowAugmentationField<T> writeOnly(
            WriteOnlyField field,
            AdditionalFieldNullPolicy nullPolicy,
            ValueProvider<? super T> valueProvider,
            String providerFailureMessage,
            String nullValueMessage) {
        return new ProtoRowAugmentationField<>(
                FieldDescriptorProto.newBuilder()
                        .setName(field.fieldName)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .build(),
                null,
                SchemaOwnership.WRITE_ONLY,
                nullPolicy,
                valueProvider,
                providerFailureMessage,
                nullValueMessage);
    }

    FieldDescriptorProto getDescriptorField() {
        return descriptorField;
    }

    @Nullable
    TableFieldSchema getTableField() {
        return tableField;
    }

    SchemaOwnership getSchemaOwnership() {
        return schemaOwnership;
    }

    AdditionalFieldNullPolicy getNullPolicy() {
        return nullPolicy;
    }

    ValueProvider<? super T> getValueProvider() {
        return valueProvider;
    }

    String getProviderFailureMessage() {
        return providerFailureMessage;
    }

    String getNullValueMessage() {
        return nullValueMessage;
    }

    private static TableFieldSchema.Type tableType(AdditionalFieldType type) {
        return TableFieldSchema.Type.valueOf(type.name());
    }

    private static FieldDescriptorProto descriptorField(TableFieldSchema tableField) {
        try {
            Descriptors.Descriptor descriptor =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            TableSchema.newBuilder().addFields(tableField).build());
            return descriptor.getFields().get(0).toProto().toBuilder()
                    // The Google converter uses the default locale. Additional-field names are
                    // protobuf-compatible ASCII, so retain the connector's locale-independent
                    // spelling before appending the field to the delegate descriptor.
                    .setName(tableField.getName().toLowerCase(Locale.ROOT))
                    // ProtoDescriptorAugmenter owns allocation against the delegate's used,
                    // reserved, extension, and protobuf-global field-number ranges.
                    .clearNumber()
                    .build();
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(
                    "Failed to derive the protobuf field for additional BigQuery field "
                            + tableField.getName(),
                    e);
        }
    }

    @Nullable
    private static Object encode(AdditionalFieldType type, @Nullable Object value, String fieldName)
            throws IOException {
        if (value == null) {
            return null;
        }
        try {
            switch (type) {
                case BOOL:
                    return requireType(value, Boolean.class, fieldName, "Boolean");
                case BYTES:
                    return requireType(value, ByteString.class, fieldName, "ByteString");
                case DATE:
                    return Math.toIntExact(
                            requireType(value, LocalDate.class, fieldName, "LocalDate")
                                    .toEpochDay());
                case DATETIME:
                    return CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(
                            requireType(value, LocalDateTime.class, fieldName, "LocalDateTime"));
                case DOUBLE:
                    return requireType(value, Double.class, fieldName, "Double");
                case GEOGRAPHY:
                case STRING:
                case JSON:
                    return requireType(value, String.class, fieldName, "String");
                case INT64:
                    return requireType(value, Long.class, fieldName, "Long");
                case NUMERIC:
                    return BigDecimalByteStringEncoder.encodeToNumericByteString(
                            requireType(value, BigDecimal.class, fieldName, "BigDecimal"));
                case BIGNUMERIC:
                    return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                            requireType(value, BigDecimal.class, fieldName, "BigDecimal"));
                case TIME:
                    return CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(
                            requireType(value, LocalTime.class, fieldName, "LocalTime"));
                case TIMESTAMP:
                    Instant instant = requireType(value, Instant.class, fieldName, "Instant");
                    return Math.addExact(
                            Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                            instant.getNano() / 1_000L);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported additional field type: " + type);
            }
        } catch (ArithmeticException | IllegalArgumentException | java.time.DateTimeException e) {
            throw new IOException(
                    "Value of additional field "
                            + fieldName
                            + " cannot be represented as "
                            + type
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static <V> V requireType(
            Object value, Class<V> expected, String fieldName, String expectedName) {
        if (expected.isInstance(value)) {
            return expected.cast(value);
        }
        throw new IllegalArgumentException(
                "additional field "
                        + fieldName
                        + " requires "
                        + expectedName
                        + " but received "
                        + value.getClass().getName());
    }
}
