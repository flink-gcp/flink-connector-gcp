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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField.SchemaOwnership;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField.WriteOnlyField;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoRowAugmentingSerializerTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");
    private static final TableSchema BASE_SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            field(
                                    "id",
                                    TableFieldSchema.Type.INT64,
                                    TableFieldSchema.Mode.NULLABLE))
                    .build();

    @Test
    void appendsOrderedPhysicalFieldsToTheSchemaDescriptorAndRow() throws Exception {
        Instant timestamp = Instant.parse("2026-08-14T01:02:03.123456789Z");
        List<String> providerOrder = new ArrayList<>();
        AdditionalFields<TestRecord> options =
                AdditionalFields.<TestRecord>builder()
                        .field(
                                AdditionalField.of(
                                        "__uuid",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        record -> {
                                            providerOrder.add("__uuid");
                                            return record.uuid;
                                        }))
                        .field(
                                AdditionalField.of(
                                        "__timestamp",
                                        AdditionalFieldType.TIMESTAMP,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> {
                                            providerOrder.add("__timestamp");
                                            return record.timestamp;
                                        }))
                        .build();
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(new TestSerializer(), options);

        TableSchema tableSchema = serializer.getTableSchema(DESTINATION);
        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        serializer.serialize(new TestRecord(7, "uuid-7", timestamp), DESTINATION));

        assertThat(tableSchema.getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("id", "__uuid", "__timestamp");
        assertThat(tableSchema.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.REQUIRED);
        assertThat(tableSchema.getFields(2).getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);
        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("id", "__uuid", "__timestamp");
        assertThat(row.getField(descriptor.findFieldByName("__uuid"))).isEqualTo("uuid-7");
        assertThat(row.getField(descriptor.findFieldByName("__timestamp")))
                .isEqualTo(1_786_669_323_123_456L);
        assertThat(providerOrder).containsExactly("__uuid", "__timestamp");
    }

    @Test
    void supportsEveryDeclaredSingularScalarType() throws Exception {
        LocalDate date = LocalDate.of(2026, 8, 14);
        LocalTime time = LocalTime.of(12, 34, 56, 123_456_000);
        LocalDateTime datetime = LocalDateTime.of(date, time);
        Instant timestamp = Instant.parse("2026-08-14T03:04:05.123456Z");
        BigDecimal numeric = new BigDecimal("123.456789123");
        BigDecimal bignumeric = new BigDecimal("12345678901234567890.12345678901234567890");
        ByteString bytes = ByteString.copyFromUtf8("bytes");
        AdditionalFields.Builder<TestRecord> builder = AdditionalFields.builder();
        add(builder, "bool_value", AdditionalFieldType.BOOL, true);
        add(builder, "bytes_value", AdditionalFieldType.BYTES, bytes);
        add(builder, "date_value", AdditionalFieldType.DATE, date);
        add(builder, "datetime_value", AdditionalFieldType.DATETIME, datetime);
        add(builder, "double_value", AdditionalFieldType.DOUBLE, 1.25D);
        add(builder, "geography_value", AdditionalFieldType.GEOGRAPHY, "POINT(1 2)");
        add(builder, "int64_value", AdditionalFieldType.INT64, 42L);
        add(builder, "numeric_value", AdditionalFieldType.NUMERIC, numeric);
        add(builder, "bignumeric_value", AdditionalFieldType.BIGNUMERIC, bignumeric);
        add(builder, "string_value", AdditionalFieldType.STRING, "text");
        add(builder, "time_value", AdditionalFieldType.TIME, time);
        add(builder, "timestamp_value", AdditionalFieldType.TIMESTAMP, timestamp);
        add(builder, "json_value", AdditionalFieldType.JSON, "{\"k\":1}");
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(new TestSerializer(), builder.build());

        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        serializer.serialize(new TestRecord(1, "unused", timestamp), DESTINATION));

        assertThat(row.getField(descriptor.findFieldByName("bool_value"))).isEqualTo(true);
        assertThat(row.getField(descriptor.findFieldByName("bytes_value"))).isEqualTo(bytes);
        assertThat(row.getField(descriptor.findFieldByName("date_value")))
                .isEqualTo((int) date.toEpochDay());
        assertThat(row.getField(descriptor.findFieldByName("datetime_value")))
                .isEqualTo(CivilTimeEncoder.encodePacked64DatetimeMicrosLocalDateTime(datetime));
        assertThat(row.getField(descriptor.findFieldByName("double_value"))).isEqualTo(1.25D);
        assertThat(row.getField(descriptor.findFieldByName("geography_value")))
                .isEqualTo("POINT(1 2)");
        assertThat(row.getField(descriptor.findFieldByName("int64_value"))).isEqualTo(42L);
        assertThat(row.getField(descriptor.findFieldByName("numeric_value")))
                .isEqualTo(BigDecimalByteStringEncoder.encodeToNumericByteString(numeric));
        assertThat(row.getField(descriptor.findFieldByName("bignumeric_value")))
                .isEqualTo(BigDecimalByteStringEncoder.encodeToBigNumericByteString(bignumeric));
        assertThat(row.getField(descriptor.findFieldByName("string_value"))).isEqualTo("text");
        assertThat(row.getField(descriptor.findFieldByName("time_value")))
                .isEqualTo(CivilTimeEncoder.encodePacked64TimeMicrosLocalTime(time));
        assertThat(row.getField(descriptor.findFieldByName("timestamp_value")))
                .isEqualTo(1_786_676_645_123_456L);
        assertThat(row.getField(descriptor.findFieldByName("json_value"))).isEqualTo("{\"k\":1}");
    }

    @Test
    void physicalDescriptorFieldsFollowTheBigQueryStorageMapping() throws Exception {
        for (AdditionalFieldType type : AdditionalFieldType.values()) {
            for (AdditionalFieldNullPolicy nullPolicy : AdditionalFieldNullPolicy.values()) {
                ProtoRowAugmentationField<Object> field =
                        ProtoRowAugmentationField.physical(
                                AdditionalField.of("value", type, nullPolicy, ignored -> null));
                FieldDescriptorProto expected =
                        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                        TableSchema.newBuilder()
                                                .addFields(field.getTableField())
                                                .build())
                                .getFields()
                                .get(0)
                                .toProto();

                assertThat(field.getDescriptorField().getType())
                        .as("type for %s with %s", type, nullPolicy)
                        .isEqualTo(expected.getType());
                assertThat(field.getDescriptorField().getLabel())
                        .as("label for %s with %s", type, nullPolicy)
                        .isEqualTo(expected.getLabel());
                assertThat(field.getDescriptorField().getNumber())
                        .as("field number for %s with %s", type, nullPolicy)
                        .isZero();
            }
        }
    }

    @Test
    void physicalDescriptorFieldNamesDoNotDependOnTheDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            ProtoRowAugmentationField<Object> field =
                    ProtoRowAugmentationField.physical(
                            AdditionalField.of(
                                    "INGESTION_ID",
                                    AdditionalFieldType.STRING,
                                    AdditionalFieldNullPolicy.REQUIRED,
                                    ignored -> "value"));

            assertThat(field.getDescriptorField().getName()).isEqualTo("ingestion_id");
            assertThat(field.getDescriptorField().getNumber()).isZero();
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void nullableValuesAreOmittedAndRequiredNullsFailTheRow() throws Exception {
        ProtoRowAugmentingSerializer<TestRecord> nullable =
                serializer(
                        new TestSerializer(),
                        options(
                                AdditionalField.of(
                                        "optional_value",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> null)));
        ProtoRowAugmentingSerializer<TestRecord> required =
                serializer(
                        new TestSerializer(),
                        options(
                                AdditionalField.of(
                                        "required_value",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        record -> null)));

        Descriptors.Descriptor descriptor = nullable.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        nullable.serialize(
                                new TestRecord(1, "unused", Instant.EPOCH), DESTINATION));

        assertThat(row.hasField(descriptor.findFieldByName("optional_value"))).isFalse();
        assertThatThrownBy(
                        () ->
                                required.serialize(
                                        new TestRecord(1, "unused", Instant.EPOCH), DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "The provider for required additional field required_value returned null");
    }

    @Test
    void delegateSkipsBeforeProvidersAndProviderFailuresNameTheField() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        new TestSerializer(),
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        record -> {
                                            calls.incrementAndGet();
                                            if (record.id == 2) {
                                                throw new IllegalStateException("provider detail");
                                            }
                                            return 42L;
                                        })));

        assertThat(serializer.serialize(new TestRecord(-1, "skip", Instant.EPOCH), DESTINATION))
                .isNull();
        assertThat(calls).hasValue(0);
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new TestRecord(2, "fail", Instant.EPOCH), DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage("The provider for additional field computed failed")
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new TestRecord(3, "wrong-type", Instant.EPOCH),
                                        DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("additional field computed requires String but received");
    }

    @Test
    void followsDynamicAndEvolvingDelegateDescriptors() throws Exception {
        MutableSerializer delegate = new MutableSerializer();
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        delegate,
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> record.uuid)));
        TableDestination other = TableDestination.of("project", "dataset", "other");

        Descriptors.Descriptor first = serializer.getDescriptor(DESTINATION);
        delegate.evolved = true;
        Descriptors.Descriptor evolved = serializer.getDescriptor(DESTINATION);
        Descriptors.Descriptor dynamic = serializer.getDescriptor(other);

        assertThat(first.findFieldByName("name")).isNull();
        assertThat(evolved.findFieldByName("name")).isNotNull();
        assertThat(evolved.findFieldByName("computed")).isNotNull();
        assertThat(dynamic.findFieldByName("other_value")).isNotNull();
    }

    @Test
    void derivesSchemaSurfacesOncePerDestinationAndFingerprint() throws Exception {
        AtomicInteger tableSchemaCalls = new AtomicInteger();
        AtomicInteger descriptorCalls = new AtomicInteger();
        BigQueryProtoSerializer<TestRecord> delegate =
                new BigQueryProtoSerializer<>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TableSchema getTableSchema(TableDestination destination) {
                        tableSchemaCalls.incrementAndGet();
                        return BASE_SCHEMA;
                    }

                    @Override
                    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
                        descriptorCalls.incrementAndGet();
                        return descriptor(BASE_SCHEMA);
                    }

                    @Override
                    public ByteString serialize(TestRecord element) {
                        return ByteString.EMPTY;
                    }
                };
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        delegate,
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> record.uuid)));

        serializer.prepare(DESTINATION);
        serializer.prepare(DESTINATION);
        serializer.getTableSchema(DESTINATION);
        serializer.getDescriptor(DESTINATION);
        serializer.serialize(new TestRecord(1, "first", Instant.EPOCH), DESTINATION);
        serializer.serialize(new TestRecord(2, "second", Instant.EPOCH), DESTINATION);

        assertThat(tableSchemaCalls).hasValue(1);
        assertThat(descriptorCalls).hasValue(1);
    }

    @Test
    void rejectsSchemaCollisionsBeforeProviderEvaluation() {
        AtomicInteger calls = new AtomicInteger();
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        new TestSerializer(
                                TableSchema.newBuilder()
                                        .addFields(
                                                field(
                                                        "COMPUTED",
                                                        TableFieldSchema.Type.STRING,
                                                        TableFieldSchema.Mode.NULLABLE))
                                        .build()),
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> {
                                            calls.incrementAndGet();
                                            return "value";
                                        })));

        assertThatThrownBy(() -> serializer.prepare(DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The physical BigQuery table schema must not declare additional field computed");
        assertThat(calls).hasValue(0);
    }

    @Test
    void genericPathRetainsReservedFieldAllocationAndJobGraphSerialization() throws Exception {
        Descriptors.Descriptor reserved = descriptorWithReservedRange();
        ProtoRowAugmentingSerializer<TestRecord> original =
                serializer(
                        new TestSerializer(BASE_SCHEMA, reserved),
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> record.uuid)));
        Descriptors.Descriptor descriptor = original.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        original.serialize(new TestRecord(1, "value", Instant.EPOCH), DESTINATION));

        assertThat(descriptor.findFieldByName("computed").getNumber()).isEqualTo(9);
        assertThat(row.getField(descriptor.findFieldByName("computed"))).isEqualTo("value");

        ProtoRowAugmentingSerializer<TestRecord> copy =
                InstantiationUtil.clone(
                        serializer(
                                new TestSerializer(),
                                options(
                                        AdditionalField.of(
                                                "computed",
                                                AdditionalFieldType.STRING,
                                                AdditionalFieldNullPolicy.NULLABLE,
                                                record -> record.uuid))));
        Descriptors.Descriptor clonedDescriptor = copy.getDescriptor(DESTINATION);
        DynamicMessage clonedRow =
                DynamicMessage.parseFrom(
                        clonedDescriptor,
                        copy.serialize(new TestRecord(1, "value", Instant.EPOCH), DESTINATION));

        assertThat(clonedRow.getField(clonedDescriptor.findFieldByName("computed")))
                .isEqualTo("value");
    }

    @Test
    void officialWriteOnlyContractContainsOnlyCdcPseudocolumns() {
        assertThat(WriteOnlyField.values())
                .containsExactly(
                        WriteOnlyField.CDC_CHANGE_TYPE, WriteOnlyField.CDC_SEQUENCE_NUMBER);
        ProtoRowAugmentationField<String> field =
                ProtoRowAugmentationField.writeOnly(
                        WriteOnlyField.CDC_CHANGE_TYPE,
                        AdditionalFieldNullPolicy.REQUIRED,
                        value -> "UPSERT",
                        "provider failed",
                        "provider returned null");

        assertThat(field.getSchemaOwnership()).isEqualTo(SchemaOwnership.WRITE_ONLY);
        assertThat(field.getTableField()).isNull();
        assertThat(field.getDescriptorField().getName()).isEqualTo("_change_type");
    }

    private static void add(
            AdditionalFields.Builder<TestRecord> builder,
            String name,
            AdditionalFieldType type,
            Object value) {
        builder.field(
                AdditionalField.of(
                        name, type, AdditionalFieldNullPolicy.REQUIRED, record -> value));
    }

    @SafeVarargs
    private static AdditionalFields<TestRecord> options(AdditionalField<TestRecord>... fields) {
        AdditionalFields.Builder<TestRecord> builder = AdditionalFields.builder();
        Arrays.stream(fields).forEach(builder::field);
        return builder.build();
    }

    private static ProtoRowAugmentingSerializer<TestRecord> serializer(
            BigQueryProtoSerializer<TestRecord> delegate, AdditionalFields<TestRecord> options) {
        List<ProtoRowAugmentationField<? super TestRecord>> fields = new ArrayList<>();
        options.getFields().forEach(field -> fields.add(ProtoRowAugmentationField.physical(field)));
        return new ProtoRowAugmentingSerializer<>(
                delegate,
                fields,
                "additional BigQuery field",
                "Failed to add additional fields to a serialized BigQuery row");
    }

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static Descriptors.Descriptor descriptor(TableSchema schema) {
        try {
            return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static Descriptors.Descriptor descriptorWithReservedRange() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(
                                FieldDescriptorProto.newBuilder()
                                        .setName("id")
                                        .setNumber(4)
                                        .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(7))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder().setStart(7).setEnd(9))
                        .build();
        Descriptors.FileDescriptor file =
                Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("row.proto")
                                .addMessageType(row)
                                .build(),
                        new Descriptors.FileDescriptor[0]);
        return file.findMessageTypeByName("Row");
    }

    private static final class TestRecord {
        private final long id;
        private final String uuid;
        private final Instant timestamp;

        private TestRecord(long id, String uuid, Instant timestamp) {
            this.id = id;
            this.uuid = uuid;
            this.timestamp = timestamp;
        }
    }

    private static class TestSerializer extends BigQueryProtoSerializer<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final TableSchema schema;
        private transient Descriptors.Descriptor descriptor;

        private TestSerializer() {
            this(BASE_SCHEMA);
        }

        private TestSerializer(TableSchema schema) {
            this(schema, null);
        }

        private TestSerializer(TableSchema schema, @Nullable Descriptors.Descriptor descriptor) {
            this.schema = schema;
            this.descriptor = descriptor;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = descriptor(schema);
            }
            return descriptor;
        }

        @Override
        @Nullable
        public ByteString serialize(TestRecord element) {
            if (element.id < 0) {
                return null;
            }
            Descriptors.Descriptor rowDescriptor = getDescriptor(DESTINATION);
            return DynamicMessage.newBuilder(rowDescriptor)
                    .setField(rowDescriptor.findFieldByName("id"), element.id)
                    .build()
                    .toByteString();
        }
    }

    private static final class MutableSerializer extends BigQueryProtoSerializer<TestRecord> {
        private static final long serialVersionUID = 1L;

        private boolean evolved;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema(destination);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return descriptor(schema(destination));
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return evolved;
        }

        @Override
        public ByteString serialize(TestRecord element) {
            return ByteString.EMPTY;
        }

        private TableSchema schema(TableDestination destination) {
            TableSchema.Builder builder = BASE_SCHEMA.toBuilder();
            if (destination.getTable().equals("other")) {
                builder.addFields(
                        field(
                                "other_value",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE));
            } else if (evolved) {
                builder.addFields(
                        field(
                                "name",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE));
            }
            return builder.build();
        }
    }
}
