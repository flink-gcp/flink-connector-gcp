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
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
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

    private static final int CEILING = ProtoDescriptorAugmenter.AUGMENTATION_CEILING;

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

    /**
     * The row is the delegate's bytes followed by the encoded fields rather than the delegate's row
     * parsed, given the fields and serialized again. Both decode to the same message, including
     * when the delegate's bytes already carry a value under an added field's number: a set field's
     * value is the later occurrence, and an omitted {@code NULLABLE} field leaves the delegate's.
     */
    @Test
    void encodesTheSameRowAsSettingTheFieldsOnTheParsedDelegateRow() throws Exception {
        AdditionalField<TestRecord> set =
                AdditionalField.of(
                        "set_value",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.NULLABLE,
                        record -> record.uuid);
        AdditionalField<TestRecord> omitted =
                AdditionalField.of(
                        "omitted_value",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.NULLABLE,
                        record -> null);
        Descriptors.Descriptor augmented =
                serializer(new TestSerializer(), options(set, omitted)).getDescriptor(DESTINATION);
        int setNumber = augmented.findFieldByName("set_value").getNumber();
        int omittedNumber = augmented.findFieldByName("omitted_value").getNumber();
        TestSerializer stray =
                new TestSerializer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ByteString serialize(TestRecord element) {
                        try {
                            ByteString.Output out = ByteString.newOutput();
                            CodedOutputStream coded = CodedOutputStream.newInstance(out);
                            coded.writeString(setNumber, "stale-set");
                            coded.writeString(omittedNumber, "stale-omitted");
                            coded.flush();
                            return super.serialize(element).concat(out.toByteString());
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                };
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(stray, options(set, omitted));
        TestRecord record = new TestRecord(5, "fresh", Instant.EPOCH);
        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);

        DynamicMessage expected =
                DynamicMessage.newBuilder(descriptor)
                        .mergeFrom(stray.serialize(record))
                        .setField(descriptor.findFieldByName("set_value"), "fresh")
                        .buildPartial();
        DynamicMessage actual =
                DynamicMessage.parseFrom(descriptor, serializer.serialize(record, DESTINATION));

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getField(descriptor.findFieldByName("set_value"))).isEqualTo("fresh");
        assertThat(actual.getField(descriptor.findFieldByName("omitted_value")))
                .isEqualTo("stale-omitted");
    }

    /**
     * Each of these bytes would read the appended fields as part of a field it leaves open, so the
     * row is refused before any provider runs, as parsing it used to refuse it.
     */
    @Test
    void refusesDelegateBytesThatATailCannotFollow() {
        byte[][] malformed = {
            // Field 1, length-delimited, declares 5 bytes and carries 1.
            {0x0A, 0x05, 0x61},
            // Field 1, a group that is never closed.
            {0x0B},
            // An end-group tag for a group that was never opened.
            {0x0C},
            // A varint that never terminates.
            {0x08, (byte) 0x80}
        };
        for (byte[] bytes : malformed) {
            AtomicInteger calls = new AtomicInteger();
            TestSerializer delegate =
                    new TestSerializer() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public ByteString serialize(TestRecord element) {
                            return ByteString.copyFrom(bytes);
                        }
                    };
            ProtoRowAugmentingSerializer<TestRecord> serializer =
                    serializer(
                            delegate,
                            options(
                                    AdditionalField.of(
                                            "source",
                                            AdditionalFieldType.STRING,
                                            AdditionalFieldNullPolicy.REQUIRED,
                                            record -> {
                                                calls.incrementAndGet();
                                                return "s";
                                            })));

            assertThatThrownBy(
                            () ->
                                    serializer.serialize(
                                            new TestRecord(1, "u", Instant.EPOCH), DESTINATION))
                    .as("bytes %s", Arrays.toString(bytes))
                    .isInstanceOf(InvalidProtocolBufferException.class);
            assertThat(calls).as("bytes %s", Arrays.toString(bytes)).hasValue(0);
        }
    }

    /**
     * Under a {@code proto3} delegate descriptor an appended field has no presence, so a provider
     * returning its default value encodes nothing, and a tail alone would leave the delegate's
     * value under that number in place. The row must still be the parse-and-set row, and its tail
     * must carry what {@code FILE_LOADS} reads, including a value kept for a {@code null} provider.
     */
    @Test
    void matchesTheParsedRowWhenAProto3DelegateCarriesAnAppendedNumber() throws Exception {
        Descriptors.Descriptor proto3 = proto3Descriptor();
        AdditionalField<TestRecord> defaulted =
                AdditionalField.of(
                        "defaulted_value",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.NULLABLE,
                        record -> "");
        AdditionalField<TestRecord> omitted =
                AdditionalField.of(
                        "omitted_value",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.NULLABLE,
                        record -> null);
        Descriptors.Descriptor augmented =
                serializer(new TestSerializer(BASE_SCHEMA, proto3), options(defaulted, omitted))
                        .getDescriptor(DESTINATION);
        int defaultedNumber = augmented.findFieldByName("defaulted_value").getNumber();
        int omittedNumber = augmented.findFieldByName("omitted_value").getNumber();
        TestSerializer stray =
                new TestSerializer(BASE_SCHEMA, proto3) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ByteString serialize(TestRecord element) {
                        try {
                            ByteString.Output out = ByteString.newOutput();
                            CodedOutputStream coded = CodedOutputStream.newInstance(out);
                            coded.writeString(defaultedNumber, "stale-defaulted");
                            coded.writeString(omittedNumber, "stale-omitted");
                            coded.flush();
                            return super.serialize(element).concat(out.toByteString());
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                };
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(stray, options(defaulted, omitted));
        TestRecord record = new TestRecord(5, "unused", Instant.EPOCH);
        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);
        Descriptors.FieldDescriptor defaultedField = descriptor.findFieldByName("defaulted_value");
        Descriptors.FieldDescriptor omittedField = descriptor.findFieldByName("omitted_value");

        DynamicMessage expected =
                DynamicMessage.newBuilder(descriptor)
                        .mergeFrom(stray.serialize(record))
                        .setField(defaultedField, "")
                        .buildPartial();
        SerializedRow row = serializer.serializeRow(record, DESTINATION);
        DynamicMessage actual = DynamicMessage.parseFrom(descriptor, row.toByteString());

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getField(defaultedField)).isEqualTo("");
        assertThat(row.getSerializedSize()).isEqualTo(row.toByteString().size());
        DynamicMessage tail = row.getTail(descriptor);
        assertThat(tail.hasField(defaultedField)).isFalse();
        assertThat(tail.getField(omittedField)).isEqualTo("stale-omitted");
    }

    @Test
    void handsADelegateMessageOverWithoutEncodingIt() throws Exception {
        MessageSerializer delegate = new MessageSerializer();
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        delegate,
                        options(
                                AdditionalField.of(
                                        "source",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        record -> record.uuid)));
        Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);

        SerializedRow row =
                serializer.serializeRow(new TestRecord(9, "u", Instant.EPOCH), DESTINATION);
        DynamicMessage decoded = DynamicMessage.parseFrom(descriptor, row.toByteString());

        assertThat(row.getBase(delegate.getDescriptor(DESTINATION))).isSameAs(delegate.last);
        assertThat(row.getTail(descriptor).getAllFields())
                .containsOnlyKeys(descriptor.findFieldByName("source"));
        assertThat(row.getSerializedSize()).isEqualTo(row.toByteString().size());
        assertThat(decoded.getField(descriptor.findFieldByName("id"))).isEqualTo(9L);
        assertThat(decoded.getField(descriptor.findFieldByName("source"))).isEqualTo("u");
        assertThat(serializer.serializeRow(new TestRecord(-1, "skip", Instant.EPOCH), DESTINATION))
                .isNull();
    }

    /**
     * A serializer outside the connector can hand over a message the connector did not build: one
     * carrying an appended number as an unknown field, or one of another descriptor instance whose
     * own field uses it. Either is walked as its bytes would be, so the row is the parse-and-set
     * row and its tail carries what {@code FILE_LOADS} reads, including a value kept for a {@code
     * null} provider, which a tail concatenated after a trusted message would leave behind.
     */
    @Test
    void walksAHandedOverMessageThatCarriesAnAppendedNumber() throws Exception {
        AdditionalField<TestRecord> omitted =
                AdditionalField.of(
                        "omitted_value",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.NULLABLE,
                        record -> null);
        int number =
                serializer(new MessageSerializer(), options(omitted))
                        .getDescriptor(DESTINATION)
                        .findFieldByName("omitted_value")
                        .getNumber();
        Descriptors.Descriptor other =
                descriptor(
                        BASE_SCHEMA.toBuilder()
                                .addFields(
                                        field(
                                                "stray",
                                                TableFieldSchema.Type.STRING,
                                                TableFieldSchema.Mode.NULLABLE))
                                .build());
        Descriptors.Descriptor renumbered =
                Descriptors.FileDescriptor.buildFrom(
                                FileDescriptorProto.newBuilder()
                                        .setName("renumbered.proto")
                                        .addMessageType(
                                                other.toProto().toBuilder()
                                                        .setField(
                                                                1,
                                                                other
                                                                        .toProto()
                                                                        .getField(1)
                                                                        .toBuilder()
                                                                        .setNumber(number)
                                                                        .build()))
                                        .build(),
                                new Descriptors.FileDescriptor[0])
                        .findMessageTypeByName(other.getName());

        for (StrayMessageSerializer delegate :
                Arrays.asList(
                        new StrayMessageSerializer(number, null),
                        new StrayMessageSerializer(number, renumbered))) {
            ProtoRowAugmentingSerializer<TestRecord> serializer =
                    serializer(delegate, options(omitted));
            Descriptors.Descriptor descriptor = serializer.getDescriptor(DESTINATION);
            Descriptors.FieldDescriptor omittedField = descriptor.findFieldByName("omitted_value");
            TestRecord record = new TestRecord(3, "unused", Instant.EPOCH);

            SerializedRow row = serializer.serializeRow(record, DESTINATION);
            DynamicMessage expected =
                    DynamicMessage.newBuilder(descriptor)
                            .mergeFrom(delegate.last.toByteString())
                            .buildPartial();

            String shape = delegate.describe();
            assertThat(DynamicMessage.parseFrom(descriptor, row.toByteString()))
                    .as(shape)
                    .isEqualTo(expected);
            assertThat(row.getTail(descriptor).getField(omittedField)).as(shape).isEqualTo("stale");
            assertThat(row.getSerializedSize()).as(shape).isEqualTo(row.toByteString().size());
        }
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

        // Rows follow the descriptor they were serialized against, on both sides of the evolution
        // and on both destinations. The augmentation fields are resolved when the surfaces are
        // derived, so a resolution held past a re-derivation would set a field of the previous
        // descriptor here — which protobuf rejects per row, inside the sink's row-failure handling.
        DynamicMessage evolvedRow =
                DynamicMessage.parseFrom(
                        evolved,
                        serializer.serialize(
                                new TestRecord(1, "uuid-1", Instant.EPOCH), DESTINATION));
        DynamicMessage dynamicRow =
                DynamicMessage.parseFrom(
                        dynamic,
                        serializer.serialize(new TestRecord(2, "uuid-2", Instant.EPOCH), other));

        assertThat(evolvedRow.getField(evolved.findFieldByName("computed"))).isEqualTo("uuid-1");
        assertThat(dynamicRow.getField(dynamic.findFieldByName("computed"))).isEqualTo("uuid-2");
    }

    @Test
    void derivesSchemaSurfacesOncePerDestinationAndFingerprint() throws Exception {
        AtomicInteger tableSchemaCalls = new AtomicInteger();
        AtomicInteger descriptorCalls = new AtomicInteger();
        BigQueryProtoSerializationSchema<TestRecord> delegate =
                new BigQueryProtoSerializationSchema<>() {
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

    /**
     * A delegate that changes its schema while serializing an element leaves the derivation to
     * {@code serialize}, inside the writers' row-failure boundary; its failure carries the type the
     * writers rethrow (#1954).
     */
    @Test
    void marksASchemaConflictFoundWhileSerializingAsTheDestinations() throws Exception {
        BigQueryProtoSerializationSchema<TestRecord> delegate =
                new BigQueryProtoSerializationSchema<>() {
                    private static final long serialVersionUID = 1L;

                    private boolean evolved;

                    @Override
                    public TableSchema getTableSchema(TableDestination destination) {
                        return evolved
                                ? BASE_SCHEMA.toBuilder()
                                        .addFields(
                                                field(
                                                        "COMPUTED",
                                                        TableFieldSchema.Type.STRING,
                                                        TableFieldSchema.Mode.NULLABLE))
                                        .build()
                                : BASE_SCHEMA;
                    }

                    @Override
                    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
                        return descriptor(getTableSchema(destination));
                    }

                    @Override
                    public Object getSchemaFingerprint(TableDestination destination) {
                        return evolved;
                    }

                    @Override
                    public ByteString serialize(TestRecord element) {
                        evolved = true;
                        // Not a well-formed encoding either: the destination's failure outranks
                        // the record's, as it did when the engine parsed these bytes.
                        return ByteString.copyFrom(new byte[] {0x0C});
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

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new TestRecord(1, "uuid-1", Instant.EPOCH), DESTINATION))
                .isInstanceOf(SchemaDerivationException.class)
                .hasMessageContaining(DESTINATION.toString())
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The physical BigQuery table schema must not declare additional field computed");
    }

    /** The delegate's own failure stays the element's, for the writers' failure policy (#1954). */
    @Test
    void leavesADelegateFailureUnmarked() {
        IllegalStateException poison = new IllegalStateException("poison record");
        ProtoRowAugmentingSerializer<TestRecord> serializer =
                serializer(
                        new TestSerializer() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public ByteString serialize(TestRecord element) {
                                throw poison;
                            }
                        },
                        options(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        record -> record.uuid)));
        serializer.prepare(DESTINATION);

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new TestRecord(1, "uuid-1", Instant.EPOCH), DESTINATION))
                .isSameAs(poison);
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

        assertThat(descriptor.findFieldByName("computed").getNumber()).isEqualTo(CEILING - 5);
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
            BigQueryProtoSerializationSchema<TestRecord> delegate,
            AdditionalFields<TestRecord> options) {
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

    /** The base schema's descriptor, declared {@code proto3}, where scalars have no presence. */
    private static Descriptors.Descriptor proto3Descriptor() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(
                                FieldDescriptorProto.newBuilder()
                                        .setName("id")
                                        .setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .build();
        return Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("row3.proto")
                                .setSyntax("proto3")
                                .addMessageType(row)
                                .build(),
                        new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Row");
    }

    private static Descriptors.Descriptor descriptorWithReservedRange() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(
                                FieldDescriptorProto.newBuilder()
                                        .setName("id")
                                        .setNumber(CEILING)
                                        .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(CEILING - 2)
                                        .setEnd(CEILING))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(CEILING - 4)
                                        .setEnd(CEILING - 2))
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

    private static class TestSerializer extends BigQueryProtoSerializationSchema<TestRecord> {
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

    /** Hands its row message over; encoding it through {@link #serialize} is a test failure. */
    private static final class MessageSerializer
            extends BigQueryProtoSerializationSchema<TestRecord>
            implements ProtoRowMessageProvider<TestRecord> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;
        private transient Message last;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return BASE_SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = descriptor(BASE_SCHEMA);
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(TestRecord element) {
            throw new AssertionError("the message must be handed over, not encoded");
        }

        @Override
        @Nullable
        public Message serializeToMessage(TestRecord element) {
            if (element.id < 0) {
                return null;
            }
            Descriptors.Descriptor rowDescriptor = getDescriptor(DESTINATION);
            last =
                    DynamicMessage.newBuilder(rowDescriptor)
                            .setField(rowDescriptor.findFieldByName("id"), element.id)
                            .build();
            return last;
        }
    }

    /**
     * Hands over a message carrying {@code "stale"} under {@code number}: as an unknown field of
     * its own descriptor, or, given {@code messageDescriptor}, as that descriptor's second field,
     * which {@code messageDescriptor} numbers {@code number}.
     */
    private static final class StrayMessageSerializer
            extends BigQueryProtoSerializationSchema<TestRecord>
            implements ProtoRowMessageProvider<TestRecord> {
        private static final long serialVersionUID = 1L;

        private final int number;
        @Nullable private final transient Descriptors.Descriptor messageDescriptor;
        private transient Descriptors.Descriptor descriptor;
        private transient Message last;

        private StrayMessageSerializer(
                int number, @Nullable Descriptors.Descriptor messageDescriptor) {
            this.number = number;
            this.messageDescriptor = messageDescriptor;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return BASE_SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = descriptor(BASE_SCHEMA);
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(TestRecord element) {
            return serializeToMessage(element).toByteString();
        }

        String describe() {
            return messageDescriptor == null
                    ? "an unknown field of the serializer's descriptor"
                    : "a field of another descriptor";
        }

        @Override
        public Message serializeToMessage(TestRecord element) {
            if (messageDescriptor != null) {
                last =
                        DynamicMessage.newBuilder(messageDescriptor)
                                .setField(messageDescriptor.findFieldByName("id"), element.id)
                                .setField(messageDescriptor.findFieldByNumber(number), "stale")
                                .build();
                return last;
            }
            Descriptors.Descriptor rowDescriptor = getDescriptor(DESTINATION);
            last =
                    DynamicMessage.newBuilder(rowDescriptor)
                            .setField(rowDescriptor.findFieldByName("id"), element.id)
                            .setUnknownFields(
                                    UnknownFieldSet.newBuilder()
                                            .addField(
                                                    number,
                                                    UnknownFieldSet.Field.newBuilder()
                                                            .addLengthDelimited(
                                                                    ByteString.copyFromUtf8(
                                                                            "stale"))
                                                            .build())
                                            .build())
                            .build();
            return last;
        }
    }

    private static final class MutableSerializer
            extends BigQueryProtoSerializationSchema<TestRecord> {
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
