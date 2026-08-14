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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentingSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcProtoRowFieldsTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("project", "dataset", "table");
    private static final TableSchema TABLE_SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("id")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();
    private static final Descriptors.Descriptor ROW_DESCRIPTOR = descriptor(TABLE_SCHEMA);

    @Test
    void addsChangeTypeAndCanonicalSequenceWithoutChangingThePhysicalSchema() throws Exception {
        TestSerializer serializer = new TestSerializer();
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(serializer, record -> record.sequence);

        TestRecord upsert = new TestRecord(7, CdcChangeType.UPSERT, "a/00000000000000ff");
        TestRecord delete = new TestRecord(7, CdcChangeType.DELETE, "B/100");

        assertRow(cdcSerializer, upsert, "UPSERT", "A/00000000000000FF");
        assertRow(cdcSerializer, delete, "DELETE", "B/100");
        assertThat(serializer.getTableSchema(DESTINATION)).isEqualTo(TABLE_SCHEMA);
        assertThat(serializer.getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("id");
    }

    @Test
    void omitsTheSequenceFieldWhenNoProviderIsConfigured() throws Exception {
        TestSerializer serializer = new TestSerializer();
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(
                        serializer,
                        CdcOptions.<TestRecord>builder(record -> record.changeType).build());

        Descriptors.Descriptor descriptor = cdcSerializer.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor,
                        cdcSerializer.serialize(
                                new TestRecord(9, CdcChangeType.UPSERT, null), DESTINATION));

        assertThat(descriptor.findFieldByName(CdcProtoRowFields.CHANGE_TYPE_FIELD)).isNotNull();
        assertThat(descriptor.findFieldByName(CdcProtoRowFields.SEQUENCE_NUMBER_FIELD)).isNull();
        assertThat(row.getField(descriptor.findFieldByName("id"))).isEqualTo(9L);
    }

    @Test
    void skippedRowsDoNotInvokeEitherProvider() throws Exception {
        AtomicInteger providerCalls = new AtomicInteger();
        TestSerializer serializer = new TestSerializer();
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(
                        serializer,
                        CdcOptions.<TestRecord>builder(
                                        record -> {
                                            providerCalls.incrementAndGet();
                                            return record.changeType;
                                        })
                                .sequenceNumberProvider(
                                        record -> {
                                            providerCalls.incrementAndGet();
                                            return record.sequence;
                                        })
                                .build());

        assertThat(
                        cdcSerializer.serialize(
                                new TestRecord(-1, CdcChangeType.UPSERT, "1"), DESTINATION))
                .isNull();
        assertThat(providerCalls).hasValue(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "1/", "/1", "10000000000000000", "1/2/3/4/5", "0x1", "not-hex"})
    void rejectsInvalidSequences(String sequence) {
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(new TestSerializer(), record -> sequence);

        assertThatThrownBy(
                        () ->
                                cdcSerializer.serialize(
                                        new TestRecord(1, CdcChangeType.UPSERT, sequence),
                                        DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("one to four slash-separated hexadecimal sections");
    }

    @Test
    void rejectsNullMetadataAndWrapsProviderFailures() {
        ProtoRowAugmentingSerializer<TestRecord> nullChange =
                cdcSerializer(
                        new TestSerializer(),
                        CdcOptions.<TestRecord>builder(record -> null).build());
        ProtoRowAugmentingSerializer<TestRecord> nullSequence =
                cdcSerializer(new TestSerializer(), record -> null);
        ProtoRowAugmentingSerializer<TestRecord> failedProvider =
                cdcSerializer(
                        new TestSerializer(),
                        record -> {
                            throw new IllegalArgumentException("provider detail");
                        });

        assertThatThrownBy(
                        () ->
                                nullChange.serialize(
                                        new TestRecord(1, CdcChangeType.UPSERT, null), DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage("The CDC change type provider returned null");
        assertThatThrownBy(
                        () ->
                                nullSequence.serialize(
                                        new TestRecord(1, CdcChangeType.UPSERT, null), DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage("The CDC sequence number provider returned null");
        assertThatThrownBy(
                        () ->
                                failedProvider.serialize(
                                        new TestRecord(1, CdcChangeType.UPSERT, "1"), DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage("The CDC sequence number provider failed")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preservesNestedDescriptorsAndSkipsReservedFieldNumbers() throws Exception {
        Descriptors.Descriptor nested = nestedDescriptorWithFieldNumber(18_999);
        BigQueryProtoSerializer<TestRecord> serializer = descriptorOnlySerializer(nested);
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(serializer, record -> "1");

        Descriptors.Descriptor augmented = cdcSerializer.getDescriptor(DESTINATION);

        assertThat(augmented.getContainingType().getName()).isEqualTo("Envelope");
        assertThat(augmented.findFieldByName(CdcProtoRowFields.CHANGE_TYPE_FIELD).getNumber())
                .isEqualTo(20_000);
        assertThat(augmented.findFieldByName(CdcProtoRowFields.SEQUENCE_NUMBER_FIELD).getNumber())
                .isEqualTo(20_001);
    }

    @Test
    void followsDelegateDescriptorEvolution() throws Exception {
        Descriptors.Descriptor first =
                descriptorWithFields(optionalField("id", 1, FieldDescriptorProto.Type.TYPE_INT64));
        Descriptors.Descriptor second =
                descriptorWithFields(
                        optionalField("id", 1, FieldDescriptorProto.Type.TYPE_INT64),
                        optionalField("name", 2, FieldDescriptorProto.Type.TYPE_STRING));
        AtomicInteger schemaVersion = new AtomicInteger();
        BigQueryProtoSerializer<TestRecord> serializer =
                new BigQueryProtoSerializer<>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TableSchema getTableSchema(TableDestination destination) {
                        return TABLE_SCHEMA;
                    }

                    @Override
                    public Descriptors.Descriptor getDescriptor(TableDestination destination) {
                        return schemaVersion.get() == 0 ? first : second;
                    }

                    @Override
                    public Object getSchemaFingerprint(TableDestination destination) {
                        return schemaVersion.get();
                    }

                    @Override
                    public ByteString serialize(TestRecord element) {
                        return ByteString.EMPTY;
                    }
                };
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(serializer, record -> "1");

        Descriptors.Descriptor firstAugmented = cdcSerializer.getDescriptor(DESTINATION);
        schemaVersion.set(1);
        Descriptors.Descriptor secondAugmented = cdcSerializer.getDescriptor(DESTINATION);

        assertThat(firstAugmented.findFieldByName("name")).isNull();
        assertThat(secondAugmented.findFieldByName("name")).isNotNull();
        assertThat(secondAugmented.findFieldByName(CdcProtoRowFields.CHANGE_TYPE_FIELD))
                .isNotNull();
        assertThat(secondAugmented).isNotSameAs(firstAugmented);
    }

    @Test
    void preservesFileDependencies() throws Exception {
        Descriptors.Descriptor base = descriptorWithDependency();
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(descriptorOnlySerializer(base), record -> "1");

        Descriptors.Descriptor augmented = cdcSerializer.getDescriptor(DESTINATION);

        assertThat(augmented.findFieldByName("shared").getMessageType().getFullName())
                .isEqualTo("dependency.Shared");
        assertThat(augmented.getFile().getDependencies())
                .extracting(Descriptors.FileDescriptor::getName)
                .containsExactly("dependency.proto");
    }

    @Test
    void skipsMessageReservedAndExtensionRanges() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(optionalField("id", 4, FieldDescriptorProto.Type.TYPE_INT64))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(7))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder().setStart(7).setEnd(9))
                        .build();
        Descriptors.Descriptor base = descriptor(row, new Descriptors.FileDescriptor[0]);
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(descriptorOnlySerializer(base), record -> "1");

        Descriptors.Descriptor augmented = cdcSerializer.getDescriptor(DESTINATION);

        assertThat(augmented.findFieldByName(CdcProtoRowFields.CHANGE_TYPE_FIELD).getNumber())
                .isEqualTo(9);
        assertThat(augmented.findFieldByName(CdcProtoRowFields.SEQUENCE_NUMBER_FIELD).getNumber())
                .isEqualTo(10);
    }

    @Test
    void rejectsPhysicalPseudocolumnCollisions() throws Exception {
        Descriptors.Descriptor collision =
                descriptorWithFields(
                        FieldDescriptorProto.newBuilder()
                                .setName("_CHANGE_TYPE")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());
        ProtoRowAugmentingSerializer<TestRecord> cdcSerializer =
                cdcSerializer(descriptorOnlySerializer(collision), record -> "1");

        assertThatThrownBy(() -> cdcSerializer.getDescriptor(DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not declare BigQuery CDC pseudocolumn");
    }

    @Test
    void serializerAndProvidersSurviveJobGraphSerialization() throws Exception {
        ProtoRowAugmentingSerializer<TestRecord> copy =
                InstantiationUtil.clone(
                        cdcSerializer(new TestSerializer(), record -> record.sequence));

        assertRow(copy, new TestRecord(3, CdcChangeType.DELETE, "abc"), "DELETE", "ABC");
    }

    private static ProtoRowAugmentingSerializer<TestRecord> cdcSerializer(
            BigQueryProtoSerializer<TestRecord> serializer,
            CdcSequenceNumberProvider<TestRecord> sequenceProvider) {
        return cdcSerializer(
                serializer,
                CdcOptions.<TestRecord>builder(record -> record.changeType)
                        .sequenceNumberProvider(sequenceProvider)
                        .build());
    }

    private static ProtoRowAugmentingSerializer<TestRecord> cdcSerializer(
            BigQueryProtoSerializer<TestRecord> serializer, CdcOptions<TestRecord> options) {
        return new ProtoRowAugmentingSerializer<>(
                serializer,
                CdcProtoRowFields.create(options),
                "BigQuery CDC pseudocolumn",
                "Failed to add BigQuery CDC metadata to a serialized row");
    }

    private static void assertRow(
            ProtoRowAugmentingSerializer<TestRecord> cdcSerializer,
            TestRecord record,
            String expectedChangeType,
            String expectedSequence)
            throws Exception {
        Descriptors.Descriptor descriptor = cdcSerializer.getDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(descriptor, cdcSerializer.serialize(record, DESTINATION));
        assertThat(row.getField(descriptor.findFieldByName("id"))).isEqualTo(record.id);
        assertThat(row.getField(descriptor.findFieldByName(CdcProtoRowFields.CHANGE_TYPE_FIELD)))
                .isEqualTo(expectedChangeType);
        assertThat(
                        row.getField(
                                descriptor.findFieldByName(
                                        CdcProtoRowFields.SEQUENCE_NUMBER_FIELD)))
                .isEqualTo(expectedSequence);
    }

    private static Descriptors.Descriptor descriptor(TableSchema schema) {
        try {
            return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static Descriptors.Descriptor nestedDescriptorWithFieldNumber(int number)
            throws Descriptors.DescriptorValidationException {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(
                                FieldDescriptorProto.newBuilder()
                                        .setName("id")
                                        .setNumber(number)
                                        .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .build();
        DescriptorProto envelope =
                DescriptorProto.newBuilder().setName("Envelope").addNestedType(row).build();
        Descriptors.FileDescriptor file =
                Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("nested.proto")
                                .addMessageType(envelope)
                                .build(),
                        new Descriptors.FileDescriptor[0]);
        return file.findMessageTypeByName("Envelope").findNestedTypeByName("Row");
    }

    private static Descriptors.Descriptor descriptorWithFields(FieldDescriptorProto... fields)
            throws Descriptors.DescriptorValidationException {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addAllField(java.util.List.of(fields))
                        .build();
        return descriptor(row, new Descriptors.FileDescriptor[0]);
    }

    private static Descriptors.Descriptor descriptor(
            DescriptorProto row, Descriptors.FileDescriptor[] dependencies)
            throws Descriptors.DescriptorValidationException {
        FileDescriptorProto.Builder fileProto =
                FileDescriptorProto.newBuilder().setName("row.proto").addMessageType(row);
        for (Descriptors.FileDescriptor dependency : dependencies) {
            fileProto.addDependency(dependency.getName());
        }
        Descriptors.FileDescriptor file =
                Descriptors.FileDescriptor.buildFrom(fileProto.build(), dependencies);
        return file.findMessageTypeByName("Row");
    }

    private static Descriptors.Descriptor descriptorWithDependency()
            throws Descriptors.DescriptorValidationException {
        Descriptors.FileDescriptor dependency =
                Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("dependency.proto")
                                .setPackage("dependency")
                                .addMessageType(
                                        DescriptorProto.newBuilder().setName("Shared").build())
                                .build(),
                        new Descriptors.FileDescriptor[0]);
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(
                                optionalField("shared", 1, FieldDescriptorProto.Type.TYPE_MESSAGE)
                                        .toBuilder()
                                        .setTypeName(".dependency.Shared"))
                        .build();
        return descriptor(row, new Descriptors.FileDescriptor[] {dependency});
    }

    private static FieldDescriptorProto optionalField(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    private static BigQueryProtoSerializer<TestRecord> descriptorOnlySerializer(
            Descriptors.Descriptor descriptor) {
        return new BigQueryProtoSerializer<>() {
            private static final long serialVersionUID = 1L;

            @Override
            public TableSchema getTableSchema(TableDestination destination) {
                return TABLE_SCHEMA;
            }

            @Override
            public Descriptors.Descriptor getDescriptor(TableDestination destination) {
                return descriptor;
            }

            @Override
            public ByteString serialize(TestRecord element) {
                return ByteString.EMPTY;
            }
        };
    }

    private static final class TestSerializer extends BigQueryProtoSerializer<TestRecord> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TABLE_SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return ROW_DESCRIPTOR;
        }

        @Override
        @Nullable
        public ByteString serialize(TestRecord element) {
            if (element.id < 0) {
                return null;
            }
            return DynamicMessage.newBuilder(ROW_DESCRIPTOR)
                    .setField(ROW_DESCRIPTOR.findFieldByName("id"), element.id)
                    .build()
                    .toByteString();
        }
    }

    private static final class TestRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long id;
        private final CdcChangeType changeType;
        @Nullable private final String sequence;

        private TestRecord(long id, CdcChangeType changeType, @Nullable String sequence) {
            this.id = id;
            this.changeType = changeType;
            this.sequence = sequence;
        }
    }
}
