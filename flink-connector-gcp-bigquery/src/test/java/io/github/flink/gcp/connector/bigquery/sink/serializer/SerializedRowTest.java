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

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerializedRowTest {

    private static final TableSchema BASE =
            TableSchema.newBuilder()
                    .addFields(field("id", TableFieldSchema.Type.INT64))
                    .addFields(field("name", TableFieldSchema.Type.STRING))
                    .build();

    private static final TableSchema AUGMENTED =
            BASE.toBuilder().addFields(field("source", TableFieldSchema.Type.STRING)).build();

    @Test
    void handsTheSerializersOwnMessageOverForItsDescriptor() throws Exception {
        Descriptors.Descriptor descriptor = descriptor(BASE);
        DynamicMessage base = base(descriptor);

        assertThat(SerializedRow.ofMessage(base).getBase(descriptor)).isSameAs(base);
    }

    @Test
    void parsesTheBaseForAnotherDescriptorInstance() throws Exception {
        Descriptors.Descriptor serializers = descriptor(BASE);
        Descriptors.Descriptor readers = descriptor(BASE);
        DynamicMessage base = base(serializers);

        Message fromMessage = SerializedRow.ofMessage(base).getBase(readers);
        Message fromBytes = SerializedRow.ofBytes(base.toByteString()).getBase(readers);

        assertThat(fromMessage).isNotSameAs(base);
        assertThat(fromMessage.getDescriptorForType()).isSameAs(readers);
        assertThat(fromMessage.toByteString()).isEqualTo(base.toByteString());
        assertThat(fromBytes.getDescriptorForType()).isSameAs(readers);
        assertThat(fromBytes.toByteString()).isEqualTo(base.toByteString());
    }

    /**
     * An unknown field under a declared number is encoded after the declared value, so the bytes
     * decode to the unknown field's value; the base must be the row the bytes encode (#2047).
     */
    @Test
    void parsesAMessageWhoseUnknownFieldCarriesADeclaredNumber() throws Exception {
        Descriptors.Descriptor descriptor = descriptor(BASE);
        int id = descriptor.findFieldByName("id").getNumber();
        DynamicMessage base =
                base(descriptor).toBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                id,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addVarint(9L)
                                                        .build())
                                        .build())
                        .build();

        Message read = SerializedRow.ofMessage(base).getBase(descriptor);

        assertThat(base.getField(descriptor.findFieldByName("id"))).isEqualTo(7L);
        assertThat(read.getField(descriptor.findFieldByName("id"))).isEqualTo(9L);
        assertThat(read).isEqualTo(DynamicMessage.parseFrom(descriptor, base.toByteString()));
    }

    @Test
    void reportsBaseBytesTheReaderCannotParse() {
        Descriptors.Descriptor descriptor = descriptor(BASE);
        // A field-1 tag with a varint that never terminates.
        ByteString truncated = ByteString.copyFrom(new byte[] {0x08, (byte) 0x80});

        assertThatThrownBy(() -> SerializedRow.ofBytes(truncated).getBase(descriptor))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void rereadsATailBuiltAgainstAnotherAugmentedDescriptorInstance() throws Exception {
        Descriptors.Descriptor built = descriptor(AUGMENTED);
        Descriptors.Descriptor held = descriptor(AUGMENTED);
        DynamicMessage tail = tail(built);
        SerializedRow row = SerializedRow.ofMessage(base(descriptor(BASE))).withTail(tail);

        assertThat(row.getTail(built)).isSameAs(tail);
        DynamicMessage reread = row.getTail(held);
        assertThat(reread.getDescriptorForType()).isSameAs(held);
        assertThat(reread.getField(held.findFieldByName("source"))).isEqualTo("s");
        assertThat(SerializedRow.ofMessage(base(descriptor(BASE))).getTail(held)).isNull();
    }

    /**
     * A tail sets none of the serializer's fields, so it is never initialized when the descriptor
     * has a {@code REQUIRED} one; re-reading it must not require that.
     */
    @Test
    void rereadsATailWhoseDescriptorHasARequiredSerializerField() throws Exception {
        TableSchema required =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("id")
                                        .setType(TableFieldSchema.Type.INT64)
                                        .setMode(TableFieldSchema.Mode.REQUIRED))
                        .addFields(field("source", TableFieldSchema.Type.STRING))
                        .build();
        Descriptors.Descriptor built = descriptor(required);
        Descriptors.Descriptor held = descriptor(required);
        SerializedRow row = SerializedRow.ofBytes(ByteString.EMPTY).withTail(tail(built));

        DynamicMessage reread = row.getTail(held);

        assertThat(reread.getField(held.findFieldByName("source"))).isEqualTo("s");
    }

    @Test
    void encodesTheBaseFollowedByTheTailAndCountsBothWithoutEncoding() throws Exception {
        Descriptors.Descriptor baseDescriptor = descriptor(BASE);
        Descriptors.Descriptor augmented = descriptor(AUGMENTED);
        DynamicMessage base = base(baseDescriptor);
        DynamicMessage tail = tail(augmented);

        for (SerializedRow row :
                new SerializedRow[] {
                    SerializedRow.ofMessage(base).withTail(tail),
                    SerializedRow.ofBytes(base.toByteString()).withTail(tail)
                }) {
            ByteString bytes = row.toByteString();
            DynamicMessage decoded = DynamicMessage.parseFrom(augmented, bytes);

            assertThat(bytes).isEqualTo(base.toByteString().concat(tail.toByteString()));
            assertThat(row.getSerializedSize()).isEqualTo(bytes.size());
            assertThat(decoded.getField(augmented.findFieldByName("id"))).isEqualTo(7L);
            assertThat(decoded.getField(augmented.findFieldByName("name"))).isEqualTo("n");
            assertThat(decoded.getField(augmented.findFieldByName("source"))).isEqualTo("s");
        }
        assertThat(SerializedRow.ofMessage(base).toByteString()).isEqualTo(base.toByteString());
        assertThat(SerializedRow.ofMessage(base).getSerializedSize())
                .isEqualTo(base.getSerializedSize());
    }

    @Test
    void reportsWhetherWellFormedBaseBytesCarryAGivenNumber() throws Exception {
        Descriptors.Descriptor descriptor = descriptor(BASE);
        ByteString bytes = base(descriptor).toByteString();

        assertThat(SerializedRow.ofBytes(bytes).checkWellFormed(descriptor, new int[] {9, 2}))
                .isTrue();
        assertThat(SerializedRow.ofBytes(bytes).checkWellFormed(descriptor, new int[] {9, 10}))
                .isFalse();
    }

    /**
     * A message of the serializer's own descriptor instance without unknown fields can carry no
     * number chosen outside that descriptor, so it is not walked; the numbers given here are the
     * descriptor's own only to show that the walk did not run.
     */
    @Test
    void trustsAMessageOfTheSerializersOwnDescriptorWithoutUnknownFields() throws Exception {
        Descriptors.Descriptor descriptor = descriptor(BASE);

        assertThat(
                        SerializedRow.ofMessage(base(descriptor))
                                .checkWellFormed(descriptor, new int[] {1}))
                .isFalse();
    }

    /**
     * A serializer outside the connector may hand over a message of another descriptor, whose own
     * fields can use a number the tail will set; that message is walked as its bytes would be.
     */
    @Test
    void walksAMessageOfAnotherDescriptorInstance() throws Exception {
        Descriptors.Descriptor serializers = descriptor(BASE);
        DynamicMessage base = base(descriptor(BASE));

        assertThat(SerializedRow.ofMessage(base).checkWellFormed(serializers, new int[] {9, 2}))
                .isTrue();
        assertThat(SerializedRow.ofMessage(base).checkWellFormed(serializers, new int[] {9, 10}))
                .isFalse();
    }

    /** An unknown field of the serializer's own descriptor can carry a number the tail will set. */
    @Test
    void walksAMessageThatCarriesUnknownFields() throws Exception {
        Descriptors.Descriptor descriptor = descriptor(BASE);
        DynamicMessage base =
                base(descriptor).toBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                9,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addVarint(1L)
                                                        .build())
                                        .build())
                        .build();
        SerializedRow row = SerializedRow.ofMessage(base);

        assertThat(row.checkWellFormed(descriptor, new int[] {9})).isTrue();
        assertThat(row.checkWellFormed(descriptor, new int[] {10})).isFalse();
    }

    /**
     * Protobuf sizes a message as an unchecked {@code int}, so shared {@code ByteString} references
     * past 2 GiB report a negative size; the row must read as over every limit instead.
     */
    @Test
    void reportsASizeProtobufOverflowedAsOverEveryLimit() {
        Descriptors.Descriptor descriptor =
                descriptor(
                        TableSchema.newBuilder()
                                .addFields(
                                        TableFieldSchema.newBuilder()
                                                .setName("blobs")
                                                .setType(TableFieldSchema.Type.BYTES)
                                                .setMode(TableFieldSchema.Mode.REPEATED))
                                .build());
        ByteString mebibyte = ByteString.copyFrom(new byte[1 << 20]);
        DynamicMessage.Builder huge = DynamicMessage.newBuilder(descriptor);
        for (int i = 0; i < 2048; i++) {
            huge.addRepeatedField(descriptor.findFieldByName("blobs"), mebibyte);
        }

        DynamicMessage overflowing = huge.build();

        assertThat(SerializedRow.ofMessage(overflowing).getSerializedSize())
                .isEqualTo(Long.MAX_VALUE);
        // The tail can overflow on its own under a small base.
        assertThat(
                        SerializedRow.ofBytes(ByteString.copyFromUtf8("x"))
                                .withTail(overflowing)
                                .getSerializedSize())
                .isEqualTo(Long.MAX_VALUE);
    }

    private static DynamicMessage base(Descriptors.Descriptor descriptor) {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 7L)
                .setField(descriptor.findFieldByName("name"), "n")
                .build();
    }

    private static DynamicMessage tail(Descriptors.Descriptor augmented) {
        return DynamicMessage.newBuilder(augmented)
                .setField(augmented.findFieldByName("source"), "s")
                .buildPartial();
    }

    private static TableFieldSchema field(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static Descriptors.Descriptor descriptor(TableSchema schema) {
        try {
            return BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }
}
