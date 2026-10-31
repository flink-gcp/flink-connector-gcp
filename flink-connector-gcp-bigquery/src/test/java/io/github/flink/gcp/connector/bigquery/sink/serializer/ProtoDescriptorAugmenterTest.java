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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoDescriptorAugmenterTest {

    private static final int CEILING = ProtoDescriptorAugmenter.AUGMENTATION_CEILING;

    private static final int MAX_FIELD_NUMBER = (1 << 29) - 1;

    @Test
    void preservesNestedPlacementAndDependenciesWhileAllocatingFieldNumbers() throws Exception {
        Descriptors.FileDescriptor dependency = dependency();
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(messageField("shared", CEILING, ".dependency.Shared"))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(CEILING - 2)
                                        .setEnd(CEILING))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(CEILING - 4)
                                        .setEnd(CEILING - 2))
                        .build();
        DescriptorProto envelope =
                DescriptorProto.newBuilder().setName("Envelope").addNestedType(row).build();
        Descriptors.FileDescriptor file =
                Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("row.proto")
                                .addDependency(dependency.getName())
                                .addMessageType(envelope)
                                .build(),
                        new Descriptors.FileDescriptor[] {dependency});
        Descriptors.Descriptor base =
                file.findMessageTypeByName("Envelope").findNestedTypeByName("Row");

        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        base,
                        List.of(
                                field("first", FieldDescriptorProto.Type.TYPE_STRING),
                                field("second", FieldDescriptorProto.Type.TYPE_INT64)),
                        "test field");

        assertThat(augmented.getContainingType().getName()).isEqualTo("Envelope");
        assertThat(augmented.findFieldByName("shared").getMessageType().getFullName())
                .isEqualTo("dependency.Shared");
        assertThat(augmented.getFile().getDependencies())
                .extracting(Descriptors.FileDescriptor::getName)
                .containsExactly("dependency.proto");
        // Counted down from the ceiling, past the base field, the reserved and the extension range.
        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(CEILING - 5);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(CEILING - 6);
    }

    @Test
    void appendedFieldNumbersDoNotFollowTheBaseFields() throws Exception {
        List<FieldDescriptorProto> additions =
                List.of(
                        field("first", FieldDescriptorProto.Type.TYPE_STRING),
                        field("second", FieldDescriptorProto.Type.TYPE_INT64));

        Descriptors.Descriptor before =
                ProtoDescriptorAugmenter.augment(
                        descriptor(optionalField("a", 1), optionalField("b", 2)),
                        additions,
                        "test field");
        Descriptors.Descriptor after =
                ProtoDescriptorAugmenter.augment(
                        descriptor(
                                optionalField("a", 1),
                                optionalField("b", 2),
                                optionalField("c", 3)),
                        additions,
                        "test field");

        // The column the base appended takes number 3, which no appended field holds, so a row
        // serialized under the first descriptor reads the same under the second.
        assertThat(before.findFieldByName("first").getNumber()).isEqualTo(CEILING);
        assertThat(before.findFieldByName("second").getNumber()).isEqualTo(CEILING - 1);
        assertThat(after.findFieldByName("first").getNumber()).isEqualTo(CEILING);
        assertThat(after.findFieldByName("second").getNumber()).isEqualTo(CEILING - 1);
    }

    @Test
    void takesAFreeNumberAboveATakenOne() throws Exception {
        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        descriptor(optionalField("id", CEILING - 1)),
                        List.of(
                                field("first", FieldDescriptorProto.Type.TYPE_STRING),
                                field("second", FieldDescriptorProto.Type.TYPE_INT64)),
                        "test field");

        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(CEILING);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(CEILING - 2);
    }

    @Test
    void rangeEndsAreFreeNumbers() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(optionalField("id", 1))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(CEILING - 2)
                                        .setEnd(CEILING))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(CEILING - 5)
                                        .setEnd(CEILING - 3))
                        .build();

        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        descriptor(row),
                        List.of(
                                field("first", FieldDescriptorProto.Type.TYPE_STRING),
                                field("second", FieldDescriptorProto.Type.TYPE_STRING),
                                field("third", FieldDescriptorProto.Type.TYPE_STRING)),
                        "test field");

        // A DescriptorProto range's end is exclusive, so each range's end is free.
        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(CEILING);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(CEILING - 3);
        assertThat(augmented.findFieldByName("third").getNumber()).isEqualTo(CEILING - 6);
    }

    @Test
    void skipsTheProtobufReservedRangeBelowAnExtensionRange() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(optionalField("id", 18_999))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(20_000)
                                        .setEnd(MAX_FIELD_NUMBER + 1))
                        .build();

        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        descriptor(row),
                        List.of(field("added", FieldDescriptorProto.Type.TYPE_STRING)),
                        "test field");

        assertThat(augmented.findFieldByName("added").getNumber()).isEqualTo(18_998);
    }

    @Test
    void countsUpAboveTheBaseWhenNothingAtOrBelowTheCeilingIsFree() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(optionalField("id", 300_000))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(1)
                                        .setEnd(CEILING + 1))
                        .build();

        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        descriptor(row),
                        List.of(
                                field("first", FieldDescriptorProto.Type.TYPE_STRING),
                                field("second", FieldDescriptorProto.Type.TYPE_STRING)),
                        "test field");

        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(300_001);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(300_002);
    }

    @Test
    void countsUpAboveTheCeilingOnceTheFreeNumbersBelowItRunOut() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(optionalField("id", 1))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(3)
                                        .setEnd(CEILING + 2))
                        .build();

        Descriptors.Descriptor augmented =
                ProtoDescriptorAugmenter.augment(
                        descriptor(row),
                        List.of(
                                field("first", FieldDescriptorProto.Type.TYPE_STRING),
                                field("second", FieldDescriptorProto.Type.TYPE_STRING)),
                        "test field");

        // Number 2 is the only free one at or below the ceiling; the ceiling's successor is
        // reserved too, so the second field lands just above the reserved range.
        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(2);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(CEILING + 2);
    }

    @Test
    void failsWhenNoFieldNumberIsLeft() throws Exception {
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(1)
                                        .setEnd(MAX_FIELD_NUMBER + 1))
                        .build();

        assertThatThrownBy(
                        () ->
                                ProtoDescriptorAugmenter.augment(
                                        descriptor(row),
                                        List.of(
                                                field(
                                                        "added",
                                                        FieldDescriptorProto.Type.TYPE_STRING)),
                                        "test field"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The protobuf row descriptor has no field number available for"
                                + " augmentation");
    }

    @Test
    void rejectsCaseInsensitiveBaseAndAdditionCollisions() throws Exception {
        Descriptors.Descriptor base = descriptor(optionalField("id", 1));

        assertThatThrownBy(
                        () ->
                                ProtoDescriptorAugmenter.augment(
                                        base,
                                        List.of(field("ID", FieldDescriptorProto.Type.TYPE_STRING)),
                                        "test field"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The physical row descriptor must not declare test field ID");

        assertThatThrownBy(
                        () ->
                                ProtoDescriptorAugmenter.augment(
                                        descriptor(),
                                        List.of(
                                                field(
                                                        "value",
                                                        FieldDescriptorProto.Type.TYPE_STRING),
                                                field(
                                                        "VALUE",
                                                        FieldDescriptorProto.Type.TYPE_INT64)),
                                        "test field"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The physical row descriptor must not declare test field VALUE");
    }

    @Test
    void rejectsCallerAssignedFieldNumbers() throws Exception {
        FieldDescriptorProto numbered = optionalField("added", 7);

        assertThatThrownBy(
                        () ->
                                ProtoDescriptorAugmenter.augment(
                                        descriptor(), List.of(numbered), "test field"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "An augmented protobuf field must not declare its own field number: added");
    }

    private static Descriptors.FileDescriptor dependency() throws Exception {
        return Descriptors.FileDescriptor.buildFrom(
                FileDescriptorProto.newBuilder()
                        .setName("dependency.proto")
                        .setPackage("dependency")
                        .addMessageType(DescriptorProto.newBuilder().setName("Shared"))
                        .build(),
                new Descriptors.FileDescriptor[0]);
    }

    private static Descriptors.Descriptor descriptor(FieldDescriptorProto... fields)
            throws Exception {
        return descriptor(
                DescriptorProto.newBuilder().setName("Row").addAllField(List.of(fields)).build());
    }

    private static Descriptors.Descriptor descriptor(DescriptorProto row) throws Exception {
        Descriptors.FileDescriptor file =
                Descriptors.FileDescriptor.buildFrom(
                        FileDescriptorProto.newBuilder()
                                .setName("row.proto")
                                .addMessageType(row)
                                .build(),
                        new Descriptors.FileDescriptor[0]);
        return file.findMessageTypeByName("Row");
    }

    private static FieldDescriptorProto field(String name, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    private static FieldDescriptorProto optionalField(String name, int number) {
        return field(name, FieldDescriptorProto.Type.TYPE_STRING).toBuilder()
                .setNumber(number)
                .build();
    }

    private static FieldDescriptorProto messageField(String name, int number, String typeName) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(typeName)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }
}
