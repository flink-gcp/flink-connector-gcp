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

    @Test
    void preservesNestedPlacementAndDependenciesWhileAllocatingFieldNumbers() throws Exception {
        Descriptors.FileDescriptor dependency = dependency();
        DescriptorProto row =
                DescriptorProto.newBuilder()
                        .setName("Row")
                        .addField(messageField("shared", 18_999, ".dependency.Shared"))
                        .addReservedRange(
                                DescriptorProto.ReservedRange.newBuilder()
                                        .setStart(20_000)
                                        .setEnd(20_002))
                        .addExtensionRange(
                                DescriptorProto.ExtensionRange.newBuilder()
                                        .setStart(20_002)
                                        .setEnd(20_004))
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
        assertThat(augmented.findFieldByName("first").getNumber()).isEqualTo(20_004);
        assertThat(augmented.findFieldByName("second").getNumber()).isEqualTo(20_005);
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
        DescriptorProto row =
                DescriptorProto.newBuilder().setName("Row").addAllField(List.of(fields)).build();
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
