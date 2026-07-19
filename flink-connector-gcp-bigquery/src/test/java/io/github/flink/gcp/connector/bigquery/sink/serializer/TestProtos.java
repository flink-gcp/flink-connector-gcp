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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TimestampProto;

/**
 * Programmatically built test descriptors (no protoc code generation needed): a proto3 file with an
 * {@code AllTypes} message covering the whole type-mapping matrix, plus a recursive message.
 */
final class TestProtos {

    private TestProtos() {}

    static Descriptors.Descriptor allTypes() {
        return file().findMessageTypeByName("AllTypes");
    }

    static Descriptors.Descriptor recursive() {
        return file().findMessageTypeByName("Recursive");
    }

    private static Descriptors.FileDescriptor file() {
        try {
            return Descriptors.FileDescriptor.buildFrom(
                    fileProto(), new Descriptors.FileDescriptor[] {TimestampProto.getDescriptor()});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static DescriptorProtos.FileDescriptorProto fileProto() {
        DescriptorProtos.EnumDescriptorProto color =
                DescriptorProtos.EnumDescriptorProto.newBuilder()
                        .setName("Color")
                        .addValue(enumValue("COLOR_UNSPECIFIED", 0))
                        .addValue(enumValue("RED", 1))
                        .addValue(enumValue("BLUE", 2))
                        .build();

        DescriptorProtos.DescriptorProto nested =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Nested")
                        .addField(
                                scalar(
                                        "s",
                                        1,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(
                                scalar(
                                        "n",
                                        2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .build();

        DescriptorProtos.DescriptorProto mapEntry =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("FMapEntry")
                        .setOptions(
                                DescriptorProtos.MessageOptions.newBuilder()
                                        .setMapEntry(true)
                                        .build())
                        .addField(
                                scalar(
                                        "key",
                                        1,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(
                                scalar(
                                        "value",
                                        2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .build();

        DescriptorProtos.DescriptorProto allTypes =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("AllTypes")
                        .addNestedType(mapEntry)
                        .addField(
                                scalar(
                                        "f_int32",
                                        1,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                        .addField(
                                scalar(
                                        "f_int64",
                                        2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                        .addField(
                                scalar(
                                        "f_uint32",
                                        3,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32))
                        .addField(
                                scalar(
                                        "f_uint64",
                                        4,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64))
                        .addField(
                                scalar(
                                        "f_float",
                                        5,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT))
                        .addField(
                                scalar(
                                        "f_double",
                                        6,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE))
                        .addField(
                                scalar(
                                        "f_bool",
                                        7,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                        .addField(
                                scalar(
                                        "f_string",
                                        8,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(
                                scalar(
                                        "f_bytes",
                                        9,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))
                        .addField(
                                message("f_enum", 10, ".test.Color", false).toBuilder()
                                        .setType(
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_ENUM)
                                        .build())
                        .addField(message("f_ts", 11, ".google.protobuf.Timestamp", false))
                        .addField(message("f_nested", 12, ".test.Nested", false))
                        .addField(
                                scalar(
                                                "f_rep_string",
                                                13,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING)
                                        .toBuilder()
                                        .setLabel(
                                                DescriptorProtos.FieldDescriptorProto.Label
                                                        .LABEL_REPEATED)
                                        .build())
                        .addField(message("f_map", 14, ".test.AllTypes.FMapEntry", true))
                        .addField(message("f_json", 15, ".test.Nested", false))
                        .build();

        DescriptorProtos.DescriptorProto recursive =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Recursive")
                        .addField(message("child", 1, ".test.Recursive", false))
                        .build();

        return DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .setPackage("test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addEnumType(color)
                .addMessageType(nested)
                .addMessageType(allTypes)
                .addMessageType(recursive)
                .build();
    }

    private static DescriptorProtos.FieldDescriptorProto scalar(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    private static DescriptorProtos.FieldDescriptorProto message(
            String name, int number, String typeName, boolean repeated) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(typeName)
                .setLabel(
                        repeated
                                ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    private static DescriptorProtos.EnumValueDescriptorProto enumValue(String name, int number) {
        return DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .build();
    }
}
