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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Adds fields to a protobuf row descriptor without changing its surrounding file structure. */
@Internal
public final class ProtoDescriptorAugmenter {

    private static final int MAX_FIELD_NUMBER = (1 << 29) - 1;
    private static final int PROTOBUF_RESERVED_START = 19_000;
    private static final int PROTOBUF_RESERVED_END = 20_000;

    private ProtoDescriptorAugmenter() {}

    /**
     * Returns {@code base} with the unnumbered {@code fields} appended to its selected message.
     *
     * <p>The assigned numbers follow the existing maximum while skipping protobuf-global,
     * message-reserved, and extension ranges. The rebuilt descriptor retains nested-message
     * placement and file dependencies. {@code fieldDescription} identifies an added field in a
     * collision error.
     */
    public static Descriptors.Descriptor augment(
            Descriptors.Descriptor base,
            List<FieldDescriptorProto> fields,
            String fieldDescription) {
        DescriptorProto augmented = addFields(base.toProto(), fields, fieldDescription);
        List<String> descriptorPath = descriptorPath(base);
        FileDescriptorProto.Builder file = base.getFile().toProto().toBuilder();
        int topLevelIndex = findTopLevelMessage(file, descriptorPath.get(0));
        file.setMessageType(
                topLevelIndex,
                replaceNested(file.getMessageType(topLevelIndex), descriptorPath, 1, augmented));

        try {
            Descriptors.FileDescriptor[] dependencies =
                    base.getFile().getDependencies().toArray(new Descriptors.FileDescriptor[0]);
            Descriptors.FileDescriptor rebuilt =
                    Descriptors.FileDescriptor.buildFrom(file.build(), dependencies);
            Descriptors.Descriptor result = rebuilt.findMessageTypeByName(descriptorPath.get(0));
            for (int i = 1; i < descriptorPath.size(); i++) {
                result = result.findNestedTypeByName(descriptorPath.get(i));
            }
            if (result == null) {
                throw new IllegalStateException(
                        "The rebuilt augmented row descriptor was not found");
            }
            return result;
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to augment the protobuf row descriptor", e);
        }
    }

    private static DescriptorProto addFields(
            DescriptorProto base, List<FieldDescriptorProto> fields, String fieldDescription) {
        validateFields(base, fields, fieldDescription);
        int previousNumber =
                base.getFieldList().stream()
                        .mapToInt(FieldDescriptorProto::getNumber)
                        .max()
                        .orElse(0);
        DescriptorProto.Builder result = base.toBuilder();
        for (FieldDescriptorProto field : fields) {
            previousNumber = nextFieldNumber(base, previousNumber);
            result.addField(field.toBuilder().setNumber(previousNumber));
        }
        return result.build();
    }

    private static void validateFields(
            DescriptorProto base, List<FieldDescriptorProto> additions, String fieldDescription) {
        List<FieldDescriptorProto> seen = new ArrayList<>(base.getFieldList());
        for (FieldDescriptorProto addition : additions) {
            if (addition.getNumber() != 0) {
                throw new IllegalArgumentException(
                        "An augmented protobuf field must not declare its own field number: "
                                + addition.getName());
            }
            for (FieldDescriptorProto field : seen) {
                if (field.getName().equalsIgnoreCase(addition.getName())) {
                    throw new IllegalStateException(
                            "The physical row descriptor must not declare "
                                    + fieldDescription
                                    + " "
                                    + addition.getName());
                }
            }
            seen.add(addition);
        }
    }

    private static int nextFieldNumber(DescriptorProto descriptor, int after) {
        long candidate = (long) after + 1;
        while (candidate <= MAX_FIELD_NUMBER) {
            if (candidate >= PROTOBUF_RESERVED_START && candidate < PROTOBUF_RESERVED_END) {
                candidate = PROTOBUF_RESERVED_END;
                continue;
            }
            int number = (int) candidate;
            int rangeEnd = forbiddenRangeEnd(descriptor, number);
            if (rangeEnd > number) {
                candidate = rangeEnd;
                continue;
            }
            return number;
        }
        throw new IllegalStateException(
                "The protobuf row descriptor has no field number available for augmentation");
    }

    private static int forbiddenRangeEnd(DescriptorProto descriptor, int number) {
        for (DescriptorProto.ReservedRange range : descriptor.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return range.getEnd();
            }
        }
        for (DescriptorProto.ExtensionRange range : descriptor.getExtensionRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return range.getEnd();
            }
        }
        return number;
    }

    private static List<String> descriptorPath(Descriptors.Descriptor descriptor) {
        List<String> reversed = new ArrayList<>();
        for (Descriptors.Descriptor current = descriptor;
                current != null;
                current = current.getContainingType()) {
            reversed.add(current.getName());
        }
        Collections.reverse(reversed);
        return reversed;
    }

    private static int findTopLevelMessage(FileDescriptorProto.Builder file, String name) {
        for (int i = 0; i < file.getMessageTypeCount(); i++) {
            if (file.getMessageType(i).getName().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("The base row descriptor is absent from its file");
    }

    private static DescriptorProto replaceNested(
            DescriptorProto current,
            List<String> descriptorPath,
            int pathIndex,
            DescriptorProto replacement) {
        if (pathIndex == descriptorPath.size()) {
            return replacement;
        }
        DescriptorProto.Builder result = current.toBuilder();
        String nestedName = descriptorPath.get(pathIndex);
        for (int i = 0; i < current.getNestedTypeCount(); i++) {
            if (current.getNestedType(i).getName().equals(nestedName)) {
                result.setNestedType(
                        i,
                        replaceNested(
                                current.getNestedType(i),
                                descriptorPath,
                                pathIndex + 1,
                                replacement));
                return result.build();
            }
        }
        throw new IllegalStateException("The base nested row descriptor is absent from its file");
    }
}
