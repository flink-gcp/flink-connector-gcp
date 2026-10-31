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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Adds fields to a protobuf row descriptor without changing its surrounding file structure. */
@Internal
public final class ProtoDescriptorAugmenter {

    /**
     * The number the appended fields count down from: the highest field number whose tag still
     * encodes in three bytes. A base descriptor that grows additively takes the numbers after its
     * own highest one, so it reaches an appended field's number only once that highest number nears
     * this one: past 261,000 fields for a descriptor numbered from one, while a BigQuery table
     * holds at most 10,000 columns. A message whose extension range covers this number gets the
     * numbers just below that range instead, which its own fields reach sooner, and a message with
     * no free number at or below it gets numbers above both it and its own fields.
     */
    static final int AUGMENTATION_CEILING = (1 << 18) - 1;

    private static final int MAX_FIELD_NUMBER = (1 << 29) - 1;

    private static final int PROTOBUF_RESERVED_START = 19_000;
    private static final int PROTOBUF_RESERVED_END = 20_000;

    private ProtoDescriptorAugmenter() {}

    /**
     * Returns {@code base} with the unnumbered {@code fields} appended to its selected message.
     *
     * <p>The assigned numbers count down from {@code AUGMENTATION_CEILING} in declaration order,
     * skipping the base's own field numbers and the protobuf-global, message-reserved, and
     * extension ranges. They depend on the base descriptor only where it claims a number in that
     * region, so a base that evolves by appending fields after its highest number leaves them
     * unchanged, and a row serialized before the evolution keeps its appended fields under the
     * evolved descriptor. When no number at or below the ceiling is free, the remaining fields take
     * the free numbers above both the ceiling and the base's highest field number. The rebuilt
     * descriptor retains nested-message placement and file dependencies. {@code fieldDescription}
     * identifies an added field in a collision error.
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
        Set<Integer> taken = new HashSet<>();
        for (FieldDescriptorProto field : base.getFieldList()) {
            taken.add(field.getNumber());
        }
        DescriptorProto.Builder result = base.toBuilder();
        int previousNumber = AUGMENTATION_CEILING + 1;
        boolean countingUp = false;
        for (FieldDescriptorProto field : fields) {
            int number = countingUp ? 0 : previousFieldNumber(base, taken, previousNumber);
            if (number == 0) {
                // Nothing free at or below the ceiling: continue above it and the base's own
                // fields, as the numbering did before it counted down, rather than refuse a
                // descriptor that has numbers free.
                if (!countingUp) {
                    countingUp = true;
                    previousNumber = Math.max(AUGMENTATION_CEILING, highest(taken));
                }
                number = nextFieldNumber(base, previousNumber);
            }
            previousNumber = number;
            result.addField(field.toBuilder().setNumber(number));
        }
        return result.build();
    }

    private static int highest(Set<Integer> numbers) {
        int highest = 0;
        for (int number : numbers) {
            highest = Math.max(highest, number);
        }
        return highest;
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

    private static int previousFieldNumber(
            DescriptorProto descriptor, Set<Integer> taken, int before) {
        int candidate = before - 1;
        while (candidate >= 1) {
            int below = belowForbiddenRange(descriptor, candidate);
            if (below != candidate) {
                candidate = below;
            } else if (taken.contains(candidate)) {
                candidate--;
            } else {
                return candidate;
            }
        }
        return 0;
    }

    /**
     * Returns the first number above {@code after} outside the protobuf-reserved, message-reserved,
     * and extension ranges. {@code after} is at least the base's highest field number, so no base
     * field holds a number above it.
     */
    private static int nextFieldNumber(DescriptorProto descriptor, int after) {
        long candidate = (long) after + 1;
        while (candidate <= MAX_FIELD_NUMBER) {
            int number = (int) candidate;
            int above = aboveForbiddenRange(descriptor, number);
            if (above == number) {
                return number;
            }
            candidate = above;
        }
        throw new IllegalStateException(
                "The protobuf row descriptor has no field number available for augmentation");
    }

    /**
     * Returns the end of the protobuf-reserved, message-reserved, or extension range holding {@code
     * number}, which is the first number above it, or {@code number} itself when no range holds it.
     */
    private static int aboveForbiddenRange(DescriptorProto descriptor, int number) {
        if (number >= PROTOBUF_RESERVED_START && number < PROTOBUF_RESERVED_END) {
            return PROTOBUF_RESERVED_END;
        }
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

    /**
     * Returns the number just below the protobuf-reserved, message-reserved, or extension range
     * holding {@code number}, or {@code number} itself when no range holds it. Both message ranges'
     * ends are exclusive in a {@link DescriptorProto}.
     */
    private static int belowForbiddenRange(DescriptorProto descriptor, int number) {
        if (number >= PROTOBUF_RESERVED_START && number < PROTOBUF_RESERVED_END) {
            return PROTOBUF_RESERVED_START - 1;
        }
        for (DescriptorProto.ReservedRange range : descriptor.getReservedRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return range.getStart() - 1;
            }
        }
        for (DescriptorProto.ExtensionRange range : descriptor.getExtensionRangeList()) {
            if (number >= range.getStart() && number < range.getEnd()) {
                return range.getStart() - 1;
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
