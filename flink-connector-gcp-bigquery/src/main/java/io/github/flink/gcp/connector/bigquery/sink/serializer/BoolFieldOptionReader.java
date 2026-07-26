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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnknownFieldSet;

import java.util.List;
import java.util.Map;

/**
 * Reads a boolean custom option off a protobuf field by its {@code google.protobuf.FieldOptions}
 * extension number, without requiring the generated extension class.
 *
 * <p>The same annotation reaches the connector in one of two forms, depending on how the descriptor
 * was obtained, and both have to be handled:
 *
 * <ul>
 *   <li>As a <em>known</em> extension, when the generated extension class is on the classpath and
 *       registered it. The value is then a regular extension field of the options message and is
 *       reachable by number through {@code getAllFields()} — no {@code ExtensionRegistry} and no
 *       compile-time dependency on the generated class is needed to find it.
 *   <li>As an <em>unknown</em> field, when the descriptor was built from a serialized {@code
 *       FileDescriptorSet}. protobuf-java does not resolve custom options against the descriptor
 *       pool — not even when the file declaring the extension is a direct dependency of the file
 *       being built — so the value stays in {@code getUnknownFields()}. For descriptors that arrive
 *       as bytes this is the normal case rather than a fallback.
 * </ul>
 */
@Internal
final class BoolFieldOptionReader {

    private BoolFieldOptionReader() {}

    /**
     * Returns whether the given field carries the boolean field option with the given extension
     * number, set to {@code true}. An absent option, and an option explicitly set to {@code false},
     * both return {@code false}.
     *
     * @param field the field whose options are inspected
     * @param extensionNumber the extension number of the option within {@code
     *     google.protobuf.FieldOptions}
     * @return whether the option is present and true
     * @throws IllegalArgumentException if an option with that number is present but is not a
     *     singular boolean. Where the option is an unknown field only its encoding is available, so
     *     an integer option holding 0 or 1 is indistinguishable from a bool and is accepted.
     */
    static boolean isSetToTrue(Descriptors.FieldDescriptor field, int extensionNumber) {
        DescriptorProtos.FieldOptions options = field.getOptions();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                options.getAllFields().entrySet()) {
            Descriptors.FieldDescriptor option = entry.getKey();
            if (option.isExtension() && option.getNumber() == extensionNumber) {
                Preconditions.checkArgument(
                        !option.isRepeated()
                                && option.getJavaType()
                                        == Descriptors.FieldDescriptor.JavaType.BOOLEAN,
                        "Field option %s (number %s) on field %s is not a singular bool but %s%s;"
                                + " a JSON field option must be declared as"
                                + " 'optional bool ... = %s'",
                        option.getFullName(),
                        extensionNumber,
                        field.getFullName(),
                        option.isRepeated() ? "repeated " : "",
                        option.getJavaType(),
                        extensionNumber);
                return (Boolean) entry.getValue();
            }
        }
        UnknownFieldSet unknownFields = options.getUnknownFields();
        if (!unknownFields.hasField(extensionNumber)) {
            return false;
        }
        List<Long> varints = unknownFields.getField(extensionNumber).getVarintList();
        // Here the encoding is all there is to go on, so it has to carry the type check that the
        // known-extension path gets from the descriptor — otherwise the same .proto would be
        // accepted or rejected depending only on how the user obtained the descriptor. A singular
        // bool is exactly one varint of 0 or 1; a repeated option, an enum, an integer outside
        // {0, 1}, and anything length-delimited or fixed-width are all a *different* option at this
        // number and must not be read as true. An integer option holding 0 or 1 stays
        // indistinguishable from a bool, which is irreducible without the declared type.
        Preconditions.checkArgument(
                varints.size() == 1 && (varints.get(0) == 0L || varints.get(0) == 1L),
                "Field option number %s on field %s is not encoded as a singular bool and so is a"
                        + " different option; a JSON field option must be declared as"
                        + " 'optional bool ... = %s'",
                extensionNumber,
                field.getFullName(),
                extensionNumber);
        return varints.get(0) != 0L;
    }
}
