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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnknownFieldSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 *
 * <p>Extension numbers in protobuf's private range have no registry, so two annotation protos can
 * pick the same one independently. When the caller knows the option's full name, a declaration
 * found under a different name is therefore treated as a <em>different option</em> that merely
 * shares the number, and the field is left alone rather than becoming a marked column. The value's
 * own options message never carries that name, but the extension's <em>declaration</em> is usually
 * reachable — either as the known extension itself, or through the descriptor's transitive file
 * dependencies, which a {@code FileDescriptorSet} normally includes even though protobuf will not
 * use them to resolve the value.
 *
 * <p>The name rules out a declaration that is <em>not</em> the expected one. It cannot arbitrate
 * between two rival declarations that are both in the pool: an unresolved option records only its
 * number, so nothing says which of them it was written against. Only a resolved extension — the
 * generated class on the classpath — carries that identity in the value itself.
 */
@Internal
final class BoolFieldOptionReader {

    private static final String FIELD_OPTIONS =
            DescriptorProtos.FieldOptions.getDescriptor().getFullName();

    private BoolFieldOptionReader() {}

    /**
     * Returns whether the given field carries the boolean field option with the given extension
     * number, set to {@code true}. An absent option, and an option explicitly set to {@code false},
     * both return {@code false}.
     *
     * @param field the field whose options are inspected
     * @param extensionNumber the extension number of the option within {@code
     *     google.protobuf.FieldOptions}
     * @param expectedName the option's expected full name, or {@code null} when the caller supplied
     *     only a number. When given, a declaration found under a different name is a different
     *     option sharing the number and yields {@code false}
     * @return whether the option is present and true
     * @throws IllegalArgumentException if an option with that number is present, is not ruled out
     *     by {@code expectedName}, and is not a singular boolean. Where the declaration cannot be
     *     resolved at all, only the encoding is available, so an integer option holding 0 or 1 is
     *     indistinguishable from a bool and is accepted.
     */
    static boolean isSetToTrue(
            Descriptors.FieldDescriptor field, int extensionNumber, String expectedName) {
        DescriptorProtos.FieldOptions options = field.getOptions();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                options.getAllFields().entrySet()) {
            Descriptors.FieldDescriptor option = entry.getKey();
            if (option.isExtension() && option.getNumber() == extensionNumber) {
                if (isDifferentOption(option, expectedName)) {
                    return false;
                }
                checkIsSingularBool(option, field, extensionNumber);
                return (Boolean) entry.getValue();
            }
        }
        UnknownFieldSet unknownFields = options.getUnknownFields();
        if (!unknownFields.hasField(extensionNumber)) {
            return false;
        }
        // The value is unresolved, but the declaration may still be in the pool. When it is, it
        // gives both the name — so a collision on the number can be ruled out — and the declared
        // type, which is a stricter check than the encoding can offer.
        Descriptors.FieldDescriptor declaration =
                findDeclaration(field, extensionNumber, expectedName);
        List<Long> varints = unknownFields.getField(extensionNumber).getVarintList();
        if (declaration != null) {
            if (isDifferentOption(declaration, expectedName)) {
                return false;
            }
            checkIsSingularBool(declaration, field, extensionNumber);
        } else {
            // With no declaration the encoding is all there is to go on, so it has to stand in for
            // the type check the descriptor would otherwise give — else the same .proto would be
            // accepted or rejected depending only on how the user obtained the descriptor. A
            // singular bool is one varint of 0 or 1; a repeated option, an enum, an integer outside
            // {0, 1}, and anything length-delimited or fixed-width are a *different* option at this
            // number and must not be read as true. An integer option holding 0 or 1 stays
            // indistinguishable, which is irreducible without the declared type.
            Preconditions.checkArgument(
                    varints.size() == 1 && (varints.get(0) == 0L || varints.get(0) == 1L),
                    "Field option number %s on field %s is not encoded as a singular bool and so is"
                            + " a different option; a field option read by this connector must be"
                            + " declared as 'optional bool ... = %s'",
                    extensionNumber,
                    field.getFullName(),
                    extensionNumber);
        }
        // Protobuf allows a singular scalar to appear more than once on the wire and keeps the last
        // occurrence, so once the declaration has proven the option is a bool this must not insist
        // on exactly one varint.
        return !varints.isEmpty() && varints.get(varints.size() - 1) != 0L;
    }

    /**
     * Returns whether the given declaration is a different option that merely shares the configured
     * number. Always {@code false} when the caller supplied no name, since then the number is the
     * only identity available.
     */
    private static boolean isDifferentOption(
            Descriptors.FieldDescriptor declaration, String expectedName) {
        return expectedName != null && !expectedName.equals(declaration.getFullName());
    }

    private static void checkIsSingularBool(
            Descriptors.FieldDescriptor declaration,
            Descriptors.FieldDescriptor field,
            int extensionNumber) {
        Preconditions.checkArgument(
                !declaration.isRepeated()
                        && declaration.getJavaType()
                                == Descriptors.FieldDescriptor.JavaType.BOOLEAN,
                "Field option %s (number %s) on field %s is not a singular bool but %s%s; a field"
                        + " option read by this connector must be declared as 'optional bool ... ="
                        + " %s'",
                declaration.getFullName(),
                extensionNumber,
                field.getFullName(),
                declaration.isRepeated() ? "repeated " : "",
                declaration.getJavaType(),
                extensionNumber);
    }

    /**
     * Looks for the extension's declaration among the field's own file and its transitive
     * dependencies. protobuf will not use those to resolve the option's <em>value</em>, but the
     * declaration itself is there whenever the annotations proto travelled with the schema, which
     * is the usual shape of a {@code FileDescriptorSet}.
     *
     * <p>Both file-level and message-scoped {@code extend} blocks are searched: nesting the block
     * inside a scoping message is a common way to keep an annotation out of the package namespace,
     * and missing it would quietly drop the option back to the encoding heuristic.
     *
     * <p>Nothing stops a pool from holding two declarations at the same number — protoc rejects
     * that within one compilation, but a merged pool need not — so a declaration carrying the name
     * the caller expects wins over one that merely matches the number.
     *
     * @return the declaration, or {@code null} if the annotations proto is not in the pool
     */
    private static Descriptors.FieldDescriptor findDeclaration(
            Descriptors.FieldDescriptor field, int extensionNumber, String expectedName) {
        Set<String> visited = new HashSet<>();
        Deque<Descriptors.FileDescriptor> pendingFiles = new ArrayDeque<>();
        pendingFiles.push(field.getFile());
        Descriptors.FieldDescriptor byNumber = null;
        while (!pendingFiles.isEmpty()) {
            Descriptors.FileDescriptor file = pendingFiles.pop();
            // A FileDescriptor can only be built from already-built dependencies, so the graph is a
            // DAG and this only saves repeated work on diamond imports.
            if (!visited.add(file.getFullName())) {
                continue;
            }
            for (Descriptors.FieldDescriptor candidate : extensionsOf(file)) {
                if (candidate.getNumber() != extensionNumber
                        || !FIELD_OPTIONS.equals(candidate.getContainingType().getFullName())) {
                    continue;
                }
                if (expectedName != null && expectedName.equals(candidate.getFullName())) {
                    return candidate;
                }
                if (byNumber == null) {
                    byNumber = candidate;
                }
            }
            pendingFiles.addAll(file.getDependencies());
        }
        return byNumber;
    }

    /** Every extension declared in the file, at file level or nested inside any message. */
    private static List<Descriptors.FieldDescriptor> extensionsOf(Descriptors.FileDescriptor file) {
        List<Descriptors.FieldDescriptor> extensions = new ArrayList<>(file.getExtensions());
        Deque<Descriptors.Descriptor> pending = new ArrayDeque<>(file.getMessageTypes());
        while (!pending.isEmpty()) {
            Descriptors.Descriptor message = pending.pop();
            extensions.addAll(message.getExtensions());
            pending.addAll(message.getNestedTypes());
        }
        return extensions;
    }
}
