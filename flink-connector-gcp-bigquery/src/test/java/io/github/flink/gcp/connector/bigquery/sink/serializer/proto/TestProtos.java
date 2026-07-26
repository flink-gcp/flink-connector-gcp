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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import io.github.flink.gcp.connector.bigquery.testproto.AllTypes;
import io.github.flink.gcp.connector.bigquery.testproto.Presence;
import io.github.flink.gcp.connector.bigquery.testproto.Proto2Presence;
import io.github.flink.gcp.connector.bigquery.testproto.Recursive;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnown;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownEmpty;

import java.util.ArrayList;
import java.util.List;

/**
 * Test descriptors, from two sources.
 *
 * <p>Ordinary shapes come from real {@code .proto} sources under {@code src/test/protobuf},
 * compiled by protoc at build time: the {@code AllTypes} type-mapping matrix, a recursive message,
 * the well-known-type matrix, and the presence fixtures whose {@code oneof} and proto3 {@code
 * optional} spellings are what motivated the codegen (#132).
 *
 * <p>What remains here is hand-built because protoc <em>cannot produce it</em>, not as a leftover.
 * The annotation fixtures need custom options left as unknown fields, an annotations proto missing
 * from the descriptor pool, two extensions claiming one number, and one option repeated on the wire
 * where its declaration is singular — the descriptor and wire states that #50's decisions are
 * about, and that protoc resolves away by construction. {@code CaseCollision} needs two fields
 * differing only by case, which protoc accepts but whose generated Java does not compile.
 */
final class TestProtos {

    /** A bool field option marking a field as a BigQuery {@code JSON} column. */
    static final int JSON_OPTION_NUMBER = 50000;

    /**
     * A second, unrelated bool field option: a field carrying only it must not become JSON unless
     * this number is configured too.
     */
    static final int OTHER_OPTION_NUMBER = 50001;

    /** A string field option, used to check that a non-bool option at the number is rejected. */
    static final int NON_BOOL_OPTION_NUMBER = 50002;

    /**
     * An int64 field option. Unlike a string option it is varint-encoded, so it is
     * indistinguishable from a bool by wire type alone — which is the only signal available once
     * the option arrives as an unknown field.
     */
    static final int NON_BOOL_VARINT_OPTION_NUMBER = 50003;

    /** A repeated bool field option: the right wire type, the wrong arity. */
    static final int REPEATED_BOOL_OPTION_NUMBER = 50004;

    /**
     * A bool option declared inside a scoping message rather than at file level — a common way to
     * keep an annotation out of the package namespace, and invisible to a declaration search that
     * only looks at {@code FileDescriptor.getExtensions()}.
     */
    static final int SCOPED_OPTION_NUMBER = 50005;

    /** Full name of the scoped option: note the {@code Scope} segment. */
    static final String SCOPED_OPTION_FULL_NAME = "annot.Scope.scoped_json";

    /**
     * A <em>different</em> annotations proto claiming the same number as {@link
     * #JSON_OPTION_NUMBER}, as two teams picking from protobuf's unregistered private range would.
     */
    static final String COLLIDING_OPTION_FULL_NAME = "other.json";

    private static final String ANNOTATIONS_PROTO = "annot.proto";
    private static final String COLLIDING_ANNOTATIONS_PROTO = "other_annot.proto";

    private TestProtos() {}

    /** The whole type-mapping matrix, from {@code src/test/protobuf/test.proto}. */
    static Descriptors.Descriptor allTypes() {
        return AllTypes.getDescriptor();
    }

    /** A self-referencing message, which BigQuery cannot represent. */
    static Descriptors.Descriptor recursive() {
        return Recursive.getDescriptor();
    }

    /** Every proto3 presence shape, from {@code src/test/protobuf/presence.proto}. */
    static Descriptors.Descriptor presence() {
        return Presence.getDescriptor();
    }

    /**
     * Every well-known type the converters recognise, plus {@code Any}, which they deliberately do
     * not. From {@code src/test/protobuf/wellknown.proto}.
     */
    static Descriptors.Descriptor wellKnown() {
        return WellKnown.getDescriptor();
    }

    /**
     * A {@code google.protobuf.Empty} field: a message with no fields, and so a {@code STRUCT} with
     * no columns, which BigQuery cannot represent.
     */
    static Descriptors.Descriptor wellKnownEmpty() {
        return WellKnownEmpty.getDescriptor();
    }

    /**
     * proto2, where a {@code required} field has presence and is mandatory all the same — the case
     * a presence test alone gets wrong. From {@code src/test/protobuf/presence2.proto}.
     */
    static Descriptors.Descriptor proto2Presence() {
        return Proto2Presence.getDescriptor();
    }

    /**
     * Two fields differing only by case, which the Storage Write API cannot tell apart because it
     * lowercases descriptor field names.
     *
     * <p>Hand-built, and necessarily so: protoc accepts the collision but the Java it generates
     * does not compile, since {@code ID} and {@code id} both yield {@code ID_FIELD_NUMBER}. That is
     * not a reason to drop the case — a descriptor that can carry the collision is one built at
     * runtime or read from a {@code FileDescriptorSet}, which is exactly the input a hand-built
     * fixture models.
     */
    static Descriptors.Descriptor caseCollision() {
        return caseCollisionFile().findMessageTypeByName("CaseCollision");
    }

    /**
     * The {@code Annotated} message with its field options resolved as <em>known</em> extensions,
     * which is what a descriptor obtained from generated code looks like.
     */
    static Descriptors.Descriptor annotated() {
        return annotatedFile(false).findMessageTypeByName("Annotated");
    }

    /**
     * The very same message with its field options left as <em>unknown</em> fields, which is what a
     * descriptor built from a serialized {@code FileDescriptorSet} looks like: protobuf-java does
     * not resolve custom options against the descriptor pool, not even when the file declaring the
     * extension is a direct dependency of the file being built.
     */
    static Descriptors.Descriptor annotatedFromBytes() {
        return annotatedFile(true).findMessageTypeByName("Annotated");
    }

    /**
     * The same message again, with the options unresolved <em>and</em> the annotations proto absent
     * from the descriptor pool — a {@code FileDescriptorSet} built without the unused import. This
     * is the only form in which nothing but the wire encoding identifies the option, so it is what
     * exercises the encoding fallback; every other fixture resolves the declaration.
     */
    static Descriptors.Descriptor annotatedWithoutAnnotationsProto() {
        return annotatedFile(true, false).findMessageTypeByName("Annotated");
    }

    /** A message whose option-marked field is neither a message nor a string. */
    static Descriptors.Descriptor annotatedBadType() {
        return annotatedFile(false).findMessageTypeByName("AnnotatedBadType");
    }

    /**
     * A message annotated by a <em>different</em> annotations proto that happens to use the same
     * extension number, in the unknown-field form. Nothing but the declaration's full name
     * separates it from the real marker.
     */
    static Descriptors.Descriptor collidingAnnotated() {
        DescriptorProtos.FileDescriptorProto proto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("colliding.proto")
                        .setPackage("colliding")
                        .setSyntax("proto3")
                        .addDependency(COLLIDING_ANNOTATIONS_PROTO)
                        .addMessageType(
                                DescriptorProtos.DescriptorProto.newBuilder()
                                        .setName("Colliding")
                                        .addField(
                                                withOptions(
                                                        scalar(
                                                                "c_string",
                                                                1,
                                                                DescriptorProtos
                                                                        .FieldDescriptorProto.Type
                                                                        .TYPE_STRING),
                                                        collidingOption())))
                        .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(
                            DescriptorProtos.FileDescriptorProto.parseFrom(proto.toByteString()),
                            new Descriptors.FileDescriptor[] {collidingAnnotationsFile()})
                    .findMessageTypeByName("Colliding");
        } catch (InvalidProtocolBufferException | Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * A message whose descriptor pool holds <em>both</em> annotations protos, each declaring an
     * extension at {@link #JSON_OPTION_NUMBER}, with the field annotated by ours. protoc rejects
     * that conflict within one compilation, but a pool assembled from several sources can hold it,
     * and then only the expected name says which declaration is the right one.
     *
     * <p>The rival is declared <em>first</em>, because the declaration search walks dependencies
     * breadth-first in declaration order — so the rival is the first number match, and a search
     * that simply took it would answer with the wrong name.
     */
    static Descriptors.Descriptor ambiguouslyAnnotated() {
        DescriptorProtos.FileDescriptorProto proto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("ambiguous.proto")
                        .setPackage("ambiguous")
                        .setSyntax("proto3")
                        .addDependency(COLLIDING_ANNOTATIONS_PROTO)
                        .addDependency(ANNOTATIONS_PROTO)
                        .addMessageType(
                                DescriptorProtos.DescriptorProto.newBuilder()
                                        .setName("Ambiguous")
                                        .addField(
                                                withOptions(
                                                        scalar(
                                                                "m_string",
                                                                1,
                                                                DescriptorProtos
                                                                        .FieldDescriptorProto.Type
                                                                        .TYPE_STRING),
                                                        boolOption(JSON_OPTION_NUMBER, true))))
                        .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(
                            DescriptorProtos.FileDescriptorProto.parseFrom(proto.toByteString()),
                            new Descriptors.FileDescriptor[] {
                                collidingAnnotationsFile(), annotationsFile()
                            })
                    .findMessageTypeByName("Ambiguous");
        } catch (InvalidProtocolBufferException | Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The JSON option as a {@code GeneratedExtension}, built the way protoc's output does it — so
     * the builder overload that takes one can be covered without adding a codegen step to the
     * build.
     */
    static GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean>
            jsonOptionExtension() {
        GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean> extension =
                GeneratedMessage.newFileScopedGeneratedExtension(Boolean.class, null);
        extension.internalInit(extension(JSON_OPTION_NUMBER));
        return extension;
    }

    /** The rival annotations proto's option at the same number, as a {@code GeneratedExtension}. */
    static GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean>
            collidingOptionExtension() {
        GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean> extension =
                GeneratedMessage.newFileScopedGeneratedExtension(Boolean.class, null);
        for (Descriptors.FieldDescriptor candidate : collidingAnnotationsFile().getExtensions()) {
            if (candidate.getNumber() == JSON_OPTION_NUMBER) {
                extension.internalInit(candidate);
                return extension;
            }
        }
        throw new AssertionError("No colliding extension built");
    }

    /** The full name the JSON option is declared under in the synthetic annotations proto. */
    static final String JSON_OPTION_FULL_NAME = "annot.json";

    /**
     * The one hand-built message left outside the annotation fixtures: two fields differing only by
     * case, which protoc accepts but whose generated Java does not compile.
     */
    private static Descriptors.FileDescriptor caseCollisionFile() {
        DescriptorProtos.DescriptorProto caseCollision =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("CaseCollision")
                        .addField(
                                scalar(
                                        "ID",
                                        1,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(
                                scalar(
                                        "id",
                                        2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build();
        DescriptorProtos.FileDescriptorProto proto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("case_collision.proto")
                        .setPackage("test")
                        .setSyntax("proto3")
                        .addMessageType(caseCollision)
                        .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static Descriptors.FileDescriptor annotatedFile(boolean throughBytes) {
        return annotatedFile(throughBytes, true);
    }

    private static Descriptors.FileDescriptor annotatedFile(
            boolean throughBytes, boolean withAnnotationsProto) {
        DescriptorProtos.FileDescriptorProto proto = annotatedFileProto();
        if (!withAnnotationsProto) {
            proto = proto.toBuilder().clearDependency().build();
        }
        if (throughBytes) {
            // Round-tripping without an extension registry is exactly what building a descriptor
            // from a serialized FileDescriptorSet does: the options survive as unknown fields.
            try {
                proto = DescriptorProtos.FileDescriptorProto.parseFrom(proto.toByteString());
            } catch (InvalidProtocolBufferException e) {
                throw new AssertionError(e);
            }
        }
        try {
            return Descriptors.FileDescriptor.buildFrom(
                    proto,
                    withAnnotationsProto
                            ? new Descriptors.FileDescriptor[] {annotationsFile()}
                            : new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static DescriptorProtos.FileDescriptorProto annotatedFileProto() {
        DescriptorProtos.DescriptorProto payload =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("APayload")
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

        DescriptorProtos.DescriptorProto annotatedNested =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("AnnotatedNested")
                        .addField(
                                withOptions(
                                        scalar(
                                                "n_json",
                                                1,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .addField(
                                scalar(
                                        "n_plain",
                                        2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .build();

        DescriptorProtos.DescriptorProto annotated =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Annotated")
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_string",
                                                1,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .addField(
                                withOptions(
                                        message("a_message", 2, ".annotated.APayload", false),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .addField(
                                scalar(
                                        "a_plain",
                                        3,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_false",
                                                4,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        boolOption(JSON_OPTION_NUMBER, false)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_other",
                                                5,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        boolOption(OTHER_OPTION_NUMBER, true)))
                        .addField(
                                withOptions(
                                        repeated(
                                                scalar(
                                                        "a_rep_string",
                                                        6,
                                                        DescriptorProtos.FieldDescriptorProto.Type
                                                                .TYPE_STRING)),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_labeled",
                                                7,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        stringOption(NON_BOOL_OPTION_NUMBER, "not-a-bool")))
                        .addField(message("a_nested", 8, ".annotated.AnnotatedNested", false))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_leveled",
                                                9,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        longOption(NON_BOOL_VARINT_OPTION_NUMBER, 7L)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_flagged",
                                                10,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        repeatedBoolOption(
                                                REPEATED_BOOL_OPTION_NUMBER, false, true)))
                        .addField(
                                withOptions(
                                        repeated(
                                                message(
                                                        "a_rep_message",
                                                        11,
                                                        ".annotated.APayload",
                                                        true)),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_scoped",
                                                12,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        boolOption(SCOPED_OPTION_NUMBER, true)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_leveled_one",
                                                13,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        longOption(NON_BOOL_VARINT_OPTION_NUMBER, 1L)))
                        .addField(
                                withOptions(
                                        scalar(
                                                "a_twice",
                                                14,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_STRING),
                                        repeatedVarintOnTheWire(JSON_OPTION_NUMBER, 1L, 1L)))
                        .build();

        DescriptorProtos.DescriptorProto badType =
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("AnnotatedBadType")
                        .addField(
                                withOptions(
                                        scalar(
                                                "b_int",
                                                1,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_INT64),
                                        boolOption(JSON_OPTION_NUMBER, true)))
                        .build();

        return DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("annotated.proto")
                .setPackage("annotated")
                .setSyntax("proto3")
                .addDependency(ANNOTATIONS_PROTO)
                .addMessageType(payload)
                .addMessageType(annotatedNested)
                .addMessageType(annotated)
                .addMessageType(badType)
                .build();
    }

    private static Descriptors.FileDescriptor collidingAnnotationsFile() {
        DescriptorProtos.FileDescriptorProto proto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName(COLLIDING_ANNOTATIONS_PROTO)
                        .setPackage("other")
                        .setSyntax("proto2")
                        .addDependency("google/protobuf/descriptor.proto")
                        .addExtension(
                                fieldOption(
                                        "json",
                                        JSON_OPTION_NUMBER,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                        .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(
                    proto, new Descriptors.FileDescriptor[] {DescriptorProtos.getDescriptor()});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    private static DescriptorProtos.FieldOptions collidingOption() {
        for (Descriptors.FieldDescriptor extension : collidingAnnotationsFile().getExtensions()) {
            if (extension.getNumber() == JSON_OPTION_NUMBER) {
                return DescriptorProtos.FieldOptions.newBuilder().setField(extension, true).build();
            }
        }
        throw new AssertionError("No colliding extension built");
    }

    private static Descriptors.FileDescriptor annotationsFile() {
        try {
            return Descriptors.FileDescriptor.buildFrom(
                    annotationsFileProto(),
                    new Descriptors.FileDescriptor[] {DescriptorProtos.getDescriptor()});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * {@code extend google.protobuf.FieldOptions { ... }}, as a private annotations proto would.
     */
    private static DescriptorProtos.FileDescriptorProto annotationsFileProto() {
        return DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(ANNOTATIONS_PROTO)
                .setPackage("annot")
                .setSyntax("proto2")
                .addDependency("google/protobuf/descriptor.proto")
                .addExtension(
                        fieldOption(
                                "json",
                                JSON_OPTION_NUMBER,
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                .addExtension(
                        fieldOption(
                                "other",
                                OTHER_OPTION_NUMBER,
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                .addExtension(
                        fieldOption(
                                "label",
                                NON_BOOL_OPTION_NUMBER,
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addExtension(
                        fieldOption(
                                "level",
                                NON_BOOL_VARINT_OPTION_NUMBER,
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addExtension(
                        repeated(
                                fieldOption(
                                        "flags",
                                        REPEATED_BOOL_OPTION_NUMBER,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)))
                // extend inside a scoping message, not at file level.
                .addMessageType(
                        DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("Scope")
                                .addExtension(
                                        fieldOption(
                                                "scoped_json",
                                                SCOPED_OPTION_NUMBER,
                                                DescriptorProtos.FieldDescriptorProto.Type
                                                        .TYPE_BOOL)))
                .build();
    }

    private static DescriptorProtos.FieldDescriptorProto fieldOption(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setExtendee(".google.protobuf.FieldOptions")
                .build();
    }

    private static DescriptorProtos.FieldOptions boolOption(int number, boolean value) {
        return DescriptorProtos.FieldOptions.newBuilder()
                .setField(extension(number), value)
                .build();
    }

    private static DescriptorProtos.FieldOptions stringOption(int number, String value) {
        return DescriptorProtos.FieldOptions.newBuilder()
                .setField(extension(number), value)
                .build();
    }

    /**
     * A singular option whose value appears more than once on the wire. Protobuf permits this and
     * keeps the last occurrence; it is only reachable by writing the unknown fields directly, since
     * setting the extension would collapse it.
     */
    private static DescriptorProtos.FieldOptions repeatedVarintOnTheWire(
            int number, long... values) {
        UnknownFieldSet.Field.Builder field = UnknownFieldSet.Field.newBuilder();
        for (long value : values) {
            field.addVarint(value);
        }
        return DescriptorProtos.FieldOptions.newBuilder()
                .setUnknownFields(
                        UnknownFieldSet.newBuilder().addField(number, field.build()).build())
                .build();
    }

    private static DescriptorProtos.FieldOptions longOption(int number, long value) {
        return DescriptorProtos.FieldOptions.newBuilder()
                .setField(extension(number), value)
                .build();
    }

    private static DescriptorProtos.FieldOptions repeatedBoolOption(int number, boolean... values) {
        Descriptors.FieldDescriptor extension = extension(number);
        DescriptorProtos.FieldOptions.Builder builder = DescriptorProtos.FieldOptions.newBuilder();
        for (boolean value : values) {
            builder.addRepeatedField(extension, value);
        }
        return builder.build();
    }

    private static Descriptors.FieldDescriptor extension(int number) {
        Descriptors.FileDescriptor file = annotationsFile();
        List<Descriptors.FieldDescriptor> candidates = new ArrayList<>(file.getExtensions());
        for (Descriptors.Descriptor message : file.getMessageTypes()) {
            candidates.addAll(message.getExtensions());
        }
        for (Descriptors.FieldDescriptor extension : candidates) {
            if (extension.getNumber() == number) {
                return extension;
            }
        }
        throw new AssertionError("No extension with number " + number + " in " + ANNOTATIONS_PROTO);
    }

    private static DescriptorProtos.FieldDescriptorProto withOptions(
            DescriptorProtos.FieldDescriptorProto field, DescriptorProtos.FieldOptions options) {
        return field.toBuilder().setOptions(options).build();
    }

    private static DescriptorProtos.FieldDescriptorProto repeated(
            DescriptorProtos.FieldDescriptorProto field) {
        return field.toBuilder()
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
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
}
