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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import com.google.api.FieldBehaviorProto;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BoolFieldOptionReader}.
 *
 * <p>Every case is exercised against both descriptor forms — options as known extensions and
 * options as unknown fields — because which one the connector meets depends on how the user
 * obtained the descriptor, not on anything the connector controls.
 */
class BoolFieldOptionReaderTest {

    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {
                "a_string",
                "a_message",
                "a_false",
                "a_other",
                "a_labeled",
                "a_leveled",
                "a_scoped"
            })
    void theTwoDescriptorFormsReallyDifferOnTheWire(String name) {
        // Guards every parameterised case below: if a fixture field ever ended up in the same form
        // in both descriptors, its "unknown fields" case would silently test the other path. Runs
        // over each annotated field, not just one, because the option types differ between them and
        // it is the odd ones out — a_labeled, a_leveled — that decide the rejection cases.
        Descriptors.FieldDescriptor known = field(false, name);
        Descriptors.FieldDescriptor unknown = field(true, name);

        assertThat(known.getOptions().getAllFields().keySet())
                .extracting(Descriptors.FieldDescriptor::getNumber)
                .isEqualTo(
                        new ArrayList<>(unknown.getOptions().getUnknownFields().asMap().keySet()));
        assertThat(known.getOptions().getUnknownFields().asMap()).isEmpty();
        assertThat(unknown.getOptions().getAllFields()).isEmpty();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void findsTheOptionSetToTrue(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_string"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isTrue();
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_message"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isTrue();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void treatsAnExplicitFalseAsNotSet(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_false"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void returnsFalseWhenTheFieldCarriesNoOptions(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_plain"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void ignoresAnOptionWithADifferentNumber(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_other"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void rejectsANonBoolOptionAtTheConfiguredNumber(boolean throughBytes) {
        Descriptors.FieldDescriptor field = field(throughBytes, "a_labeled");

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.NON_BOOL_OPTION_NUMBER, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(TestProtos.NON_BOOL_OPTION_NUMBER))
                .hasMessageContaining("a_labeled");
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void rejectsANonBoolOptionThatIsVarintEncoded(boolean throughBytes) {
        // Both forms resolve the declaration here, so both are caught by its declared type. The
        // varint case still earns its place: the string option above is length-delimited and would
        // be rejected by wire type alone, which proves nothing about a type check.
        Descriptors.FieldDescriptor field = field(throughBytes, "a_leveled");

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.NON_BOOL_VARINT_OPTION_NUMBER, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(TestProtos.NON_BOOL_VARINT_OPTION_NUMBER))
                .hasMessageContaining("a_leveled");
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void rejectsARepeatedBoolOption(boolean throughBytes) {
        // Right wire type, wrong arity — and the fixture's last element is true, so reading it as
        // "last one wins" would report a JSON column.
        Descriptors.FieldDescriptor field = field(throughBytes, "a_flagged");

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.REPEATED_BOOL_OPTION_NUMBER, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a_flagged");
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void ignoresAnUnrelatedOptionThatSharesTheNumber(boolean throughBytes) {
        // Protobuf's private extension range has no registry, so two annotation protos can pick the
        // same number independently. Given the expected name, a declaration found under a different
        // one is a different option and the field stays a plain column. The message case is the
        // dangerous one: it would otherwise become JSON text instead of a STRUCT, silently, in an
        // auto-created table.
        for (String name : new String[] {"a_string", "a_message"}) {
            assertThat(
                            BoolFieldOptionReader.isSetToTrue(
                                    field(throughBytes, name),
                                    TestProtos.JSON_OPTION_NUMBER,
                                    "someone.else.json"))
                    .as(name)
                    .isFalse();
        }
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void matchesOnTheExpectedName(boolean throughBytes) {
        // Counterpart of the test above, on the same fields with the name that does match — so a
        // reader that simply returned false whenever a name was supplied would not pass both.
        for (String name : new String[] {"a_string", "a_message"}) {
            assertThat(
                            BoolFieldOptionReader.isSetToTrue(
                                    field(throughBytes, name),
                                    TestProtos.JSON_OPTION_NUMBER,
                                    "annot.json"))
                    .as(name)
                    .isTrue();
        }
    }

    @Test
    void fallsBackToTheEncodingWhenTheAnnotationsProtoIsAbsent() {
        // The last resort: options unresolved *and* no declaration anywhere in the pool. Every
        // other
        // fixture resolves the declaration, so without this the encoding heuristic would not be
        // exercised at all — and it is the only thing standing between an unrelated varint option
        // and a wrongly typed column.
        Descriptors.Descriptor descriptor = TestProtos.annotatedWithoutAnnotationsProto();
        assertThat(descriptor.getFile().getDependencies()).isEmpty();

        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                descriptor.findFieldByName("a_string"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isTrue();
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                descriptor.findFieldByName("a_false"),
                                TestProtos.JSON_OPTION_NUMBER,
                                null))
                .isFalse();
        // The repeated-bool and string options are still caught, by arity and by wire type.
        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        descriptor.findFieldByName("a_flagged"),
                                        TestProtos.REPEATED_BOOL_OPTION_NUMBER,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        descriptor.findFieldByName("a_labeled"),
                                        TestProtos.NON_BOOL_OPTION_NUMBER,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAVarintOptionOutsideTheBoolRangeWithoutADeclaration() {
        Descriptors.FieldDescriptor field =
                TestProtos.annotatedWithoutAnnotationsProto().findFieldByName("a_leveled");

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.NON_BOOL_VARINT_OPTION_NUMBER, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not encoded as a singular bool");
    }

    @Test
    void cannotTellAnIntegerOptionFromABoolWithoutADeclaration() {
        // The genuinely irreducible case the @throws contract admits to, pinned so it stays honest:
        // an int64 option holding 1 is byte-for-byte a bool set to true. a_leveled (= 7) is caught
        // only because 7 falls outside {0, 1}; this one cannot be.
        Descriptors.FieldDescriptor field =
                TestProtos.annotatedWithoutAnnotationsProto().findFieldByName("a_leveled_one");

        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field, TestProtos.NON_BOOL_VARINT_OPTION_NUMBER, null))
                .isTrue();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void findsAnOptionDeclaredInsideAScopingMessage(boolean throughBytes) {
        // `extend` nested in a message is common style for keeping an annotation out of the package
        // namespace. A declaration search looking only at file-level extensions would miss it and
        // silently drop both the name check and the declared-type check for that option.
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_scoped"),
                                TestProtos.SCOPED_OPTION_NUMBER,
                                TestProtos.SCOPED_OPTION_FULL_NAME))
                .isTrue();
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_scoped"),
                                TestProtos.SCOPED_OPTION_NUMBER,
                                "annot.scoped_json"))
                .as("a file-level name must not match the scoped declaration")
                .isFalse();
    }

    @Test
    void rulesOutARivalAnnotationsProtoAtTheSameNumber() {
        // Not a synthetic name this time: a second annotations proto really declaring bool 50000,
        // reached through the message's own dependency. Only the full name separates the two.
        Descriptors.FieldDescriptor field =
                TestProtos.collidingAnnotated().findFieldByName("c_string");

        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field,
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.JSON_OPTION_FULL_NAME))
                .isFalse();
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field,
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.COLLIDING_OPTION_FULL_NAME))
                .as("its own owner's configuration still matches")
                .isTrue();
        assertThat(BoolFieldOptionReader.isSetToTrue(field, TestProtos.JSON_OPTION_NUMBER, null))
                .as("without a name the number is the only identity, so it still matches")
                .isTrue();
    }

    @Test
    void picksTheExpectedDeclarationWhenTwoShareTheNumber() {
        // Both annotations protos are in this message's pool, each declaring number 50000, and the
        // rival is declared first — the search walks dependencies breadth-first in declaration
        // order, so the rival is the first number match. Taking it would answer with the rival's
        // name and report "different option", and the field would stop being a JSON column even
        // though its own annotation is the configured one.
        Descriptors.FieldDescriptor field =
                TestProtos.ambiguouslyAnnotated().findFieldByName("m_string");

        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field,
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.JSON_OPTION_FULL_NAME))
                .isTrue();
        // The other side of the same coin, and a real limit worth pinning: once both declarations
        // are in the pool the two are indistinguishable, because an unresolved option records only
        // its number — nothing says which declaration it was written against. The name rules out a
        // foreign declaration (see rulesOutARivalAnnotationsProtoAtTheSameNumber, where only the
        // rival is in the pool); it cannot arbitrate between two that are both present.
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field,
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.COLLIDING_OPTION_FULL_NAME))
                .isTrue();
    }

    @Test
    void acceptsABoolWrittenTwiceOnTheWireOnceTheDeclarationIsKnown() {
        // Protobuf lets a singular scalar appear more than once and keeps the last occurrence. The
        // encoding heuristic insists on exactly one varint, so applying it after the declaration
        // has
        // already proven the option is a bool would reject a perfectly legitimate option — it must
        // only stand in when nothing else can answer.
        Descriptors.FieldDescriptor field = field(true, "a_twice");
        assertThat(
                        field.getOptions()
                                .getUnknownFields()
                                .getField(TestProtos.JSON_OPTION_NUMBER)
                                .getVarintList())
                .hasSize(2);

        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field,
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.JSON_OPTION_FULL_NAME))
                .isTrue();
    }

    @Test
    void resolvesTheDeclaredTypeThroughTheDependencyGraph() {
        // protobuf will not use a declared dependency to resolve an option's *value*, but the
        // declaration is still in the pool. Finding it turns the encoding heuristic into an exact
        // type check — which is the only thing that can distinguish a varint-encoded int64 option
        // from a bool.
        Descriptors.FieldDescriptor field = field(true, "a_leveled");
        assertThat(field.getOptions().getUnknownFields().asMap())
                .containsKey(TestProtos.NON_BOOL_VARINT_OPTION_NUMBER);

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.NON_BOOL_VARINT_OPTION_NUMBER, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("annot.level")
                .hasMessageContaining("LONG");
    }

    @Test
    void findsExtensionsOnDescriptorsFromGeneratedCode() {
        // The synthetic fixtures above build their extension descriptors at runtime; this pins the
        // known-extension path against protobuf code that protoc actually generated, where the
        // option is registered by the generated class rather than by this test.
        Descriptors.FieldDescriptor writeStream =
                AppendRowsRequest.getDescriptor().findFieldByName("write_stream");
        int fieldBehavior = FieldBehaviorProto.fieldBehavior.getNumber();

        assertThat(writeStream.getOptions().getAllFields().keySet())
                .anyMatch(option -> option.getNumber() == fieldBehavior);
        // google.api.field_behavior is a repeated enum, so it is found by number and then rejected
        // for its type rather than going unnoticed.
        assertThatThrownBy(
                        () -> BoolFieldOptionReader.isSetToTrue(writeStream, fieldBehavior, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("google.api.field_behavior");
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                writeStream, TestProtos.JSON_OPTION_NUMBER, null))
                .isFalse();
    }

    private static Descriptors.FieldDescriptor field(boolean throughBytes, String name) {
        Descriptors.Descriptor descriptor =
                throughBytes ? TestProtos.annotatedFromBytes() : TestProtos.annotated();
        Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
        assertThat(field).as("field %s", name).isNotNull();
        return field;
    }
}
