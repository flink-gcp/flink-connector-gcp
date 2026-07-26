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

import com.google.api.FieldBehaviorProto;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @Test
    void theTwoDescriptorFormsReallyDifferOnTheWire() {
        // Guards the parameterisation below: if both fixtures ever ended up in the same form, every
        // "unknown fields" case would silently be testing the known-extension path instead.
        Descriptors.FieldDescriptor known = field(false, "a_string");
        Descriptors.FieldDescriptor unknown = field(true, "a_string");

        assertThat(known.getOptions().getAllFields()).isNotEmpty();
        assertThat(known.getOptions().getUnknownFields().asMap()).isEmpty();
        assertThat(unknown.getOptions().getAllFields()).isEmpty();
        assertThat(unknown.getOptions().getUnknownFields().asMap())
                .containsKey(TestProtos.JSON_OPTION_NUMBER);
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void findsTheOptionSetToTrue(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_string"), TestProtos.JSON_OPTION_NUMBER))
                .isTrue();
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_message"), TestProtos.JSON_OPTION_NUMBER))
                .isTrue();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void treatsAnExplicitFalseAsNotSet(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_false"), TestProtos.JSON_OPTION_NUMBER))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void returnsFalseWhenTheFieldCarriesNoOptions(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_plain"), TestProtos.JSON_OPTION_NUMBER))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void ignoresAnOptionWithADifferentNumber(boolean throughBytes) {
        assertThat(
                        BoolFieldOptionReader.isSetToTrue(
                                field(throughBytes, "a_other"), TestProtos.JSON_OPTION_NUMBER))
                .isFalse();
    }

    @ParameterizedTest(name = "throughBytes={0}")
    @ValueSource(booleans = {false, true})
    void rejectsANonBoolOptionAtTheConfiguredNumber(boolean throughBytes) {
        Descriptors.FieldDescriptor field = field(throughBytes, "a_labeled");

        assertThatThrownBy(
                        () ->
                                BoolFieldOptionReader.isSetToTrue(
                                        field, TestProtos.NON_BOOL_OPTION_NUMBER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(TestProtos.NON_BOOL_OPTION_NUMBER))
                .hasMessageContaining("a_labeled");
    }

    @Test
    void findsExtensionsOnDescriptorsFromGeneratedCode() {
        // The synthetic fixtures above build their extension descriptors at runtime; this pins the
        // known-extension path against protobuf code that protoc actually generated, where the
        // option is registered by the generated class rather than by this test.
        Descriptors.FieldDescriptor writeStream =
                AppendRowsRequest.getDescriptor().findFieldByName("write_stream");
        int fieldBehavior = FieldBehaviorProto.fieldBehavior.getNumber();

        assertThat(writeStream.getOptions().getUnknownFields().asMap()).isEmpty();
        // google.api.field_behavior is a repeated enum, so it is found by number and then rejected
        // for its type rather than going unnoticed.
        assertThatThrownBy(() -> BoolFieldOptionReader.isSetToTrue(writeStream, fieldBehavior))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("google.api.field_behavior");
        assertThat(BoolFieldOptionReader.isSetToTrue(writeStream, TestProtos.JSON_OPTION_NUMBER))
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
