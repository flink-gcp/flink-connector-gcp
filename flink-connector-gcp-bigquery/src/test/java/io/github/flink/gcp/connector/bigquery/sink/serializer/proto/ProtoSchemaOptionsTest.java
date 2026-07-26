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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link ProtoSchemaOptions}. */
class ProtoSchemaOptionsTest {

    @Test
    void defaultsMapNothingToJson() {
        assertThat(ProtoSchemaOptions.defaults().getJsonFieldPaths()).isEmpty();
        assertThat(ProtoSchemaOptions.defaults().getJsonFieldOptions()).isEmpty();
    }

    @Test
    void accumulatesFieldOptionNumbers() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(50000)
                        .jsonFieldOptionNumber(50001)
                        .jsonFieldOptionNumber(50002)
                        .build();

        // containsOnly, not containsOnlyKeys plus containsValues: the latter ignores duplicates, so
        // it would pass with one of the three mapped to a name.
        assertThat(options.getJsonFieldOptions())
                .containsOnly(entry(50000, null), entry(50001, null), entry(50002, null));
    }

    @Test
    void keepsTheNamedEntryWhenTheSameNumberIsRegisteredTwice() {
        // Order must not matter: an unnamed entry beside a named one would match anything at that
        // number, which is exactly what the name is there to prevent.
        ProtoSchemaOptions nameLast =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .build();
        ProtoSchemaOptions nameFirst =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .jsonFieldOptionNumber(TestProtos.JSON_OPTION_NUMBER)
                        .build();

        assertThat(nameLast.getJsonFieldOptions())
                .containsExactly(
                        entry(TestProtos.JSON_OPTION_NUMBER, TestProtos.JSON_OPTION_FULL_NAME));
        assertThat(nameFirst.getJsonFieldOptions())
                .containsExactly(
                        entry(TestProtos.JSON_OPTION_NUMBER, TestProtos.JSON_OPTION_FULL_NAME));
    }

    @Test
    void theLastNameWinsWhenTwoExtensionsClaimOneNumber() {
        // Pathological but expressible, and the map can only hold one entry per number: two
        // *named* registrations at the same number resolve last-call-wins, the conventional builder
        // semantic. Stated here because neither choice is self-evidently right.
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .jsonFieldOption(TestProtos.collidingOptionExtension())
                        .build();

        assertThat(options.getJsonFieldOptions())
                .containsExactly(
                        entry(
                                TestProtos.JSON_OPTION_NUMBER,
                                TestProtos.COLLIDING_OPTION_FULL_NAME));
    }

    @ParameterizedTest
    // 1 and 999 are valid field numbers but not valid FieldOptions *extension* numbers: it declares
    // "extensions 1000 to max".
    @ValueSource(ints = {0, -1, 1, 999, 536870912, 19000, 19999})
    void rejectsFieldOptionNumbersProtobufCannotUse(int extensionNumber) {
        assertThatThrownBy(
                        () -> ProtoSchemaOptions.builder().jsonFieldOptionNumber(extensionNumber))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON field option number")
                .hasMessageContaining(String.valueOf(extensionNumber));
    }

    @ParameterizedTest
    @ValueSource(ints = {1000, 18999, 20000, 50000, 536870911})
    void acceptsFieldOptionNumbersProtobufCanUse(int extensionNumber) {
        assertThat(
                        ProtoSchemaOptions.builder()
                                .jsonFieldOptionNumber(extensionNumber)
                                .build()
                                .getJsonFieldOptions())
                .containsOnlyKeys(extensionNumber);
    }

    @Test
    void capturesTheNumberAndNameFromAGeneratedExtension() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .build();

        assertThat(options.getJsonFieldOptions())
                .containsExactly(
                        entry(TestProtos.JSON_OPTION_NUMBER, TestProtos.JSON_OPTION_FULL_NAME));
    }

    @Test
    void survivesJavaSerializationWhenConfiguredFromAnExtension() throws Exception {
        // GeneratedExtension holds a protobuf descriptor and is not Serializable, so the builder
        // must keep its number and name rather than the extension. If that ever regressed, these
        // options would stop travelling in the job graph.
        ProtoSchemaOptions copy =
                InstantiationUtil.clone(
                        ProtoSchemaOptions.builder()
                                .jsonFieldOption(TestProtos.jsonOptionExtension())
                                .build());

        assertThat(copy.getJsonFieldOptions())
                .containsExactly(
                        entry(TestProtos.JSON_OPTION_NUMBER, TestProtos.JSON_OPTION_FULL_NAME));
    }

    @Test
    void keepsSeparateNumbersSeparate() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOption(TestProtos.jsonOptionExtension())
                        .jsonFieldOptionNumber(TestProtos.OTHER_OPTION_NUMBER)
                        .build();

        assertThat(options.getJsonFieldOptions())
                .containsOnly(
                        entry(TestProtos.JSON_OPTION_NUMBER, TestProtos.JSON_OPTION_FULL_NAME),
                        entry(TestProtos.OTHER_OPTION_NUMBER, null));
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        // The options travel in the job graph inside the serializer, so both mechanisms have to
        // come back after a round trip.
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldPath("payload")
                        .jsonFieldOptionNumber(50000)
                        .build();

        ProtoSchemaOptions copy = InstantiationUtil.clone(options);

        assertThat(copy.getJsonFieldPaths()).containsExactly("payload");
        assertThat(copy.getJsonFieldOptions()).containsExactly(entry(50000, null));
    }
}
