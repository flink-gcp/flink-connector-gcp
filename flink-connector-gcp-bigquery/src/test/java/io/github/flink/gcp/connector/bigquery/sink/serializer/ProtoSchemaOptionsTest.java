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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtoSchemaOptions}. */
class ProtoSchemaOptionsTest {

    @Test
    void defaultsMapNothingToJson() {
        assertThat(ProtoSchemaOptions.defaults().getJsonFieldPaths()).isEmpty();
        assertThat(ProtoSchemaOptions.defaults().getJsonFieldOptionNumber()).isZero();
    }

    @Test
    void keepsTheLastFieldOptionNumber() {
        ProtoSchemaOptions options =
                ProtoSchemaOptions.builder()
                        .jsonFieldOptionNumber(50000)
                        .jsonFieldOptionNumber(50001)
                        .build();

        assertThat(options.getJsonFieldOptionNumber()).isEqualTo(50001);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 536870912, 19000, 19999})
    void rejectsFieldOptionNumbersProtobufCannotUse(int extensionNumber) {
        assertThatThrownBy(
                        () -> ProtoSchemaOptions.builder().jsonFieldOptionNumber(extensionNumber))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jsonFieldOptionNumber");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 18999, 20000, 50000, 536870911})
    void acceptsFieldOptionNumbersProtobufCanUse(int extensionNumber) {
        assertThat(
                        ProtoSchemaOptions.builder()
                                .jsonFieldOptionNumber(extensionNumber)
                                .build()
                                .getJsonFieldOptionNumber())
                .isEqualTo(extensionNumber);
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
        assertThat(copy.getJsonFieldOptionNumber()).isEqualTo(50000);
    }
}
