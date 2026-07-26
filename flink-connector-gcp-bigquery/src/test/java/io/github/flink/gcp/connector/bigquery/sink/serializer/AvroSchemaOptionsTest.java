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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroSchemaOptions}. */
class AvroSchemaOptionsTest {

    @Test
    void defaultsMapNothingAndKeepSchemaNullability() {
        AvroSchemaOptions options = AvroSchemaOptions.defaults();

        assertThat(options.getJsonFieldPaths()).isEmpty();
        assertThat(options.isAllFieldsNullable()).isFalse();
        assertThat(options.isJsonField("anything")).isFalse();
    }

    @Test
    void jsonFieldPathsAccumulateAcrossCalls() {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder()
                        .jsonFieldPath("a")
                        .jsonFieldPaths(Arrays.asList("b.c", "d"))
                        .jsonFieldPath("a")
                        .build();

        assertThat(options.getJsonFieldPaths()).containsExactlyInAnyOrder("a", "b.c", "d");
        assertThat(options.isJsonField("b.c")).isTrue();
        assertThat(options.isJsonField("b")).isFalse();
    }

    @Test
    void allFieldsNullableIsOptIn() {
        assertThat(AvroSchemaOptions.builder().build().isAllFieldsNullable()).isFalse();
        assertThat(AvroSchemaOptions.builder().allFieldsNullable().build().isAllFieldsNullable())
                .isTrue();
    }

    @Test
    void returnedPathsAreUnmodifiable() {
        AvroSchemaOptions options = AvroSchemaOptions.builder().jsonFieldPath("a").build();

        assertThatThrownBy(() -> options.getJsonFieldPaths().add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void buildingTwiceFromOneBuilderDoesNotShareState() {
        AvroSchemaOptions.Builder builder = AvroSchemaOptions.builder().jsonFieldPath("a");
        AvroSchemaOptions first = builder.build();
        builder.jsonFieldPath("b");

        assertThat(first.getJsonFieldPaths()).containsExactly("a");
        assertThat(builder.build().getJsonFieldPaths()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void rejectsNullPaths() {
        assertThatThrownBy(() -> AvroSchemaOptions.builder().jsonFieldPath(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroSchemaOptions.builder().jsonFieldPaths(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder().jsonFieldPath("a.b").allFieldsNullable().build();

        AvroSchemaOptions copy = InstantiationUtil.clone(options);

        assertThat(copy.getJsonFieldPaths()).containsExactly("a.b");
        assertThat(copy.isAllFieldsNullable()).isTrue();
    }
}
