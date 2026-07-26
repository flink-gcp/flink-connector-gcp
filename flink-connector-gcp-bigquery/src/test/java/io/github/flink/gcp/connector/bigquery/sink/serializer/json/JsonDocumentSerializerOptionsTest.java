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

package io.github.flink.gcp.connector.bigquery.sink.serializer.json;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonDocumentSerializerOptions}. */
class JsonDocumentSerializerOptionsTest {

    @Test
    void defaultsFailOnAnUnknownField() {
        assertThat(JsonDocumentSerializerOptions.defaults().isIgnoreUnknownFields()).isFalse();
    }

    @Test
    void ignoreUnknownFieldsIsOptIn() {
        assertThat(JsonDocumentSerializerOptions.builder().build().isIgnoreUnknownFields())
                .isFalse();
        assertThat(
                        JsonDocumentSerializerOptions.builder()
                                .ignoreUnknownFields()
                                .build()
                                .isIgnoreUnknownFields())
                .isTrue();
    }

    @Test
    void buildingTwiceFromOneBuilderDoesNotShareState() {
        JsonDocumentSerializerOptions.Builder builder = JsonDocumentSerializerOptions.builder();
        JsonDocumentSerializerOptions first = builder.build();
        builder.ignoreUnknownFields();

        assertThat(first.isIgnoreUnknownFields()).isFalse();
        assertThat(builder.build().isIgnoreUnknownFields()).isTrue();
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        JsonDocumentSerializerOptions copy =
                InstantiationUtil.clone(
                        JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());

        assertThat(copy.isIgnoreUnknownFields()).isTrue();
    }
}
