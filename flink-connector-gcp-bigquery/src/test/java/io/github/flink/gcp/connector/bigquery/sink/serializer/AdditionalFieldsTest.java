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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdditionalFieldsTest {

    @Test
    void preservesFieldOrderAndSurvivesJobGraphSerialization() throws Exception {
        AdditionalFields<String> options =
                AdditionalFields.<String>builder()
                        .field(
                                AdditionalField.of(
                                        "__uuid",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        value -> value))
                        .field(
                                AdditionalField.of(
                                        "__timestamp",
                                        AdditionalFieldType.TIMESTAMP,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        value -> null))
                        .build();

        AdditionalFields<String> copy = InstantiationUtil.clone(options);

        assertThat(copy.getFields())
                .extracting(AdditionalField::getName)
                .containsExactly("__uuid", "__timestamp");
        assertThat(copy.getFields().get(0).getValueProvider().getValue("id")).isEqualTo("id");
    }

    @Test
    void rejectsEmptyDuplicateAndInvalidDeclarations() {
        AdditionalField<String> uuid =
                AdditionalField.of(
                        "__uuid",
                        AdditionalFieldType.STRING,
                        AdditionalFieldNullPolicy.REQUIRED,
                        value -> value);

        assertThatThrownBy(() -> AdditionalFields.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("AdditionalFields requires at least one field");
        assertThatThrownBy(
                        () ->
                                AdditionalFields.<String>builder()
                                        .field(uuid)
                                        .field(
                                                AdditionalField.of(
                                                        "__UUID",
                                                        AdditionalFieldType.STRING,
                                                        AdditionalFieldNullPolicy.NULLABLE,
                                                        value -> value))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate additional field name: __UUID");
        assertThatThrownBy(
                        () ->
                                AdditionalField.of(
                                        "not-a-proto-name",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.NULLABLE,
                                        value -> value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protobuf-compatible identifier");
        assertThatThrownBy(() -> AdditionalFields.<String>builder().field(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("field must not be null");
    }
}
