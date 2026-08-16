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

package io.github.flink.gcp.connector.bigquery.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaUpdateOptions}. */
class SchemaUpdateOptionsTest {

    @Test
    void defaultsDisableEverything() {
        SchemaUpdateOptions options = SchemaUpdateOptions.defaults();

        assertThat(options.isAllowNewFields()).isFalse();
        assertThat(options.isAllowFieldRelaxation()).isFalse();
        assertThat(options.isEnabled()).isFalse();
    }

    @Test
    void eitherFlagEnablesUpdates() {
        assertThat(SchemaUpdateOptions.builder().allowNewFields().build().isEnabled()).isTrue();
        assertThat(SchemaUpdateOptions.builder().allowFieldRelaxation().build().isEnabled())
                .isTrue();
    }

    @Test
    void buildersSetTheirFlags() {
        SchemaUpdateOptions options =
                SchemaUpdateOptions.builder().allowNewFields().allowFieldRelaxation().build();

        assertThat(options.isAllowNewFields()).isTrue();
        assertThat(options.isAllowFieldRelaxation()).isTrue();
    }

    @Test
    void equalsHashCodeAndToStringReflectTheFlags() {
        SchemaUpdateOptions newFields = SchemaUpdateOptions.builder().allowNewFields().build();

        assertThat(newFields)
                .isEqualTo(SchemaUpdateOptions.builder().allowNewFields().build())
                .hasSameHashCodeAs(SchemaUpdateOptions.builder().allowNewFields().build())
                .isNotEqualTo(SchemaUpdateOptions.defaults());
        assertThat(newFields.toString())
                .contains("allowNewFields=true")
                .contains("allowFieldRelaxation=false");
    }
}
