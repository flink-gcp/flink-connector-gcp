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

package io.github.flink.gcp.connector.bigtable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDestination}. */
class TableDestinationTest {

    @Test
    void exposesItsComponentsAndRendersThemDotted() {
        TableDestination destination = TableDestination.of("my-project", "my-instance", "orders");

        assertThat(destination.getProject()).isEqualTo("my-project");
        assertThat(destination.getInstance()).isEqualTo("my-instance");
        assertThat(destination.getTable()).isEqualTo("orders");
        assertThat(destination).hasToString("my-project.my-instance.orders");
    }

    @Test
    void isIdentifiedByProjectInstanceAndTable() {
        TableDestination destination = TableDestination.of("p", "i", "t");

        assertThat(destination)
                .isEqualTo(TableDestination.of("p", "i", "t"))
                .hasSameHashCodeAs(TableDestination.of("p", "i", "t"))
                // A table id is unique within an instance only, so the instance is part of it.
                .isNotEqualTo(TableDestination.of("p", "other", "t"))
                .isNotEqualTo(TableDestination.of("other", "i", "t"))
                .isNotEqualTo(TableDestination.of("p", "i", "other"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", " padded", "padded ", "with/slash"})
    void rejectsMalformedComponents(String value) {
        assertThatThrownBy(() -> TableDestination.of(value, "i", "t"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableDestination.of("p", value, "t"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableDestination.of("p", "i", value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> TableDestination.of(null, "i", "t"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
