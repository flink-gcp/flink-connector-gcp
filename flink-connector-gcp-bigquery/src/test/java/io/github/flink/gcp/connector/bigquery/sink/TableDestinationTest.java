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

package io.github.flink.gcp.connector.bigquery.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDestination}. */
class TableDestinationTest {

    @Test
    void exposesComponentsAndPath() {
        TableDestination destination = TableDestination.of("my-project", "my_dataset", "my_table");

        assertThat(destination.getProject()).isEqualTo("my-project");
        assertThat(destination.getDataset()).isEqualTo("my_dataset");
        assertThat(destination.getTable()).isEqualTo("my_table");
        assertThat(destination.toTablePath())
                .isEqualTo("projects/my-project/datasets/my_dataset/tables/my_table");
        assertThat(destination).hasToString("my-project.my_dataset.my_table");
    }

    @Test
    void rejectsBlankComponents() {
        assertThatThrownBy(() -> TableDestination.of(" ", "d", "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project");
        assertThatThrownBy(() -> TableDestination.of("p", null, "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataset");
        assertThatThrownBy(() -> TableDestination.of("p", "d", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table");
    }

    @Test
    void equalsAndHashCodeUseAllComponents() {
        TableDestination a = TableDestination.of("p", "d", "t");
        TableDestination b = TableDestination.of("p", "d", "t");
        TableDestination c = TableDestination.of("p", "d", "other");

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
    }
}
