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

package io.github.flink.gcp.connector.base.catalog;

import org.apache.flink.table.catalog.CatalogDatabase;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ReadOnlyCatalogDatabase}. */
class ReadOnlyCatalogDatabaseTest {

    @Test
    void theCommentIsTheDescription() {
        CatalogDatabase database = new ReadOnlyCatalogDatabase(Collections.emptyMap(), "Sales");

        assertThat(database.getComment()).isEqualTo("Sales");
        assertThat(database.getDescription()).contains("Sales");
        assertThat(database.getDetailedDescription()).contains("Sales");
        CatalogDatabase uncommented = new ReadOnlyCatalogDatabase(Collections.emptyMap(), null);
        assertThat(uncommented.getComment()).isNull();
        assertThat(uncommented.getDescription()).isEmpty();
        assertThat(uncommented.getDetailedDescription()).isEmpty();
    }

    @Test
    void thePropertiesAreACopyNobodyCanChange() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k", "v");
        CatalogDatabase database = new ReadOnlyCatalogDatabase(properties, null);
        properties.put("k", "changed");

        assertThat(database.getProperties()).containsExactly(Map.entry("k", "v"));
        assertThatThrownBy(() -> database.getProperties().put("k", "w"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aCopyKeepsTheCommentAndMayReplaceTheProperties() {
        CatalogDatabase database =
                new ReadOnlyCatalogDatabase(Collections.singletonMap("k", "v"), "Sales");

        assertThat(database.copy().getProperties()).containsExactly(Map.entry("k", "v"));
        assertThat(database.copy().getComment()).isEqualTo("Sales");
        CatalogDatabase replaced = database.copy(Collections.singletonMap("x", "y"));
        assertThat(replaced.getProperties()).containsExactly(Map.entry("x", "y"));
        assertThat(replaced.getComment()).isEqualTo("Sales");
    }
}
