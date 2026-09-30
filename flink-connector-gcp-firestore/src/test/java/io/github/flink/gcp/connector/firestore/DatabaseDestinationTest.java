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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseDestinationTest {

    @Test
    void theDefaultDatabaseIsTheOneEveryProjectHas() {
        DatabaseDestination database = DatabaseDestination.of("p");

        assertThat(database.getDatabaseId()).isEqualTo("(default)");
        assertThat(database).hasToString("projects/p/databases/(default)");
        assertThat(database).isEqualTo(DatabaseDestination.of("p", "(default)"));
    }

    @Test
    void identityIsProjectAndDatabase() throws Exception {
        DatabaseDestination database = DatabaseDestination.of("p", "orders");

        assertThat(database).isNotEqualTo(DatabaseDestination.of("p", "other"));
        assertThat(database).isNotEqualTo(DatabaseDestination.of("q", "orders"));
        assertThat(database.hashCode()).isEqualTo(DatabaseDestination.of("p", "orders").hashCode());
        assertThat(InstantiationUtil.clone(database)).isEqualTo(database);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "a/b", " a", "a "})
    void aComponentThatWouldAddressAnotherResourceIsRejected(String component) {
        assertThatThrownBy(() -> DatabaseDestination.of(component, "d"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project");
        assertThatThrownBy(() -> DatabaseDestination.of("p", component))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("databaseId");
    }
}
