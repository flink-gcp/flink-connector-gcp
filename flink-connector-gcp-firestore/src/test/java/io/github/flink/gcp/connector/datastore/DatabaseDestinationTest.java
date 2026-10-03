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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseDestinationTest {

    @Test
    void theDefaultDatabaseHasTheEmptyIdTheDatastoreApiUses() {
        DatabaseDestination byProject = DatabaseDestination.of("p");
        DatabaseDestination byName = DatabaseDestination.of("p", "(default)");
        DatabaseDestination byEmptyId = DatabaseDestination.of("p", "");

        assertThat(byProject.getDatabaseId()).isEmpty();
        assertThat(byName).isEqualTo(byProject).hasSameHashCodeAs(byProject);
        assertThat(byEmptyId).isEqualTo(byProject);
        assertThat(byProject).hasToString("projects/p/databases/(default)");
    }

    @Test
    void aNamedDatabaseKeepsItsId() throws Exception {
        DatabaseDestination named = DatabaseDestination.of("p", "orders");

        assertThat(named.getProject()).isEqualTo("p");
        assertThat(named.getDatabaseId()).isEqualTo("orders");
        assertThat(named).hasToString("projects/p/databases/orders");
        assertThat(named).isNotEqualTo(DatabaseDestination.of("p"));
        assertThat(InstantiationUtil.clone(named)).isEqualTo(named);
    }

    @Test
    void aComponentThatWouldAddressAnotherResourceIsRefused() {
        assertThatThrownBy(() -> DatabaseDestination.of("p/q"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project");
        assertThatThrownBy(() -> DatabaseDestination.of("p", " orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("databaseId");
        assertThatThrownBy(() -> DatabaseDestination.of("p", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
