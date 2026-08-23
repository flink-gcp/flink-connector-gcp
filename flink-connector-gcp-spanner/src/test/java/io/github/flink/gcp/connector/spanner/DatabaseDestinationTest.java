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

package io.github.flink.gcp.connector.spanner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DatabaseDestination}. */
class DatabaseDestinationTest {

    @Test
    void exposesItsComponentsAndRendersThemAsASpannerResourceName() {
        DatabaseDestination database =
                DatabaseDestination.of("my-project", "my-instance", "orders-db");

        assertThat(database.getProject()).isEqualTo("my-project");
        assertThat(database.getInstance()).isEqualTo("my-instance");
        assertThat(database.getDatabase()).isEqualTo("orders-db");
        // The resource name Spanner itself uses, so a dead-letter consumer can key on it and an
        // operator can paste it into gcloud.
        assertThat(database)
                .hasToString("projects/my-project/instances/my-instance/databases/orders-db");
    }

    @Test
    void isIdentifiedByProjectInstanceAndDatabase() {
        DatabaseDestination database = DatabaseDestination.of("p", "i", "d");

        assertThat(database)
                .isEqualTo(DatabaseDestination.of("p", "i", "d"))
                .hasSameHashCodeAs(DatabaseDestination.of("p", "i", "d"))
                // A database id is unique within an instance only, so the instance is part of it.
                .isNotEqualTo(DatabaseDestination.of("p", "other", "d"))
                .isNotEqualTo(DatabaseDestination.of("other", "i", "d"))
                .isNotEqualTo(DatabaseDestination.of("p", "i", "other"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", " padded", "padded ", "with/slash"})
    void rejectsMalformedComponents(String value) {
        assertThatThrownBy(() -> DatabaseDestination.of(value, "i", "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatabaseDestination.of("p", value, "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatabaseDestination.of("p", "i", value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> DatabaseDestination.of(null, "i", "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatabaseDestination.of("p", null, "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatabaseDestination.of("p", "i", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void namesTheOffendingComponent() {
        assertThatThrownBy(() -> DatabaseDestination.of("p", "i", " padded"))
                .hasMessageContaining("database")
                .hasMessageContaining("padded");
    }
}
