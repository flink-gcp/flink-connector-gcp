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

package io.github.flink.gcp.connector.pubsub.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubscriptionDestination}. */
class SubscriptionDestinationTest {

    @Test
    void exposesTheResourcePathUsedByThePubSubApi() {
        SubscriptionDestination destination = SubscriptionDestination.of("my-project", "my-sub");

        assertThat(destination.getProject()).isEqualTo("my-project");
        assertThat(destination.getSubscription()).isEqualTo("my-sub");
        assertThat(destination.toSubscriptionPath())
                .isEqualTo("projects/my-project/subscriptions/my-sub");
        assertThat(destination).hasToString("my-project/my-sub");
    }

    @Test
    void equalsAndHashCodeAreDefinedOverProjectAndSubscription() {
        SubscriptionDestination destination = SubscriptionDestination.of("project", "sub");

        assertThat(destination).isEqualTo(SubscriptionDestination.of("project", "sub"));
        assertThat(destination).hasSameHashCodeAs(SubscriptionDestination.of("project", "sub"));
        assertThat(destination).isNotEqualTo(SubscriptionDestination.of("project", "other"));
        assertThat(destination).isNotEqualTo(SubscriptionDestination.of("other", "sub"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", " leading", "trailing ", "with/slash"})
    void rejectsMalformedComponents(String value) {
        assertThatThrownBy(() -> SubscriptionDestination.of(value, "sub"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project");
        assertThatThrownBy(() -> SubscriptionDestination.of("project", value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("subscription");
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> SubscriptionDestination.of(null, "sub"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubscriptionDestination.of("project", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
