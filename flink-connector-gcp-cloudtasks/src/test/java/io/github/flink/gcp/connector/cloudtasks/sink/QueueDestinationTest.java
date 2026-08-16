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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link QueueDestination}. */
class QueueDestinationTest {

    @Test
    void rendersTheCloudTasksQueuePath() {
        QueueDestination destination =
                QueueDestination.of("my-project", "asia-northeast1", "webhooks");

        assertThat(destination.toQueuePath())
                .isEqualTo("projects/my-project/locations/asia-northeast1/queues/webhooks");
        assertThat(destination.getProject()).isEqualTo("my-project");
        assertThat(destination.getLocation()).isEqualTo("asia-northeast1");
        assertThat(destination.getQueue()).isEqualTo("webhooks");
        assertThat(destination).hasToString("my-project/asia-northeast1/webhooks");
    }

    @Test
    void isIdentifiedByProjectLocationAndQueue() {
        QueueDestination destination = QueueDestination.of("p", "asia-northeast1", "q");

        assertThat(destination)
                .isEqualTo(QueueDestination.of("p", "asia-northeast1", "q"))
                .hasSameHashCodeAs(QueueDestination.of("p", "asia-northeast1", "q"))
                // Queues are regional, so the same name in another region is another queue.
                .isNotEqualTo(QueueDestination.of("p", "europe-west1", "q"))
                .isNotEqualTo(QueueDestination.of("other", "asia-northeast1", "q"))
                .isNotEqualTo(QueueDestination.of("p", "asia-northeast1", "other"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", " padded", "padded ", "with/slash"})
    void rejectsMalformedComponents(String value) {
        assertThatThrownBy(() -> QueueDestination.of(value, "asia-northeast1", "q"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDestination.of("p", value, "q"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDestination.of("p", "asia-northeast1", value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> QueueDestination.of(null, "asia-northeast1", "q"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
