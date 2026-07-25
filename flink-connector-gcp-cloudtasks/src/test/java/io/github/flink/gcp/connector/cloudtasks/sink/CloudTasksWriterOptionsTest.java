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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloudTasksWriterOptions}. */
class CloudTasksWriterOptionsTest {

    @Test
    void defaultsMatchAnEmptyBuilder() {
        CloudTasksWriterOptions defaults = CloudTasksWriterOptions.defaults();

        assertThat(defaults)
                .isEqualTo(CloudTasksWriterOptions.builder().build())
                .hasSameHashCodeAs(CloudTasksWriterOptions.builder().build());
        assertThat(defaults.getMaxInFlightTasks()).isEqualTo(1000);
        assertThat(defaults.getRetryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(defaults.getRetryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(defaults.getRetryMaxAttempts()).isEqualTo(8);
        assertThat(defaults.getNotFoundInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(defaults.getNotFoundMaxBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(defaults.getNotFoundMaxAttempts()).isEqualTo(3);
    }

    @Test
    void schedulesCarryTheConfiguredBudgets() {
        CloudTasksWriterOptions options =
                CloudTasksWriterOptions.builder()
                        .retryMaxAttempts(4)
                        .retryInitialBackoff(Duration.ofMillis(200))
                        .retryMaxBackoff(Duration.ofMillis(400))
                        .notFoundMaxAttempts(2)
                        .notFoundInitialBackoff(Duration.ofMillis(50))
                        .notFoundMaxBackoff(Duration.ofMillis(50))
                        .build();

        assertThat(options.toRetrySchedule().maxAttempts()).isEqualTo(4);
        assertThat(options.toNotFoundRetrySchedule().maxAttempts()).isEqualTo(2);
        // The NOT_FOUND schedule has no jitter, so its backoff is exact.
        assertThat(options.toNotFoundRetrySchedule().backoffMs(1)).isEqualTo(50);
        // The transient schedule is jittered by ±20% to de-synchronize parallel subtasks.
        assertThat(options.toRetrySchedule().backoffMs(1)).isBetween(160L, 240L);
    }

    @Test
    void rejectsNonPositiveCounts() {
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().maxInFlightTasks(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().notFoundMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsSubMillisecondBackoffs() {
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .retryInitialBackoff(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().notFoundMaxBackoff(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsABackoffCapBelowTheInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .retryInitialBackoff(Duration.ofSeconds(2))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxBackoff");
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .notFoundInitialBackoff(Duration.ofSeconds(2))
                                        .notFoundMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("notFoundMaxBackoff");
    }
}
