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

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
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
                        .retryInitialBackoff(Duration.ofMillis(2_000))
                        .retryMaxBackoff(Duration.ofMillis(4_000))
                        .notFoundMaxAttempts(2)
                        .notFoundInitialBackoff(Duration.ofMillis(1_000))
                        .notFoundMaxBackoff(Duration.ofMillis(1_000))
                        .build();

        assertThat(options.toRetrySchedule().maxAttempts()).isEqualTo(4);
        assertThat(options.toNotFoundRetrySchedule().maxAttempts()).isEqualTo(2);
        // Both schedules are jittered by ±25% to de-synchronize parallel subtasks — including the
        // NOT_FOUND one, which was jitter-free until #197.
        assertJittered(options.toRetrySchedule(), 1_500L, 2_500L);
        assertJittered(options.toNotFoundRetrySchedule(), 750L, 1_250L);
    }

    /**
     * Asserts the first backoff stays within the jittered range <em>and</em> varies. Staying inside
     * the range does not on its own prove the jitter is applied — a ratio regressing to zero also
     * does.
     */
    private static void assertJittered(RetrySchedule schedule, long minMs, long maxMs) {
        long first = schedule.backoffMs(1);
        boolean varies = false;
        for (int i = 0; i < 200; i++) {
            long backoff = schedule.backoffMs(1);
            assertThat(backoff).isBetween(minMs, maxMs);
            varies |= backoff != first;
        }
        assertThat(varies).as("the schedule must jitter").isTrue();
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
