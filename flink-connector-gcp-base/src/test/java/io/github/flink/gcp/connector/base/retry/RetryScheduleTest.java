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

package io.github.flink.gcp.connector.base.retry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RetrySchedule}. */
class RetryScheduleTest {

    @Test
    void backoffDoublesFromTheInitialDelayUpToTheCap() {
        RetrySchedule schedule = new RetrySchedule(10, 50, 6, 0);

        assertThat(schedule.backoffMs(1)).isEqualTo(10);
        assertThat(schedule.backoffMs(2)).isEqualTo(20);
        assertThat(schedule.backoffMs(3)).isEqualTo(40);
        assertThat(schedule.backoffMs(4)).isEqualTo(50);
        assertThat(schedule.backoffMs(5)).isEqualTo(50);
        assertThat(schedule.backoffMs(6)).isEqualTo(50);
    }

    @Test
    void aFlatScheduleStaysAtTheInitialDelay() {
        RetrySchedule schedule = new RetrySchedule(30, 30, 3, 0);

        assertThat(schedule.backoffMs(1)).isEqualTo(30);
        assertThat(schedule.backoffMs(2)).isEqualTo(30);
        assertThat(schedule.backoffMs(3)).isEqualTo(30);
    }

    @Test
    void jitterStaysWithinTheConfiguredRatioOfTheBase() {
        RetrySchedule schedule = new RetrySchedule(100, 100, 1, 0.5);

        for (int i = 0; i < 1000; i++) {
            assertThat(schedule.backoffMs(1)).isBetween(50L, 150L);
        }
    }

    @Test
    void theDefaultJitterRatioIsNonZero() {
        // Every connector's schedules map onto this one constant, and the only thing the value has
        // to be is non-zero — a zero here would silently re-synchronize every parallel subtask.
        assertThat(RetrySchedule.DEFAULT_JITTER_RATIO).isGreaterThan(0).isLessThan(1);
    }

    @Test
    void jitterNeverGoesBelowOneMillisecond() {
        RetrySchedule schedule = new RetrySchedule(1, 1, 1, 0.99);

        for (int i = 0; i < 1000; i++) {
            assertThat(schedule.backoffMs(1)).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void maxAttemptsIsReturned() {
        assertThat(new RetrySchedule(1, 1, 7, 0).maxAttempts()).isEqualTo(7);
    }

    @Test
    void invalidArgumentsAreRejected() {
        assertThatThrownBy(() -> new RetrySchedule(0, 10, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialBackoffMs");
        assertThatThrownBy(() -> new RetrySchedule(10, 9, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBackoffMs");
        assertThatThrownBy(() -> new RetrySchedule(10, 10, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
        assertThatThrownBy(() -> new RetrySchedule(10, 10, 1, -0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterRatio");
        assertThatThrownBy(() -> new RetrySchedule(10, 10, 1, 1.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jitterRatio");
    }
}
