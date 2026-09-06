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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagingConfigTest {
    @Test
    void defaultWindowAndCapsMatchTheProtocol() {
        var config = new CloudTasksStagingConfig();
        assertThat(config.getMaxStagedTasks()).isEqualTo(100_000);
        assertThat(config.getMaxStagedBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(config.authorizationDeadlineMillis(1000)).isEqualTo(3_281_000);
    }

    @Test
    void rejectsNonpositiveCapsAndInvalidWindows() {
        for (int count : new int[] {0, -1}) {
            assertThatThrownBy(() -> config(count, 1024, Duration.ofHours(1)))
                    .hasMessageContaining("maxStagedTasks");
        }
        assertThatThrownBy(() -> config(1, 0, Duration.ofHours(1)))
                .hasMessageContaining("maxStagedBytes");
        for (Duration duration :
                new Duration[] {
                    Duration.ZERO,
                    Duration.ofMinutes(5),
                    Duration.ofSeconds(320),
                    Duration.ofNanos(Long.MAX_VALUE).plusNanos(1)
                }) {
            assertThatThrownBy(() -> config(1, 1024, duration))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(
                        () ->
                                new CloudTasksStagingConfig(
                                        1,
                                        1024,
                                        Duration.ofSeconds(1),
                                        Duration.ofSeconds(-1),
                                        Duration.ofMillis(1)))
                .hasMessageContaining("clockSkewAllowance");
        assertThatThrownBy(
                        () ->
                                new CloudTasksStagingConfig(
                                        1,
                                        1024,
                                        Duration.ofSeconds(1),
                                        Duration.ZERO,
                                        Duration.ZERO))
                .hasMessageContaining("requestTimeout");
    }

    @Test
    void roundsTheWindowDownAndRejectsOverflowAndSubmillisecondWindows() {
        var config =
                new CloudTasksStagingConfig(
                        1,
                        1024,
                        Duration.ofNanos(3_000_999),
                        Duration.ofNanos(500),
                        Duration.ofNanos(500));
        assertThat(config.authorizationDeadlineMillis(10)).isEqualTo(12);
        assertThatThrownBy(() -> config.authorizationDeadlineMillis(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(
                        () ->
                                new CloudTasksStagingConfig(
                                        1,
                                        1024,
                                        Duration.ofNanos(999),
                                        Duration.ZERO,
                                        Duration.ofNanos(1)))
                .hasMessageContaining("one millisecond");
    }

    private static CloudTasksStagingConfig config(int count, long bytes, Duration retention) {
        return new CloudTasksStagingConfig(
                count, bytes, retention, Duration.ofMinutes(5), Duration.ofSeconds(20));
    }
}
