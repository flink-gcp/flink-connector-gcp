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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamOptions}. */
class BufferedStreamOptionsTest {

    @Test
    void defaultsAreValid() {
        BufferedStreamOptions options = BufferedStreamOptions.builder().build();

        assertThat(options.getMaxAppendRequestBytes())
                .isEqualTo(BufferedStreamOptions.DEFAULT_MAX_APPEND_REQUEST_BYTES);
        assertThat(options.getRecoveryInitialBackoff())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RECOVERY_INITIAL_BACKOFF);
        assertThat(options.getRecoveryMaxBackoff())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RECOVERY_MAX_BACKOFF);
        assertThat(options.getRecoveryMaxAttempts())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RECOVERY_MAX_ATTEMPTS);
        assertThat(options.getRetryInitialDelay())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_INITIAL_DELAY);
        assertThat(options.getRetryDelayMultiplier())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_DELAY_MULTIPLIER);
        assertThat(options.getRetryMaxDelay())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_MAX_DELAY);
        assertThat(options.getRetryMaxAttempts())
                .isEqualTo(BufferedStreamOptions.DEFAULT_RETRY_MAX_ATTEMPTS);
        assertThat(options.getMaxRetryDuration())
                .isEqualTo(BufferedStreamOptions.DEFAULT_MAX_RETRY_DURATION);
    }

    @Test
    void carriesConfiguredSdkRetryKnobs() {
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .retryInitialDelay(Duration.ofMillis(250))
                        .retryDelayMultiplier(1.5)
                        .retryMaxDelay(Duration.ofSeconds(15))
                        .retryMaxAttempts(7)
                        .maxRetryDuration(Duration.ofMinutes(2))
                        .build();

        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getRetryDelayMultiplier()).isEqualTo(1.5);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(options.getMaxRetryDuration()).isEqualTo(Duration.ofMinutes(2));
        assertThat(options)
                .isNotEqualTo(BufferedStreamOptions.builder().build())
                .hasToString(options.toString());
    }

    @Test
    void rejectsInvalidSdkRetryKnobs() {
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryInitialDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay");
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryDelayMultiplier(0.99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryDelayMultiplier");
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().maxRetryDuration(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .retryInitialDelay(Duration.ofSeconds(5))
                                        .retryMaxDelay(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxDelay");
    }

    @Test
    void carriesConfiguredValues() {
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .maxAppendRequestBytes(1024)
                        .recoveryInitialBackoff(Duration.ofMillis(100))
                        .recoveryMaxBackoff(Duration.ofSeconds(5))
                        .recoveryMaxAttempts(3)
                        .build();

        assertThat(options.getMaxAppendRequestBytes()).isEqualTo(1024);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(3);
    }

    @Test
    void theRecoveryScheduleIsDerivedFromTheKnobsAndJittered() {
        RetrySchedule schedule =
                BufferedStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofSeconds(1))
                        .recoveryMaxBackoff(Duration.ofSeconds(4))
                        .recoveryMaxAttempts(3)
                        .build()
                        .toRecoverySchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(3);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        // The backoffs pin that each duration reaches its own slot, in milliseconds: an
        // ordering swap is already rejected by the schedule's own precondition, a mixed-up field
        // or unit is not.
        assertThat(schedule.backoffMs(1)).isBetween(750L, 1250L);
        assertThat(schedule.backoffMs(2)).isBetween(1500L, 2500L);
    }

    @Test
    void rejectsNonPositiveValues() {
        assertThatThrownBy(() -> BufferedStreamOptions.builder().maxAppendRequestBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> BufferedStreamOptions.builder().recoveryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(5))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
    }
}
