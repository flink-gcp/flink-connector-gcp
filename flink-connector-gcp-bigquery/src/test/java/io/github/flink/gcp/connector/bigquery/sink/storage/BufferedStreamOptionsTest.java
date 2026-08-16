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
        assertThat(options.getDestinationIdleTimeout())
                .isEqualTo(BufferedStreamOptions.DEFAULT_DESTINATION_IDLE_TIMEOUT);
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
        // toString is the operator-facing dump of the configuration, so every knob must appear.
        assertThat(options.toString())
                .contains("retryInitialDelay=PT0.25S")
                .contains("retryDelayMultiplier=1.5")
                .contains("retryMaxDelay=PT15S")
                .contains("retryMaxAttempts=7")
                .contains("maxRetryDuration=PT2M");
    }

    /**
     * One knob at a time, so dropping any single field from {@code equals}/{@code hashCode} fails
     * here — setting all five at once would not.
     */
    @Test
    void equalsDistinguishesEachSdkRetryKnob() {
        BufferedStreamOptions defaults = BufferedStreamOptions.builder().build();

        assertThat(BufferedStreamOptions.builder().retryInitialDelay(Duration.ofMillis(1)).build())
                .isNotEqualTo(defaults);
        assertThat(BufferedStreamOptions.builder().retryDelayMultiplier(3.0).build())
                .isNotEqualTo(defaults);
        assertThat(BufferedStreamOptions.builder().retryMaxDelay(Duration.ofMinutes(1)).build())
                .isNotEqualTo(defaults);
        assertThat(BufferedStreamOptions.builder().retryMaxAttempts(9).build())
                .isNotEqualTo(defaults);
        assertThat(BufferedStreamOptions.builder().maxRetryDuration(Duration.ofMinutes(9)).build())
                .isNotEqualTo(defaults);
    }

    @Test
    void rejectsInvalidSdkRetryKnobs() {
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .retryInitialDelay(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay");
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryDelayMultiplier(0.99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryDelayMultiplier");
        assertThatThrownBy(() -> BufferedStreamOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts");
        assertThatThrownBy(
                        () -> BufferedStreamOptions.builder().retryMaxDelay(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay");
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
                        .destinationIdleTimeout(Duration.ofMinutes(15))
                        .recoveryInitialBackoff(Duration.ofMillis(100))
                        .recoveryMaxBackoff(Duration.ofSeconds(5))
                        .recoveryMaxAttempts(3)
                        .build();

        assertThat(options.getMaxAppendRequestBytes()).isEqualTo(1024);
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(15));
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
                        () -> BufferedStreamOptions.builder().destinationIdleTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destinationIdleTimeout");
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .destinationIdleTimeout(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be at most");
        assertThatThrownBy(
                        () -> BufferedStreamOptions.builder().recoveryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BufferedStreamOptions.builder().recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void equalityAndDiagnosticsIncludeDestinationIdleTimeout() {
        BufferedStreamOptions configured =
                BufferedStreamOptions.builder()
                        .destinationIdleTimeout(Duration.ofMinutes(2))
                        .build();

        assertThat(configured).isNotEqualTo(BufferedStreamOptions.builder().build());
        assertThat(configured.toString()).contains("destinationIdleTimeout=PT2M");
    }

    @Test
    void optionsSerializedBeforeTheIdleTimeoutWasAddedReceiveItsDefault() throws Exception {
        BufferedStreamOptions restored = BufferedStreamOptions.builder().build();
        java.lang.reflect.Field field =
                BufferedStreamOptions.class.getDeclaredField("destinationIdleTimeout");
        field.setAccessible(true);
        field.set(restored, null);

        assertThat(restored.getDestinationIdleTimeout())
                .isEqualTo(BufferedStreamOptions.DEFAULT_DESTINATION_IDLE_TIMEOUT);
        assertThat(restored).isEqualTo(BufferedStreamOptions.builder().build());
        assertThat(restored.toString()).contains("destinationIdleTimeout=PT1H");
    }

    /**
     * The recovery knobs reach a {@code RetrySchedule} through {@code toMillis()}, so a
     * sub-millisecond value used to be accepted here and rejected by the schedule's constructor —
     * on a TaskManager, as the writer or the committer was built (ADR-0068).
     */
    @Test
    void rejectsSubMillisecondRecoveryBackoffs() {
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryInitialBackoff must be at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .recoveryMaxBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxBackoff must be at least 1 millisecond");
    }

    /**
     * The {@code retry*} knobs are forwarded to gax, which gives zero meanings of its own — no
     * delay before the first retry, a cap that clamps every delay to none — so they stay settable
     * as the SDK defines them. A positive sub-millisecond value is refused instead, because gax
     * reads them with {@code toMillis()} and it would silently become that zero (ADR-0068). Both
     * delays are set together because this builder still requires the cap to be at least the
     * initial delay.
     */
    @Test
    void theSdkRetryDelaysTakeTheVendorsZero() {
        BufferedStreamOptions options =
                BufferedStreamOptions.builder()
                        .retryInitialDelay(Duration.ZERO)
                        .retryMaxDelay(Duration.ZERO)
                        .build();

        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ZERO);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ZERO);
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .retryInitialDelay(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay must be at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .retryMaxDelay(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay must be at least 1 millisecond");
    }

    /**
     * Zero is the SDK's own sentinel for "retry without a time limit" — {@code
     * StreamWriter.Builder.setMaxRetryDuration} documents it, {@code ConnectionWorker} implements
     * it by skipping the elapsed-time comparison — so an SDK setting this connector merely forwards
     * stays settable as the SDK defines it. The floor still applies to every other value, and a
     * positive sub-millisecond one is refused precisely because it would silently *become* the
     * sentinel (ADR-0068).
     */
    @Test
    void maxRetryDurationTakesTheSdkSentinelForUnlimitedRetry() {
        assertThat(
                        BufferedStreamOptions.builder()
                                .maxRetryDuration(Duration.ZERO)
                                .build()
                                .getMaxRetryDuration())
                .isEqualTo(Duration.ZERO);
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .maxRetryDuration(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryDuration must be at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                BufferedStreamOptions.builder()
                                        .maxRetryDuration(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryDuration must be at least 1 millisecond");
        assertThatThrownBy(() -> BufferedStreamOptions.builder().maxRetryDuration(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("maxRetryDuration must not be null");
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
