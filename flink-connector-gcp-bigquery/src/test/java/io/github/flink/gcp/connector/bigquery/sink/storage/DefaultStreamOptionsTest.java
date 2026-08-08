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

/** Tests for {@link DefaultStreamOptions}. */
class DefaultStreamOptionsTest {

    @Test
    void defaultsAreValid() {
        DefaultStreamOptions options = DefaultStreamOptions.builder().build();

        assertThat(options.getMaxAppendRequestBytes())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_APPEND_REQUEST_BYTES);
        assertThat(options.getRecoveryInitialBackoff())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RECOVERY_INITIAL_BACKOFF);
        assertThat(options.getRecoveryMaxBackoff())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RECOVERY_MAX_BACKOFF);
        assertThat(options.getRecoveryMaxAttempts())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RECOVERY_MAX_ATTEMPTS);
        assertThat(options.getRetryInitialDelay())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_INITIAL_DELAY);
        assertThat(options.getRetryDelayMultiplier())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_DELAY_MULTIPLIER);
        assertThat(options.getRetryMaxDelay())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_MAX_DELAY);
        assertThat(options.getRetryMaxAttempts())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_MAX_ATTEMPTS);
        assertThat(options.getMaxRetryDuration())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_RETRY_DURATION);
        assertThat(options.getMaxInflightRequests())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_REQUESTS);
        assertThat(options.getMaxInflightBytes())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_BYTES);
        assertThat(options.getMinConnectionsPerRegion())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MIN_CONNECTIONS_PER_REGION);
        assertThat(options.getMaxConnectionsPerRegion())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_CONNECTIONS_PER_REGION);
        assertThat(options.getDestinationIdleTimeout())
                .isEqualTo(DefaultStreamOptions.DEFAULT_DESTINATION_IDLE_TIMEOUT);
        assertThat(options.getFlushInterval()).isNull();
    }

    /**
     * The multiplexing default deliberately deviates from the SDK's 1000: a pooled connection is a
     * scale-up candidate above 20% of this limit, and the official guidance is to lower it.
     */
    @Test
    void inflightRequestDefaultFollowsMultiplexingGuidanceNotTheSdk() {
        assertThat(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_REQUESTS).isEqualTo(100);
    }

    @Test
    void carriesConfiguredValues() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .maxAppendRequestBytes(1024)
                        .recoveryInitialBackoff(Duration.ofMillis(100))
                        .recoveryMaxBackoff(Duration.ofSeconds(5))
                        .recoveryMaxAttempts(3)
                        .retryInitialDelay(Duration.ofMillis(250))
                        .retryDelayMultiplier(1.5)
                        .retryMaxDelay(Duration.ofSeconds(15))
                        .retryMaxAttempts(7)
                        .maxRetryDuration(Duration.ofMinutes(2))
                        .maxInflightRequests(50)
                        .maxInflightBytes(1024 * 1024)
                        .minConnectionsPerRegion(1)
                        .maxConnectionsPerRegion(4)
                        .destinationIdleTimeout(Duration.ofMinutes(30))
                        .flushInterval(Duration.ofSeconds(10))
                        .build();

        assertThat(options.getMaxAppendRequestBytes()).isEqualTo(1024);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(3);
        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getRetryDelayMultiplier()).isEqualTo(1.5);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(options.getMaxRetryDuration()).isEqualTo(Duration.ofMinutes(2));
        assertThat(options.getMaxInflightRequests()).isEqualTo(50);
        assertThat(options.getMaxInflightBytes()).isEqualTo(1024 * 1024);
        assertThat(options.getMinConnectionsPerRegion()).isEqualTo(1);
        assertThat(options.getMaxConnectionsPerRegion()).isEqualTo(4);
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(options.getFlushInterval()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void theRecoveryScheduleIsDerivedFromTheKnobsAndJittered() {
        RetrySchedule schedule =
                DefaultStreamOptions.builder()
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
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxAppendRequestBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> DefaultStreamOptions.builder().recoveryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryInitialDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryDelayMultiplier(0.99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryMaxDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxRetryDuration(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxInflightRequests(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxInflightBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().minConnectionsPerRegion(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxConnectionsPerRegion(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> DefaultStreamOptions.builder().destinationIdleTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().flushInterval(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The knob's own documentation offers a very large duration as the way to say "never evict", so
     * one too long for {@code Duration.toNanos()} is refused here rather than throwing {@code
     * ArithmeticException} from the writer's constructor on a TaskManager, failing the job as it
     * starts (#334; ADR-0068).
     */
    @Test
    void rejectsAnIdleTimeoutTooLargeForNanoseconds() {
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .destinationIdleTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destinationIdleTimeout must be at most")
                .hasMessageContaining("292 years");
        // The boundary itself is accepted, or the message would be describing a value it rejects.
        assertThat(
                        DefaultStreamOptions.builder()
                                .destinationIdleTimeout(expressible)
                                .build()
                                .getDestinationIdleTimeout())
                .isEqualTo(expressible);
    }

    @Test
    void multiplierOfExactlyOneIsAccepted() {
        assertThat(
                        DefaultStreamOptions.builder()
                                .retryDelayMultiplier(1.0)
                                .build()
                                .getRetryDelayMultiplier())
                .isEqualTo(1.0);
    }

    @Test
    void rejectsMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(5))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
    }

    @Test
    void acceptsMaxBackoffEqualToInitialBackoff() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .recoveryInitialBackoff(Duration.ofSeconds(5))
                        .recoveryMaxBackoff(Duration.ofSeconds(5))
                        .build();

        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsSdkMaxDelayBelowSdkInitialDelay() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .retryInitialDelay(Duration.ofSeconds(5))
                                        .retryMaxDelay(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxDelay");
    }

    @Test
    void acceptsSdkMaxDelayEqualToSdkInitialDelay() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .retryInitialDelay(Duration.ofSeconds(5))
                        .retryMaxDelay(Duration.ofSeconds(5))
                        .build();

        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsMaxConnectionsBelowMinConnections() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .minConnectionsPerRegion(5)
                                        .maxConnectionsPerRegion(4)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxConnectionsPerRegion");
    }

    @Test
    void acceptsMaxConnectionsEqualToMinConnections() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(5)
                        .maxConnectionsPerRegion(5)
                        .build();

        assertThat(options.getMaxConnectionsPerRegion()).isEqualTo(5);
    }

    @Test
    void equalsAndHashCodeCoverEveryKnob() {
        DefaultStreamOptions defaults = DefaultStreamOptions.builder().build();

        assertThat(DefaultStreamOptions.builder().build()).isEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().build()).hasSameHashCodeAs(defaults);
        assertThat(DefaultStreamOptions.builder().maxAppendRequestBytes(1).build())
                .isNotEqualTo(defaults);
        assertThat(
                        DefaultStreamOptions.builder()
                                .recoveryInitialBackoff(Duration.ofMillis(1))
                                .build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().recoveryMaxBackoff(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().recoveryMaxAttempts(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryInitialDelay(Duration.ofMillis(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryDelayMultiplier(3.0).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryMaxDelay(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryMaxAttempts(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxRetryDuration(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxInflightRequests(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().perDestinationMetrics(true).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxInflightBytes(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().minConnectionsPerRegion(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxConnectionsPerRegion(21).build())
                .isNotEqualTo(defaults);
        assertThat(
                        DefaultStreamOptions.builder()
                                .destinationIdleTimeout(Duration.ofMinutes(1))
                                .build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().flushInterval(Duration.ofSeconds(1)).build())
                .isNotEqualTo(defaults);
    }
}
