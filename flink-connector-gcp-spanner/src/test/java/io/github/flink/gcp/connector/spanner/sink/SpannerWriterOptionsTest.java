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

package io.github.flink.gcp.connector.spanner.sink;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerWriterOptions}. */
class SpannerWriterOptionsTest {

    @Test
    void defaultsCarryBeamsBatchLimitsAndNoServiceSideOverrides() {
        SpannerWriterOptions options = SpannerWriterOptions.defaults();

        assertThat(options.getMaxBatchCells()).isEqualTo(5_000);
        assertThat(options.getMaxBatchMutations()).isEqualTo(500);
        assertThat(options.getMaxBatchBytes()).isEqualTo(1024L * 1024);
        // Unset, so the service's own handling stays in place rather than being restated here.
        assertThat(options.getMaxCommitDelay()).isNull();
        assertThat(options.getRpcPriority()).isNull();
    }

    @Test
    void defaultsAreTheSameAsAnUntouchedBuilder() {
        assertThat(SpannerWriterOptions.defaults())
                .isEqualTo(SpannerWriterOptions.builder().build())
                .hasSameHashCodeAs(SpannerWriterOptions.builder().build());
    }

    @Test
    void theDefaultCellCapStaysWellUnderSpannersPerRequestLimit() {
        // The headroom is the point: a table whose indexes the writer could not read is counted
        // without them, and 16x is what absorbs that. A change here is a change to that argument.
        assertThat(SpannerWriterOptions.DEFAULT_MAX_BATCH_CELLS * 16).isLessThanOrEqualTo(80_000);
    }

    @Test
    void retryKnobsBecomeAJitteredSchedule() {
        SpannerWriterOptions options =
                SpannerWriterOptions.builder()
                        .retryInitialBackoff(Duration.ofMillis(20))
                        .retryMaxBackoff(Duration.ofMillis(80))
                        .retryMaxAttempts(4)
                        .build();

        RetrySchedule schedule = options.toRetrySchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(4);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        // Exponential up to the cap, then jittered — so assert the band rather than the value.
        assertThat(schedule.backoffMs(1)).isBetween(15L, 25L);
        assertThat(schedule.backoffMs(9)).isBetween(60L, 100L);
    }

    @Test
    void rejectsNonPositiveBatchLimits() {
        assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchCells(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchCells");
        assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchMutations(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchMutations");
        assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchBytes");
    }

    @Test
    void rejectsACommitDelayThatSpannerWouldRefuse() {
        assertThatThrownBy(
                        () -> SpannerWriterOptions.builder().maxCommitDelay(Duration.ofMillis(501)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCommitDelay");
        assertThatThrownBy(
                        () -> SpannerWriterOptions.builder().maxCommitDelay(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsTheCommitDelayBoundsSpannerDoes() {
        // Zero has a meaning of its own — commit without waiting to group — so it is not rejected
        // as a positivity violation the way a backoff would be.
        assertThat(
                        SpannerWriterOptions.builder()
                                .maxCommitDelay(Duration.ZERO)
                                .build()
                                .getMaxCommitDelay())
                .isEqualTo(Duration.ZERO);
        assertThat(
                        SpannerWriterOptions.builder()
                                .maxCommitDelay(SpannerWriterOptions.MAX_COMMIT_DELAY_LIMIT)
                                .build()
                                .getMaxCommitDelay())
                .isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void keepsSubMillisecondCommitDelaysBecauseTheClientForwardsThem() {
        // The client copies seconds and nanos straight onto the request, so rounding here would
        // silently change what the service was asked for.
        Duration halfMilli = Duration.ofNanos(500_000);

        assertThat(
                        SpannerWriterOptions.builder()
                                .maxCommitDelay(halfMilli)
                                .build()
                                .getMaxCommitDelay())
                .isEqualTo(halfMilli);
    }

    @Test
    void rejectsRetryBackoffsBelowAMillisecondBecauseTheScheduleIsInMilliseconds() {
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .retryInitialBackoff(Duration.ofNanos(999_999)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialBackoff");
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .retryMaxBackoff(Duration.ofNanos(999_999)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxBackoff");
    }

    @Test
    void rejectsABackoffCapBelowTheFirstBackoff() {
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .retryInitialBackoff(Duration.ofSeconds(10))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxBackoff");
    }

    @Test
    void rejectsNonPositiveRetryAttempts() {
        assertThatThrownBy(() -> SpannerWriterOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts");
    }

    @Test
    void rejectsNullOptionalKnobs() {
        assertThatThrownBy(() -> SpannerWriterOptions.builder().maxCommitDelay(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerWriterOptions.builder().rpcPriority(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void everyKnobParticipatesInEqualityAndIsRendered() {
        SpannerWriterOptions options =
                SpannerWriterOptions.builder()
                        .maxBatchCells(11)
                        .maxBatchMutations(22)
                        .maxBatchBytes(33)
                        .maxCommitDelay(Duration.ofMillis(44))
                        .rpcPriority(SpannerRpcPriority.LOW)
                        .retryInitialBackoff(Duration.ofMillis(55))
                        .retryMaxBackoff(Duration.ofMillis(66))
                        .retryMaxAttempts(7)
                        .build();

        assertThat(options).isNotEqualTo(SpannerWriterOptions.defaults());
        assertThat(options.toString())
                .contains("maxBatchCells=11")
                .contains("maxBatchMutations=22")
                .contains("maxBatchBytes=33")
                .contains("maxCommitDelay=PT0.044S")
                .contains("rpcPriority=LOW")
                .contains("retryInitialBackoff=PT0.055S")
                .contains("retryMaxBackoff=PT0.066S")
                .contains("retryMaxAttempts=7");
    }
}
