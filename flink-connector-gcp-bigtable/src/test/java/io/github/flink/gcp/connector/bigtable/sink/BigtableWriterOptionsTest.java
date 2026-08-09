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

package io.github.flink.gcp.connector.bigtable.sink;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableWriterOptions}. */
class BigtableWriterOptionsTest {

    @Test
    void defaultsLeaveTheBatchThresholdsToTheClient() {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();

        // Null rather than a restatement of the client's 100 / 20 MB: an unset threshold has to
        // stay unset all the way to the settings builder, or a client retune would be overridden.
        assertThat(options.getBatchElementCount()).isNull();
        assertThat(options.getBatchByteSize()).isNull();
        assertThat(options.getMaxInFlightMutations()).isEqualTo(1000);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(100);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(10);
        assertThat(options).isEqualTo(BigtableWriterOptions.builder().build());
    }

    @Test
    void carriesEveryConfiguredValue() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder()
                        .batchElementCount(50)
                        .batchByteSize(1024)
                        .maxInFlightMutations(7)
                        .maxInFlightBytes(4096)
                        .maxConsecutiveRejections(5)
                        .build();

        assertThat(options.getBatchElementCount()).isEqualTo(50L);
        assertThat(options.getBatchByteSize()).isEqualTo(1024L);
        assertThat(options.getMaxInFlightMutations()).isEqualTo(7);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(4096L);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(5);
        assertThat(options.toString())
                .contains(
                        "batchElementCount=50",
                        "maxInFlightMutations=7",
                        "maxConsecutiveRejections=5");
    }

    @Test
    void isValueBased() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder().maxInFlightMutations(7).build();

        assertThat(options)
                .isEqualTo(BigtableWriterOptions.builder().maxInFlightMutations(7).build())
                .hasSameHashCodeAs(BigtableWriterOptions.builder().maxInFlightMutations(7).build())
                .isNotEqualTo(BigtableWriterOptions.defaults())
                .isNotEqualTo(
                        BigtableWriterOptions.builder()
                                .maxInFlightMutations(7)
                                .batchElementCount(50)
                                .build());
    }

    @Test
    void rejectsNonPositiveValues() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        assertThatThrownBy(() -> builder.batchElementCount(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchByteSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightMutations(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRejectionBoundTakesOnlyPositiveValuesOrTheUnboundedSentinel() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        // Zero has no meaning here: "no rejection tolerated" is 1, and a bound of zero would
        // silently override the dropping handler the user configured.
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConsecutiveRejections");
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(-2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(
                        builder.maxConsecutiveRejections(BigtableWriterOptions.UNBOUNDED)
                                .build()
                                .getMaxConsecutiveRejections())
                .isEqualTo(BigtableWriterOptions.UNBOUNDED);
    }

    @Test
    void theRecoveryKnobsDescribeTheJitteredSchedule() {
        RetrySchedule schedule =
                BigtableWriterOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(100))
                        .recoveryMaxBackoff(Duration.ofSeconds(2))
                        .recoveryMaxAttempts(3)
                        .build()
                        .toRecoverySchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(3);
        // Asserted directly rather than through backoff draws: an unjittered 100/200/400 sits
        // inside every per-attempt band below, so only this catches a schedule built without it.
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        // The per-attempt bands are deterministic for every jitter draw (base ±25%), and they
        // pin the initial backoff and the doubling.
        assertThat(schedule.backoffMs(1)).isBetween(75L, 125L);
        assertThat(schedule.backoffMs(2)).isBetween(150L, 250L);
        assertThat(schedule.backoffMs(3)).isBetween(300L, 500L);
    }

    @Test
    void theRecoveryBackoffCapBinds() {
        // Cap equal to the initial backoff: a schedule that ignored it would double to 2 s by
        // attempt 2, whose jittered draw (1.5-2.5 s) can never fall inside the capped band.
        RetrySchedule schedule =
                BigtableWriterOptions.builder()
                        .recoveryInitialBackoff(Duration.ofSeconds(1))
                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                        .recoveryMaxAttempts(2)
                        .build()
                        .toRecoverySchedule();

        assertThat(schedule.backoffMs(2)).isBetween(750L, 1_250L);
    }

    @Test
    void theRecoveryKnobsRejectSubMillisecondAndInvertedBudgets() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        assertThatThrownBy(() -> builder.recoveryInitialBackoff(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryInitialBackoff");
        assertThatThrownBy(() -> builder.recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                BigtableWriterOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(20))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff must be at least recoveryInitialBackoff");
    }
}
