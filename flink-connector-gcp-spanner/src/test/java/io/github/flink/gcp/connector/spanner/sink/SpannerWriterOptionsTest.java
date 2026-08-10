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
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.testutils.LogCapture;
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
    void theDefaultCellCapStaysWellUnderTheCeilingAKnobMayBeRaisedTo() {
        // The headroom is the point: a table whose indexes the writer could not read is counted
        // without them, and 16x is what absorbs that. A change to either number is a change to that
        // argument. Widened to long so that raising the default past Integer.MAX_VALUE / 16 fails
        // here rather than wrapping negative and passing.
        assertThat((long) SpannerWriterOptions.DEFAULT_MAX_BATCH_CELLS * 16)
                .isLessThanOrEqualTo(SpannerWriterOptions.MAX_BATCH_CELLS_LIMIT);
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
        // Negatives as well as zero: a negative cap clears a ceiling check too, so only the
        // positivity check stands between it and a writer that flushes an empty batch forever.
        for (int nonPositive : new int[] {0, -1}) {
            assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchCells(nonPositive))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBatchCells must be positive");
            assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchMutations(nonPositive))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBatchMutations must be positive");
            assertThatThrownBy(() -> SpannerWriterOptions.builder().maxBatchBytes(nonPositive))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBatchBytes must be positive");
        }
    }

    @Test
    void rejectsBatchLimitsAboveWhatSpannerDocuments() {
        // Without these a job configured past the byte limit builds fine and dies on a task
        // manager, one request-level refusal per batch, for a mistake that was visible at
        // submission. The cell ceiling is the precautionary half — Spanner documents no
        // request-level mutation count either way. The message is asserted on
        // "must be at most" rather than the knob name: both checks in each setter name the knob, so
        // the name alone would let the positivity check satisfy this test — which it does if a
        // ceiling ever reaches its type's maximum and `+ 1` wraps negative.
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .maxBatchCells(
                                                SpannerWriterOptions.MAX_BATCH_CELLS_LIMIT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchCells must be at most");
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .maxBatchMutations(
                                                SpannerWriterOptions.MAX_BATCH_MUTATIONS_LIMIT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchMutations must be at most");
        assertThatThrownBy(
                        () ->
                                SpannerWriterOptions.builder()
                                        .maxBatchBytes(
                                                SpannerWriterOptions.MAX_BATCH_BYTES_LIMIT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchBytes must be at most");
    }

    @Test
    void theMutationCeilingIsDerivedFromTheCellOneRatherThanRepeatingIt() {
        // Every mutation costs at least one cell, so a batch never holds more mutations than cells:
        // the mutation ceiling *is* the cell ceiling. Pinned as equality rather than as 80,000 so
        // that raising the cell ceiling carries this one instead of leaving it cutting below what a
        // batch may legally hold.
        assertThat(SpannerWriterOptions.MAX_BATCH_MUTATIONS_LIMIT)
                .isEqualTo(SpannerWriterOptions.MAX_BATCH_CELLS_LIMIT);
    }

    @Test
    void acceptsBatchLimitsAtExactlyWhatSpannerDocuments() {
        // Set through the constants and asserted against the documented figures, so that changing
        // either constant is changing what this connector claims the documentation says.
        SpannerWriterOptions options =
                SpannerWriterOptions.builder()
                        .maxBatchCells(SpannerWriterOptions.MAX_BATCH_CELLS_LIMIT)
                        .maxBatchMutations(SpannerWriterOptions.MAX_BATCH_MUTATIONS_LIMIT)
                        .maxBatchBytes(SpannerWriterOptions.MAX_BATCH_BYTES_LIMIT)
                        .build();

        assertThat(options.getMaxBatchCells()).isEqualTo(80_000);
        assertThat(options.getMaxBatchMutations()).isEqualTo(80_000);
        // 100 MiB, the looser of the two readings of what a batch write request may weigh (#441).
        assertThat(options.getMaxBatchBytes()).isEqualTo(100L * 1024 * 1024);
    }

    @Test
    void warnsWhenTheMutationCapCannotTakeEffect() {
        // The warning is the whole feature: the configuration works, so nothing else — no
        // exception, no changed value — would tell a user their mutation cap is inert.
        try (LogCapture capture = LogCapture.of(SpannerWriterOptions.class)) {
            SpannerWriterOptions options =
                    SpannerWriterOptions.builder()
                            .maxBatchCells(100)
                            .maxBatchMutations(101)
                            .build();

            // Accepted unchanged, not clamped — the warning is advice, not a correction.
            assertThat(options.getMaxBatchMutations()).isEqualTo(101);
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("maxBatchMutations is 101")
                    .contains("maxBatchCells is 100")
                    .contains("can never take effect");
        }
    }

    @Test
    void theDefaultsDoNotTripTheWarning() {
        // Load-bearing, and not obvious: initializing this class runs DEFAULTS = builder().build(),
        // and a task manager holding a deserialized instance has initialized it. So defaults that
        // satisfied the warn condition would put the line in every task manager's log, for a job
        // that configured neither knob.
        assertThat(SpannerWriterOptions.DEFAULT_MAX_BATCH_MUTATIONS)
                .isLessThanOrEqualTo(SpannerWriterOptions.DEFAULT_MAX_BATCH_CELLS);
    }

    @Test
    void doesNotWarnWhenTheMutationCapCanTakeEffect() {
        // The boundary: equal caps are reachable, since a mutation may cost exactly one cell.
        try (LogCapture capture = LogCapture.of(SpannerWriterOptions.class)) {
            SpannerWriterOptions.builder().maxBatchCells(100).maxBatchMutations(100).build();
            SpannerWriterOptions.builder().build();

            assertThat(capture.getMessages()).isEmpty();
        }
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
