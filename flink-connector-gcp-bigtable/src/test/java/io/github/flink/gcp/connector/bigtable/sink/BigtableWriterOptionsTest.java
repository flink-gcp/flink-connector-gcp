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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableWriterOptions}. */
class BigtableWriterOptionsTest {

    @Test
    void defaultsLeaveTheBatchThresholdsToTheClient() {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();

        // Null rather than a restatement of the client's 100 / 20 MiB: an unset threshold has to
        // stay unset all the way to the settings builder, or a client retune would be overridden.
        assertThat(options.getBatchElementCountThreshold()).isNull();
        assertThat(options.getBatchRequestByteThreshold()).isNull();
        assertThat(options.getMaxInFlightEntries()).isEqualTo(1000);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(100);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(10);
        assertThat(options.getMaxActiveInstances()).isEqualTo(16);
        assertThat(options).isEqualTo(BigtableWriterOptions.builder().build());
    }

    @Test
    void carriesEveryConfiguredValue() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder()
                        .batchElementCountThreshold(50)
                        .batchRequestByteThreshold(1024)
                        .maxInFlightEntries(7)
                        .maxInFlightBytes(4096)
                        .maxConsecutiveRejections(5)
                        .maxActiveInstances(3)
                        .build();

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(50L);
        assertThat(options.getBatchRequestByteThreshold()).isEqualTo(1024L);
        assertThat(options.getMaxInFlightEntries()).isEqualTo(7);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(4096L);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(5);
        assertThat(options.getMaxActiveInstances()).isEqualTo(3);
        assertThat(options.toString())
                .contains(
                        "batchElementCountThreshold=50",
                        "maxInFlightEntries=7",
                        "maxConsecutiveRejections=5",
                        "maxActiveInstances=3");
    }

    @Test
    void theDestinationKnobsCarryTheirValuesAndTheirDefaults() {
        assertThat(BigtableWriterOptions.defaults().getDestinationIdleTimeout())
                .isEqualTo(Duration.ofHours(1));
        assertThat(BigtableWriterOptions.defaults().isPerDestinationMetrics()).isFalse();

        BigtableWriterOptions options =
                BigtableWriterOptions.builder()
                        .destinationIdleTimeout(Duration.ofMinutes(15))
                        .perDestinationMetrics(true)
                        .build();

        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(15));
        assertThat(options.isPerDestinationMetrics()).isTrue();
        assertThat(options.toString())
                .contains("destinationIdleTimeout=PT15M", "perDestinationMetrics=true");
        assertThat(options).isNotEqualTo(BigtableWriterOptions.defaults());
    }

    @Test
    void theIdleTimeoutTakesTheLargestDurationANanosecondClockCanExpress() {
        // Its own documentation offers a very large duration as the way to say "never evict", so
        // the ceiling is what keeps that instruction from throwing ArithmeticException out of the
        // writer's constructor on a task manager instead (ADR-0068).
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        assertThat(builder.destinationIdleTimeout(expressible).build().getDestinationIdleTimeout())
                .isEqualTo(expressible);
        assertThatThrownBy(() -> builder.destinationIdleTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.destinationIdleTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.destinationIdleTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isValueBased() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder().maxInFlightEntries(7).build();

        assertThat(options)
                .isEqualTo(BigtableWriterOptions.builder().maxInFlightEntries(7).build())
                .hasSameHashCodeAs(BigtableWriterOptions.builder().maxInFlightEntries(7).build())
                .isNotEqualTo(BigtableWriterOptions.defaults())
                .isNotEqualTo(
                        BigtableWriterOptions.builder()
                                .maxInFlightEntries(7)
                                .batchElementCountThreshold(50)
                                .build());
    }

    @Test
    void rejectsNonPositiveValues() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        assertThatThrownBy(() -> builder.batchElementCountThreshold(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchRequestByteThreshold(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightEntries(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxActiveInstances(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxActiveInstances must be positive");
    }

    @Test
    void anAbsentMaxActiveInstancesFieldUsesTheNewDefault() throws Exception {
        BigtableWriterOptions restored = BigtableWriterOptions.builder().build();
        java.lang.reflect.Field field =
                BigtableWriterOptions.class.getDeclaredField("maxActiveInstances");
        field.setAccessible(true);
        // Java deserialization bypasses the constructor and leaves a field absent from an older
        // stream at its JVM default. Set the options object itself to that state rather than the
        // builder, which an old stream never contained.
        field.set(restored, 0);

        assertThat(restored.getMaxActiveInstances()).isEqualTo(16);
        assertThat(restored)
                .isEqualTo(BigtableWriterOptions.defaults())
                .hasSameHashCodeAs(BigtableWriterOptions.defaults());
        assertThat(restored.toString()).isEqualTo(BigtableWriterOptions.defaults().toString());
    }

    @Test
    void rejectsBatchThresholdsTheClientWouldRefuseToBuildAClientFor() {
        // Without these a job configured past either ceiling submits fine and then dies on a task
        // manager as the writer opens: the client's own settings builder requires each threshold
        // to stay strictly below the matching flow-control budget and throws otherwise, so what
        // the job gets is "Failed to create a Bigtable mutation batcher" rather than a big batch.
        // Asserted on "must be at most" rather than on the knob name: both checks in each setter
        // name the knob, so the name alone would be satisfied by the positivity check. The figures
        // are asserted too, because the message is the whole of what the user gets at submission
        // and an unfilled placeholder would still carry the prefix.
        assertThatThrownBy(
                        () ->
                                BigtableWriterOptions.builder()
                                        .batchElementCountThreshold(
                                                BigtableWriterOptions
                                                                .MAX_BATCH_ELEMENT_COUNT_THRESHOLD_LIMIT
                                                        + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchElementCountThreshold must be at most 19999")
                .hasMessageContaining("20000 entries");
        assertThatThrownBy(
                        () ->
                                BigtableWriterOptions.builder()
                                        .batchRequestByteThreshold(
                                                BigtableWriterOptions
                                                                .MAX_BATCH_REQUEST_BYTE_THRESHOLD_LIMIT
                                                        + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchRequestByteThreshold must be at most 104857599")
                .hasMessageContaining("104857600 bytes (100 MiB)");
    }

    @Test
    void acceptsBatchThresholdsAtExactlyTheirCeilings() {
        // Set through the constants and asserted against the figures they stand for, so that
        // changing either constant is changing what this connector claims about the client: one
        // under the 20,000 entries and one byte under the 100 MiB its flow controller admits.
        // DefaultMutationBatcherFactoryTest is the other half — that the client really does accept
        // a threshold at exactly these values.
        BigtableWriterOptions options =
                BigtableWriterOptions.builder()
                        .batchElementCountThreshold(
                                BigtableWriterOptions.MAX_BATCH_ELEMENT_COUNT_THRESHOLD_LIMIT)
                        .batchRequestByteThreshold(
                                BigtableWriterOptions.MAX_BATCH_REQUEST_BYTE_THRESHOLD_LIMIT)
                        .build();

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(19_999L);
        assertThat(options.getBatchRequestByteThreshold()).isEqualTo(100L * 1024 * 1024 - 1);
    }

    @Test
    void bothCeilingsAreOneUnderTheClientsOwnFlowControlBudgets() {
        // The derivation, asserted against the client rather than against a number written twice:
        // its settings builder requires each batch threshold to stay *strictly* below the matching
        // budget, so one under is the largest value a client can be built with. Reading the budgets
        // from the SDK is what makes a client release that moves either one fail here — and what
        // catches a ceiling raised by one, which the reject/accept pair above cannot see, since it
        // is written in terms of the same constants.
        FlowControlSettings flowControl =
                BigtableDataSettings.newBuilderForEmulator("localhost", 1)
                        .setProjectId("p")
                        .setInstanceId("i")
                        .stubSettings()
                        .bulkMutateRowsSettings()
                        .getBatchingSettings()
                        .getFlowControlSettings();

        assertThat(BigtableWriterOptions.MAX_BATCH_ELEMENT_COUNT_THRESHOLD_LIMIT)
                .isEqualTo(flowControl.getMaxOutstandingElementCount() - 1);
        assertThat(BigtableWriterOptions.MAX_BATCH_REQUEST_BYTE_THRESHOLD_LIMIT)
                .isEqualTo(flowControl.getMaxOutstandingRequestBytes() - 1);
    }

    @Test
    void warnsWhenAnInFlightBoundIsAboveTheClientsOwnBudget() {
        // The warning is the whole feature: past the client's budget the configuration still
        // works — it is the client that bounds the sink, blocking the task thread — so nothing
        // else, no exception and no changed value, would tell a user their bound is inert.
        try (LogCapture capture = LogCapture.of(BigtableWriterOptions.class)) {
            BigtableWriterOptions options =
                    BigtableWriterOptions.builder()
                            .maxInFlightEntries(20_001)
                            .maxInFlightBytes(100L * 1024 * 1024 + 1)
                            .build();

            // Accepted unchanged, not clamped — the warning is advice, not a correction, because
            // a resolver spreading records over several instances draws on several budgets.
            assertThat(options.getMaxInFlightEntries()).isEqualTo(20_001);
            assertThat(capture.getMessages())
                    .hasSize(2)
                    .anySatisfy(
                            message ->
                                    assertThat(message)
                                            .contains("maxInFlightEntries is 20001")
                                            .contains("20000 entries")
                                            .contains("blocks* the task thread"))
                    .anySatisfy(
                            message ->
                                    assertThat(message)
                                            .contains("maxInFlightBytes is 104857601")
                                            .contains("104857600 bytes (100 MiB)"));
        }
    }

    @Test
    void doesNotWarnAtOrBelowTheClientsOwnBudget() {
        // The boundary is exact and worth pinning: the client admits its whole budget and blocks
        // on the request past it, so a bound *equal* to the budget still binds first. The
        // defaults are built here too — initializing this class runs DEFAULTS = builder().build(),
        // so defaults that tripped the warning would put both lines in every task manager's log
        // for a job that configured neither knob.
        try (LogCapture capture = LogCapture.of(BigtableWriterOptions.class)) {
            BigtableWriterOptions.builder()
                    .maxInFlightEntries(20_000)
                    .maxInFlightBytes(100L * 1024 * 1024)
                    .build();
            BigtableWriterOptions.builder().build();

            assertThat(capture.getMessages()).isEmpty();
        }
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
