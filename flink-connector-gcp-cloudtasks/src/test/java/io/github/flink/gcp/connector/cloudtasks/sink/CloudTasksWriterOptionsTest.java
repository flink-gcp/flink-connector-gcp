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

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.UnaryOperator;

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
        assertThat(defaults.getChannelPoolSize()).isNull();
        assertThat(defaults.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(defaults.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(defaults.getRecoveryMaxAttempts()).isEqualTo(8);
        assertThat(defaults.getNotFoundRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(defaults.getNotFoundRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(defaults.getNotFoundRecoveryMaxAttempts()).isEqualTo(3);
    }

    @Test
    void schedulesCarryTheConfiguredBudgets() {
        CloudTasksWriterOptions options =
                CloudTasksWriterOptions.builder()
                        .recoveryMaxAttempts(4)
                        .recoveryInitialBackoff(Duration.ofMillis(2_000))
                        .recoveryMaxBackoff(Duration.ofMillis(4_000))
                        .notFoundRecoveryMaxAttempts(2)
                        .notFoundRecoveryInitialBackoff(Duration.ofMillis(1_000))
                        .notFoundRecoveryMaxBackoff(Duration.ofMillis(1_000))
                        .build();

        assertThat(options.toRecoverySchedule().maxAttempts()).isEqualTo(4);
        assertThat(options.toNotFoundRecoverySchedule().maxAttempts()).isEqualTo(2);
        // Both schedules are jittered to de-synchronize parallel subtasks backing off against the
        // same queue.
        assertThat(options.toRecoverySchedule().jitterRatio())
                .isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        assertThat(options.toNotFoundRecoverySchedule().jitterRatio())
                .isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        // The backoffs pin that each budget's durations reach its own schedule, in milliseconds.
        assertThat(options.toRecoverySchedule().backoffMs(1)).isBetween(1_500L, 2_500L);
        assertThat(options.toNotFoundRecoverySchedule().backoffMs(1)).isBetween(750L, 1_250L);
    }

    @Test
    void rejectsNonPositiveCounts() {
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().maxInFlightTasks(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().channelPoolSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().notFoundRecoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsSubMillisecondBackoffs() {
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksWriterOptions.builder().notFoundRecoveryMaxBackoff(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsABackoffCapBelowTheInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(2))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
        assertThatThrownBy(
                        () ->
                                CloudTasksWriterOptions.builder()
                                        .notFoundRecoveryInitialBackoff(Duration.ofSeconds(2))
                                        .notFoundRecoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("notFoundRecoveryMaxBackoff");
    }

    @Test
    void optionsWithTheSameKnobsAreEqualAndDifferingOnesAreNot() {
        // Only equal instances were ever compared, so nothing held the field chain: a mutant
        // dropping any knob from equals - or from hashCode, which a sink's compiled-plan spec
        // hashes along with it - left every assertion in this class passing.
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated())
                .isNotEqualTo(CloudTasksWriterOptions.defaults());
        assertThat(fullyPopulated().toString())
                .startsWith("CloudTasksWriterOptions{maxInFlightTasks=17")
                .contains("recoveryInitialBackoff=PT0.2S")
                .contains("notFoundRecoveryInitialBackoff=PT0.6S")
                .contains("perDestinationMetrics=true");
    }

    @Test
    void everyKnobIsPartOfTheIdentity() {
        // One variation per knob, so a knob missing from equals fails here by name rather than as
        // one opaque assertion over a fully-populated pair. Each variation also checks the hash:
        // isNotEqualTo never consults hashCode, so a knob dropped from hashCode alone would
        // otherwise stay invisible — the fixed knob values make the inequality deterministic.
        assertThat(variedBy(builder -> builder.maxInFlightTasks(18)))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.channelPoolSize(5)))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.recoveryInitialBackoff(Duration.ofMillis(300))))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.recoveryMaxBackoff(Duration.ofSeconds(30))))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.recoveryMaxAttempts(9)))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(
                        variedBy(
                                builder ->
                                        builder.notFoundRecoveryInitialBackoff(
                                                Duration.ofMillis(700))))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.notFoundRecoveryMaxBackoff(Duration.ofSeconds(4))))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.notFoundRecoveryMaxAttempts(5)))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
        assertThat(variedBy(builder -> builder.perDestinationMetrics(false)))
                .isNotEqualTo(fullyPopulated())
                .doesNotHaveSameHashCodeAs(fullyPopulated());
    }

    /** The fully populated knob set with one knob overridden, which is what varies the identity. */
    private static CloudTasksWriterOptions variedBy(
            UnaryOperator<CloudTasksWriterOptions.Builder> variation) {
        return variation.apply(fullyPopulatedBuilder()).build();
    }

    private static CloudTasksWriterOptions fullyPopulated() {
        return fullyPopulatedBuilder().build();
    }

    private static CloudTasksWriterOptions.Builder fullyPopulatedBuilder() {
        return CloudTasksWriterOptions.builder()
                .maxInFlightTasks(17)
                .channelPoolSize(2)
                .recoveryInitialBackoff(Duration.ofMillis(200))
                .recoveryMaxBackoff(Duration.ofSeconds(20))
                .recoveryMaxAttempts(4)
                .notFoundRecoveryInitialBackoff(Duration.ofMillis(600))
                .notFoundRecoveryMaxBackoff(Duration.ofSeconds(3))
                .notFoundRecoveryMaxAttempts(2)
                .perDestinationMetrics(true);
    }
}
