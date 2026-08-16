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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubSubscriberOptions}. */
class PubSubSubscriberOptionsTest {

    @Test
    void defaultsLeaveSdkKnobsUnsetAndKeepSourceDefaults() {
        PubSubSubscriberOptions options = PubSubSubscriberOptions.defaults();

        assertThat(options.getFlowControlMaxOutstandingElementCount()).isNull();
        assertThat(options.getFlowControlMaxOutstandingRequestBytes()).isNull();
        // Unset, which resolves to twice the effective flow-control limits rather than to a
        // number of their own — PausedSplitBufferLimits is where that fallback lives.
        assertThat(options.getPausedSplitBufferMaxMessages()).isNull();
        assertThat(options.getPausedSplitBufferMaxBytes()).isNull();
        assertThat(options.getParallelPullCount()).isNull();
        assertThat(options.getMaxAckExtensionPeriod()).isNull();
        assertThat(options.getMinDurationPerAckExtension()).isNull();
        assertThat(options.getMaxDurationPerAckExtension()).isNull();
        assertThat(options.getAwaitAckConfirmation()).isNull();
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getMaxRecordsPerFetch()).isEqualTo(1_000);
        assertThat(options.getFirstCheckpointTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(PubSubSubscriberOptions.builder().build()).isEqualTo(options);
    }

    @Test
    void overridesAreKept() {
        PubSubSubscriberOptions options = fullyPopulated();

        assertThat(options.getFlowControlMaxOutstandingElementCount()).isEqualTo(500L);
        assertThat(options.getFlowControlMaxOutstandingRequestBytes()).isEqualTo(1_048_576L);
        assertThat(options.getPausedSplitBufferMaxMessages()).isEqualTo(400L);
        assertThat(options.getPausedSplitBufferMaxBytes()).isEqualTo(524_288L);
        assertThat(options.getParallelPullCount()).isNull();
        assertThat(options.getMaxAckExtensionPeriod()).isEqualTo(Duration.ofMinutes(30));
        assertThat(options.getMinDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.getMaxDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(60));
        assertThat(options.getAwaitAckConfirmation()).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getMaxRecordsPerFetch()).isEqualTo(250);
        assertThat(options.getFirstCheckpointTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void rejectsNonPositiveValues() {
        PubSubSubscriberOptions.Builder builder = PubSubSubscriberOptions.builder();

        assertThatThrownBy(() -> builder.flowControlMaxOutstandingElementCount(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flowControlMaxOutstandingElementCount");
        assertThatThrownBy(() -> builder.flowControlMaxOutstandingRequestBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flowControlMaxOutstandingRequestBytes");
        assertThatThrownBy(() -> builder.pausedSplitBufferMaxMessages(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pausedSplitBufferMaxMessages");
        assertThatThrownBy(() -> builder.pausedSplitBufferMaxBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pausedSplitBufferMaxBytes");
        assertThatThrownBy(() -> builder.parallelPullCount(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelPullCount");
        assertThatThrownBy(() -> builder.maxAckExtensionPeriod(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAckExtensionPeriod");
        assertThatThrownBy(() -> builder.minDurationPerAckExtension(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minDurationPerAckExtension");
        assertThatThrownBy(() -> builder.maxDurationPerAckExtension(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDurationPerAckExtension");
        assertThatThrownBy(() -> builder.awaitAckConfirmation(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("awaitAckConfirmation");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        assertThatThrownBy(() -> builder.maxRecordsPerFetch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRecordsPerFetch");
        assertThatThrownBy(() -> builder.firstCheckpointTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("firstCheckpointTimeout");

        // Zero is the boundary the others reject and this one accepts: it disables the detector.
        assertThat(
                        builder.firstCheckpointTimeout(Duration.ZERO)
                                .build()
                                .getFirstCheckpointTimeout())
                .isEqualTo(Duration.ZERO);
    }

    /**
     * The client library reads a zero {@code maxAckExtensionPeriod} as "disable auto deadline
     * extensions", so the knob stays settable as the SDK defines it (ADR-0068). It takes no
     * millisecond floor, unlike the {@code retry*} knobs gax truncates: {@code MessageDispatcher}
     * spends it as {@code now().plus(period)} at nanosecond resolution, so a sub-millisecond value
     * is a very short budget rather than a zero in disguise. Only a negative is refused.
     */
    @Test
    void maxAckExtensionPeriodTakesTheVendorsZeroAndRefusesOnlyANegative() {
        assertThat(
                        PubSubSubscriberOptions.builder()
                                .maxAckExtensionPeriod(Duration.ZERO)
                                .build()
                                .getMaxAckExtensionPeriod())
                .isEqualTo(Duration.ZERO);
        assertThat(
                        PubSubSubscriberOptions.builder()
                                .maxAckExtensionPeriod(Duration.ofNanos(500_000))
                                .build()
                                .getMaxAckExtensionPeriod())
                .isEqualTo(Duration.ofNanos(500_000));
        assertThatThrownBy(
                        () ->
                                PubSubSubscriberOptions.builder()
                                        .maxAckExtensionPeriod(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAckExtensionPeriod must not be negative");
    }

    /**
     * Both budgets are spent in whole milliseconds — {@code future.get(toMillis())} and {@code
     * latch.await(toMillis())} — where a sub-millisecond value means zero: a confirmation that
     * times out before it can arrive, and a shutdown that waits for nothing. The sink's knob of the
     * same name keeps no floor, because it is spent in nanoseconds: the check follows the
     * conversion, not the name (ADR-0068).
     */
    @Test
    void rejectsSubMillisecondBudgets() {
        PubSubSubscriberOptions.Builder builder = PubSubSubscriberOptions.builder();

        assertThatThrownBy(() -> builder.awaitAckConfirmation(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("awaitAckConfirmation must be at least 1 millisecond");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be at least 1 millisecond");
    }

    /**
     * Both budgets are refused past what {@code Duration.toNanos()} can express (#334; ADR-0068).
     * {@code firstCheckpointTimeout} is the one with the crash — {@code MissingCheckpointDetector}
     * converts it in its constructor, so a longer budget fails the reader as it is built on a
     * TaskManager. {@code shutdownTimeout} is spent in milliseconds and would not throw; it takes
     * the same ceiling because it is the same knob name, with the same "effectively unbounded"
     * reading, as the sink's.
     */
    @Test
    void rejectsBudgetsTooLargeForNanoseconds() {
        PubSubSubscriberOptions.Builder builder = PubSubSubscriberOptions.builder();
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThatThrownBy(() -> builder.firstCheckpointTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("firstCheckpointTimeout must be at most")
                .hasMessageContaining("292 years");
        assertThatThrownBy(() -> builder.shutdownTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be at most")
                .hasMessageContaining("292 years");

        // A budget absurd enough to overflow toMillis() is still answered by the message that
        // names the knob. shutdownTimeout is the one setter carrying both a ceiling and a
        // millisecond floor, and the floor converts: run first, it would answer this with an
        // ArithmeticException naming nothing (ADR-0068).
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be at most");

        // The boundary itself is accepted, or each message would describe a value it rejects.
        PubSubSubscriberOptions options =
                builder.firstCheckpointTimeout(expressible).shutdownTimeout(expressible).build();
        assertThat(options.getFirstCheckpointTimeout()).isEqualTo(expressible);
        assertThat(options.getShutdownTimeout()).isEqualTo(expressible);
    }

    @Test
    void rejectsAMinimumAckExtensionAtOrAboveTheMaximum() {
        // The SDK enforces this itself, but with a message-less argument check.
        assertThatThrownBy(
                        () ->
                                PubSubSubscriberOptions.builder()
                                        .minDurationPerAckExtension(Duration.ofSeconds(60))
                                        .maxDurationPerAckExtension(Duration.ofSeconds(60))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("minDurationPerAckExtension")
                .hasMessageContaining("maxDurationPerAckExtension");
    }

    @Test
    void acceptsOneAckExtensionBoundWithoutTheOther() {
        assertThat(
                        PubSubSubscriberOptions.builder()
                                .minDurationPerAckExtension(Duration.ofSeconds(60))
                                .build()
                                .getMaxDurationPerAckExtension())
                .isNull();
    }

    @Test
    void equalsAndHashCode() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated())
                .isNotEqualTo(PubSubSubscriberOptions.defaults());
    }

    @Test
    void roundTripsJavaSerialization() throws Exception {
        PubSubSubscriberOptions options = fullyPopulated();

        assertThat(
                        InstantiationUtil.<PubSubSubscriberOptions>deserializeObject(
                                InstantiationUtil.serializeObject(options),
                                getClass().getClassLoader()))
                .isEqualTo(options);
    }

    /**
     * Every knob set to a non-default value except {@code parallelPullCount}, which is the one with
     * a cross-option constraint (the source builder rejects it under ordered consumption), so the
     * fixture stays combinable with any ordering mode. Reused by {@link PubSubSourceBuilderTest}.
     */
    static PubSubSubscriberOptions fullyPopulated() {
        return PubSubSubscriberOptions.builder()
                .flowControlMaxOutstandingElementCount(500)
                .flowControlMaxOutstandingRequestBytes(1_048_576)
                .pausedSplitBufferMaxMessages(400)
                .pausedSplitBufferMaxBytes(524_288)
                .maxAckExtensionPeriod(Duration.ofMinutes(30))
                .minDurationPerAckExtension(Duration.ofSeconds(15))
                .maxDurationPerAckExtension(Duration.ofSeconds(60))
                .awaitAckConfirmation(Duration.ofSeconds(20))
                .shutdownTimeout(Duration.ofSeconds(3))
                .maxRecordsPerFetch(250)
                .firstCheckpointTimeout(Duration.ofMinutes(2))
                .build();
    }
}
