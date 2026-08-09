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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubPublisherOptions}. */
class PubSubPublisherOptionsTest {

    /**
     * An options instance with every knob set that can be set at once, shared by the override and
     * round-trip tests (also reused by the builder round trip in {@code PubSubSinkBuilderTest}).
     * Ordering is enabled here, which costs exactly two knobs: {@code retryTotalTimeout} and {@code
     * retryMaxAttempts} are rejected beside it (#310), so "every knob" is no longer literal. Those
     * two are carried by {@link #fullyPopulatedWithBoundedRetries()} instead, and the two instances
     * together cover every field.
     */
    static PubSubPublisherOptions fullyPopulated() {
        return PubSubPublisherOptions.builder()
                .batchElementCountThreshold(5)
                .batchRequestByteThreshold(1_000)
                .batchDelayThreshold(Duration.ofMillis(20))
                .enableMessageOrdering(true)
                .maxInFlightBytes(1_048_576)
                .retryInitialDelay(Duration.ofMillis(50))
                .retryDelayMultiplier(2.0)
                .retryMaxDelay(Duration.ofSeconds(5))
                .retryInitialRpcTimeout(Duration.ofSeconds(3))
                .retryRpcTimeoutMultiplier(1.5)
                .retryMaxRpcTimeout(Duration.ofSeconds(30))
                .maxInFlightMessages(42)
                .recoveryInitialBackoff(Duration.ofMillis(100))
                .recoveryMaxBackoff(Duration.ofSeconds(1))
                .recoveryMaxAttempts(3)
                .publishProgressTimeout(Duration.ofSeconds(90))
                .shutdownTimeout(Duration.ofSeconds(45))
                .maxConsecutiveRejections(9)
                .build();
    }

    /**
     * The other half of {@link #fullyPopulated()}: the two retry knobs an ordering-enabled
     * publisher would ignore, on a sink that does not enable ordering.
     */
    static PubSubPublisherOptions fullyPopulatedWithBoundedRetries() {
        return PubSubPublisherOptions.builder()
                .retryTotalTimeout(Duration.ofSeconds(120))
                .retryMaxAttempts(7)
                .build();
    }

    @Test
    void defaultsLeaveSdkKnobsUnsetAndKeepSinkDefaults() {
        PubSubPublisherOptions defaults = PubSubPublisherOptions.defaults();

        assertThat(defaults.getBatchElementCountThreshold()).isNull();
        assertThat(defaults.getBatchRequestByteThreshold()).isNull();
        assertThat(defaults.getBatchDelayThreshold()).isNull();
        assertThat(defaults.getRetryTotalTimeout()).isNull();
        assertThat(defaults.getRetryInitialDelay()).isNull();
        assertThat(defaults.getRetryDelayMultiplier()).isNull();
        assertThat(defaults.getRetryMaxDelay()).isNull();
        assertThat(defaults.getRetryInitialRpcTimeout()).isNull();
        assertThat(defaults.getRetryRpcTimeoutMultiplier()).isNull();
        assertThat(defaults.getRetryMaxRpcTimeout()).isNull();
        assertThat(defaults.getRetryMaxAttempts()).isNull();
        assertThat(defaults.isEnableMessageOrdering()).isFalse();
        assertThat(defaults.getMaxInFlightMessages()).isEqualTo(1000);
        assertThat(defaults.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(defaults.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(defaults.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(defaults.getRecoveryMaxAttempts()).isEqualTo(10);
        assertThat(defaults.getPublishProgressTimeout()).isEqualTo(Duration.ofSeconds(600));
        assertThat(defaults.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(defaults.getMaxConsecutiveRejections()).isEqualTo(100);
        assertThat(defaults.hasBatchingOverrides()).isFalse();
        assertThat(defaults.hasRetryOverrides()).isFalse();
        assertThat(defaults).isEqualTo(PubSubPublisherOptions.builder().build());
    }

    @Test
    void theRejectionBoundTakesOnlyPositiveValuesOrTheUnboundedSentinel() {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();

        // Zero has no meaning here: "no rejection tolerated" is 1, and a bound of zero would
        // silently override the dropping handler the user configured.
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConsecutiveRejections");
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(-2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(
                        builder.maxConsecutiveRejections(PubSubPublisherOptions.UNBOUNDED)
                                .build()
                                .getMaxConsecutiveRejections())
                .isEqualTo(PubSubPublisherOptions.UNBOUNDED);
        assertThat(
                        PubSubPublisherOptions.builder()
                                .maxConsecutiveRejections(5)
                                .build()
                                .getMaxConsecutiveRejections())
                .isEqualTo(5);
    }

    @Test
    void overridesAreKept() {
        PubSubPublisherOptions options = fullyPopulated();

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(5);
        assertThat(options.getBatchRequestByteThreshold()).isEqualTo(1_000);
        assertThat(options.getBatchDelayThreshold()).isEqualTo(Duration.ofMillis(20));
        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ofMillis(50));
        assertThat(options.getRetryDelayMultiplier()).isEqualTo(2.0);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRetryInitialRpcTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getRetryRpcTimeoutMultiplier()).isEqualTo(1.5);
        assertThat(options.getRetryMaxRpcTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.isEnableMessageOrdering()).isTrue();
        // The two knobs ordering costs; their values are covered by the sibling instance, in
        // boundedRetriesAreKeptWithoutMessageOrdering.
        assertThat(options.getRetryTotalTimeout()).isNull();
        assertThat(options.getRetryMaxAttempts()).isNull();
        assertThat(options.getMaxInFlightMessages()).isEqualTo(42);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(1_048_576);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(3);
        assertThat(options.getPublishProgressTimeout()).isEqualTo(Duration.ofSeconds(90));
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(45));
        assertThat(options.hasBatchingOverrides()).isTrue();
        assertThat(options.hasRetryOverrides()).isTrue();
    }

    @Test
    void rejectsNonPositiveValues() {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();

        assertThatThrownBy(() -> builder.batchElementCountThreshold(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchElementCountThreshold");
        assertThatThrownBy(() -> builder.batchRequestByteThreshold(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchRequestByteThreshold");
        assertThatThrownBy(() -> builder.batchDelayThreshold(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchDelayThreshold");
        assertThatThrownBy(() -> builder.batchDelayThreshold(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("batchDelayThreshold");
        assertThatThrownBy(() -> builder.retryTotalTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout");
        assertThatThrownBy(() -> builder.retryInitialDelay(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay");
        assertThatThrownBy(() -> builder.retryDelayMultiplier(0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryDelayMultiplier");
        assertThatThrownBy(() -> builder.retryMaxDelay(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay");
        assertThatThrownBy(() -> builder.retryInitialRpcTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialRpcTimeout");
        assertThatThrownBy(() -> builder.retryRpcTimeoutMultiplier(0.9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryRpcTimeoutMultiplier");
        assertThatThrownBy(() -> builder.retryMaxRpcTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout");
        assertThatThrownBy(() -> builder.retryMaxAttempts(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts");
        assertThatThrownBy(() -> builder.maxInFlightMessages(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightMessages");
        // Zero would make the write admission predicate hold with nothing in flight, and yield()
        // blocks until a mail arrives — a task hang rather than backpressure.
        assertThatThrownBy(() -> builder.maxInFlightBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightBytes");
        assertThatThrownBy(() -> builder.maxInFlightBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightBytes");
        assertThatThrownBy(() -> builder.recoveryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryInitialBackoff");
        assertThatThrownBy(() -> builder.recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxBackoff");
        assertThatThrownBy(() -> builder.recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxAttempts");
        assertThatThrownBy(() -> builder.publishProgressTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishProgressTimeout");
        assertThatThrownBy(() -> builder.publishProgressTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishProgressTimeout");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
    }

    @Test
    void aByteBoundIsAvailableWithMessageOrdering() {
        // The point of #85: the SDK flow-control byte limit that used to be the only byte bound
        // could not be combined with ordering (it leaks a permit per publish cancelled on a paused
        // key), leaving ordered sinks — where a paused key holds its whole cascade — with no byte
        // bound at all. The writer-owned cap has no such restriction.
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .enableMessageOrdering(true)
                        .maxInFlightBytes(1_000)
                        .build();

        assertThat(options.isEnableMessageOrdering()).isTrue();
        assertThat(options.getMaxInFlightBytes()).isEqualTo(1_000);
    }

    /**
     * Every {@code retry*} knob here is forwarded to gax's {@code RetrySettings}, which gives zero
     * meanings of its own: a zero total timeout bounds retries by the attempt count instead, a zero
     * RPC timeout lets the call run indefinitely, and a zero delay is gax's own default. They stay
     * settable as the SDK defines them; a positive sub-millisecond value is refused instead,
     * because gax reads them with {@code toMillis()} and it would silently become that zero
     * (ADR-0068).
     */
    @Test
    void theSdkRetryKnobsTakeTheVendorsZero() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .retryTotalTimeout(Duration.ZERO)
                        .retryInitialDelay(Duration.ZERO)
                        .retryMaxDelay(Duration.ZERO)
                        .retryInitialRpcTimeout(Duration.ZERO)
                        .retryMaxRpcTimeout(Duration.ZERO)
                        .build();

        assertThat(options.getRetryTotalTimeout()).isEqualTo(Duration.ZERO);
        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ZERO);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ZERO);
        assertThat(options.getRetryInitialRpcTimeout()).isEqualTo(Duration.ZERO);
        assertThat(options.getRetryMaxRpcTimeout()).isEqualTo(Duration.ZERO);

        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();
        Duration halfAMilli = Duration.ofNanos(500_000);
        assertThatThrownBy(() -> builder.retryTotalTimeout(halfAMilli))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout must be at least 1 millisecond");
        assertThatThrownBy(() -> builder.retryInitialDelay(halfAMilli))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay must be at least 1 millisecond");
        assertThatThrownBy(() -> builder.retryMaxDelay(halfAMilli))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay must be at least 1 millisecond");
        assertThatThrownBy(() -> builder.retryInitialRpcTimeout(halfAMilli))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialRpcTimeout must be at least 1 millisecond");
        assertThatThrownBy(() -> builder.retryMaxRpcTimeout(halfAMilli))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout must be at least 1 millisecond");
    }

    @Test
    void rejectsSubMillisecondRecoveryBackoffs() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryMaxBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    /**
     * The floor the source's knob of this name carries is deliberately absent here: this budget is
     * spent in nanoseconds by {@code BoundedShutdown}, so a sub-millisecond value waits for exactly
     * what it says, and the floor's message — "it is applied at millisecond granularity" — would be
     * false. The check follows the conversion, not the knob name (ADR-0068), and pinning the
     * accepted value is what makes a later "unification" of the two knobs fail here rather than
     * quietly narrowing this one.
     */
    @Test
    void aSubMillisecondShutdownTimeoutIsAcceptedBecauseItIsSpentInNanoseconds() {
        Duration halfAMilli = Duration.ofNanos(500_000);

        assertThat(
                        PubSubPublisherOptions.builder()
                                .shutdownTimeout(halfAMilli)
                                .build()
                                .getShutdownTimeout())
                .isEqualTo(halfAMilli);
    }

    @Test
    void rejectsRecoveryMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(5))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
    }

    @Test
    void theRecoveryScheduleIsDerivedFromTheKnobsAndJittered() {
        RetrySchedule schedule =
                PubSubPublisherOptions.builder()
                        .recoveryInitialBackoff(Duration.ofSeconds(1))
                        .recoveryMaxBackoff(Duration.ofSeconds(4))
                        .recoveryMaxAttempts(7)
                        .build()
                        .toRecoverySchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(7);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        // The backoffs pin that each duration reaches its own slot, in milliseconds: an
        // ordering swap is already rejected by the schedule's own precondition, a mixed-up field
        // or unit is not.
        assertThat(schedule.backoffMs(1)).isBetween(750L, 1250L);
        assertThat(schedule.backoffMs(2)).isBetween(1500L, 2500L);
    }

    @Test
    void zeroRetryMaxAttemptsIsAccepted() {
        assertThat(
                        PubSubPublisherOptions.builder()
                                .retryMaxAttempts(0)
                                .build()
                                .getRetryMaxAttempts())
                .isEqualTo(0);
    }

    /**
     * The knob's own documentation offers a very long {@code Duration} as the way to say
     * "effectively unbounded", so one too long for {@code Duration.toNanos()} has to be refused
     * here rather than throwing on a TaskManager at the first wait (#334; ADR-0068).
     */
    @Test
    void rejectsAProgressBudgetTooLargeForNanoseconds() {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThatThrownBy(() -> builder.publishProgressTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishProgressTimeout must be at most")
                .hasMessageContaining("292 years");
        // The boundary itself is accepted, or the message would be describing a value it rejects.
        assertThat(builder.publishProgressTimeout(expressible).build().getPublishProgressTimeout())
                .isEqualTo(expressible);
    }

    /**
     * The same bound on the budget that reaches {@code BoundedShutdown}, which converts it when the
     * writer's close starts the teardown — so an unbounded value would throw {@code
     * ArithmeticException} out of a close on a TaskManager, where it reaches Flink's teardown path
     * and not a user's {@code try} (#334; ADR-0068).
     */
    @Test
    void rejectsAShutdownBudgetTooLargeForNanoseconds() {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThatThrownBy(() -> builder.shutdownTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be at most")
                .hasMessageContaining("292 years");
        // The boundary itself is accepted, or the message would be describing a value it rejects.
        assertThat(builder.shutdownTimeout(expressible).build().getShutdownTimeout())
                .isEqualTo(expressible);
    }

    @Test
    void equalsAndHashCode() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated());
        assertThat(fullyPopulated()).isNotEqualTo(PubSubPublisherOptions.defaults());
    }

    @Test
    void roundTripsJavaSerialization() throws Exception {
        for (PubSubPublisherOptions options :
                List.of(fullyPopulated(), fullyPopulatedWithBoundedRetries())) {
            byte[] bytes = InstantiationUtil.serializeObject(options);
            PubSubPublisherOptions copy =
                    InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

            assertThat(copy).isEqualTo(options);
        }
    }

    /**
     * The knobs an ordering-enabled SDK publisher would overwrite are refused rather than accepted
     * and ignored (#310). Both orders of application, since a builder is a call sequence and the
     * check runs at {@code build()}.
     */
    @Test
    void rejectsBoundedRetriesBesideMessageOrdering() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .retryTotalTimeout(Duration.ofSeconds(30))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryTotalTimeout")
                .hasMessageContaining("enableMessageOrdering")
                // Only the knob that was set: being told to remove one you never configured is how
                // a correct message still costs a reader time.
                .hasMessageNotContaining("retryMaxAttempts");
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .retryMaxAttempts(3)
                                        .enableMessageOrdering(true)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxAttempts")
                .hasMessageContaining("enableMessageOrdering")
                .hasMessageNotContaining("retryTotalTimeout");
        // Zero is rejected like any other value, though it is the one case where the setting is
        // harmless: the SDK already reads 0 as "unlimited", which is what ordering imposes anyway.
        // Exempting it would make the rule "explicitly set" depend on the value, and a knob
        // accepted at 0 and refused at 1 is a worse surprise than a uniform refusal.
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .retryMaxAttempts(0)
                                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void theRejectionNamesBothKnobsWhenBothAreSet() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .enableMessageOrdering(true)
                                        .retryTotalTimeout(Duration.ofSeconds(30))
                                        .retryMaxAttempts(3)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryTotalTimeout(...) and retryMaxAttempts(...)");
    }

    @Test
    void theOtherRetryKnobsCombineWithMessageOrdering() {
        // Only two of the eight are overwritten by the SDK, so the check must not have widened to
        // "no retry override with ordering": fullyPopulated() sets the other six beside ordering.
        PubSubPublisherOptions options = fullyPopulated();

        assertThat(options.isEnableMessageOrdering()).isTrue();
        assertThat(options.hasRetryOverrides()).isTrue();
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void boundedRetriesAreKeptWithoutMessageOrdering() {
        PubSubPublisherOptions options = fullyPopulatedWithBoundedRetries();

        assertThat(options.isEnableMessageOrdering()).isFalse();
        assertThat(options.getRetryTotalTimeout()).isEqualTo(Duration.ofSeconds(120));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(7);
    }
}
