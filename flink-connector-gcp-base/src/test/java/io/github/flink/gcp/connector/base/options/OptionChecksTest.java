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

package io.github.flink.gcp.connector.base.options;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OptionChecks}. */
class OptionChecksTest {

    @Test
    void checkPositiveReturnsTheDurationItWasGiven() {
        Duration duration = Duration.ofSeconds(30);

        // Returned rather than merely validated: most call sites assign the result.
        assertThat(OptionChecks.checkPositive(duration, "shutdownTimeout")).isSameAs(duration);
    }

    @Test
    void checkPositiveRejectsNullZeroAndNegative() {
        assertThatThrownBy(() -> OptionChecks.checkPositive(null, "shutdownTimeout"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("shutdownTimeout must not be null");
        assertThatThrownBy(() -> OptionChecks.checkPositive(Duration.ZERO, "shutdownTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be positive");
        assertThatThrownBy(
                        () -> OptionChecks.checkPositive(Duration.ofSeconds(-1), "shutdownTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout must be positive");
    }

    @Test
    void checkAtLeastOneMilliAcceptsTheBoundaryAndRejectsOneNanosecondLess() {
        Duration boundary = Duration.ofMillis(1);

        // The boundary itself is accepted — a knob whose consumer reads whole milliseconds may be
        // set to one — and returned, because most call sites assign the result.
        assertThat(OptionChecks.checkAtLeastOneMilli(boundary, "retryInitialBackoff"))
                .isSameAs(boundary);
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilli(
                                        boundary.minusNanos(1), "retryInitialBackoff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialBackoff must be at least 1 millisecond");
    }

    /**
     * The floor subsumes positivity rather than deferring to {@link OptionChecks#checkPositive},
     * and the messages are what say so: a user told only "must be positive" sets 500 µs next and is
     * rejected a second time. Asserting the floor's own wording for zero and for a negative is what
     * fails if the implementation ever grows a {@code checkPositive} ahead of the conversion.
     */
    @Test
    void checkAtLeastOneMilliRejectsNullZeroAndNegativeWithTheFloorsOwnMessage() {
        assertThatThrownBy(() -> OptionChecks.checkAtLeastOneMilli(null, "retryInitialBackoff"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("retryInitialBackoff must not be null");
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilli(
                                        Duration.ZERO, "retryInitialBackoff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialBackoff must be at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilli(
                                        Duration.ofSeconds(-1), "retryInitialBackoff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialBackoff must be at least 1 millisecond");
    }

    /**
     * Both halves of the message are load-bearing, and a copy drops one or the other: the
     * parenthetical is the only place a user learns <em>why</em> a positive duration was refused,
     * and the value is what disambiguates a builder chain setting several durations (ADR-0068).
     */
    @Test
    void theFloorMessageSaysWhyAndCarriesTheValue() {
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilli(
                                        Duration.ofNanos(500_000), "retryInitialBackoff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("it is applied at millisecond granularity")
                .hasMessageContaining("PT0.0005S");
    }

    /**
     * Accidental in all five private copies this check replaced, and kept deliberately: {@link
     * Duration#toMillis()} overflows past about 292 million years. The exception type is odd, but
     * the landing site is the one that matters — the setter, on the client, with the value still in
     * the user's hand — so converting it to an {@code IllegalArgumentException} would buy nothing.
     */
    @Test
    void anAbsurdlyLongDurationStillFailsAtTheSetter() {
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilli(
                                        Duration.ofSeconds(Long.MAX_VALUE), "retryInitialBackoff"))
                .isInstanceOf(ArithmeticException.class);
    }

    /**
     * The zero-tolerant variant is the floor plus one exemption, and the exemption is the vendor's
     * rather than a convenience: gax, the Pub/Sub subscriber and the BigQuery Storage writer each
     * document a meaning for zero. Everything else it does is {@link
     * OptionChecks#checkAtLeastOneMilli}'s job, message included, which is what the sub-millisecond
     * case pins — that value is refused precisely because it would arrive as the sentinel.
     */
    @Test
    void checkAtLeastOneMilliOrZeroForwardsZeroAndRejectsEverythingElseBelowAMillisecond() {
        assertThat(OptionChecks.checkAtLeastOneMilliOrZero(Duration.ZERO, "retryTotalTimeout"))
                .isEqualTo(Duration.ZERO);
        assertThat(
                        OptionChecks.checkAtLeastOneMilliOrZero(
                                Duration.ofMillis(1), "retryTotalTimeout"))
                .isEqualTo(Duration.ofMillis(1));
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilliOrZero(
                                        Duration.ofNanos(500_000), "retryTotalTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout must be at least 1 millisecond");
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkAtLeastOneMilliOrZero(
                                        Duration.ofSeconds(-1), "retryTotalTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout must be at least 1 millisecond");
        assertThatThrownBy(() -> OptionChecks.checkAtLeastOneMilliOrZero(null, "retryTotalTimeout"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("retryTotalTimeout must not be null");
    }

    @Test
    void theCeilingIsWhatANanosecondClockCanExpress() {
        // The constant is what every caller's javadoc names, so it is asserted rather than
        // assumed: toNanos() is defined exactly to Long.MAX_VALUE, and one nanosecond more throws.
        assertThat(OptionChecks.MAX_EXPRESSIBLE_IN_NANOS.toNanos()).isEqualTo(Long.MAX_VALUE);
        assertThatThrownBy(() -> OptionChecks.MAX_EXPRESSIBLE_IN_NANOS.plusNanos(1).toNanos())
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void checkExpressibleInNanosAcceptsTheBoundaryAndRejectsOneNanosecondMore() {
        Duration boundary = OptionChecks.MAX_EXPRESSIBLE_IN_NANOS;

        // The boundary itself is accepted, or the message would describe a value it rejects.
        assertThat(OptionChecks.checkExpressibleInNanos(boundary, "flushTimeout"))
                .isSameAs(boundary);
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkExpressibleInNanos(
                                        boundary.plusNanos(1), "flushTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushTimeout must be at most");
    }

    /**
     * The year count is the half a reader can act on: {@code Duration.toString()} renders the
     * ceiling as an hour count with a fractional second, and through the Table API that string is
     * the whole of what a SQL user is shown (ADR-0068). Pinned here so dropping it fails.
     */
    @Test
    void theCeilingMessageSaysHowLongTheCeilingIs() {
        assertThatThrownBy(
                        () ->
                                OptionChecks.checkExpressibleInNanos(
                                        Duration.ofDays(400_000), "flushTimeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PT2562047H47M16.854775807S")
                .hasMessageContaining("292 years");
    }

    @Test
    void checkExpressibleInNanosRejectsNull() {
        // A budget knob reaches this without checkPositive when zero is meaningful — the
        // first-checkpoint watchdog, which zero disables — so this is not the other check's job.
        assertThatThrownBy(() -> OptionChecks.checkExpressibleInNanos(null, "flushTimeout"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("flushTimeout must not be null");
    }

    @Test
    void aNegativeBudgetIsNotTheCeilingChecksBusiness() {
        // It passes, and that is deliberate: BoundedShutdown accepts a spent budget (it gives up
        // at once) and positivity is the callers' own check, so folding it in here would reject a
        // value one consumer relies on.
        Duration negative = Duration.ofSeconds(-1);

        assertThat(OptionChecks.checkExpressibleInNanos(negative, "timeout")).isSameAs(negative);
    }
}
