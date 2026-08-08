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
