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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Validation shared by the source's option builders.
 *
 * <p>Package-private, so sharing it costs no public API — which is why the sink's copy of {@link
 * #checkPositive} stays where it is: that one would need a public type to cross the package
 * boundary.
 */
final class OptionChecks {

    /** The largest {@code Duration} a nanosecond budget can express, about 292 years. */
    private static final Duration MAX_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

    private OptionChecks() {}

    /**
     * Returns the budget, having checked it can be converted to nanoseconds.
     *
     * <p>A longer one throws {@link ArithmeticException} from {@code Duration.toNanos()} instead —
     * on a TaskManager, when the reader that spends it is built or torn down, rather than here on
     * the client (#334; the rule is ADR-0068).
     *
     * @param duration the budget to check
     * @param name the option name, for the failure message
     * @return the budget
     */
    static Duration checkExpressibleInNanos(Duration duration, String name) {
        Preconditions.checkArgument(
                duration.compareTo(MAX_TIMEOUT) <= 0,
                "%s must be at most %s (about 292 years)",
                name,
                MAX_TIMEOUT);
        return duration;
    }

    /**
     * Returns the duration, having checked it is present and positive.
     *
     * @param duration the duration to check
     * @param name the option name, for the failure message
     * @return the duration
     */
    static Duration checkPositive(Duration duration, String name) {
        Preconditions.checkNotNull(duration, "%s must not be null", name);
        Preconditions.checkArgument(
                !duration.isZero() && !duration.isNegative(), "%s must be positive", name);
        return duration;
    }
}
