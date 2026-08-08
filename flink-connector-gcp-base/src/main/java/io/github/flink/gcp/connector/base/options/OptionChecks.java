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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * The checks every connector's option builders repeat, in one place.
 *
 * <p>Both are about a {@link Duration} a user hands a builder, and both exist so the failure lands
 * on the client rather than on a TaskManager, where a job has already started. Both clear the base
 * module's multiple-consumer bar on their own: the ceiling has nine call sites across base, pubsub
 * and bigquery, and positivity has thirty-one across pubsub and bigquery (ADR-0068).
 *
 * <p>What they replace is not merely repetition. The ceiling constant stood in six files with its
 * message written out in eight, so a new budget knob was correct only if its author remembered to
 * copy four lines and word them the same way. Positivity had **three** message shapes for one check
 * — {@code "x must be positive"}, {@code "x must be positive: <value>"} and {@code "x must be
 * positive, but was <value>"} — which is what a rule with no single implementation decays into. The
 * value-carrying form won: a builder chain setting several durations otherwise leaves the user
 * guessing which one the rejection meant.
 *
 * <p>Deliberately not a general-purpose precondition library: the next check needs an argument of
 * its own, and "it is a precondition too" is not one.
 */
@Internal
public final class OptionChecks {

    /**
     * The largest budget a nanosecond clock can express, about 292 years.
     *
     * <p>Package-private: callers name the ceiling in their {@code @param} lines as {@code
     * Duration.ofNanos(Long.MAX_VALUE)}, which is the value itself and needs no import, so nothing
     * outside this package has asked for the symbol. Widen it when something does, not before.
     */
    static final Duration MAX_EXPRESSIBLE_IN_NANOS = Duration.ofNanos(Long.MAX_VALUE);

    private OptionChecks() {}

    /**
     * Returns the duration, having checked it is present and positive.
     *
     * @param duration the duration to check
     * @param name the option name, for the failure message
     * @return the duration
     */
    public static Duration checkPositive(Duration duration, String name) {
        Preconditions.checkNotNull(duration, "%s must not be null", name);
        Preconditions.checkArgument(
                !duration.isZero() && !duration.isNegative(),
                "%s must be positive: %s",
                name,
                duration);
        return duration;
    }

    /**
     * Returns the budget, having checked it can be converted to nanoseconds.
     *
     * <p>A longer one throws {@link ArithmeticException} from {@link Duration#toNanos()} instead —
     * on a TaskManager, out of a teardown or a constructor, rather than here on the client, which
     * is the whole point of the check (ADR-0068). Whether a given knob needs it is a property of
     * what spends the budget, so the reason belongs at the call site.
     *
     * <p><b>The message names the year count as well as the value</b>, and that is load-bearing
     * rather than decoration: {@link Duration#toString()} renders the ceiling as {@code
     * PT2562047H47M16.854775807S}, which was measured to be exactly what a SQL user is shown, and
     * no reader turns an hour count with a fractional second on it into "292 years". Tests pin the
     * year count, so removing it fails rather than quietly making every rejection unreadable.
     *
     * @param duration the budget to check
     * @param name the option name, for the failure message
     * @return the budget
     */
    public static Duration checkExpressibleInNanos(Duration duration, String name) {
        Preconditions.checkNotNull(duration, "%s must not be null", name);
        Preconditions.checkArgument(
                duration.compareTo(MAX_EXPRESSIBLE_IN_NANOS) <= 0,
                "%s must be at most %s (about 292 years)",
                name,
                MAX_EXPRESSIBLE_IN_NANOS);
        return duration;
    }
}
