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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * The checks every connector's option builders repeat, in one place.
 *
 * <p>All of them are about a {@link Duration} a user hands a builder, and all of them exist so the
 * failure lands on the client rather than on a TaskManager, where a job has already started. Each
 * clears the base module's multiple-consumer bar on its own; ADR-0068 carries the dated call-site
 * survey behind that claim.
 *
 * <p>What they replace is not merely repetition. The ceiling constant stood in six files with its
 * message written out in eight, so a new budget knob was correct only if its author remembered to
 * copy four lines and word them the same way. Positivity had **three** message shapes for one check
 * — {@code "x must be positive"}, {@code "x must be positive: <value>"} and {@code "x must be
 * positive, but was <value>"} — which is what a rule with no single implementation decays into. The
 * value-carrying form won: a builder chain setting several durations otherwise leaves the user
 * guessing which one the rejection meant. The millisecond floor was five private copies in two
 * message shapes and two mechanics, and what a copy of it silently dropped was not the wording but
 * the coverage: eleven setters converting a user value with {@code toMillis()} never had one.
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
     * Returns the duration, having checked it is present and at least one millisecond.
     *
     * <p>Subsumes {@link #checkPositive}: zero and negative durations fail this check too, with
     * this message rather than that one. Deliberately — a knob whose floor is a millisecond rejects
     * {@code Duration.ZERO} for the same reason it rejects 500 µs, and a rejection saying only
     * "must be positive" costs the user a second round trip when their next attempt is 500 µs.
     *
     * <p>Which knobs need it is a property of what consumes the value, never of the knob's name: it
     * belongs wherever a user's {@code Duration} is converted with {@link Duration#toMillis()} and
     * a zero would be harmful — a {@code RetrySchedule}, a processing-time timer, a bounded {@code
     * await}, a millisecond field of a Google API request, or gax's retry algorithm, which
     * truncates its own delays the same way. Two knobs of one name, on a sink and on a source, can
     * therefore have different floors (ADR-0068).
     *
     * <p>A duration longer than about 292 million years throws {@link ArithmeticException} out of
     * {@link Duration#toMillis()} instead of being rejected here. It still lands at the setter,
     * which is where a failure belongs, so the conversion is left as it is and a test pins it.
     *
     * @param duration the duration to check
     * @param name the option name, for the failure message
     * @return the duration
     */
    public static Duration checkAtLeastOneMilli(Duration duration, String name) {
        Preconditions.checkNotNull(duration, "%s must not be null", name);
        Preconditions.checkArgument(
                duration.toMillis() >= 1,
                "%s must be at least 1 millisecond (it is applied at millisecond granularity): %s",
                name,
                duration);
        return duration;
    }

    /**
     * Returns the duration, having checked it is present and either zero or at least one
     * millisecond.
     *
     * <p>For a knob this project **forwards to a vendor SDK that gives zero a meaning of its own**
     * — gax reads a zero {@code totalTimeout} as "use the attempt count instead" and a zero RPC
     * timeout as "let the call run indefinitely", and the BigQuery Storage writer reads a zero
     * {@code maxRetryDuration} as "retry without a time limit". A setting this project merely
     * passes through stays settable as the SDK defines it, so zero is forwarded rather than
     * refused.
     *
     * <p>The floor and the exemption are the same argument, not a compromise between two: the
     * vendor reads these values with {@code toMillis()}, so a <em>positive</em> sub-millisecond
     * value would arrive as zero and silently become the sentinel — which is how a retry ceiling
     * set to give up almost at once turns into unlimited retry. Refusing it is what keeps zero
     * meaning only what the user typed.
     *
     * <p>Whether zero is legal is therefore a property of the SDK on the other side, never a
     * loosening applied for convenience: a knob this project spends itself takes {@link
     * #checkAtLeastOneMilli}, and a forwarded knob the vendor does <em>not</em> truncate takes
     * neither — a floor promising millisecond granularity where none applies would be promising
     * something untrue (the Pub/Sub subscriber's {@code maxAckExtensionPeriod} is that case).
     *
     * @param duration the duration to check
     * @param name the option name, for the failure message
     * @return the duration
     */
    public static Duration checkAtLeastOneMilliOrZero(Duration duration, String name) {
        Preconditions.checkNotNull(duration, "%s must not be null", name);
        return duration.isZero() ? duration : checkAtLeastOneMilli(duration, name);
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
