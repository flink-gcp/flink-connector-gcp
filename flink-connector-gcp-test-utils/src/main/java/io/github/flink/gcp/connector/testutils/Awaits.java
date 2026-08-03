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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Deadline-bounded polling for test assertions. */
@Internal
public final class Awaits {

    /**
     * Polls until the condition holds or the timeout passes, then fails naming what was awaited.
     *
     * <p>For conditions observed outside a running job's output — a subscription appearing, a
     * static observer filling up. Waiting on the job's own records goes through {@link
     * Drains#drainDistinct} instead, which also watches for the job ending.
     *
     * @param what what is being awaited, phrased to follow "Timed out waiting for"
     * @param timeout how long to wait
     * @param condition the condition, polled every 100 ms
     */
    public static void await(String what, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        await(what, timeout, condition, () -> "");
    }

    /**
     * Polls as {@link #await(String, Duration, BooleanSupplier)} does, and on timeout appends what
     * {@code diagnosis} reports to the failure message.
     *
     * <p>A boolean condition says only that something did not happen, which in CI is the whole of
     * what a reader gets: a timed-out {@code await} in a run nobody can reproduce is a dead end
     * unless the message itself carries the state it timed out in. {@link Drains#drainDistinct}
     * already has this property — it returns the shortfall, so the assertion that asked for the
     * elements reports the ones that arrived — and this is the same affordance for a condition that
     * has no elements to return. Composing one greppable line is the intended shape.
     *
     * <p>The diagnosis is evaluated once, only on timeout, and a throwing supplier is reported in
     * place of its text rather than propagated: a path that runs only when a test has already
     * failed must not be the thing that destroys the evidence it exists to capture.
     *
     * @param what what is being awaited, phrased to follow "Timed out waiting for"
     * @param timeout how long to wait
     * @param condition the condition, polled every 100 ms
     * @param diagnosis the state to append to the failure message, appended after a space
     */
    public static void await(
            String what, Duration timeout, BooleanSupplier condition, Supplier<String> diagnosis)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "Timed out waiting for "
                        + what
                        + " (waited "
                        + timeout
                        + ")."
                        + describe(diagnosis));
    }

    /** Renders the diagnosis, or what went wrong producing it, as a suffix. */
    private static String describe(Supplier<String> diagnosis) {
        String described;
        try {
            described = diagnosis.get();
        } catch (Throwable t) {
            return " The diagnosis itself threw " + t + ".";
        }
        return described == null || described.isEmpty() ? "" : " " + described;
    }

    private Awaits() {}
}
