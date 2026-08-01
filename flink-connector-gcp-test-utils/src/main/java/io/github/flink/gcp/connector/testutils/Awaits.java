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

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** Deadline-bounded polling for test assertions. */
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
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out waiting for " + what + " (waited " + timeout + ").");
    }

    private Awaits() {}
}
