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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.function.LongSupplier;

/**
 * Paces one writer subtask with Datastore's 500/50/5 ramp-up guidance: start a new workload at no
 * more than 500 operations per second, and raise that by at most 50% every five minutes.
 *
 * <p>The sink's whole budget starts at {@value #BASE_OPS_PER_SECOND} operations per second, shared
 * evenly among the subtasks writing, and is multiplied by 1.5 for every five minutes past the first
 * five since this subtask's first operation, growing continuously at whole-minute steps. Each
 * subtask's share never drops below one operation per second. Apache Beam's {@code
 * RampupThrottlingFn} has the same shape and was read as a design reference.
 *
 * <p>Operations are counted in one-second windows: once a window's budget is spent, {@link
 * #acquire()} sleeps until the window ends. Task-thread only.
 */
@Internal
final class RampUpThrottle {

    /** The sink-wide starting rate the guidance names. */
    @VisibleForTesting static final int BASE_OPS_PER_SECOND = 500;

    private static final long RAMP_UP_INTERVAL_MINUTES = 5;
    private static final long WINDOW_MILLIS = 1000;

    /** Sleeps the task thread; a seam so a test can advance a fake clock instead. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final int parallelism;
    private final LongSupplier clockMillis;
    private final Sleeper sleeper;

    private boolean started;
    private long startMillis;
    private long windowStartMillis;
    private int usedInWindow;

    /**
     * Creates a throttle.
     *
     * @param parallelism the number of subtasks sharing the budget, positive
     * @param clockMillis a clock in milliseconds; monotonic in production
     * @param sleeper how to wait for the next window
     */
    RampUpThrottle(int parallelism, LongSupplier clockMillis, Sleeper sleeper) {
        Preconditions.checkArgument(parallelism > 0, "parallelism must be positive");
        this.parallelism = parallelism;
        this.clockMillis = clockMillis;
        this.sleeper = sleeper;
    }

    /** Returns the number of subtasks the budget is shared among. */
    @VisibleForTesting
    int parallelism() {
        return parallelism;
    }

    /**
     * Takes one operation from the budget, waiting for the next window if this one's is spent.
     *
     * @return the milliseconds spent waiting
     * @throws InterruptedException if the wait is interrupted
     */
    long acquire() throws InterruptedException {
        long waited = 0;
        while (true) {
            long now = clockMillis.getAsLong();
            // A flag, not a sentinel value: a monotonic clock's readings may be negative.
            if (!started) {
                started = true;
                startMillis = now;
                windowStartMillis = now;
            }
            if (now - windowStartMillis >= WINDOW_MILLIS || now < windowStartMillis) {
                // A clock stepping backwards — a test's, since production's is monotonic — opens
                // a new window rather than stalling the writer until it catches up again.
                windowStartMillis = now;
                usedInWindow = 0;
            }
            if (usedInWindow < budget(now - startMillis, parallelism)) {
                usedInWindow++;
                return waited;
            }
            long wait = windowStartMillis + WINDOW_MILLIS - now;
            sleeper.sleep(wait);
            waited += wait;
        }
    }

    /**
     * Returns one subtask's operations per second after the given time since its first operation.
     *
     * @param elapsedMillis the time since the first operation
     * @param parallelism the number of subtasks sharing the budget
     * @return the per-second budget, at least one
     */
    @VisibleForTesting
    static int budget(long elapsedMillis, int parallelism) {
        long elapsedMinutes = Math.max(0, elapsedMillis) / 60_000;
        double growth =
                Math.max(
                        0.0,
                        (elapsedMinutes - RAMP_UP_INTERVAL_MINUTES)
                                / (double) RAMP_UP_INTERVAL_MINUTES);
        double budget = BASE_OPS_PER_SECOND / (double) parallelism * Math.pow(1.5, growth);
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1.0, budget));
    }
}
