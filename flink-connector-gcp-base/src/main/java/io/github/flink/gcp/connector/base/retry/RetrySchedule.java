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

package io.github.flink.gcp.connector.base.retry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

/**
 * An immutable retry schedule: exponential backoff from an initial delay up to a cap, a bounded
 * number of attempts, and optional proportional jitter (to de-synchronize parallel subtasks
 * retrying against the same destination).
 */
@Internal
public final class RetrySchedule {

    /**
     * The jitter every schedule in this project uses, unless it has a recorded reason not to.
     *
     * <p>One number rather than a per-site choice, because the value is not load-bearing: the
     * jitter is mean-preserving (the backoff is multiplied by a factor in {@code [1 - ratio, 1 +
     * ratio]}, so the expected delay stays the design value) and all it has to do is break the
     * synchronization between parallel subtasks retrying against the same destination. Only being
     * non-zero serves that purpose, so a site picking its own ratio would be picking a number
     * nothing distinguishes.
     *
     * <p>Not a builder knob on any connector's options: there is no workload for which a particular
     * ratio is the right answer, which is the test the {@code recovery*} and {@code retry*} knobs
     * pass and this does not.
     */
    public static final double DEFAULT_JITTER_RATIO = 0.25;

    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxAttempts;
    private final double jitterRatio;

    /**
     * Creates a schedule.
     *
     * @param initialBackoffMs the first backoff
     * @param maxBackoffMs the backoff cap
     * @param maxAttempts the maximum number of attempts
     * @param jitterRatio the proportional jitter, in {@code [0, 1)}
     */
    public RetrySchedule(
            long initialBackoffMs, long maxBackoffMs, int maxAttempts, double jitterRatio) {
        Preconditions.checkArgument(initialBackoffMs > 0, "initialBackoffMs must be positive");
        Preconditions.checkArgument(
                maxBackoffMs >= initialBackoffMs, "maxBackoffMs must be >= initialBackoffMs");
        Preconditions.checkArgument(maxAttempts > 0, "maxAttempts must be positive");
        Preconditions.checkArgument(
                jitterRatio >= 0 && jitterRatio < 1, "jitterRatio must be in [0, 1)");
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.maxAttempts = maxAttempts;
        this.jitterRatio = jitterRatio;
    }

    /** Returns the maximum number of attempts. */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the jittered backoff after the given attempt (1-based): the initial delay doubled per
     * attempt up to the cap, multiplied by a random factor in {@code [1 - jitterRatio, 1 +
     * jitterRatio]}.
     */
    public long backoffMs(int attempt) {
        long base = initialBackoffMs;
        for (int i = 1; i < attempt && base < maxBackoffMs; i++) {
            base = Math.min(base * 2, maxBackoffMs);
        }
        if (jitterRatio == 0) {
            return base;
        }
        double factor = 1 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterRatio;
        return Math.max(1, (long) (base * factor));
    }
}
