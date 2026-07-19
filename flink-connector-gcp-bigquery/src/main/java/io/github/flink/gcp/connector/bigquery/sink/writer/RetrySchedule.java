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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

/**
 * An immutable retry schedule: exponential backoff from an initial delay up to a cap, a bounded
 * number of attempts, and optional proportional jitter (to de-synchronize parallel subtasks
 * retrying against the same table).
 */
@Internal
final class RetrySchedule {

    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxAttempts;
    private final double jitterRatio;

    RetrySchedule(long initialBackoffMs, long maxBackoffMs, int maxAttempts, double jitterRatio) {
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
    int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the jittered backoff after the given attempt (1-based): the initial delay doubled per
     * attempt up to the cap, multiplied by a random factor in {@code [1 - jitterRatio, 1 +
     * jitterRatio]}.
     */
    long backoffMs(int attempt) {
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
