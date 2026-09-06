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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import java.io.Serializable;
import java.time.Duration;

/**
 * Internal staging settings. The public mode and its options are introduced with the committer.
 * This separate value leaves the eager sink configuration's serialized fields unchanged.
 */
@Internal
public final class CloudTasksStagingConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int maxStagedTasks;
    private final long maxStagedBytes;
    private final Duration nameRetention;
    private final Duration clockSkewAllowance;
    private final Duration requestTimeout;

    /** Creates the ADR-0158 default staging settings. */
    public CloudTasksStagingConfig() {
        this(
                100_000,
                64L * 1024 * 1024,
                Duration.ofHours(1),
                Duration.ofMinutes(5),
                Duration.ofSeconds(20));
    }

    /** Creates validated settings for one writer batch and its persisted authorization window. */
    public CloudTasksStagingConfig(
            int maxStagedTasks,
            long maxStagedBytes,
            Duration nameRetention,
            Duration clockSkewAllowance,
            Duration requestTimeout) {
        this.maxStagedTasks = maxStagedTasks;
        this.maxStagedBytes = maxStagedBytes;
        this.nameRetention = nameRetention;
        this.clockSkewAllowance = clockSkewAllowance;
        this.requestTimeout = requestTimeout;
        validate();
    }

    /** Rechecks settings at writer construction, including after Java deserialization. */
    public void validate() {
        Preconditions.checkArgument(maxStagedTasks > 0, "maxStagedTasks must be positive");
        Preconditions.checkArgument(maxStagedBytes > 0, "maxStagedBytes must be positive");
        checkDuration(nameRetention, "nameRetention", false);
        checkDuration(clockSkewAllowance, "clockSkewAllowance", true);
        checkDuration(requestTimeout, "requestTimeout", false);
        Preconditions.checkArgument(
                nameRetention.compareTo(clockSkewAllowance) > 0
                        && nameRetention.minus(clockSkewAllowance).compareTo(requestTimeout) > 0,
                "nameRetention must exceed clockSkewAllowance plus requestTimeout");
        Preconditions.checkArgument(
                windowMillis() > 0,
                "nameRetention minus clockSkewAllowance and requestTimeout must allow at least one millisecond");
    }

    private static void checkDuration(Duration value, String name, boolean allowZero) {
        Preconditions.checkNotNull(value, "%s must not be null", name);
        Preconditions.checkArgument(
                !value.isNegative() && (allowZero || !value.isZero()),
                "%s must be %s",
                name,
                allowZero ? "non-negative" : "positive");
        OptionChecks.checkExpressibleInNanos(value, name);
    }

    private long windowMillis() {
        return nameRetention.minus(clockSkewAllowance).minus(requestTimeout).toMillis();
    }

    /** Computes the durable deadline, rounding the positive window down to milliseconds. */
    public long authorizationDeadlineMillis(long originEpochMillis) {
        return Math.addExact(originEpochMillis, windowMillis());
    }

    /** Returns the maximum accepted records per batch. */
    public int getMaxStagedTasks() {
        return maxStagedTasks;
    }

    /** Returns the maximum accounted bytes per batch. */
    public long getMaxStagedBytes() {
        return maxStagedBytes;
    }
}
