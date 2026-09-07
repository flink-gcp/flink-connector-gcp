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

/** Immutable observation of an owned batch, containing no task payload or durable state. */
@Internal
public final class CloudTasksReplaySnapshot {
    private static final CloudTasksReplaySnapshot EMPTY = new CloudTasksReplaySnapshot(true, 0, 0);
    private final boolean empty;
    private final long oldestOrigin;
    private final long earliestDeadline;

    private CloudTasksReplaySnapshot(boolean empty, long oldestOrigin, long earliestDeadline) {
        this.empty = empty;
        this.oldestOrigin = oldestOrigin;
        this.earliestDeadline = earliestDeadline;
    }

    /** Returns an observation with no owned tasks. */
    public static CloudTasksReplaySnapshot empty() {
        return EMPTY;
    }

    /** Includes an envelope's origin and effective authorization deadline. */
    public CloudTasksReplaySnapshot include(long origin, long deadline) {
        return new CloudTasksReplaySnapshot(
                false,
                empty ? origin : Math.min(oldestOrigin, origin),
                empty ? deadline : Math.min(earliestDeadline, deadline));
    }

    /** Returns the oldest age in milliseconds, or -1 when no tasks are observed. */
    public long ageMillis(long now) {
        return empty ? -1 : positiveDifference(now, oldestOrigin);
    }

    /** Returns the shortest remaining budget in milliseconds, or -1 for an empty observation. */
    public long remainingMillis(long now) {
        return empty ? -1 : positiveDifference(earliestDeadline, now);
    }

    private static long positiveDifference(long later, long earlier) {
        if (later <= earlier) {
            return 0;
        }
        long difference = later - earlier;
        return difference < 0 ? Long.MAX_VALUE : difference;
    }
}
