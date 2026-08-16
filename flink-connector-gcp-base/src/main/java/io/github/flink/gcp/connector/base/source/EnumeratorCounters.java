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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.util.Preconditions;

/**
 * The three counters a {@link PullAssignmentSplitEnumerator} reports, registered by the connector
 * under its own names.
 *
 * <p>The connector registers them rather than handing over three name strings, so that every
 * registration in this repository stays a {@code metricGroup.counter(<Product>MetricNames.X)} call
 * in the module that owns the name — which is the shape {@code scripts/check-metric-docs.py} reads
 * to decide a documented metric is registered.
 *
 * <p>The counters are read by the reporter thread while the coordinator thread increments them,
 * which is why {@link #unregistered()} answers with {@link ThreadSafeSimpleCounter}s and why a
 * connector registers the same kind.
 */
@Internal
public final class EnumeratorCounters {

    private final Counter splitsAssigned;
    private final Counter splitsReturned;
    private final Counter plansCompleted;

    /**
     * Creates the set.
     *
     * @param splitsAssigned counts splits handed to a reader
     * @param splitsReturned counts splits a failed reader gave back
     * @param plansCompleted counts completed planning calls, which is one for a job that planned
     *     and zero for one that restored a plan
     */
    public EnumeratorCounters(
            Counter splitsAssigned, Counter splitsReturned, Counter plansCompleted) {
        this.splitsAssigned =
                Preconditions.checkNotNull(splitsAssigned, "splitsAssigned must not be null");
        this.splitsReturned =
                Preconditions.checkNotNull(splitsReturned, "splitsReturned must not be null");
        this.plansCompleted =
                Preconditions.checkNotNull(plansCompleted, "plansCompleted must not be null");
    }

    /**
     * Returns counters attached to no metric group, for an enumerator whose context offered none.
     *
     * @return unregistered counters
     */
    public static EnumeratorCounters unregistered() {
        return new EnumeratorCounters(
                new ThreadSafeSimpleCounter(),
                new ThreadSafeSimpleCounter(),
                new ThreadSafeSimpleCounter());
    }

    Counter splitsAssigned() {
        return splitsAssigned;
    }

    Counter splitsReturned() {
        return splitsReturned;
    }

    Counter plansCompleted() {
        return plansCompleted;
    }
}
