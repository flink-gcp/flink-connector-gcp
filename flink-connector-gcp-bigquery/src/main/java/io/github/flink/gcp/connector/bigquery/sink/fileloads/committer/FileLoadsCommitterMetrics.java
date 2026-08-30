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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Thread-safe metrics for the bounded FILE_LOADS destination executor. */
@Internal
public final class FileLoadsCommitterMetrics {

    private final Counter loadJobsSubmitted;
    private final AtomicInteger queuedDestinations = new AtomicInteger();
    private final AtomicInteger activeDestinations = new AtomicInteger();
    private final AtomicLong lastCommitDurationMillis = new AtomicLong();
    private final LongSupplier nanoTime;
    private long nextAttempt;
    private long currentAttempt;
    private long commitStartedNanos;

    /** Registers the committer metrics once for the committer's lifetime. */
    public FileLoadsCommitterMetrics(SinkCommitterMetricGroup metricGroup) {
        this(metricGroup, System::nanoTime);
    }

    @VisibleForTesting
    FileLoadsCommitterMetrics(SinkCommitterMetricGroup metricGroup, LongSupplier nanoTime) {
        this(
                metricGroup.counter(
                        BigQueryMetricNames.LOAD_JOBS_SUBMITTED, new ThreadSafeSimpleCounter()),
                nanoTime);
        metricGroup.gauge(BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS, queuedDestinations::get);
        metricGroup.gauge(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS, activeDestinations::get);
        metricGroup.gauge(
                BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS,
                this::currentCommitDurationMillis);
        metricGroup.gauge(
                BigQueryMetricNames.LAST_COMMIT_DURATION_MILLIS, lastCommitDurationMillis::get);
    }

    private FileLoadsCommitterMetrics(Counter loadJobsSubmitted, LongSupplier nanoTime) {
        this.loadJobsSubmitted = loadJobsSubmitted;
        this.nanoTime = nanoTime;
    }

    /** Creates unregistered state for a serial orchestration test. */
    public static FileLoadsCommitterMetrics unregistered(Counter loadJobsSubmitted) {
        return new FileLoadsCommitterMetrics(loadJobsSubmitted, System::nanoTime);
    }

    /** Returns the load-job counter shared by all worker threads. */
    public Counter loadJobsSubmitted() {
        return loadJobsSubmitted;
    }

    /** Starts one non-empty commit attempt before its destination plan is available. */
    public synchronized long commitStarted() {
        queuedDestinations.set(0);
        activeDestinations.set(0);
        commitStartedNanos = nanoTime.getAsLong();
        currentAttempt = ++nextAttempt;
        return currentAttempt;
    }

    /** Publishes the number of destination actions in the next commit phase. */
    public synchronized void destinationsPlanned(long attempt, int destinations) {
        if (currentAttempt == attempt) {
            queuedDestinations.set(destinations);
        }
    }

    /** Moves one destination from the local queue to active execution. */
    public synchronized void destinationStarted(long attempt) {
        startActiveWork(attempt);
    }

    /** Marks one destination's temporary-table cleanup active. */
    public synchronized void cleanupStarted(long attempt) {
        startActiveWork(attempt);
    }

    private void startActiveWork(long attempt) {
        if (currentAttempt != attempt) {
            return;
        }
        queuedDestinations.updateAndGet(queued -> Math.max(0, queued - 1));
        activeDestinations.incrementAndGet();
    }

    /** Marks one active destination complete. */
    public synchronized void destinationFinished(long attempt) {
        finishActiveWork(attempt);
    }

    /** Marks one destination's temporary-table cleanup complete. */
    public synchronized void cleanupFinished(long attempt) {
        finishActiveWork(attempt);
    }

    /** Ends the current commit attempt and clears its live state. */
    public synchronized void commitFinished(long attempt) {
        if (currentAttempt != attempt) {
            return;
        }
        lastCommitDurationMillis.set(
                TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - commitStartedNanos));
        currentAttempt = 0;
        commitStartedNanos = 0;
        queuedDestinations.set(0);
        activeDestinations.set(0);
    }

    private synchronized long currentCommitDurationMillis() {
        return currentAttempt == 0
                ? 0
                : TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - commitStartedNanos);
    }

    private void finishActiveWork(long attempt) {
        if (currentAttempt != attempt) {
            return;
        }
        activeDestinations.updateAndGet(active -> Math.max(0, active - 1));
    }
}
