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

import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileLoadsCommitterMetrics}. */
class FileLoadsCommitterMetricsTest {

    @Test
    void reportsCurrentAndLastDurationFromTheCommitBoundaries() {
        TestSinkCommitterMetricGroup group = TestSinkCommitterMetricGroup.create();
        AtomicLong nanoTime = new AtomicLong(1);
        FileLoadsCommitterMetrics metrics = new FileLoadsCommitterMetrics(group, nanoTime::get);

        long attempt = metrics.commitStarted();
        metrics.destinationsPlanned(attempt, 1);
        metrics.destinationStarted(attempt);
        metrics.destinationFinished(attempt);
        metrics.cleanupStarted(attempt);
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(7));

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isOne();
        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                .isEqualTo(7);

        metrics.cleanupFinished(attempt);
        metrics.commitFinished(attempt);

        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                .isZero();
        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.LAST_COMMIT_DURATION_MILLIS))
                .isEqualTo(7);
    }

    @Test
    void lateWorkerCompletionCannotChangeTheNextAttemptsActiveDestinations() {
        TestSinkCommitterMetricGroup group = TestSinkCommitterMetricGroup.create();
        FileLoadsCommitterMetrics metrics = new FileLoadsCommitterMetrics(group, () -> 1);

        long firstAttempt = metrics.commitStarted();
        metrics.destinationsPlanned(firstAttempt, 2);
        metrics.destinationStarted(firstAttempt);
        metrics.destinationStarted(firstAttempt);
        metrics.destinationFinished(firstAttempt);

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isOne();

        metrics.commitFinished(firstAttempt);

        long secondAttempt = metrics.commitStarted();
        metrics.destinationsPlanned(secondAttempt, 1);
        metrics.destinationStarted(secondAttempt);
        metrics.destinationFinished(firstAttempt);

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isOne();

        metrics.commitFinished(secondAttempt);
        metrics.destinationFinished(secondAttempt);

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
    }

    @Test
    void lateWorkerStartCannotMakeQueuedDestinationsNegative() {
        TestSinkCommitterMetricGroup group = TestSinkCommitterMetricGroup.create();
        FileLoadsCommitterMetrics metrics = new FileLoadsCommitterMetrics(group, () -> 1);

        long attempt = metrics.commitStarted();
        metrics.destinationsPlanned(attempt, 1);
        metrics.commitFinished(attempt);
        metrics.destinationStarted(attempt);

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();

        metrics.destinationFinished(attempt);

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
    }
}
