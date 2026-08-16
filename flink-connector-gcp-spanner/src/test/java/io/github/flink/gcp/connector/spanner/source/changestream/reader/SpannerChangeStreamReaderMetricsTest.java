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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import io.github.flink.gcp.connector.spanner.SpannerMetricValues;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerChangeStreamReaderMetricsTest {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final AtomicLong now = new AtomicLong(1_000);
    private final SpannerChangeStreamReaderMetrics metrics =
            new SpannerChangeStreamReaderMetrics(metricGroup, now::get);

    @Test
    void queryLifecycleAndHeartbeatHealthUseReporterVisibleLiteralNames() {
        SpannerChangeStreamReaderMetrics.QueryTiming timing = metrics.opening(split("a", 900, 100));
        now.set(1_100);
        metrics.opened(timing);

        assertThat(counter("changeStreamQueriesStarted")).isEqualTo(1);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(1);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(0L);

        now.set(1_299);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(1L);
        metrics.recordReturned(timing, true);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(0L);
        assertThat(gauge("lastChangeStreamRecordWaitMillis")).isEqualTo(0L);

        now.set(1_400);
        metrics.resumed(timing);
        now.set(1_575);
        metrics.recordReturned(timing, false);
        assertThat(gauge("lastChangeStreamRecordWaitMillis")).isEqualTo(175L);

        metrics.terminated(timing);
        metrics.terminated(timing);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(0);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(0L);
    }

    @Test
    void initialQueryDoesNotContributeToMissedHeartbeatIntervals() {
        SpannerChangeStreamReaderMetrics.QueryTiming timing =
                metrics.opening(
                        SpannerChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 100));
        metrics.opened(timing);
        now.set(10_000);

        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(0L);
    }

    @Test
    void queuedLagUsesTheOldestPositionAndClearsWithTheQueue() {
        metrics.queued(Arrays.asList(split("a", 950, 100), split("b", 700, 100)));

        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(2);
        assertThat(gauge("queuedChangeStreamPartitionLagMillis")).isEqualTo(300L);

        now.set(600);
        assertThat(gauge("queuedChangeStreamPartitionLagMillis")).isEqualTo(0L);
        metrics.queued(Collections.emptyList());
        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(0);
        assertThat(gauge("queuedChangeStreamPartitionLagMillis")).isEqualTo(0L);
    }

    @Test
    void failedOpenAndTerminalBeforeOpenCannotLeaveAnActiveGauge() {
        SpannerChangeStreamReaderMetrics.QueryTiming failed = metrics.opening(split("a", 900, 100));
        metrics.openFailed(failed);

        SpannerChangeStreamReaderMetrics.QueryTiming raced = metrics.opening(split("b", 900, 100));
        metrics.terminated(raced);
        metrics.opened(raced);

        assertThat(counter("changeStreamQueriesStarted")).isEqualTo(1);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(0);
    }

    @Test
    void heartbeatClockStartsWhenTheQueryHasOpened() {
        SpannerChangeStreamReaderMetrics.QueryTiming timing = metrics.opening(split("a", 900, 100));
        now.set(10_000);
        metrics.opened(timing);

        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(0L);
        now.set(10_199);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(1L);
    }

    @Test
    void elapsedTimeClampsFutureValuesAndSaturatesOverflow() {
        assertThat(SpannerMetricValues.elapsedMillis(100, 200)).isZero();
        assertThat(SpannerMetricValues.elapsedMillis(Long.MAX_VALUE, Long.MIN_VALUE))
                .isEqualTo(Long.MAX_VALUE);
    }

    private long counter(String name) {
        return listener.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private Object gauge(String name) {
        return listener.<Gauge<?>>getGauge(name).orElseThrow(AssertionError::new).getValue();
    }

    private static SpannerChangeStreamPartitionSplit split(
            String token, long positionMillis, long heartbeatMillis) {
        Instant position = Instant.ofEpochMilli(positionMillis);
        return new SpannerChangeStreamPartitionSplit(
                token,
                Collections.singletonList(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                position,
                null,
                heartbeatMillis,
                position,
                PartitionLifecycleState.RUNNING,
                position);
    }
}
