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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamReaderMetricsTest {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final AtomicLong now = new AtomicLong(20_000L);
    private final BigtableChangeStreamReaderMetrics metrics =
            new BigtableChangeStreamReaderMetrics(metricGroup, now::get);

    @Test
    void reportsConcurrentReadsQueueLagAndMinimumAssignedWatermark() {
        BigtableChangeStreamReaderMetrics.ReadTiming first = metrics.opening();
        BigtableChangeStreamReaderMetrics.ReadTiming second = metrics.opening();
        metrics.started(first);
        metrics.started(second);
        metrics.assigned(
                java.util.Arrays.asList(
                        split("active-newer", 10_000L), split("active-older", 5_000L)),
                Collections.singletonList(split("queued", 8_000L)));

        assertThat(counter("changeStreamReadsStarted")).isEqualTo(2);
        assertThat(gauge("activeChangeStreamReads")).isEqualTo(2);
        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(1);
        assertThat(gauge("queuedChangeStreamPartitionLagMillis")).isEqualTo(12_000L);
        assertThat(gauge("partitionLowWatermarkMillis")).isEqualTo(5_000L);

        // 11 s elapsed over the opener's five-second HEARTBEAT_INTERVAL is 2 whole intervals, so
        // this expectation belongs to that constant: raise it and the arithmetic below changes.
        now.set(31_000L);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(2);
        metrics.recordReturned(first);
        assertThat(gauge("missedHeartbeatIntervals")).isEqualTo(2);
        metrics.recordReturned(second);
        assertThat(gauge("missedHeartbeatIntervals")).isZero();
        metrics.terminated(first);
        assertThat(gauge("activeChangeStreamReads")).isEqualTo(1);
        metrics.terminated(second);
        metrics.terminated(second);
        assertThat(gauge("activeChangeStreamReads")).isZero();
    }

    @Test
    void classifiesUserAndGarbageCollectionMutations() {
        Instant commit = Instant.parse("2026-08-13T00:00:00Z");
        metrics.mutation(TestChangeStreamRecords.mutation(commit, commit, "user"));
        metrics.mutation(TestChangeStreamRecords.garbageCollectionMutation(commit, commit, "gc"));

        metrics.heartbeat();
        metrics.closeStream();
        metrics.skipped();
        metrics.entriesFiltered(3);
        metrics.skippedWithoutChange();

        assertThat(counter("changeStreamMutationsRead")).isEqualTo(2);
        assertThat(counter("changeStreamUserMutationsRead")).isEqualTo(1);
        assertThat(counter("changeStreamGarbageCollectionMutationsRead")).isEqualTo(1);
        assertThat(counter("changeStreamHeartbeatsRead")).isEqualTo(1);
        assertThat(counter("changeStreamCloseStreamsRead")).isEqualTo(1);
        assertThat(counter("recordsSkipped")).isEqualTo(1);
        assertThat(counter("changeStreamMutationEntriesFiltered")).isEqualTo(3);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isEqualTo(1);
    }

    @Test
    void aFailedOpenAndTerminalBeforeStartDoNotLeaveAnActiveRead() {
        BigtableChangeStreamReaderMetrics.ReadTiming failed = metrics.opening();
        metrics.terminated(failed);

        BigtableChangeStreamReaderMetrics.ReadTiming raced = metrics.opening();
        metrics.terminated(raced);
        metrics.started(raced);

        assertThat(counter("changeStreamReadsStarted")).isEqualTo(1);
        assertThat(gauge("activeChangeStreamReads")).isZero();
    }

    private long counter(String name) {
        return listener.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private long gauge(String name) {
        Object value = listener.getGauge(name).orElseThrow(AssertionError::new).getValue();
        return ((Number) value).longValue();
    }

    private static ChangeStreamPartitionSplit split(String id, long lowWatermarkMillis) {
        return new ChangeStreamPartitionSplit(
                id,
                ByteStringRange.unbounded(),
                Collections.emptyList(),
                Instant.ofEpochMilli(lowWatermarkMillis));
    }
}
