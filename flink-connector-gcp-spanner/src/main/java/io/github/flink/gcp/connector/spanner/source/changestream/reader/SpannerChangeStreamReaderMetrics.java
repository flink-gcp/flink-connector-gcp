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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.SpannerMetricValues;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Reporter-visible state for one Spanner Change Streams source reader. */
@Internal
final class SpannerChangeStreamReaderMetrics {

    private static final long NO_POSITION = Long.MAX_VALUE;

    private final Counter queriesStarted;
    private final Counter recordsSkipped;
    private final AtomicInteger queuedPartitions = new AtomicInteger();
    private final AtomicLong oldestQueuedPositionMillis = new AtomicLong(NO_POSITION);
    private final AtomicLong lastRecordWaitMillis = new AtomicLong();
    private final Set<QueryTiming> activeQueries = ConcurrentHashMap.newKeySet();
    private final LongSupplier currentTimeMillis;

    SpannerChangeStreamReaderMetrics(SourceReaderMetricGroup group) {
        this(group, System::currentTimeMillis);
    }

    SpannerChangeStreamReaderMetrics(
            SourceReaderMetricGroup group, LongSupplier currentTimeMillis) {
        Preconditions.checkNotNull(group, "group must not be null");
        this.currentTimeMillis =
                Preconditions.checkNotNull(currentTimeMillis, "currentTimeMillis must not be null");
        queriesStarted = group.counter(SpannerMetricNames.CHANGE_STREAM_QUERIES_STARTED);
        recordsSkipped = group.counter(SpannerMetricNames.RECORDS_SKIPPED);
        group.gauge(
                SpannerMetricNames.ACTIVE_CHANGE_STREAM_QUERIES,
                (Gauge<Integer>) activeQueries::size);
        group.gauge(
                SpannerMetricNames.QUEUED_CHANGE_STREAM_PARTITIONS,
                (Gauge<Integer>) queuedPartitions::get);
        group.gauge(
                SpannerMetricNames.QUEUED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::queuedPartitionLagMillis);
        group.gauge(
                SpannerMetricNames.MISSED_HEARTBEAT_INTERVALS,
                (Gauge<Long>) this::missedHeartbeatIntervals);
        group.gauge(
                SpannerMetricNames.LAST_CHANGE_STREAM_RECORD_WAIT_MILLIS,
                (Gauge<Long>) lastRecordWaitMillis::get);
    }

    QueryTiming opening(SpannerChangeStreamPartitionSplit split) {
        long now = currentTimeMillis.getAsLong();
        return new QueryTiming(split.getPartitionToken() != null, split.getHeartbeatMillis(), now);
    }

    void opened(QueryTiming timing) {
        queriesStarted.inc();
        synchronized (timing) {
            timing.opened = true;
            if (!timing.recordReturned) {
                timing.lastRecordMillis.set(currentTimeMillis.getAsLong());
            }
            if (!timing.terminal) {
                activeQueries.add(timing);
            }
        }
    }

    void openFailed(QueryTiming timing) {
        terminated(timing);
    }

    void terminated(QueryTiming timing) {
        synchronized (timing) {
            if (timing.terminal) {
                return;
            }
            timing.terminal = true;
            if (timing.opened) {
                activeQueries.remove(timing);
            }
        }
    }

    void recordReturned(QueryTiming timing, boolean heartbeat) {
        long now = currentTimeMillis.getAsLong();
        synchronized (timing) {
            timing.recordReturned = true;
            timing.lastRecordMillis.set(now);
        }
        if (!heartbeat) {
            lastRecordWaitMillis.set(
                    SpannerMetricValues.elapsedMillis(now, timing.waitStartedMillis.get()));
        }
    }

    void resumed(QueryTiming timing) {
        timing.waitStartedMillis.set(currentTimeMillis.getAsLong());
    }

    void skipped() {
        recordsSkipped.inc();
    }

    void queued(Collection<SpannerChangeStreamPartitionSplit> splits) {
        queuedPartitions.set(splits.size());
        long oldest = NO_POSITION;
        for (SpannerChangeStreamPartitionSplit split : splits) {
            oldest = Math.min(oldest, split.getCurrentPosition().toEpochMilli());
        }
        oldestQueuedPositionMillis.set(oldest);
    }

    void closed() {
        for (QueryTiming timing : activeQueries.toArray(new QueryTiming[0])) {
            terminated(timing);
        }
        queuedPartitions.set(0);
        oldestQueuedPositionMillis.set(NO_POSITION);
    }

    private long queuedPartitionLagMillis() {
        long oldest = oldestQueuedPositionMillis.get();
        return oldest == NO_POSITION
                ? 0
                : SpannerMetricValues.elapsedMillis(currentTimeMillis.getAsLong(), oldest);
    }

    private long missedHeartbeatIntervals() {
        long now = currentTimeMillis.getAsLong();
        long maximum = 0;
        for (QueryTiming timing : activeQueries) {
            if (timing.nonInitialPartition) {
                maximum =
                        Math.max(
                                maximum,
                                SpannerMetricValues.elapsedMillis(
                                                now, timing.lastRecordMillis.get())
                                        / timing.heartbeatMillis);
            }
        }
        return maximum;
    }

    static final class QueryTiming {

        private final boolean nonInitialPartition;
        private final long heartbeatMillis;
        private final AtomicLong waitStartedMillis;
        private final AtomicLong lastRecordMillis;
        private boolean opened;
        private boolean terminal;
        private boolean recordReturned;

        private QueryTiming(
                boolean nonInitialPartition, long heartbeatMillis, long openedAtMillis) {
            this.nonInitialPartition = nonInitialPartition;
            this.heartbeatMillis = heartbeatMillis;
            this.waitStartedMillis = new AtomicLong(openedAtMillis);
            this.lastRecordMillis = new AtomicLong(openedAtMillis);
        }
    }
}
