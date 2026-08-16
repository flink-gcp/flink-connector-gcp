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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.BigtableMetricValues;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Reporter-visible state for one Bigtable Change Streams reader subtask. */
@Internal
public final class BigtableChangeStreamReaderMetrics {

    private static final long NO_POSITION = Long.MAX_VALUE;

    private final Counter mutations;
    private final Counter heartbeats;
    private final Counter skipped;
    private final Counter readsStarted;
    private final Counter closeStreams;
    private final Counter userMutations;
    private final Counter garbageCollectionMutations;
    private final Counter mutationEntriesFiltered;
    private final Counter recordsSkippedWithoutChange;
    private final Set<ReadTiming> activeReads = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> activeLowWatermarks = new ConcurrentHashMap<>();
    private final AtomicInteger queuedPartitions = new AtomicInteger();
    private final AtomicLong oldestQueuedPositionMillis = new AtomicLong(NO_POSITION);
    private final LongSupplier currentTimeMillis;

    public BigtableChangeStreamReaderMetrics(SourceReaderMetricGroup group) {
        this(group, System::currentTimeMillis);
    }

    BigtableChangeStreamReaderMetrics(
            SourceReaderMetricGroup group, LongSupplier currentTimeMillis) {
        Preconditions.checkNotNull(group, "group must not be null");
        this.currentTimeMillis =
                Preconditions.checkNotNull(currentTimeMillis, "currentTimeMillis must not be null");
        mutations = group.counter(BigtableMetricNames.CHANGE_STREAM_MUTATIONS_READ);
        heartbeats = group.counter(BigtableMetricNames.CHANGE_STREAM_HEARTBEATS_READ);
        skipped = group.counter(BigtableMetricNames.RECORDS_SKIPPED);
        readsStarted = group.counter(BigtableMetricNames.CHANGE_STREAM_READS_STARTED);
        closeStreams = group.counter(BigtableMetricNames.CHANGE_STREAM_CLOSE_STREAMS_READ);
        userMutations = group.counter(BigtableMetricNames.CHANGE_STREAM_USER_MUTATIONS_READ);
        garbageCollectionMutations =
                group.counter(BigtableMetricNames.CHANGE_STREAM_GARBAGE_COLLECTION_MUTATIONS_READ);
        mutationEntriesFiltered =
                group.counter(BigtableMetricNames.CHANGE_STREAM_MUTATION_ENTRIES_FILTERED);
        recordsSkippedWithoutChange =
                group.counter(BigtableMetricNames.CHANGE_STREAM_RECORDS_SKIPPED_WITHOUT_CHANGE);
        group.gauge(
                BigtableMetricNames.ACTIVE_CHANGE_STREAM_READS, (Gauge<Integer>) activeReads::size);
        group.gauge(
                BigtableMetricNames.QUEUED_CHANGE_STREAM_PARTITIONS,
                (Gauge<Integer>) queuedPartitions::get);
        group.gauge(
                BigtableMetricNames.QUEUED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::queuedPartitionLagMillis);
        group.gauge(
                BigtableMetricNames.MISSED_HEARTBEAT_INTERVALS,
                (Gauge<Long>) this::missedHeartbeatIntervals);
        group.gauge(
                BigtableMetricNames.PARTITION_LOW_WATERMARK_MILLIS,
                (Gauge<Long>) this::minimumAssignedLowWatermarkMillis);
    }

    ReadTiming opening() {
        return new ReadTiming(currentTimeMillis.getAsLong());
    }

    void started(ReadTiming timing) {
        readsStarted.inc();
        synchronized (timing) {
            timing.started = true;
            timing.lastResponseMillis.set(currentTimeMillis.getAsLong());
            if (!timing.terminal) {
                activeReads.add(timing);
            }
        }
    }

    void recordReturned(ReadTiming timing) {
        timing.lastResponseMillis.set(currentTimeMillis.getAsLong());
    }

    void terminated(ReadTiming timing) {
        synchronized (timing) {
            if (timing.terminal) {
                return;
            }
            timing.terminal = true;
            if (timing.started) {
                activeReads.remove(timing);
            }
        }
    }

    void mutation(ChangeStreamMutation mutation) {
        mutations.inc();
        if (mutation.getType() == ChangeStreamMutation.MutationType.USER) {
            userMutations.inc();
        } else {
            garbageCollectionMutations.inc();
        }
    }

    void heartbeat() {
        heartbeats.inc();
    }

    void closeStream() {
        closeStreams.inc();
    }

    void skipped() {
        skipped.inc();
    }

    void entriesFiltered(long count) {
        mutationEntriesFiltered.inc(count);
    }

    void skippedWithoutChange() {
        recordsSkippedWithoutChange.inc();
    }

    void assigned(
            Collection<ChangeStreamPartitionSplit> active,
            Collection<ChangeStreamPartitionSplit> queued) {
        activeLowWatermarks.clear();
        queuedPartitions.set(queued.size());
        long oldestQueued = NO_POSITION;
        for (ChangeStreamPartitionSplit split : active) {
            activeLowWatermarks.put(split.splitId(), split.getLowWatermark().toEpochMilli());
        }
        for (ChangeStreamPartitionSplit split : queued) {
            long position = split.getLowWatermark().toEpochMilli();
            oldestQueued = Math.min(oldestQueued, position);
        }
        oldestQueuedPositionMillis.set(oldestQueued);
    }

    void progress(String splitId, java.time.Instant lowWatermark) {
        activeLowWatermarks.computeIfPresent(
                splitId, (ignored, previous) -> lowWatermark.toEpochMilli());
    }

    void closed() {
        for (ReadTiming timing : activeReads.toArray(new ReadTiming[0])) {
            terminated(timing);
        }
        queuedPartitions.set(0);
        activeLowWatermarks.clear();
        oldestQueuedPositionMillis.set(NO_POSITION);
    }

    private long queuedPartitionLagMillis() {
        long oldest = oldestQueuedPositionMillis.get();
        return oldest == NO_POSITION
                ? 0
                : BigtableMetricValues.elapsedMillis(currentTimeMillis.getAsLong(), oldest);
    }

    private long missedHeartbeatIntervals() {
        long now = currentTimeMillis.getAsLong();
        long maximum = 0;
        for (ReadTiming timing : activeReads) {
            maximum =
                    Math.max(
                            maximum,
                            BigtableMetricValues.elapsedMillis(now, timing.lastResponseMillis.get())
                                    / DataClientChangeStreamOpener.HEARTBEAT_INTERVAL.toMillis());
        }
        return maximum;
    }

    private long minimumAssignedLowWatermarkMillis() {
        long minimum = oldestQueuedPositionMillis.get();
        for (long active : activeLowWatermarks.values()) {
            minimum = Math.min(minimum, active);
        }
        return minimum == NO_POSITION ? 0 : minimum;
    }

    static final class ReadTiming {
        private final AtomicLong lastResponseMillis;
        private boolean started;
        private boolean terminal;

        private ReadTiming(long openedAtMillis) {
            lastResponseMillis = new AtomicLong(openedAtMillis);
        }
    }
}
