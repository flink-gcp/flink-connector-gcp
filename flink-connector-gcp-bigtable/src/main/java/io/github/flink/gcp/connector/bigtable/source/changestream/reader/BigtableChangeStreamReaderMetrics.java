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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/** Metrics registered by one change-stream source reader. */
@Internal
public final class BigtableChangeStreamReaderMetrics {

    private final Counter mutations;
    private final Counter heartbeats;
    private final Counter skipped;
    private final AtomicLong lowWatermarkMillis = new AtomicLong();

    public BigtableChangeStreamReaderMetrics(SourceReaderMetricGroup group) {
        mutations = group.counter(BigtableMetricNames.CHANGE_STREAM_MUTATIONS_READ);
        heartbeats = group.counter(BigtableMetricNames.CHANGE_STREAM_HEARTBEATS_READ);
        skipped = group.counter(BigtableMetricNames.RECORDS_SKIPPED);
        group.gauge(BigtableMetricNames.PARTITION_LOW_WATERMARK_MILLIS, lowWatermarkMillis::get);
    }

    void mutation(Instant lowWatermark) {
        mutations.inc();
        advance(lowWatermark);
    }

    void heartbeat(Instant lowWatermark) {
        heartbeats.inc();
        advance(lowWatermark);
    }

    void skipped() {
        skipped.inc();
    }

    private void advance(Instant lowWatermark) {
        lowWatermarkMillis.set(lowWatermark.toEpochMilli());
    }
}
