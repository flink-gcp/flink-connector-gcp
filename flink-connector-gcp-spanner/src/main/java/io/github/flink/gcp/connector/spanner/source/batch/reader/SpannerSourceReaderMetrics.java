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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.spanner.SpannerMetricNames;

/**
 * The batch reader's own counters, beside the ones Flink registers for every source.
 *
 * <p>Thread-safety is chosen per counter rather than uniformly, because they are incremented from
 * different threads: rows and re-read partitions are counted by the split reader on a fetcher
 * thread, while skipped rows are counted by the record emitter on the task thread.
 *
 * <p>There is deliberately <b>no bytes-read counter</b>. The client hands over a decoded {@code
 * Struct} and reports nothing about what it cost on the wire, so any number here would be this
 * connector's arithmetic while looking exactly like the quantity Spanner bills for. There is also
 * no rows-remaining gauge: a partition is an opaque token, and nothing says how many rows are left
 * inside one.
 */
@Internal
public class SpannerSourceReaderMetrics {

    private final Counter rowsRead;
    private final Counter recordsSkipped;
    private final Counter partitionsReread;

    /**
     * Registers the counters.
     *
     * @param metricGroup the reader's metric group
     */
    public SpannerSourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
        this.rowsRead =
                metricGroup.counter(SpannerMetricNames.ROWS_READ, new ThreadSafeSimpleCounter());
        this.recordsSkipped = metricGroup.counter(SpannerMetricNames.RECORDS_SKIPPED);
        this.partitionsReread =
                metricGroup.counter(
                        SpannerMetricNames.PARTITIONS_REREAD, new ThreadSafeSimpleCounter());
    }

    /** Counts one input row accepted into a fetch batch. Called from a fetcher thread. */
    public void rowRead() {
        rowsRead.inc();
    }

    /** Counts one row the deserializer produced no record for. Called from the task thread. */
    public void recordSkipped() {
        recordsSkipped.inc();
    }

    /**
     * Counts one partition opened again from its start after a wake-up cancelled it part-way.
     *
     * <p>The rows it had already handed on are delivered a second time, so this is the counter that
     * explains duplicate output from a run that never failed. Called from a fetcher thread.
     */
    public void partitionReread() {
        partitionsReread.inc();
    }
}
