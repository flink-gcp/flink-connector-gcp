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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;

/**
 * The scan reader's own counters, beside the ones Flink registers for every source.
 *
 * <p>Thread-safety is chosen per counter rather than uniformly, because the two are incremented
 * from different threads: rows are counted by the split reader on a fetcher thread, while skipped
 * rows are counted by the record emitter on the task thread.
 *
 * <p>There is deliberately <b>no bytes-read counter</b>. A {@code Row} does not report its
 * serialized size, and summing its keys, qualifiers and values would produce a number the client
 * library does not sanction while looking exactly like the quantity Bigtable bills for. There is
 * also no records-remaining gauge: the samples estimate bytes for a table's sections and nothing
 * says how many rows are left inside a range.
 */
@Internal
public class BigtableSourceReaderMetrics {

    private final Counter rowsRead;
    private final Counter recordsSkipped;

    /**
     * Registers the counters.
     *
     * @param metricGroup the reader's metric group
     */
    public BigtableSourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
        this.rowsRead =
                metricGroup.counter(BigtableMetricNames.ROWS_READ, new ThreadSafeSimpleCounter());
        this.recordsSkipped = metricGroup.counter(BigtableMetricNames.RECORDS_SKIPPED);
    }

    /** Counts one row pulled off a stream. Called from a fetcher thread. */
    public void rowRead() {
        rowsRead.inc();
    }

    /** Counts one row the deserializer produced no record for. Called from the task thread. */
    public void recordSkipped() {
        recordsSkipped.inc();
    }
}
