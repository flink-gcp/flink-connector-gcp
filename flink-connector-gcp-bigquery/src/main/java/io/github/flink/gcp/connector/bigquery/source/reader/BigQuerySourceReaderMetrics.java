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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;

/**
 * The reader's metrics.
 *
 * <p>Three threads increment these. {@code rowsRead} and {@code bytesRead} are counted where the
 * rows arrive, on the split fetcher's thread, and are therefore thread-safe counters: a fetcher
 * thread is torn down and recreated over a reader's life, so a plain counter could both lose
 * increments across that handover and fail to publish them to the reporter. {@code readRetries} is
 * counted from the client library's own retry scheduler — a thread this connector neither owns nor
 * can name — so it takes the same treatment. {@code recordsSkipped} is counted by the record
 * emitter on the task thread, where a plain counter is what the rest of this connector uses.
 *
 * <p>No lag gauge is registered: the Storage Read API reports an estimated row count for a whole
 * session and nothing per stream, so a per-subtask "records behind" figure would be a guess, and a
 * wrong lag number is worse than none.
 */
@Internal
public final class BigQuerySourceReaderMetrics {

    private final Counter rowsRead;
    private final Counter bytesRead;
    private final Counter readRetries;
    private final Counter recordsSkipped;

    /**
     * Registers the reader's metrics.
     *
     * @param metricGroup the reader's metric group
     */
    public BigQuerySourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        this.rowsRead =
                metricGroup.counter(BigQueryMetricNames.ROWS_READ, new ThreadSafeSimpleCounter());
        this.bytesRead =
                metricGroup.counter(BigQueryMetricNames.BYTES_READ, new ThreadSafeSimpleCounter());
        this.readRetries =
                metricGroup.counter(
                        BigQueryMetricNames.READ_RETRIES, new ThreadSafeSimpleCounter());
        this.recordsSkipped = metricGroup.counter(BigQueryMetricNames.RECORDS_SKIPPED);
    }

    /**
     * Counts one retried attempt at a read stream, made by the client library rather than by this
     * connector.
     *
     * <p>Public because the thing that calls it is the client's own retry listener, wired where the
     * reader is created rather than from inside this package.
     */
    public void readRetried() {
        readRetries.inc();
    }

    /**
     * Counts rows arriving in one response block.
     *
     * @param rows the block's row count
     */
    void rowsRead(long rows) {
        rowsRead.inc(rows);
    }

    /**
     * Counts the serialized bytes of one response block.
     *
     * @param bytes the block's serialized size
     */
    void bytesRead(long bytes) {
        bytesRead.inc(bytes);
    }

    /** Counts an input row whose deserializer call emitted no output and returned successfully. */
    void recordSkipped() {
        recordsSkipped.inc();
    }
}
