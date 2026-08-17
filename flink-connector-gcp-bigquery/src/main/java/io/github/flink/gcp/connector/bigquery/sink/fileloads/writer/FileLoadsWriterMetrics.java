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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

/**
 * The FILE_LOADS writer's metrics.
 *
 * <p>This writer has no per-record RPC — a record is encoded into a staging file on Cloud Storage,
 * and the load job that turns those files into rows runs in the committer. Two consequences.
 * <b>There is no error-class dimension here</b>: a record either reaches the staging file or is
 * routed to the failure handler by the serializer or the staging-format conversion, and neither
 * carries a service status. And <b>{@code numRecordsSend} and {@code numBytesSend} are counted at
 * different moments</b>: a record is counted when the file writer accepts it, while its bytes are
 * only known when the file is finished, so the byte counter advances in file-sized steps and lags
 * the record counter by the currently open files.
 *
 * <p>{@code numBytesSend} is therefore the <em>staged</em> size — encoded in the configured staging
 * format and compressed with whatever codec {@code StagedFileWriter} uses — rather than the payload
 * the records carried. It is the number that predicts what the load job reads.
 *
 * <p>{@code currentSendTime} is deliberately left unset: nothing here corresponds to a request the
 * service answers.
 */
@Internal
final class FileLoadsWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter filesStaged;
    private final DestinationMetrics destinations;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     * @param perDestinationMetrics whether {@code FileLoadsOptions.perDestinationMetrics} is set
     */
    FileLoadsWriterMetrics(SinkWriterMetricGroup metricGroup, boolean perDestinationMetrics) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(BigQueryMetricNames.RECORDS_SKIPPED);
        this.filesStaged = metricGroup.counter(BigQueryMetricNames.FILES_STAGED);
        this.destinations = DestinationMetrics.of(metricGroup, perDestinationMetrics);
    }

    /**
     * Registers the gauge reading the writer's own map. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param openDestinations destinations holding conversion state
     */
    void bindWriterState(Gauge<Integer> openDestinations) {
        metricGroup.gauge(BigQueryMetricNames.OPEN_DESTINATIONS, openDestinations);
    }

    /**
     * Returns the per-destination counters for a table.
     *
     * @param destination the table
     * @return its counters, a no-op unless per-destination metrics are switched on
     */
    DestinationMetrics.Counters forTable(TableDestination destination) {
        return destinations.forDestination(destination.toString());
    }

    /**
     * Counts one record written to a staging file.
     *
     * @param table the destination's counters, from {@link #forTable}
     */
    void recordStaged(DestinationMetrics.Counters table) {
        numRecordsSend.inc();
        table.recordSent();
    }

    /**
     * Counts one finished staging file and the bytes it holds.
     *
     * @param bytes the file's size on Cloud Storage
     */
    void fileFinished(long bytes) {
        filesStaged.inc();
        numBytesSend.inc(bytes);
    }

    /**
     * Counts one record routed to the failure handler — one the serializer rejected, or one the
     * Avro conversion could not encode.
     *
     * @param table the destination's counters, from {@link #forTable}
     */
    void recordFailed(DestinationMetrics.Counters table) {
        numRecordsSendErrors.inc();
        table.sendFailed();
    }

    /** Counts one routing failure before a destination exists. */
    void recordFailedWithoutDestination() {
        numRecordsSendErrors.inc();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}.
     *
     * <p>It is neither a send nor a failure, and nothing else in the writer reports it — without
     * this counter a serializer skipping every record is indistinguishable from a stream that
     * carried none, which is the one way the skip contract can hide a bug.
     *
     * <p>Takes no {@link DestinationMetrics.Counters}, so it is not broken down per table even when
     * {@code perDestinationMetrics} is set: the serializer is handed the record alone, so its
     * decision cannot depend on the destination, and attributing a skip to the table the record
     * would have gone to would read as a property of that table.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }
}
