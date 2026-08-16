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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

/**
 * The default-stream (at-least-once) writer's metrics.
 *
 * <p>The counters are plain, not thread-safe, which is why every increment site is on the task
 * thread. Append completions arrive on gRPC callback threads, and those deliberately count nothing:
 * a repairable failure is parked in the in-flight map for the task thread to classify, and the one
 * failure a callback owns outright — a terminal one — is counted when {@code checkAsyncError}
 * surfaces it, again on the task thread.
 *
 * <p><b>{@code numRecordsSend} counts records, not append attempts.</b> A batch is counted once,
 * inside the append that first hands it to the client library; the re-appends this writer issues
 * while repairing a destination go through a different call site and are counted as {@code
 * appendRetries} instead. Retry volume by status is read from the error-class counters, which
 * <em>do</em> count every failed attempt.
 *
 * <p>{@code currentSendTime} is deliberately left unset: an append may be re-issued across several
 * backoffs and a table creation, so the interval between hand-off and acknowledgement would
 * describe this writer's repair budget rather than the service's response time.
 */
@Internal
final class DefaultStreamWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter appendRetries;
    private final Counter tablesCreated;
    private final Counter schemaReconciliations;
    private final ErrorClassCounters errorClasses;
    private final DestinationMetrics destinations;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     * @param perDestinationMetrics whether {@code DefaultStreamOptions.perDestinationMetrics} is
     *     set
     */
    DefaultStreamWriterMetrics(SinkWriterMetricGroup metricGroup, boolean perDestinationMetrics) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(BigQueryMetricNames.RECORDS_SKIPPED);
        this.appendRetries = metricGroup.counter(BigQueryMetricNames.APPEND_RETRIES);
        this.tablesCreated = metricGroup.counter(BigQueryMetricNames.TABLES_CREATED);
        this.schemaReconciliations =
                metricGroup.counter(BigQueryMetricNames.SCHEMA_RECONCILIATIONS);
        this.errorClasses = new ErrorClassCounters(metricGroup);
        this.destinations = DestinationMetrics.of(metricGroup, perDestinationMetrics);
    }

    /**
     * Registers the gauges reading the writer's own collections. Separate from the constructor
     * because the writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param inFlightBatches appends the service has not answered
     * @param openDestinations destinations holding a live stream writer
     */
    void bindWriterState(Gauge<Integer> inFlightBatches, Gauge<Integer> openDestinations) {
        metricGroup.gauge(BigQueryMetricNames.IN_FLIGHT_BATCHES, inFlightBatches);
        metricGroup.gauge(BigQueryMetricNames.OPEN_DESTINATIONS, openDestinations);
    }

    /**
     * Returns the per-destination counters for a table. Looked up rather than cached on the
     * writer's per-destination state: the sends this writer counts are per <em>batch</em>, and its
     * state is rebuilt by every repair, so a cached handle would buy one map read per append at the
     * cost of threading it through the rebuild path.
     *
     * @param destination the table
     * @return its counters, a no-op unless per-destination metrics are switched on
     */
    DestinationMetrics.Counters forTable(TableDestination destination) {
        return destinations.forDestination(destination.toString());
    }

    /**
     * Counts one batch handed to the client library.
     *
     * @param table the destination's counters, from {@link #forTable}
     * @param rowCount the batch's row count
     * @param rowBytes the batch's serialized row bytes
     */
    void batchAppended(DestinationMetrics.Counters table, int rowCount, long rowBytes) {
        numRecordsSend.inc(rowCount);
        numBytesSend.inc(rowBytes);
        table.recordsSent(rowCount);
    }

    /**
     * Counts one row routed to the failure handler, whether the serializer rejected it, it exceeded
     * the per-row limit, or the service rejected it by index.
     *
     * @param table the destination's counters, from {@link #forTable}
     */
    void rowFailed(DestinationMetrics.Counters table) {
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
     * <p>Named after the record rather than the row, unlike its siblings here: a skipped record
     * never became one. It is neither a send nor a failure, and nothing else in the writer reports
     * it — without this counter a serializer skipping every record is indistinguishable from a
     * stream that carried none, which is the one way the skip contract can hide a bug.
     *
     * <p>Takes no {@link DestinationMetrics.Counters}, so it is not broken down per table even when
     * {@code perDestinationMetrics} is set: the serializer is handed the record alone, so its
     * decision cannot depend on the destination, and attributing a skip to the table the record
     * would have gone to would read as a property of that table.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /** Counts one append re-issued while repairing a destination. */
    void appendRetried() {
        appendRetries.inc();
    }

    /** Counts one destination-table creation request made by the sink. */
    void tableCreated() {
        tablesCreated.inc();
    }

    /** Counts one table schema updated to cover the serializer's. */
    void schemaReconciled() {
        schemaReconciliations.inc();
    }

    /**
     * Counts one failed append under the status code that classifies it — every failure the task
     * thread classifies, first attempts and re-appends alike, so the sum over the transient codes
     * is the retry volume the {@link #appendRetries} counter measures from the other side.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void appendFailed(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }
}
