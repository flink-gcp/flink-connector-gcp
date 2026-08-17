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
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;

import javax.annotation.Nullable;

/**
 * The buffered-stream (exactly-once) writer's metrics.
 *
 * <p>Every increment is on the task thread: this writer drains its own append futures rather than
 * registering completion callbacks, so plain counters are enough.
 *
 * <p><b>{@code numRecordsSend} counts records, not append attempts.</b> A batch is counted once,
 * inside the append that first hands it to the client library — the pipelined append of {@code
 * sendAppend}, or the first probe of a restored stream. Everything this writer re-appends while
 * recovering (a same-offset resend, an offset-shifting replay, a further probe attempt) is counted
 * as {@code appendRetries} instead, so a job working through an outage does not report itself as a
 * busier one.
 *
 * <p>There are deliberately no per-destination counters here, and no {@code openDestinations} or
 * {@code tablesCreated}. Schema updates are reported by the aggregate {@code schemaReconciliations}
 * counter, like the default-stream writer.
 *
 * <p>{@code currentSendTime} is deliberately left unset, for the reason the default-stream writer's
 * metrics record.
 */
@Internal
final class BufferedStreamWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter appendRetries;
    private final Counter schemaReconciliations;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     */
    BufferedStreamWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(BigQueryMetricNames.RECORDS_SKIPPED);
        this.appendRetries = metricGroup.counter(BigQueryMetricNames.APPEND_RETRIES);
        this.schemaReconciliations =
                metricGroup.counter(BigQueryMetricNames.SCHEMA_RECONCILIATIONS);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauge reading the writer's own queue. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param inFlightAppends appends the service has not acknowledged
     */
    void bindWriterState(Gauge<Integer> inFlightAppends) {
        metricGroup.gauge(BigQueryMetricNames.IN_FLIGHT_APPENDS, inFlightAppends);
    }

    /**
     * Counts one batch handed to the client library for the first time.
     *
     * @param rows the batch
     */
    void batchAppended(ProtoRows rows) {
        numRecordsSend.inc(rows.getSerializedRowsCount());
        numBytesSend.inc(rowBytes(rows));
    }

    /**
     * Counts one row routed to the failure handler, whatever sent it there — an unroutable record,
     * a serializer rejection, the per-row limit, or a service rejection by index.
     */
    void rowFailed() {
        numRecordsSendErrors.inc();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}.
     *
     * <p>Named after the record rather than the row, unlike its siblings here: a skipped record
     * never became one. It is neither a send nor a failure, and nothing else in the writer reports
     * it — without this counter a serializer skipping every record is indistinguishable from a
     * stream that carried none, which is the one way the skip contract can hide a bug.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /** Counts one append re-issued while recovering. */
    void appendRetried() {
        appendRetries.inc();
    }

    /** Counts one table schema update applied by this writer. */
    void schemaReconciled() {
        schemaReconciliations.inc();
    }

    /**
     * Counts one failed append under the status code that classifies it — every failure the writer
     * classifies, first attempts and re-appends alike, so the sum over the transient codes is the
     * retry volume the {@link #appendRetries} counter measures from the other side.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void appendFailed(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }

    /**
     * The batch's serialized row bytes — payload volume, the same quantity {@code
     * maxAppendRequestBytes} batches on, rather than the request's wire size.
     */
    private static long rowBytes(ProtoRows rows) {
        return rows.getSerializedRowsList().stream().mapToLong(ByteString::size).sum();
    }
}
