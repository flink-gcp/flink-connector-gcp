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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;

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
 * <p>There are deliberately no per-destination counters here, and no {@code openDestinations},
 * {@code tablesCreated} or {@code schemaReconciliations}: this write method takes a fixed
 * destination whose schema is pinned when the stream is created, so each would be a constant.
 *
 * <p>{@code currentSendTime} is deliberately left unset, for the reason the default-stream writer's
 * metrics record.
 */
@Internal
final class BufferedStreamWriterMetrics {

    static final String IN_FLIGHT_APPENDS = "inFlightAppends";
    static final String APPEND_RETRIES = "appendRetries";

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter appendRetries;
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
        this.appendRetries = metricGroup.counter(APPEND_RETRIES);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauge reading the writer's own queue. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param inFlightAppends appends the service has not acknowledged
     */
    void bindWriterState(Gauge<Integer> inFlightAppends) {
        metricGroup.gauge(IN_FLIGHT_APPENDS, inFlightAppends);
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
     * Counts one row routed to the failure handler, whether the serializer rejected it, it exceeded
     * the per-row limit, or the service rejected it by index.
     */
    void rowFailed() {
        numRecordsSendErrors.inc();
    }

    /** Counts one append re-issued while recovering. */
    void appendRetried() {
        appendRetries.inc();
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
