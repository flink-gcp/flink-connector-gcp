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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;

import javax.annotation.Nullable;

/**
 * The Spanner sink writer's metrics.
 *
 * <p>The counters are plain, not thread-safe, and need not be: this writer sends synchronously from
 * the task thread and no callback thread ever touches them.
 *
 * <p><b>{@code numRecordsSend} counts records, not send attempts.</b> The writer owns the retry
 * loop here — the client library does not retry {@code batchWrite} at all — so without that rule a
 * database having a bad minute would inflate the throughput a dashboard reads. {@link
 * #mutationsRetried} is where the retry volume is, and it is this connector's alone: on the sibling
 * sinks the same work happens inside the SDK and is invisible.
 *
 * <p>There are no per-destination counters. The sink writes one database but any number of its
 * tables, so a per-table breakdown would be the meaningful cut — and its cardinality is the
 * serializer's to decide, not the sink's, which is not a bill a connector should sign on a user's
 * behalf.
 *
 * <p>{@code currentSendTime} is deliberately left unset: a batch write's latency covers a whole
 * request of unrelated mutations, so attributing it to records would say nothing an operator can
 * act on.
 */
@Internal
final class SpannerWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter mutationsRetried;
    private final Counter batchesSent;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     */
    SpannerWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(SpannerMetricNames.RECORDS_SKIPPED);
        this.mutationsRetried = metricGroup.counter(SpannerMetricNames.MUTATIONS_RETRIED);
        this.batchesSent = metricGroup.counter(SpannerMetricNames.BATCHES_SENT);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauges reading the writer's batch. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param bufferedMutations mutations held for the next flush
     * @param bufferedCells their cost against Spanner's per-request mutation limit
     * @param bufferedBytes their estimated size
     */
    void bindWriterState(
            Gauge<Integer> bufferedMutations,
            Gauge<Integer> bufferedCells,
            Gauge<Long> bufferedBytes) {
        metricGroup.gauge(SpannerMetricNames.BUFFERED_MUTATIONS, bufferedMutations);
        // The two weights are separate gauges because they answer different questions: which of the
        // three batch limits is the one actually firing, and therefore which knob to move. A cell
        // count near its cap on a small byte count means an index-heavy table, not a large payload.
        metricGroup.gauge(SpannerMetricNames.BUFFERED_CELLS, bufferedCells);
        metricGroup.gauge(SpannerMetricNames.BUFFERED_BYTES, bufferedBytes);
    }

    /**
     * Counts one record handed to the service. Called on a mutation's first send only, never on a
     * retry.
     *
     * @param estimatedSize the mutation's estimated size
     */
    void mutationSent(long estimatedSize) {
        numRecordsSend.inc();
        numBytesSend.inc(estimatedSize);
    }

    /**
     * Counts mutations re-sent after a transient failure, one per mutation per re-send — so a batch
     * of 500 retried twice contributes 1000. It measures the work the retry loop is doing, which is
     * what decides whether the retry budget is set sensibly.
     *
     * @param count how many mutations are being re-sent
     */
    void mutationsRetried(int count) {
        mutationsRetried.inc(count);
    }

    /** Counts one batch write request, first attempts and re-sends alike. */
    void batchSent() {
        batchesSent.inc();
    }

    /**
     * Counts one record routed to the failure handler, whether the serializer rejected it or the
     * service refused the mutation, and whether the handler then dropped it or failed the job.
     */
    void mutationFailed() {
        numRecordsSendErrors.inc();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}.
     *
     * <p>Named after the record rather than the mutation, unlike its siblings here: a skipped
     * record never became one. It is neither a send nor a failure, and nothing else in the writer
     * reports it — without this counter a serializer skipping every record is indistinguishable
     * from a stream that carried none, which is the one way the skip contract can hide a bug.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /**
     * Counts one failed write, under the status code that classifies it.
     *
     * <p>Counted per mutation the service refused and per request-level failure — including the
     * transient ones the writer then retries, which is the difference from the sibling sinks, where
     * the SDK absorbs its own retries and only reports what it gave up on. Here the sum over the
     * transient codes <em>is</em> the connector's retry cause breakdown, and it is meant to be read
     * beside {@code mutationsRetried}.
     *
     * <p>Records the serializer rejected are not counted here. They carry no status, so they would
     * all land under {@code UNCLASSIFIED} and bury the failures that genuinely carry none; {@code
     * numRecordsSendErrors} is where they are visible. Every sibling sink draws the line in the
     * same place.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void writeFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }
}
