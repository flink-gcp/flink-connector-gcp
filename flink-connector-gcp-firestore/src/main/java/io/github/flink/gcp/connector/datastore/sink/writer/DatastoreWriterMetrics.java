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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;

import javax.annotation.Nullable;

/**
 * The Datastore sink writer's metrics, all counted on the task thread.
 *
 * <p><b>{@code numRecordsSend} counts records, not attempts.</b> A mutation re-sent by the retry
 * loop or alone to confirm a rejection is the same record, counted once when it first joined a
 * commit; {@code mutationsRetried} and {@code mutationsConfirmedAlone} count the extra work.
 *
 * <p>There are no per-destination counters: the sink writes one database but any number of its
 * kinds, and that cardinality is the serializer's to decide. {@code currentSendTime} is left unset,
 * as on the sibling sinks: a commit's latency covers unrelated writes.
 */
@Internal
final class DatastoreWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter mutationsRetried;
    private final Counter batchesSent;
    private final Counter mutationsConfirmedAlone;
    private final Counter throttledMillis;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     */
    DatastoreWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(DatastoreMetricNames.RECORDS_SKIPPED);
        this.mutationsRetried = metricGroup.counter(DatastoreMetricNames.MUTATIONS_RETRIED);
        this.batchesSent = metricGroup.counter(DatastoreMetricNames.BATCHES_SENT);
        this.mutationsConfirmedAlone =
                metricGroup.counter(DatastoreMetricNames.MUTATIONS_CONFIRMED_ALONE);
        this.throttledMillis = metricGroup.counter(DatastoreMetricNames.THROTTLED_MILLIS);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauges reading the writer's batch. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param bufferedMutations mutations waiting for the next commit
     * @param bufferedBytes their size on the wire
     */
    void bindWriterState(Gauge<Integer> bufferedMutations, Gauge<Long> bufferedBytes) {
        metricGroup.gauge(DatastoreMetricNames.BUFFERED_MUTATIONS, bufferedMutations);
        metricGroup.gauge(DatastoreMetricNames.BUFFERED_BYTES, bufferedBytes);
    }

    /**
     * Counts one record handed to a commit for the first time.
     *
     * @param size the mutation's size on the wire
     */
    void mutationSent(long size) {
        numRecordsSend.inc();
        numBytesSend.inc(size);
    }

    /** Counts one commit request sent, a first attempt, a retry or a solo confirmation alike. */
    void batchSent() {
        batchesSent.inc();
    }

    /**
     * Counts mutations re-sent after a transient failure, per re-send: one mutation retried three
     * times contributes three.
     *
     * @param count the mutations re-sent
     */
    void mutationsRetried(int count) {
        mutationsRetried.inc(count);
    }

    /** Counts one mutation re-sent alone to confirm a status its commit was refused with. */
    void mutationConfirmedAlone() {
        mutationsConfirmedAlone.inc();
    }

    /**
     * Counts time the ramp-up throttle held the writer.
     *
     * @param millis the milliseconds waited
     */
    void throttled(long millis) {
        throttledMillis.inc(millis);
    }

    /**
     * Counts one record routed to the failure handler, whether the serializer rejected it or the
     * service refused the mutation, and whether the handler then dropped it or failed the job.
     */
    void mutationFailed() {
        numRecordsSendErrors.inc();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}. It is neither a send nor
     * a failure, and nothing else in the writer reports it.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /**
     * Counts one failed call under the status that classifies it: each transient failure the writer
     * retries, and each final verdict. A commit refused with a status the writer then confirms
     * mutation by mutation is counted by those confirmations, not by the commit, since its status
     * may answer only one of them.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void writeFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }
}
