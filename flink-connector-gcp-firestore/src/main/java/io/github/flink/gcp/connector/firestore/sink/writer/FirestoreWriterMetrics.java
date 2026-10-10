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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;

import javax.annotation.Nullable;

/**
 * The Firestore sink writer's metrics.
 *
 * <p>Every counter but one is plain and counted on the task thread. {@code writesRetried} is the
 * exception: the client library's retry listener counts it on the {@code BulkWriter}'s executor, so
 * it is thread-safe.
 *
 * <p><b>{@code numRecordsSend} counts records, not attempts.</b> The client library retries a write
 * internally, and a write the writer re-sends alone to confirm a rejection is not counted again.
 *
 * <p>There is no {@code batchesSent}: the client library batches internally and exposes no hook for
 * a request it sends, so the count could only be a guess. There are no per-destination counters
 * either: the sink writes one database but any number of its collections, and that cardinality is
 * the serializer's to decide. {@code currentSendTime} is left unset, as on the sibling sinks: a
 * request's latency covers unrelated writes.
 */
@Internal
final class FirestoreWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter writesRetried;
    private final Counter bulkWritersReplaced;
    private final Counter writesConfirmedAlone;
    private final Counter idsRedrawn;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     */
    FirestoreWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(FirestoreMetricNames.RECORDS_SKIPPED);
        this.writesRetried =
                metricGroup.counter(
                        FirestoreMetricNames.WRITES_RETRIED, new ThreadSafeSimpleCounter());
        this.bulkWritersReplaced = metricGroup.counter(FirestoreMetricNames.BULK_WRITERS_REPLACED);
        this.writesConfirmedAlone =
                metricGroup.counter(FirestoreMetricNames.WRITES_CONFIRMED_ALONE);
        this.idsRedrawn = metricGroup.counter(FirestoreMetricNames.IDS_REDRAWN);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauges reading the writer's state. Separate from the constructor because the
     * writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param inFlightWrites writes handed to the client and not yet answered
     * @param inFlightBytes their estimated size
     * @param parkedWrites writes waiting to be re-sent alone
     */
    void bindWriterState(
            Gauge<Integer> inFlightWrites, Gauge<Long> inFlightBytes, Gauge<Integer> parkedWrites) {
        metricGroup.gauge(FirestoreMetricNames.IN_FLIGHT_WRITES, inFlightWrites);
        metricGroup.gauge(FirestoreMetricNames.IN_FLIGHT_BYTES, inFlightBytes);
        metricGroup.gauge(FirestoreMetricNames.PARKED_WRITES, parkedWrites);
    }

    /**
     * Counts one record handed to the client. Called on a write's first submission only.
     *
     * @param estimatedSize the write's estimated size
     */
    void writeSent(long estimatedSize) {
        numRecordsSend.inc();
        numBytesSend.inc(estimatedSize);
    }

    /** Counts one write attempt the client library is retrying. Safe from any thread. */
    void writeRetried() {
        writesRetried.inc();
    }

    /**
     * Counts one write re-sent alone to confirm an {@code INVALID_ARGUMENT}. The only trace of a
     * request refused as a whole whose writes then succeed alone: those are applied, so neither
     * {@code errorClass} nor {@code numRecordsSendErrors} sees them, and the park that held them is
     * drained before the next record.
     */
    void writeConfirmedAlone() {
        writesConfirmedAlone.inc();
    }

    /**
     * Counts one drawn-id create sent again under a new id after {@code ALREADY_EXISTS}. Each is
     * either a collision with an existing document, which the create left in place, or a retried
     * create that had been applied, which leaves its document twice; neither {@code errorClass} nor
     * {@code numRecordsSendErrors} sees it.
     */
    void idRedrawn() {
        idsRedrawn.inc();
    }

    /** Counts one replacement of the client library's {@code BulkWriter}. */
    void bulkWriterReplaced() {
        bulkWritersReplaced.inc();
    }

    /**
     * Counts one record routed to the failure handler, whether the serializer rejected it or the
     * service refused the write, and whether the handler then dropped it or failed the job.
     */
    void writeFailed() {
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
     * Counts one write the client library gave up on, under the status that classifies it, once its
     * verdict is final: a write parked to be confirmed alone is counted when that confirmation
     * answers, not before, since the library reports one request-level status against every write
     * of the request. Records the serializer rejected carry no status and are not counted here, as
     * on every sibling sink.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void writeFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }
}
