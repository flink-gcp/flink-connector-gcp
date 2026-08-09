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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;

import javax.annotation.Nullable;

/**
 * The Bigtable sink writer's metrics.
 *
 * <p>The counters are plain, not thread-safe: mutation completions reach the writer as mailbox
 * mails, so every increment happens on the task thread.
 *
 * <p><b>{@code numRecordsSend} counts records, not mutation attempts</b>, which the writer arranges
 * by counting on the first submission only: the isolation pass re-submits a parked mutation alone
 * to get it a verdict of its own (#239), and that is a second submission of a record already
 * counted. Retrying is otherwise the client's — a mutation this writer hands to the batcher is
 * retried inside the SDK and comes back once — and the same fact bounds what {@link #applyFailure}
 * can report: only failures the client gave up on are visible here, so the sum over the transient
 * codes is <em>not</em> this connector's retry volume, which is the one place a dashboard reading
 * all four connectors alike would be misled.
 *
 * <p>There are no per-destination counters: a sink writes one fixed table, so {@code
 * destination.TABLE.*} would be a constant restatement of the writer's own totals. Registering a
 * metric that can never distinguish anything is what the series avoids elsewhere too.
 *
 * <p>{@code currentSendTime} is deliberately left unset: the client batches mutations and completes
 * their futures asynchronously, so any latency this writer could report would measure its own
 * bookkeeping rather than the service's response time.
 */
@Internal
final class BigtableWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter tablesCreated;
    private final Counter columnFamiliesAdded;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters. The auto-creation pair is registered whatever the create
     * disposition, so a dashboard reads a zero rather than a hole under {@code CREATE_NEVER}.
     *
     * @param metricGroup the writer's metric group
     */
    BigtableWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(BigtableMetricNames.RECORDS_SKIPPED);
        this.tablesCreated = metricGroup.counter(BigtableMetricNames.TABLES_CREATED);
        this.columnFamiliesAdded = metricGroup.counter(BigtableMetricNames.COLUMN_FAMILIES_ADDED);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauges reading the writer's own in-flight counters. Separate from the
     * constructor because the writer is built with these metrics and cannot exist yet when they are
     * created.
     *
     * @param inFlightMutations mutations the service has not acknowledged
     * @param inFlightBytes their serialized size, against {@code maxInFlightBytes}
     * @param parkedMutations mutations held for the isolation pass or the auto-creation repair
     */
    void bindWriterState(
            Gauge<Integer> inFlightMutations,
            Gauge<Long> inFlightBytes,
            Gauge<Integer> parkedMutations) {
        metricGroup.gauge(BigtableMetricNames.IN_FLIGHT_MUTATIONS, inFlightMutations);
        metricGroup.gauge(BigtableMetricNames.IN_FLIGHT_BYTES, inFlightBytes);
        // Nothing else reports the parks: a mutation waiting for its solo verdict or for a repair
        // has been released from the in-flight counters and has not reached the failure handler,
        // so between the two it is invisible. Transient rather than a backlog, since both passes
        // empty their park at the next record or the next checkpoint — so a reporter reads how
        // often it catches the writer mid-isolation or mid-repair, which is what the throughput
        // cost of either looks like (#239).
        metricGroup.gauge(BigtableMetricNames.PARKED_MUTATIONS, parkedMutations);
    }

    /**
     * Counts one record handed to the client library for application. Called on a record's first
     * submission only — see the class documentation for why the isolation pass's re-submission is
     * not a second send.
     *
     * @param serializedSize the mutation's serialized size
     */
    void mutationSent(int serializedSize) {
        numRecordsSend.inc();
        numBytesSend.inc(serializedSize);
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
     * Counts the destination table created by an auto-creation repair, its declared column families
     * included — a created table's families are part of the creation, not additions.
     */
    void tableCreated() {
        tablesCreated.inc();
    }

    /**
     * Counts column families an auto-creation repair added to an <em>already-existing</em> table.
     * Kept apart from {@link #tableCreated()} because the two mean different things to an operator:
     * a created table is first contact, an amended one is schema drift repaired.
     *
     * @param count how many families the repair added
     */
    void columnFamiliesAdded(int count) {
        columnFamiliesAdded.inc(count);
    }

    /**
     * Counts one failed attempt to apply a mutation, under the status code that classifies it.
     * Named after the operation rather than the element — as {@code publishFailure} and {@code
     * createFailure} are on the sibling sinks — because it does not count what {@link
     * #mutationFailed} counts: that one counts elements routed to the handler, this one counts RPC
     * failures whether they were routed or fatal.
     *
     * <p>Every failure with a confirmed identity is counted, fatal ones included: the client has
     * already exhausted its own retries by then, so each is a distinct give-up rather than an
     * attempt. A <em>batched</em> row-level rejection is the exclusion — one request-level status
     * reported against every co-batched entry, which counted here would multiply one incident by
     * the batch size. The isolation pass counts the true rejections when it confirms them, so this
     * counter reports rejected records rather than rejected batches (#239).
     *
     * <p>The code passed in is the chain's <b>outermost</b> classifiable status, which is not
     * always the one the writer routes on: {@link BigtableErrorClassifier#classify} scans the whole
     * chain for a transient status, deliberately, so an unstable service cannot produce a dead
     * letter. A chain carrying two would therefore be counted under the outer one while being
     * treated as fatal on the inner. The counter answers "what did the mutation fail with"; gax
     * surfaces one status per failure, so the two agree in practice.
     *
     * <p>Records the serializer rejected are <em>not</em> counted here. They carry no status, so
     * they would all land under {@code UNCLASSIFIED} and bury the RPC failures that genuinely carry
     * none; {@code numRecordsSendErrors} is where they are visible. Both sibling sinks draw the
     * line in the same place.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void applyFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }
}
