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

import javax.annotation.Nullable;

/**
 * The Bigtable sink writer's metrics.
 *
 * <p>The counters are plain, not thread-safe: mutation completions reach the writer as mailbox
 * mails, so every increment happens on the task thread.
 *
 * <p><b>{@code numRecordsSend} counts records, not mutation attempts</b> — and here that costs
 * nothing to arrange, because the retries are the client's. A mutation this writer hands to the
 * batcher is retried inside the SDK and only ever comes back once, so there is no re-entered call
 * site to guard as the Pub/Sub and Cloud Tasks writers have. The same fact bounds what {@link
 * #applyFailure} can report: only failures the client gave up on are visible here, so the sum over
 * the transient codes is <em>not</em> this connector's retry volume, which is the one place a
 * dashboard reading all four connectors alike would be misled.
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

    static final String IN_FLIGHT_MUTATIONS = "inFlightMutations";
    static final String IN_FLIGHT_BYTES = "inFlightBytes";

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final ErrorClassCounters errorClasses;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     */
    BigtableWriterMetrics(SinkWriterMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    /**
     * Registers the gauges reading the writer's own in-flight counters. Separate from the
     * constructor because the writer is built with these metrics and cannot exist yet when they are
     * created.
     *
     * @param inFlightMutations mutations the service has not acknowledged
     * @param inFlightBytes their serialized size, against {@code maxInFlightBytes}
     */
    void bindWriterState(Gauge<Integer> inFlightMutations, Gauge<Long> inFlightBytes) {
        metricGroup.gauge(IN_FLIGHT_MUTATIONS, inFlightMutations);
        metricGroup.gauge(IN_FLIGHT_BYTES, inFlightBytes);
    }

    /**
     * Counts one record handed to the client library for application.
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
     * Counts one failed attempt to apply a mutation, under the status code that classifies it.
     * Named after the operation rather than the element — as {@code publishFailure} and {@code
     * createFailure} are on the sibling sinks — because it does not count what {@link
     * #mutationFailed} counts: that one counts elements routed to the handler, this one counts RPC
     * failures whether they were routed or fatal.
     *
     * <p>Every failure that reaches the writer is counted, fatal ones included: the client has
     * already exhausted its own retries by then, so each is a distinct give-up rather than an
     * attempt.
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
