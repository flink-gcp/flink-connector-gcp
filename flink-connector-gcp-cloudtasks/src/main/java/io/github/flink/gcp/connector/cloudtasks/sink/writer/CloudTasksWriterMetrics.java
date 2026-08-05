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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.cloudtasks.CloudTasksMetricNames;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;

import javax.annotation.Nullable;

/**
 * The Cloud Tasks sink writer's metrics.
 *
 * <p>The counters are plain, not thread-safe: creation completions reach the writer as mailbox
 * mails, so every increment happens on the task thread.
 *
 * <p><b>{@code numRecordsSend} counts records, not creation attempts.</b> This sink owns its
 * retries — a failed creation is parked and re-dispatched — and a record is counted once, at the
 * {@code write} that admitted it. Retry volume is read from the error-class counters instead, which
 * do count every attempt and name the status each one failed with.
 *
 * <p>{@code currentSendTime} is deliberately left unset: a creation may sit parked through several
 * backoffs, so the interval between admission and completion would describe this writer's retry
 * budget rather than the service's response time.
 */
@Internal
final class CloudTasksWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter tasksDeduplicated;
    private final ErrorClassCounters errorClasses;
    private final DestinationMetrics destinations;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     * @param perDestinationMetrics whether {@code CloudTasksWriterOptions.perDestinationMetrics} is
     *     set
     */
    CloudTasksWriterMetrics(SinkWriterMetricGroup metricGroup, boolean perDestinationMetrics) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(CloudTasksMetricNames.RECORDS_SKIPPED);
        this.tasksDeduplicated = metricGroup.counter(CloudTasksMetricNames.TASKS_DEDUPLICATED);
        this.errorClasses = new ErrorClassCounters(metricGroup);
        this.destinations = DestinationMetrics.of(metricGroup, perDestinationMetrics);
    }

    /**
     * Registers the gauges reading the writer's own counters. Separate from the constructor because
     * the writer is built with these metrics and cannot exist yet when they are created.
     *
     * @param inFlightTasks creations the service has not answered
     * @param parkedTasks creations waiting out a retry backoff
     */
    void bindWriterState(Gauge<Integer> inFlightTasks, Gauge<Integer> parkedTasks) {
        metricGroup.gauge(CloudTasksMetricNames.IN_FLIGHT_TASKS, inFlightTasks);
        metricGroup.gauge(CloudTasksMetricNames.PARKED_TASKS, parkedTasks);
    }

    /**
     * Returns the per-destination counters for a queue. Cloud Tasks resolves a destination per
     * record, so unlike the Pub/Sub sink there is no per-destination state to cache these in; the
     * lookup is a map read on the queue path the request already carries.
     *
     * @param destination the queue
     * @return its counters, a no-op unless per-destination metrics are switched on
     */
    DestinationMetrics.Counters forQueue(QueueDestination destination) {
        return destinations.forDestination(destination.toQueuePath());
    }

    /**
     * Counts one record handed to the client library for creation.
     *
     * @param queue the destination's counters, from {@link #forQueue}
     * @param serializedSize the task's serialized size
     */
    void taskCreated(DestinationMetrics.Counters queue, int serializedSize) {
        numRecordsSend.inc();
        numBytesSend.inc(serializedSize);
        queue.recordSent();
    }

    /**
     * Counts one record routed to the failure handler, whether the serializer rejected it, the task
     * id extractor threw, or the service refused it.
     *
     * @param queue the destination's counters, from {@link #forQueue}
     */
    void taskFailed(DestinationMetrics.Counters queue) {
        numRecordsSendErrors.inc();
        queue.sendFailed();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}.
     *
     * <p>Named after the record rather than the task, unlike its siblings here: a skipped record
     * never became one. It is neither a send nor a failure, and nothing else in the writer reports
     * it — without this counter a serializer skipping every record is indistinguishable from a
     * stream that carried none, which is the one way the skip contract can hide a bug.
     *
     * <p>Takes no {@link DestinationMetrics.Counters}, so it is not broken down per queue even when
     * {@code perDestinationMetrics} is set: the serializer is handed the record alone, so its
     * decision cannot depend on the destination, and attributing a skip to the queue the record
     * would have gone to would read as a property of that queue.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /**
     * Counts one failed creation attempt under the status code that classifies it. Unlike the
     * Pub/Sub sink, every attempt is counted, retryable ones included: those are this sink's own
     * retries, and the sum over the retryable codes is what a separate retries counter would have
     * reported.
     *
     * <p>The code passed in is the chain's <b>outermost</b> classifiable status, which is not
     * always the one the writer acts on: routing scans the whole chain for a transient status
     * (deliberately, so an unstable service cannot produce a dead letter). A chain carrying two
     * would therefore be counted under the outer one while being retried on the inner. The counter
     * answers "what did the creation fail with"; gax surfaces one status per failure, so the two
     * agree in practice.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void createFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }

    /** Counts one named task Cloud Tasks already held, which is the deduplication asked for. */
    void taskDeduplicated() {
        tasksDeduplicated.inc();
    }
}
