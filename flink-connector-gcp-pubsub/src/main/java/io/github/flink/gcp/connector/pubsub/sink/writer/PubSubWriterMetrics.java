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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.ResidueCounter;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import javax.annotation.Nullable;

/**
 * The Pub/Sub sink writer's metrics.
 *
 * <p>The counters are plain, not thread-safe, because every increment happens on the task thread:
 * publish completions reach the writer as mailbox mails rather than mutating its state from the
 * SDK's callback threads. That is the opposite of {@code PubSubSourceReaderMetrics}, whose counters
 * are incremented from the client library's threads.
 *
 * <p><b>{@code numRecordsSend} counts records, not publish attempts.</b> A message republished
 * after its topic was auto-created is counted once, on the attempt the {@code write} that admitted
 * it made: the increment sits inside {@code PubSubWriter.publishTo}, <em>after</em> the publish is
 * accepted and under that method's {@code firstAttempt} flag, so a repair re-entering it counts
 * nothing and a publish that throws synchronously — registering no callback, reaching the client
 * not at all — is not counted either (ADR-0010). {@code numBytesSend} follows it, and is therefore
 * payload volume rather than wire volume.
 *
 * <p><b>{@code publisherShutdownsAbandoned} is the exception to all of that</b>: its value is a
 * process-wide count, not this writer's own, so every subtask in a JVM reports the same number. It
 * has to be, because the quantity is what accumulates <em>across</em> restart attempts — see {@code
 * PubSubShutdownResidue}. It counts the <em>sink's</em> publishers only; a dead-letter queue's
 * teardowns are counted and reported apart, by {@code PubSubDeadLetterQueueMetrics}, because that
 * queue registers on whichever sink hosts it and would otherwise collide with this name here.
 *
 * <p>{@code currentSendTime} is deliberately left unset: the SDK batches publishes and completes
 * their futures asynchronously, so any number this writer could produce would measure its own
 * bookkeeping rather than the service's latency.
 */
@Internal
final class PubSubWriterMetrics {

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;
    private final Counter recordsSkipped;
    private final Counter topicsCreated;
    private final Counter capacityEvictions;
    private final Counter idleEvictions;
    private final ErrorClassCounters errorClasses;
    private final DestinationMetrics destinations;

    /**
     * Registers the writer's counters.
     *
     * @param metricGroup the writer's metric group
     * @param perDestinationMetrics whether {@code PubSubPublisherOptions.perDestinationMetrics} is
     *     set
     */
    PubSubWriterMetrics(SinkWriterMetricGroup metricGroup, boolean perDestinationMetrics) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = metricGroup.getNumRecordsSendCounter();
        this.numBytesSend = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();
        this.recordsSkipped = metricGroup.counter(PubSubMetricNames.RECORDS_SKIPPED);
        this.topicsCreated = metricGroup.counter(PubSubMetricNames.TOPICS_CREATED);
        this.capacityEvictions = metricGroup.counter(PubSubMetricNames.CAPACITY_EVICTIONS);
        this.idleEvictions = metricGroup.counter(PubSubMetricNames.IDLE_EVICTIONS);
        this.errorClasses = new ErrorClassCounters(metricGroup);
        this.destinations = DestinationMetrics.of(metricGroup, perDestinationMetrics);
        // Here rather than in bindWriterState, because it reads no writer state — and it is the
        // one metric here whose storage does not describe only *this* writer. An eviction-time
        // overrun increments it while this metric group remains registered and can therefore be
        // observed on the running attempt. A teardown this writer's final close abandons is
        // ordinarily reported by a later attempt: measured, not assumed — a probe with a reporter
        // at 10 ms (Flink's default is 10 s) scraped ~90 times per run and never once saw a counter
        // the writer only incremented in close() above zero, across four runs, while a counter
        // incremented during the run was seen at its full value. Process-wide storage preserves
        // both shapes and describes the resources actually retained by the JVM (#311, #1132).
        //
        // A Counter rather than a Gauge, because the quantity is a cumulative count of events and
        // that is what the naming convention calls a counter. Registering a caller-supplied
        // Counter is what lets the instrument be right while the storage stays process-wide.
        metricGroup.counter(
                PubSubMetricNames.PUBLISHER_SHUTDOWNS_ABANDONED,
                new ResidueCounter(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED));
    }

    /**
     * Registers the gauges reading the writer's own counters. Separate from the constructor because
     * the writer is built with these metrics and so cannot exist yet when they are created — the
     * shape {@code PubSubSourceReaderMetrics.bindAckTracker} uses.
     *
     * @param inFlightMessages publishes not yet acknowledged
     * @param inFlightBytes serialized size of those publishes
     * @param parkedMessages messages held for a destination's next repair
     * @param activePublishers publishers currently retained by the writer
     */
    void bindWriterState(
            Gauge<Integer> inFlightMessages,
            Gauge<Long> inFlightBytes,
            Gauge<Integer> parkedMessages,
            Gauge<Integer> activePublishers) {
        metricGroup.gauge(PubSubMetricNames.IN_FLIGHT_MESSAGES, inFlightMessages);
        metricGroup.gauge(PubSubMetricNames.IN_FLIGHT_BYTES, inFlightBytes);
        metricGroup.gauge(PubSubMetricNames.PARKED_MESSAGES, parkedMessages);
        metricGroup.gauge(PubSubMetricNames.ACTIVE_PUBLISHERS, activePublishers);
    }

    /**
     * Returns the per-destination counters for a topic, which the caller is expected to hold
     * alongside its own per-topic state rather than look up per record.
     *
     * @param destination the topic
     * @return its counters, a no-op unless per-destination metrics are switched on
     */
    DestinationMetrics.Counters forTopic(TopicDestination destination) {
        return destinations.forDestination(destination.toTopicPath());
    }

    /**
     * Counts one record handed to the client library for publishing.
     *
     * @param topic the destination's counters, from {@link #forTopic}
     * @param serializedSize the message's serialized size
     */
    void messagePublished(DestinationMetrics.Counters topic, int serializedSize) {
        numRecordsSend.inc();
        numBytesSend.inc(serializedSize);
        topic.recordSent();
    }

    /**
     * Counts one record routed to the failure handler, whether it failed to serialize or was
     * rejected by the service.
     *
     * @param topic the destination's counters, from {@link #forTopic}
     */
    void messageFailed(DestinationMetrics.Counters topic) {
        numRecordsSendErrors.inc();
        topic.sendFailed();
    }

    /**
     * Counts one record the serializer skipped by returning {@code null}.
     *
     * <p>Named after the record rather than the message, unlike its siblings here: a skipped record
     * never became one. It is neither a send nor a failure, and nothing else in the writer reports
     * it — without this counter a serializer skipping every record is indistinguishable from a
     * stream that carried none, which is the one way the skip contract can hide a bug.
     *
     * <p>Takes no {@link DestinationMetrics.Counters}, so it is not broken down per topic even when
     * {@code perDestinationMetrics} is set: the serializer is handed the record alone, so its
     * decision cannot depend on the destination, and attributing a skip to the topic the record
     * would have gone to would read as a property of that topic.
     */
    void recordSkipped() {
        recordsSkipped.inc();
    }

    /**
     * Counts one publish failure under the status code that classifies it.
     *
     * @param code the status code, or {@code null} for a failure carrying none
     */
    void publishFailure(@Nullable StatusCode.Code code) {
        errorClasses.count(code);
    }

    /**
     * Counts one completed topic-creation repair.
     *
     * <p>Creations, not distinct topics: the admin treats {@code ALREADY_EXISTS} as success — a
     * parallel subtask got there first — so a topic created once is counted by every subtask that
     * repaired for it. Reading it as "how often did a missing topic stall this subtask" is right;
     * reading it as "how many topics exist" is not.
     */
    void topicCreated() {
        topicsCreated.inc();
    }

    /** Counts one publisher released to admit a new destination at the active-publisher cap. */
    void capacityEviction() {
        capacityEvictions.inc();
    }

    /** Counts one publisher released after exceeding the destination idle timeout. */
    void idleEviction() {
        idleEvictions.inc();
    }
}
