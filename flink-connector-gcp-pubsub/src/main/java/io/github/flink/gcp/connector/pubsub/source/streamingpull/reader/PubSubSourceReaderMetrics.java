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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.ResidueCounter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The reader's Pub/Sub-specific metrics.
 *
 * <p>The counters are thread-safe because the message lifecycle is not confined to one thread:
 * messages are received on the client library's callback threads while acknowledgement happens on
 * the task thread.
 *
 * <p><b>{@code messagesAcked} counts acknowledgements <em>requested</em>, not confirmed.</b> On an
 * ordinary subscription the client library sends the acknowledgement asynchronously and does not
 * retry a failure — it logs a warning and stops — so a message can be counted here and still be
 * redelivered. Opt into {@code PubSubSubscriberOptions.awaitAckConfirmation(...)} to make the job
 * fail instead, and watch Cloud Monitoring's {@code subscription/oldest_unacked_message_age} to
 * detect a persistent acknowledgement failure from outside the job.
 *
 * <p><b>The two subscriber-teardown counters are the exception to all of that</b> (#358): their
 * values are process-wide rather than this reader's, and they are registered here without being
 * incremented here. They have to be for the increments a reader's {@code close()} makes — a counter
 * written there is unregistered before any reporter reads it, the measurement {@code
 * PubSubShutdownResidue} carries — though not for a park's, which tears a subscriber down while the
 * job runs. One name has one storage, so what a reader reports is what every subscriber in the
 * class loader left behind, this attempt's and earlier attempts' alike.
 *
 * <p>{@code pendingRecordsGauge} is deliberately left unset: Pub/Sub exposes no backlog through the
 * data plane, and a wrong lag number is worse than none.
 */
@Internal
public final class PubSubSourceReaderMetrics {

    private final SourceReaderMetricGroup metricGroup;
    private final Counter messagesReceived;
    private final Counter messagesAcked;
    private final Counter messagesNacked;
    private final Counter messagesDropped;
    private final Counter deserializationErrors;
    private final Counter splitsParked;

    /**
     * How many of this subtask's splits are parked right now.
     *
     * <p>Held here rather than in the split reader, and atomic, for two different readers: a
     * fetcher may be rebuilt over a reader's life — and with it the {@code PubSubSplitReader} its
     * supplier makes — while the subtask has one gauge either way, so the count has to outlive the
     * split reader; and whatever writes it, the metric reporter reads it from a thread of its own.
     * Every write is on the fetcher thread.
     */
    private final AtomicInteger parkedSplits = new AtomicInteger();

    /**
     * Registers the counters on the reader's metric group.
     *
     * @param metricGroup the reader metric group
     */
    public PubSubSourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.messagesReceived =
                metricGroup.counter(
                        PubSubMetricNames.MESSAGES_RECEIVED, new ThreadSafeSimpleCounter());
        this.messagesAcked =
                metricGroup.counter(
                        PubSubMetricNames.MESSAGES_ACKED, new ThreadSafeSimpleCounter());
        this.messagesNacked =
                metricGroup.counter(
                        PubSubMetricNames.MESSAGES_NACKED, new ThreadSafeSimpleCounter());
        this.messagesDropped =
                metricGroup.counter(
                        PubSubMetricNames.MESSAGES_DROPPED, new ThreadSafeSimpleCounter());
        this.splitsParked =
                metricGroup.counter(PubSubMetricNames.SPLITS_PARKED, new ThreadSafeSimpleCounter());
        metricGroup.gauge(PubSubMetricNames.PARKED_SPLITS, (Gauge<Integer>) parkedSplits::get);
        // Registered and never held: nothing here increments them, so a field would only invite a
        // caller to try — which the counter refuses, its mutators throwing rather than no-opping.
        // The subscribers count into the adders directly, on the thread running their close().
        metricGroup.counter(
                PubSubMetricNames.SUBSCRIBER_SHUTDOWNS_ABANDONED,
                new ResidueCounter(PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED));
        metricGroup.counter(
                PubSubMetricNames.SUBSCRIBER_FAILURES_UNREPORTED,
                new ResidueCounter(PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED));
        // Flink's own standard counter, so a deserialization failure shows up in the same place as
        // every other connector's.
        this.deserializationErrors = metricGroup.getNumRecordsInErrorsCounter();
    }

    /**
     * Registers the gauges reading the tracker's state. Separate from the constructor because the
     * tracker is built with these metrics, so it cannot exist yet when they are created.
     *
     * @param ackTracker the tracker to read
     */
    public void bindAckTracker(PubSubAckTracker ackTracker) {
        metricGroup.gauge(
                PubSubMetricNames.PENDING_ACKS, (Gauge<Integer>) ackTracker::outstandingAckCount);
        metricGroup.gauge(
                PubSubMetricNames.PENDING_CHECKPOINTS,
                (Gauge<Integer>) ackTracker::checkpointsPendingAckCount);
    }

    /** Counts one message handed over by the client library. */
    public void messageReceived() {
        messagesReceived.inc();
    }

    /**
     * Counts acknowledgements requested from the client library.
     *
     * @param count how many messages were acknowledged
     */
    public void messagesAcked(long count) {
        messagesAcked.inc(count);
    }

    /**
     * Counts messages returned to Pub/Sub for redelivery.
     *
     * @param count how many messages were nacked
     */
    public void messagesNacked(long count) {
        messagesNacked.inc(count);
    }

    /** Counts one message discarded by the deserialization failure policy. */
    public void messageDropped() {
        messagesDropped.inc();
        deserializationErrors.inc();
    }

    /** Counts one deserialization failure that did not drop its message. */
    public void deserializationFailed() {
        deserializationErrors.inc();
    }

    /** Records that a paused split's subscriber has been stopped. */
    public void splitParked() {
        splitsParked.inc();
        parkedSplits.incrementAndGet();
    }

    /** Records that a parked split has a subscriber again, or has gone away. */
    public void splitUnparked() {
        parkedSplits.decrementAndGet();
    }
}
