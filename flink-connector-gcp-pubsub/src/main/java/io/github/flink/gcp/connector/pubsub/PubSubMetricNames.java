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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers itself, in one place so that this file is the
 * connector's inventory: what it reports can be read here without opening a writer, a reader or the
 * enumerator.
 *
 * <p>Each connector has one of these, and comparing them is how the repository's metric naming
 * convention is held across connectors — a name that means the same thing in two connectors should
 * be spelled the same way, and a diff of these files is what shows it. The convention itself (a
 * counter names the event, a gauge names the state, and neither takes Flink's {@code num} prefix)
 * is recorded in the base module's detailed agent guidance.
 *
 * <p>What is <em>not</em> here: Flink's standard names, which come from {@code
 * SinkWriterMetricGroup} and {@code SourceReaderMetricGroup} accessors rather than from a name, and
 * the subgroup leaves {@code base.metrics} registers on this connector's behalf ({@code
 * errorClass.CODE.errors}, {@code destination.TOPIC.recordsSend}). The user-facing meaning of each
 * name is on the connector's documentation page, not duplicated here.
 *
 * <p><b>The dead-letter names below are this connector's and appear on somebody else's sink.</b>
 * {@code PubSubDeadLetterQueue} serves every connector in this repository and registers on the
 * metric group {@code FailureHandlerContext} hands it — the <em>host</em> sink writer's — so a
 * BigQuery or Cloud Tasks job dead-lettering to a topic reports them beside that sink's own names.
 * They are declared here because the class is Pub/Sub's, the argument that already places its
 * options on {@code reference/pubsub.md} (ADR-0009), and each carries {@code deadLetter} so that it
 * reads unambiguously in that company.
 */
@Internal
public final class PubSubMetricNames {

    // Registered by the sink writer (PubSubWriterMetrics).
    public static final String IN_FLIGHT_MESSAGES = "inFlightMessages";
    public static final String IN_FLIGHT_BYTES = "inFlightBytes";
    public static final String PARKED_MESSAGES = "parkedMessages";
    public static final String ACTIVE_PUBLISHERS = "activePublishers";
    public static final String CAPACITY_EVICTIONS = "capacityEvictions";
    public static final String IDLE_EVICTIONS = "idleEvictions";
    public static final String TOPICS_CREATED = "topicsCreated";

    /**
     * Counts an <em>event</em> — a publisher teardown the close gave up on — so it takes the
     * counter shape the naming convention prescribes, even though its value comes from a
     * process-wide total rather than this writer's own tally. The storage has to be process-wide (a
     * per-attempt tally is unregistered before any reporter reads it, measured); the
     * <em>instrument</em> does not follow from that, and a cumulative count of events is a counter.
     * The count itself is {@link PubSubShutdownResidue}.
     */
    public static final String PUBLISHER_SHUTDOWNS_ABANDONED = "publisherShutdownsAbandoned";

    // Registered by both directions, each on its own group.
    /**
     * The sink writer counts a record its serializer skipped; the source reader counts a message
     * its deserialization schema collected nothing for (ADR-0001). Separate groups, so the two
     * series never collide, and separate counter implementations — the reader's is thread-safe and
     * the writer's is not, for the reason each metrics class gives.
     */
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    // Registered by the source reader (PubSubSourceReaderMetrics).
    public static final String MESSAGES_RECEIVED = "messagesReceived";
    public static final String MESSAGES_ACKED = "messagesAcked";
    public static final String MESSAGES_NACKED = "messagesNacked";
    public static final String MESSAGES_DROPPED = "messagesDropped";
    public static final String PENDING_ACKS = "pendingAcks";
    public static final String PENDING_CHECKPOINTS = "pendingCheckpoints";

    /**
     * What this subtask's subscribers are holding that the fetch loop has not taken yet — a
     * <em>state</em>, so a gauge. It is the number the paused-split bound is evaluated against, and
     * the memory a reader that has stopped draining accumulates whether or not any split is paused
     * (#377).
     *
     * <p>Two gauges rather than one, for the reason the bound they shadow has two dimensions: which
     * of message count and byte size fills a TaskManager first depends on the message size.
     *
     * <p>Neither is {@link #PENDING_ACKS}, which counts messages received <em>or emitted</em> and
     * not yet acknowledged and so cannot tell a growing buffer from a slow checkpoint. Nor do they
     * cover the reader's whole footprint: everything already pulled has left the subscriber and
     * left these, which is up to {@code (source.reader.element.queue.capacity + 2) ×
     * maxRecordsPerFetch × splits} messages — the queue itself, plus the fetch the reader is
     * working through and the batch the fetcher cannot hand over, each of which holds one drain of
     * <em>every</em> assigned split. Measured on #377 at 3999 against a capacity of 2, a
     * 1000-message fetch and one split.
     */
    public static final String BUFFERED_MESSAGES = "bufferedMessages";

    /** {@link #BUFFERED_MESSAGES} in bytes. */
    public static final String BUFFERED_BYTES = "bufferedBytes";

    /**
     * The <em>state</em> a paused split is left in once its buffer outgrows its bound and the
     * reader stops its subscriber (#357), so it takes the gauge shape — and "parked" in the sense
     * {@link #PARKED_MESSAGES} already gives it here, held for a resumption that is expected. This
     * is the one to alert on: a split that stays parked is one an aligned group is holding
     * indefinitely, which on a healthy job does not happen.
     */
    public static final String PARKED_SPLITS = "parkedSplits";

    /**
     * The <em>event</em> behind that state, so it takes the counter shape. Both exist because
     * neither answers the other's question: a park and its resume falling between two scrapes leave
     * the gauge at zero with nothing to say they happened, while the gauge alone cannot tell one
     * long pause from an alignment cycle parking a split over and over.
     */
    public static final String SPLITS_PARKED = "splitsParked";

    /**
     * {@link #PUBLISHER_SHUTDOWNS_ABANDONED}'s counterpart for the source's subscribers, spelled
     * the same way because it means the same thing — one connector spelling one meaning twice is
     * what this file exists to hold. Everything that field's javadoc says about the instrument and
     * the process-wide storage holds here; the count itself is {@link PubSubShutdownResidue}.
     */
    public static final String SUBSCRIBER_SHUTDOWNS_ABANDONED = "subscriberShutdownsAbandoned";

    /**
     * Counts the <em>event</em> a subscriber teardown is the only report of: a failure that reached
     * the teardown having never been handed to the reader, so no job failure is coming for it
     * (#351).
     *
     * <p>Named for that property rather than for the shutdown, because the shutdown is where it is
     * <em>observed</em> and not what it is: the same branch catches a streaming failure that landed
     * after the reader's last pull, which no wording about shutting down would describe. What it
     * excludes is the opposite case — the client repeating at teardown a failure the reader already
     * has — which is counted by nothing, the job being already on its way down over that very
     * failure. The count itself is {@link PubSubShutdownResidue}.
     */
    public static final String SUBSCRIBER_FAILURES_UNREPORTED = "subscriberFailuresUnreported";

    // Registered by the split enumerator, so these are job-wide rather than per subtask.
    public static final String ASSIGNED_SPLITS = "assignedSplits";
    public static final String UNASSIGNED_READERS = "unassignedReaders";

    // Registered by the dead-letter queue (PubSubDeadLetterQueueMetrics), on the host sink
    // writer's metric group — see the class javadoc above.

    /**
     * Counts dead letters the service has <em>confirmed</em>, at the point each publish future
     * resolves, rather than at the offer that handed it to the client library. The offer-side count
     * already exists on the same metric group and is not this connector's to register: every sink
     * here increments {@code numRecordsSendErrors} immediately before calling the failure handler,
     * so under {@code sendToDeadLetterQueue(...)} it counts exactly what was offered. A hand-off
     * counter here would be that series a second time, while what nothing reports is how much of it
     * reached the topic.
     */
    public static final String DEAD_LETTERS_PUBLISHED = "deadLettersPublished";

    /**
     * The <em>state</em> the confirmed count is read against: dead letters handed to the client
     * library and not yet resolved, which {@code maxInFlightMessages} bounds.
     */
    public static final String IN_FLIGHT_DEAD_LETTERS = "inFlightDeadLetters";

    /**
     * How long the most recently completed wait for those publishes took, in milliseconds — the
     * state to read against {@code flushTimeout}, which is what a checkpoint's flush is spending.
     * Both waits the budget covers report here, the one in {@code flush()} and the one {@code
     * maxInFlightMessages} triggers inside an offer.
     */
    public static final String DEAD_LETTER_FLUSH_MILLIS = "deadLetterFlushMillis";

    /**
     * The longest of those waits so far, which is the one a reporter can actually catch (#405):
     * waits happen as often as the queue drains — once per <em>element</em> under {@code
     * WRITE_THROUGH} — so the last-wait gauge above is overwritten between two scrapes, and a
     * publish that nearly spent {@code flushTimeout} leaves no trace in it. Per task attempt, since
     * it is writer state.
     */
    public static final String LONGEST_DEAD_LETTER_FLUSH_MILLIS = "longestDeadLetterFlushMillis";

    /**
     * {@link #PUBLISHER_SHUTDOWNS_ABANDONED} for the dead-letter publisher, counted separately and
     * named separately because it is registered on a group that may already carry that name — the
     * host sink's, when the host is a Pub/Sub sink. Flink resolves such a collision by keeping the
     * metric registered first and dropping the other with a warning, so one name for both would
     * make a healthy configuration log "Metric will not be reported". Splitting them also says
     * which publisher is stalling. The count itself is {@link PubSubShutdownResidue}.
     */
    public static final String DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED =
            "deadLetterPublisherShutdownsAbandoned";

    private PubSubMetricNames() {}
}
