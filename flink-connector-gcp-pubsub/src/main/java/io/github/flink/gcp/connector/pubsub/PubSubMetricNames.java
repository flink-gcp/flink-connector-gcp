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
 * is recorded in the base module's {@code CLAUDE.md}.
 *
 * <p>What is <em>not</em> here: Flink's standard names, which come from {@code
 * SinkWriterMetricGroup} and {@code SourceReaderMetricGroup} accessors rather than from a name, and
 * the subgroup leaves {@code base.metrics} registers on this connector's behalf ({@code
 * errorClass.CODE.errors}, {@code destination.TOPIC.recordsSend}). The user-facing meaning of each
 * name is on the connector's documentation page, not duplicated here.
 */
@Internal
public final class PubSubMetricNames {

    // Registered by the sink writer (PubSubSinkWriterMetrics).
    public static final String IN_FLIGHT_MESSAGES = "inFlightMessages";
    public static final String IN_FLIGHT_BYTES = "inFlightBytes";
    public static final String PARKED_MESSAGES = "parkedMessages";
    public static final String TOPICS_CREATED = "topicsCreated";

    public static final String RECORDS_SKIPPED = "recordsSkipped";

    /**
     * Counts an <em>event</em> — a publisher teardown the close gave up on — so it takes the
     * counter shape the naming convention prescribes, even though its value comes from a
     * process-wide total rather than this writer's own tally. The storage has to be process-wide (a
     * per-attempt tally is unregistered before any reporter reads it, measured); the
     * <em>instrument</em> does not follow from that, and a cumulative count of events is a counter.
     * The count itself is {@link PubSubShutdownResidue}.
     */
    public static final String PUBLISHER_SHUTDOWNS_ABANDONED = "publisherShutdownsAbandoned";

    // Registered by the source reader (PubSubSourceReaderMetrics).
    public static final String MESSAGES_RECEIVED = "messagesReceived";
    public static final String MESSAGES_ACKED = "messagesAcked";
    public static final String MESSAGES_NACKED = "messagesNacked";
    public static final String MESSAGES_DROPPED = "messagesDropped";
    public static final String PENDING_ACKS = "pendingAcks";
    public static final String PENDING_CHECKPOINTS = "pendingCheckpoints";

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

    // Registered by the split enumerator, so these are job-wide rather than per subtask.
    public static final String ASSIGNED_SPLITS = "assignedSplits";
    public static final String UNASSIGNED_READERS = "unassignedReaders";

    private PubSubMetricNames() {}
}
