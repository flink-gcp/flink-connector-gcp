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

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Per-topic publisher plus the destination's repair and completion state.
 *
 * <p>The fields are deliberately package-visible within this one package: {@link PubSubWriter}'s
 * mail path writes the repair debt — parking messages, marking the topic missing, registering keys
 * to resume — while a {@link TopicRepairer} repair attempt reads and clears it, so both sides touch
 * the fields directly as {@code state.field}.
 */
@Internal
final class DestinationState {

    final TopicDestination destination;
    final TopicPublisher publisher;

    /**
     * Messages awaiting republish — parked for a missing topic, a cascade cancellation, or a
     * request-level rejection awaiting isolation — keyed by publish sequence so the batch is
     * republished in publish order. Sorting matters because the failure mails that park them do not
     * arrive in publish order, and republishing a key's messages out of order would break the very
     * guarantee the repair exists to preserve.
     */
    final SortedMap<Long, PubsubMessage> pendingRetries = new TreeMap<>();

    /**
     * Ordering keys of messages the failure handler dropped, which the publisher paused and will
     * never resume on its own. Drained by the next repair attempt, and mid-pass by the isolation
     * pass after each drop. Separate from {@link #pendingRetries} because the dropped message
     * itself is gone: the key needs handing back even when there is nothing left to republish for
     * it.
     */
    final Set<String> keysToResume = new LinkedHashSet<>();

    /**
     * Whether a {@code NOT_FOUND} is among the reasons this destination owes a repair — the only
     * one that calls for creating the topic. Cleared by the repair that answers it.
     */
    boolean topicMissing;

    /**
     * Whether a request-level {@code INVALID_ARGUMENT} is among the reasons this destination owes a
     * repair — the only one that calls for republishing the batch one message per request. Cleared
     * by the attempt that answers it, like {@link #topicMissing}, and re-set by any later batched
     * rejection.
     */
    boolean isolationNeeded;

    /**
     * Messages the current repair handed to the failure handler; zeroed when a repair starts, read
     * at budget exhaustion to choose between its two messages. Per destination rather than per
     * writer because that is the delta the exhaustion reports — and routing can only happen from
     * the isolation pass of the destination being repaired, since a solo verdict exists nowhere
     * else.
     */
    long routedDuringRepair;

    /**
     * Retained as the cause of a budget-exhaustion failure: the destination's {@code NOT_FOUND}, or
     * — when none was ever observed — the batched {@code INVALID_ARGUMENT} or cascade cancellation
     * that parked the batch, whichever came first.
     */
    Throwable repairCause;

    final String completionDescription;
    final String failureDescription;

    /**
     * The destination's optional per-destination counters, resolved once here rather than per
     * record — the topic's resource name is composed by the lookup.
     */
    final DestinationMetrics.Counters metrics;

    DestinationState(
            TopicDestination destination,
            TopicPublisher publisher,
            DestinationMetrics.Counters metrics) {
        this.destination = destination;
        this.publisher = publisher;
        this.completionDescription = "Complete a Pub/Sub publish to " + destination;
        this.failureDescription = "Fail a Pub/Sub publish to " + destination;
        this.metrics = metrics;
    }
}
