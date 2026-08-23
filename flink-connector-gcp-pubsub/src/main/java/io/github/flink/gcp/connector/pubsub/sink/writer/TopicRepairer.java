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
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Repairs one destination: creates its missing topic, resumes its paused ordering keys and
 * republishes its parked batch within the recovery budget, including the isolation pass that turns
 * a batched rejection into per-message verdicts.
 *
 * <p>The writer's mail path writes a destination's repair debt into its {@link DestinationState}; a
 * repair attempt here reads and clears it. Publishing and draining stay the writer's — they touch
 * the in-flight ledger and the parked-message gauge — so the repairer reaches them through the
 * {@link RepairContext} seam.
 *
 * <p>This class imports neither {@code CreateDisposition} nor {@code PubSubSinkConfig}, and that
 * absence is ADR-0006's invariant made structural: the disposition gates only the writer's {@code
 * NOT_FOUND} parking branch, so by the time a batch reaches a repair the only thing that decides a
 * creation is {@code topicMissing} — the repairer cannot consult the disposition even by accident.
 *
 * <p>Logs under the writer's category (ADR-0122, invariant 8): the constructor takes the writer's
 * logger, so an operator's log filter and the tests that pin the repair's INFO lines keep working.
 */
@Internal
final class TopicRepairer {

    /**
     * The writer-owned operations a repair needs: republishing a parked message, draining the
     * in-flight publishes, and releasing parked messages from the writer's gauge.
     */
    interface RepairContext {

        /**
         * Republishes a parked message to the destination's publisher, never counting it as a first
         * attempt. {@code soloVerdict} says the message travels as its own single-message request —
         * true only inside the isolation pass.
         */
        void republish(DestinationState state, PubsubMessage message, boolean soloVerdict)
                throws IOException;

        /**
         * Runs mailbox mails until no publish is in flight, surfacing any captured publish failure.
         */
        void drainInFlight() throws IOException, InterruptedException;

        /** Releases {@code count} messages from the writer's parked-message gauge. */
        void releaseParked(int count);
    }

    private final TopicAdmin topicAdmin;
    @Nullable private final TopicCreateOptions topicCreateOptions;
    private final RetrySchedule recoverySchedule;
    private final PubSubWriterMetrics metrics;
    private final boolean orderingEnabled;
    private final Logger log;
    private final RepairContext context;

    /**
     * Creates the repairer.
     *
     * @param topicAdmin admin used to create missing topics
     * @param topicCreateOptions settings applied to topics the repairer creates, or {@code null}
     *     for service defaults
     * @param recoverySchedule the backoff budget covering topic-metadata propagation
     * @param metrics the writer's metrics, for {@code topicsCreated}
     * @param orderingEnabled whether message ordering is enabled, which is what makes paused
     *     ordering keys exist at all
     * @param log the writer's logger, so the repair's INFO lines stay under the category an
     *     operator's filters and the tests already use
     * @param context the writer-owned operations a repair needs
     */
    TopicRepairer(
            TopicAdmin topicAdmin,
            @Nullable TopicCreateOptions topicCreateOptions,
            RetrySchedule recoverySchedule,
            PubSubWriterMetrics metrics,
            boolean orderingEnabled,
            Logger log,
            RepairContext context) {
        this.topicAdmin = topicAdmin;
        this.topicCreateOptions = topicCreateOptions;
        this.recoverySchedule = recoverySchedule;
        this.metrics = metrics;
        this.orderingEnabled = orderingEnabled;
        this.log = log;
        this.context = context;
    }

    /**
     * Republishes the destination's parked messages — creating its topic first when that is what
     * they are parked for — retrying within the recovery schedule while topic metadata propagates.
     * Each attempt drains the writer completely (repair is rare, so waiting on unrelated
     * destinations' publishes is acceptable for the simplicity of reusing one drain); an isolating
     * attempt drains once per message, so its pass publishes strictly one message at a time.
     * Failures during a retry re-enter the pending buffer through the normal callback path;
     * non-{@code NOT_FOUND} failures abort the repair from within the drain.
     *
     * <p>Only a parked {@code NOT_FOUND} creates a topic. Every other reason a batch is here — a
     * cascade of a message the failure handler dropped, a request-level {@code INVALID_ARGUMENT}
     * awaiting isolation, or a publish that reached an ordering key still paused from one — needs
     * the resume and the republish and nothing else, and issuing a {@code createTopic} for them
     * would both misreport {@code topicsCreated} and create a topic under {@code CREATE_NEVER}.
     *
     * <p>An attempt whose batch was parked for a request-level {@code INVALID_ARGUMENT} runs as an
     * <b>isolation pass</b>: each message goes out as its own single-message request, flushed and
     * drained individually, so the service answers per message. A message rejected solo is routed
     * to the failure handler by its own drain, and the ordering key that rejection paused is
     * resumed before the key's next message is republished — so one pass drains a long run of
     * invalid messages in a single attempt, and the budget keeps bounding <em>unproductive</em>
     * retrying rather than the length of a poisoned key (#269). How long a run a pass will drain is
     * bounded by {@code maxConsecutiveRejections} (#361), not by this budget: past it the drain's
     * own {@code checkAsyncError} aborts the repair. Per-key order holds because the batch is in
     * publish-sequence order and nothing else publishes during a repair.
     *
     * <p>A fatal solo failure surfaces from the pass's drain and aborts the repair with the
     * not-yet-republished remainder abandoned — in neither the pending buffer nor in flight. That
     * is safe for the same reason {@link PubSubWriter#close}'s parked-message drop is: the
     * checkpoint does not complete, so the restart replays those records.
     */
    void repair(DestinationState state) throws IOException, InterruptedException {
        // Creation is checked per attempt, not once up front: a batch parked for another reason
        // can turn out to need it, when its republish is the publish that first meets the missing
        // topic. At most once per repair all the same — the retry loop exists for topic metadata
        // propagating to the publisher, where the topic already exists and creating it again would
        // answer nothing.
        boolean topicCreated = false;
        state.routedDuringRepair = 0;
        for (int attempt = 1; ; attempt++) {
            if (state.topicMissing) {
                state.topicMissing = false;
                if (!topicCreated) {
                    topicCreated = true;
                    log.info(
                            "A publish to Pub/Sub topic {} failed because the topic does not exist;"
                                    + " creating it (CREATE_IF_NEEDED).",
                            state.destination);
                    topicAdmin.createTopic(state.destination, topicCreateOptions);
                    metrics.topicCreated();
                }
            }
            // Isolation is decided per attempt for the same reason creation is: a batch parked
            // for a NOT_FOUND can meet a request-level INVALID_ARGUMENT on its republish, and
            // only the attempt after that report can know to isolate.
            boolean isolating = state.isolationNeeded;
            state.isolationNeeded = false;
            // Keyed by publish sequence, so the batch is in the order the messages were originally
            // published however their failure mails interleaved.
            List<PubsubMessage> batch = new ArrayList<>(state.pendingRetries.values());
            state.pendingRetries.clear();
            // Every attempt resumes the batch's ordering keys first: the failure that parked the
            // batch — and every failed republish attempt since — paused them in the publisher.
            resumeOrderingKeys(state, batch);
            // Republishes are not a first attempt in either shape: these records were counted by
            // the write that admitted them.
            if (isolating) {
                for (PubsubMessage message : batch) {
                    // Released one at a time, not up front: the pass holds the rest of the batch
                    // through a drain per message, and the gauge must keep reporting what the
                    // writer still holds — a pass over a long batch is exactly when a reader
                    // watches it.
                    context.releaseParked(1);
                    context.republish(state, message, true);
                    state.publisher.flushOutstanding();
                    context.drainInFlight();
                    // A rejection routed by that drain paused its ordering key and registered it;
                    // hand the key back before its next message, or the rest of the pass comes
                    // back cancelled and every drop costs one budget attempt.
                    resumeRegisteredKeys(state);
                }
            } else {
                context.releaseParked(batch.size());
                for (PubsubMessage message : batch) {
                    context.republish(state, message, false);
                }
                state.publisher.flushOutstanding();
                context.drainInFlight();
            }
            if (state.pendingRetries.isEmpty()) {
                // The incident is over, so its cause must not outlive it: a cascade only fills
                // repairCause in when it is still null, so a value left behind here would be
                // reported as the cause of some later destination-level failure it had nothing to
                // do with. A dropped message provokes a repair of its own, so incidents on one
                // destination are not rare enough to leave that to chance.
                state.repairCause = null;
                return;
            }
            if (attempt >= recoverySchedule.maxAttempts()) {
                if (state.routedDuringRepair > 0) {
                    // Distinguished from the topic-shaped exhaustion below: this repair was
                    // draining a key whose messages the handler is dropping, and the budget ran
                    // out with messages still parked — the reader needs to know drops happened
                    // and roughly how many, not only to go looking for a topic problem. The two
                    // facts are not exclusive, so a creation is still reported here.
                    throw new IOException(
                            "Republishing to Pub/Sub topic "
                                    + state.destination
                                    + " could not drain its parked messages within the recovery"
                                    + " budget ("
                                    + attempt
                                    + " attempt(s)"
                                    + (topicCreated ? ", after creating the topic" : "")
                                    + "); "
                                    + state.routedDuringRepair
                                    + " message(s) were handed to the failure handler during the"
                                    + " repair.",
                            state.repairCause);
                }
                throw new IOException(
                        "Republishing to Pub/Sub topic "
                                + state.destination
                                + (topicCreated
                                        ? " kept failing after creating the topic ("
                                        : " kept failing (")
                                + attempt
                                + " attempt(s)).",
                        state.repairCause);
            }
            long backoffMs = recoverySchedule.backoffMs(attempt);
            log.info(
                    "Republishing to Pub/Sub topic {} still fails; backing off {} ms"
                            + " (attempt {}/{}).",
                    state.destination,
                    backoffMs,
                    attempt,
                    recoverySchedule.maxAttempts());
            Thread.sleep(backoffMs);
        }
    }

    /**
     * Resumes the distinct ordering keys of the batch, plus those of the messages the failure
     * handler dropped, on the destination's publisher.
     *
     * <p>Keys are resumed only from within a repair — here at an attempt's start, and through
     * {@link #resumeRegisteredKeys} between an isolation pass's publishes — and deliberately never
     * from {@code PubSubWriter#routeFailedMessage}. {@link PubSubWriter#write} tests {@code
     * repairNeeded} <em>before</em> {@link InFlightTracker#awaitCapacity()}, and mailbox mails —
     * the drop among them — run inside it, so a key resumed from the failure mail could be
     * published to by the rest of that same {@code write} while the key's cascades were still
     * parked: a newer message ahead of older ones, the one thing the repair exists to prevent. Left
     * paused, that racing publish comes back cancelled, is parked, and is republished in
     * publish-sequence order with the rest. The isolation pass can resume mid-batch without opening
     * that race, because the key's remaining messages are held by the pass itself in sequence order
     * and the mails its drains run only complete publishes, never issue one.
     *
     * <p>Dropped keys are drained here rather than re-resumed on every attempt: a later attempt
     * only re-pauses keys the batch republished, which the batch itself covers, and a dropped key
     * with nothing left to republish cannot be paused again by this repair.
     *
     * <p>{@code resumePublish} is a no-op for a key that is not paused, and rejects a shut-down
     * publisher — unreachable here, since a repair runs only from {@link PubSubWriter#write} or
     * {@link PubSubWriter#flush}.
     */
    private void resumeOrderingKeys(DestinationState state, List<PubsubMessage> batch) {
        if (!orderingEnabled) {
            return;
        }
        for (PubsubMessage message : batch) {
            if (!message.getOrderingKey().isEmpty()) {
                state.keysToResume.add(message.getOrderingKey());
            }
        }
        resumeRegisteredKeys(state);
    }

    /**
     * Resumes and drains the registered keys, without adding a batch's — the mid-pass complement of
     * {@link #resumeOrderingKeys} (which delegates here), run by the isolation pass after each solo
     * verdict so a key a drop just paused is handed back before the key's next republish.
     */
    private void resumeRegisteredKeys(DestinationState state) {
        if (!orderingEnabled) {
            return;
        }
        for (String orderingKey : state.keysToResume) {
            state.publisher.resumePublish(orderingKey);
        }
        state.keysToResume.clear();
    }
}
