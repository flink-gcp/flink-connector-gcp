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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.StartPosition;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Verifies the subscriptions, then assigns subscription splits to reader subtasks.
 *
 * <p>Assignment is a pure function of the subscription list, the ordering mode and the current
 * parallelism (see {@link SplitAssignmentPlan}), which makes the enumerator almost stateless:
 * returned splits need no bookkeeping because re-registering the subtask recomputes exactly the
 * same assignment, and a restore recomputes the plan from the current parallelism rather than
 * replaying a stale one.
 *
 * <p>Before any of that, {@link #start()} runs a check against the Pub/Sub admin API — creating
 * subscriptions that are missing and authorised, rejecting ones the source cannot consume, and
 * applying the start position. It runs through {@link SplitEnumeratorContext#callAsync}, so it
 * never blocks the coordinator thread, and readers that register while it is in flight are parked
 * until it finishes. That fence is also what keeps a subscriber from attaching to a subscription
 * mid-seek.
 *
 * <p><b>Invariant: a checkpoint whose state has {@code startPositionApplied == false} contains no
 * reader holding a split.</b> Splits are assigned only from the completion handler, which sets the
 * flag in the same step, and Flink orders a coordinator snapshot before any assignment issued after
 * it. This matters because the fence does not cover a <em>restored</em> reader — those re-add their
 * splits from their own operator state before registering — so it is the invariant, not the fence,
 * that makes re-applying a start position after such a restore safe: nothing had been emitted.
 *
 * <p>The upstream connector instead mints one split per registered subtask, all pointing at a
 * single hard-coded subscription — which supports neither multiple subscriptions nor ordering,
 * since every subtask then opens its own streaming pull against the same subscription.
 */
@Internal
public class PubSubSplitEnumerator
        implements SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitEnumerator.class);

    static final String ASSIGNED_SPLITS = "assignedSplits";
    static final String UNASSIGNED_READERS = "unassignedReaders";

    private final SplitEnumeratorContext<SubscriptionSplit> context;
    private final PubSubSourceConfig<?> config;
    private final SubscriptionAdmin admin;
    @Nullable private final PubSubEnumeratorState restoredState;

    private SplitAssignmentPlan plan;

    /**
     * Subtasks currently holding splits, and subtasks currently registered with none.
     *
     * <p>Sets rather than counters, because {@code addReader} is not called once per subtask: after
     * a failover the coordinator drops the subtask from its registered readers and calls it again,
     * while {@code addSplitsBack} only returns the portion of the assignment no completed
     * checkpoint covers — often nothing. Counting deltas would therefore drift upward on every
     * failover and drive the derived unassigned-splits gauge negative. Re-registration is
     * idempotent here.
     */
    private final Set<Integer> subtasksWithSplits = new HashSet<>();

    private final Set<Integer> subtasksWithoutSplits = new HashSet<>();

    /**
     * Subtasks that registered before the startup check finished, drained when it does.
     *
     * <p>A set, not a list: a parked subtask that fails is unregistered and calls {@code addReader}
     * again when it comes back, and assigning it twice would send the assignment twice. Insertion
     * ordered so the drain is deterministic.
     */
    private final Set<Integer> pendingReaders = new LinkedHashSet<>();

    private boolean preflightComplete;

    private boolean startPositionApplied;

    /** Written by {@code close()} on the scheduler thread, read by the completion handler. */
    private volatile boolean closed;

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param admin used to verify, create and seek subscriptions; closed with the enumerator
     * @param restoredState the checkpointed state when restoring, or {@code null} on a fresh start
     */
    public PubSubSplitEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context,
            PubSubSourceConfig<?> config,
            SubscriptionAdmin admin,
            @Nullable PubSubEnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.admin = admin;
        this.restoredState = restoredState;
    }

    @Override
    public void start() {
        List<SubscriptionDestination> subscriptions = config.getSubscriptions();
        if (restoredState != null && !restoredState.getSubscriptions().equals(subscriptions)) {
            LOG.warn(
                    "The configured subscriptions {} differ from the checkpointed subscriptions {};"
                            + " the configured list takes effect. Messages already delivered on a"
                            + " removed subscription but not yet acknowledged stay on it.",
                    subscriptions,
                    restoredState.getSubscriptions());
        }
        plan =
                SplitAssignmentPlan.create(
                        subscriptions, config.getOrderingMode(), context.currentParallelism());
        registerMetrics();
        if (plan.idleSubtaskCount() > 0) {
            LOG.warn(
                    "Ordering mode {} assigns one split per subscription, so {} of the {} source"
                            + " subtasks receive no work ({} subscriptions). Lower the source"
                            + " parallelism to {} or consume more subscriptions.",
                    config.getOrderingMode(),
                    plan.idleSubtaskCount(),
                    context.currentParallelism(),
                    subscriptions.size(),
                    subscriptions.size());
        }

        startPositionApplied = restoredState != null && restoredState.isStartPositionApplied();
        StartPosition startPosition = config.getStartPosition();
        // Resolved here, on the coordinator thread, so LATEST is pinned before the check starts
        // rather than drifting with however long the admin calls take.
        Instant now = Instant.now();
        Instant seekTime =
                !startPositionApplied && startPosition.requiresSeek()
                        ? startPosition.resolveSeekTime(now)
                        : null;
        if (seekTime != null && restoredState != null) {
            LOG.warn(
                    "The restored checkpoint was taken before start position {} took effect, so it"
                            + " is applied again. No subtask held a split at that point, so nothing"
                            + " already emitted is affected — but LATEST resolves against the clock"
                            + " again, discarding whatever was published in the meantime.",
                    startPosition);
        }
        boolean backwardsSeek = seekTime != null && seekTime.isBefore(now);

        LOG.info(
                "Verifying {} Pub/Sub subscription(s) before assigning splits{}; readers that"
                        + " register meanwhile wait for the check to finish.",
                subscriptions.size(),
                seekTime == null ? "" : ", seeking them to " + seekTime);
        context.callAsync(() -> preflight(seekTime, backwardsSeek), this::onPreflightComplete);
    }

    /**
     * Verifies every subscription and applies the start position.
     *
     * <p>Runs on a worker thread, so it touches nothing the coordinator thread owns — its inputs
     * arrive as arguments and the immutable configuration, and its only effect on the enumerator is
     * whether it throws.
     */
    private Void preflight(@Nullable Instant seekTime, boolean backwardsSeek) throws IOException {
        for (SubscriptionDestination subscription : config.getSubscriptions()) {
            SubscriptionInfo info = admin.describe(subscription);
            if (info == null) {
                info = createSubscription(subscription);
            }
            verify(subscription, info, backwardsSeek);
            if (seekTime != null) {
                admin.seek(subscription, seekTime);
            }
        }
        return null;
    }

    /**
     * Creates a missing subscription, if these options authorise it, and reads back its settings.
     */
    private SubscriptionInfo createSubscription(SubscriptionDestination subscription)
            throws IOException {
        SubscriptionCreateOptions options = config.getCreateOptions().get(subscription);
        if (options == null) {
            throw new IOException(
                    "Pub/Sub subscription "
                            + subscription
                            + " does not exist. Create it, or pass creation settings to"
                            + " subscription(destination, SubscriptionCreateOptions) on the source"
                            + " builder to have the source create it.");
        }
        admin.create(subscription, options);
        // Read back rather than deriving the settings from the options: a concurrent creator may
        // have won the race, in which case its settings are the ones that apply.
        SubscriptionInfo info = admin.describe(subscription);
        if (info == null) {
            throw new IOException(
                    "Pub/Sub subscription "
                            + subscription
                            + " is still reported as absent immediately after creating it.");
        }
        return info;
    }

    /** Rejects the subscription settings the source cannot work with. */
    private void verify(
            SubscriptionDestination subscription, SubscriptionInfo info, boolean backwardsSeek)
            throws IOException {
        if (config.getOrderingMode() == OrderingMode.PER_KEY && !info.isMessageOrderingEnabled()) {
            throw new IOException(
                    "orderingMode(PER_KEY) requires a subscription created with message ordering"
                            + " enabled, but "
                            + subscription
                            + " has it disabled. Pub/Sub only preserves ordering-key order on"
                            + " ordering-enabled subscriptions, and the setting is fixed at"
                            + " creation — recreate the subscription with message ordering, or use"
                            + " orderingMode(NONE).");
        }
        if (info.isExactlyOnceDeliveryEnabled()) {
            throw new IOException(
                    "Pub/Sub subscription "
                            + subscription
                            + " has exactly-once delivery enabled, which this source cannot"
                            + " consume: acknowledgement ids are invalidated on redelivery and"
                            + " expire with the acknowledgement deadline, while the source holds"
                            + " them for a whole checkpoint interval before acknowledging. Disable"
                            + " exactly-once delivery on the subscription; the source is"
                            + " at-least-once by design.");
        }
        if (nacksWithoutFailingTheJob() && !info.isDeadLetterPolicyConfigured()) {
            throw new IOException(
                    "deserializationFailurePolicy("
                            + config.getDeserializationFailurePolicy()
                            + ") requires a dead-letter policy on "
                            + subscription
                            + ", which has none. Nacking returns the message without failing the"
                            + " job, so a message the schema can never convert is redelivered"
                            + " forever; a dead-letter policy is what ends that. Add one to the"
                            + " subscription, or use deserializationFailurePolicy(DROP) or"
                            + " deserializationFailurePolicy(FAIL).");
        }
        if (backwardsSeek
                && !info.isRetainAckedMessages()
                && !info.isTopicMessageRetentionConfigured()) {
            LOG.warn(
                    "Seeking {} backwards, but it neither retains acknowledged messages nor has a"
                            + " topic that retains messages, so only messages that were never"
                            + " acknowledged are replayed. Create the subscription with"
                            + " retainAckedMessages(true), or configure message retention on the"
                            + " topic, to replay messages a consumer already acknowledged.",
                    subscription);
        }
    }

    /**
     * Returns whether the configured failure policy nacks a message and lets the job carry on.
     *
     * <p>Deliberately a property, not an enum comparison spelled out at the call site: what makes a
     * nack dangerous is not the nack, it is the job surviving it, so the message comes back and
     * fails again forever. The reader also nacks when emitting a message downstream fails, and that
     * one needs no dead-letter policy behind it because it rethrows and the job fails visibly. Any
     * future policy that nacks and continues belongs here.
     */
    private boolean nacksWithoutFailingTheJob() {
        return config.getDeserializationFailurePolicy() == DeserializationFailurePolicy.NACK;
    }

    /**
     * Runs on the coordinator thread once the check finishes, so it needs no synchronization.
     * Throwing is how a split enumerator fails the job from an asynchronous call.
     */
    private void onPreflightComplete(@Nullable Void ignored, @Nullable Throwable error) {
        if (closed) {
            // The job is being torn down; failing it now would turn a clean cancellation into a
            // failure whose cause is our own shutdown.
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to prepare the Pub/Sub subscriptions "
                            + config.getSubscriptions()
                            + " for consumption; the source cannot start.",
                    error);
        }
        startPositionApplied = true;
        preflightComplete = true;
        LOG.info(
                "Pub/Sub subscriptions verified; assigning splits to {} waiting subtask(s).",
                pendingReaders.size());
        for (int subtaskId : pendingReaders) {
            assign(subtaskId);
        }
        pendingReaders.clear();
    }

    /**
     * Registers the enumerator gauges. Both are derived from which subtasks are registered rather
     * than accumulated, so they settle once every reader has registered and stay settled across
     * failovers; a value that stays below the plan's split count means a subtask never came back.
     */
    private void registerMetrics() {
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        metricGroup.gauge(ASSIGNED_SPLITS, (Gauge<Integer>) this::assignedSplitCount);
        metricGroup.gauge(UNASSIGNED_READERS, (Gauge<Integer>) subtasksWithoutSplits::size);
        metricGroup.setUnassignedSplitsGauge(
                () -> (long) (plan.splitCount() - assignedSplitCount()));
    }

    /** Counts the splits currently held by registered subtasks. */
    private int assignedSplitCount() {
        int assigned = 0;
        for (int subtaskId : subtasksWithSplits) {
            assigned += plan.splitsFor(subtaskId).size();
        }
        return assigned;
    }

    @Override
    public void addReader(int subtaskId) {
        if (!preflightComplete) {
            LOG.info(
                    "Source subtask {} registered before the Pub/Sub subscriptions were verified;"
                            + " it waits for the check to finish.",
                    subtaskId);
            pendingReaders.add(subtaskId);
            return;
        }
        assign(subtaskId);
    }

    /** Hands a subtask its splits, or finishes it when the plan gives it none. */
    private void assign(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // It failed while it was parked. Nothing to do: the plan is deterministic, so it is
            // handed exactly the same splits when it re-registers.
            LOG.info(
                    "Source subtask {} is no longer registered; skipping its assignment.",
                    subtaskId);
            return;
        }
        List<SubscriptionSplit> splits = plan.splitsFor(subtaskId);
        if (splits.isEmpty()) {
            subtasksWithoutSplits.add(subtaskId);
            // Nothing will ever arrive for this subtask, so let it finish instead of holding the
            // watermark back for the lifetime of the job.
            LOG.info(
                    "Source subtask {} has no subscription to consume; signalling no more splits.",
                    subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        LOG.info("Assigning splits {} to source subtask {}.", splits, subtaskId);
        subtasksWithSplits.add(subtaskId);
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    @Override
    public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
        // The subtask is being reset; it re-registers through addReader and is handed the same
        // splits again. Forget it either way — the returned list is only the uncheckpointed part
        // of its assignment, so its size says nothing about what the subtask still holds.
        subtasksWithSplits.remove(subtaskId);
        subtasksWithoutSplits.remove(subtaskId);
        // Splits carry no progress state and the assignment is deterministic, so the returned
        // splits need no bookkeeping: the restarted subtask re-registers through addReader and is
        // handed exactly the same splits again.
        LOG.info(
                "Source subtask {} returned splits {}; they will be reassigned when it"
                        + " re-registers.",
                subtaskId,
                splits);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Assignment is push-based and complete from addReader onwards; readers never need to ask.
    }

    @Override
    public PubSubEnumeratorState snapshotState(long checkpointId) {
        return new PubSubEnumeratorState(config.getSubscriptions(), startPositionApplied);
    }

    @Override
    public void close() throws IOException {
        // Runs on the scheduler thread, possibly while the check is still in flight; the flag is
        // volatile so the completion handler sees it and stays quiet.
        closed = true;
        try {
            admin.close();
        } catch (Exception e) {
            throw new IOException("Failed to close the Pub/Sub subscription admin", e);
        }
    }
}
