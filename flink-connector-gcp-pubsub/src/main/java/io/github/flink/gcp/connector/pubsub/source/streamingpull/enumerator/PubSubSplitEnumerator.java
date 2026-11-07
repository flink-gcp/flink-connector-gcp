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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.base.source.EnumeratorMetrics;
import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.PubSubStartPosition;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerChunkEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerUploadedEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    /**
     * How long a restore waits, from the first reader that registers, for the other restored
     * readers to upload their deduplication ledger chunks before assigning splits without the
     * missing ones. Readers upload as soon as they start, so the wait normally ends within seconds;
     * the bound only keeps a reader that never comes back from holding the restored readers idle.
     * Counting from a registration rather than from the enumerator's start keeps slow deployment or
     * state restore out of it. A reader whose chunks miss it only costs their duplicates.
     */
    @VisibleForTesting static final Duration LEDGER_UPLOAD_TIMEOUT = Duration.ofMinutes(2);

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

    private boolean startupCheckComplete;

    /**
     * The restored subtasks whose deduplication ledger upload is still awaited (#2049); split
     * assignment waits while it is non-empty. A restore of the whole source awaits every subtask. A
     * failover that keeps this enumerator, such as the default scheduler's region failover, adds
     * each subtask it resets through {@link #addSplitsBack(List, int)}, so a region spanning every
     * source subtask is deduplicated like a whole restore. Emptied as subtasks report, or when
     * {@code LEDGER_UPLOAD_TIMEOUT} passes.
     */
    private final Set<Integer> awaitedUploads = new TreeSet<>();

    /** Uploaded chunks by split id; keyed so a re-upload after a reader failover is a no-op. */
    private final Map<String, SubscriptionSplit> uploadedChunks = new LinkedHashMap<>();

    /**
     * Counts ledger waits, so that a timeout armed for one wait cannot end a later one. A timeout
     * is armed once per wait, by the first registration while it is open.
     */
    private int ledgerWaitGeneration;

    private boolean ledgerTimeoutArmed;

    /**
     * The chunks to forward with each assignment, by subscription, once the wait ends. Dropped
     * after the readers waiting at that point are assigned: a reader that registers later was not
     * part of the wait, and a subtask that kept running is not sent a restored ledger.
     */
    @Nullable private Map<SubscriptionDestination, List<SubscriptionSplit>> ledgerToForward;

    /**
     * Every chunk forwarded since the last wait ended, by split id, kept until a checkpoint
     * completes after it. Until then a failover of one subtask restores the same checkpoint, but
     * the chunks that subtask was forwarded may have come from another subtask's state, so the
     * reset subtask is sent them again. A checkpoint completing afterwards replaces the restore
     * point, and its own ledger is what a later reset restores.
     */
    private final Map<String, SubscriptionSplit> forwardedChunks = new LinkedHashMap<>();

    private boolean startPositionApplied;

    /** Written by {@code close()} on the scheduler thread, read by the completion handler. */
    private volatile boolean closed;

    /**
     * Replaced when the metrics register; volatile because {@link #close()} runs on another thread.
     */
    private volatile EnumeratorMetrics metrics = EnumeratorMetrics.unregistered();

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

        if (deduplicates() && restoredState != null) {
            for (int subtaskId = 0; subtaskId < context.currentParallelism(); subtaskId++) {
                awaitedUploads.add(subtaskId);
            }
            ledgerWaitGeneration++;
            LOG.info(
                    "Restoring with restore deduplication on: splits are assigned once all {}"
                            + " reader(s) have uploaded their deduplication ledger, or {} after"
                            + " the first registers.",
                    context.currentParallelism(),
                    LEDGER_UPLOAD_TIMEOUT);
        }

        startPositionApplied = restoredState != null && restoredState.isStartPositionApplied();
        PubSubStartPosition startPosition = config.getStartPosition();
        // Resolved here, on the coordinator thread, so LATEST is pinned before the check starts
        // rather than drifting with however long the admin calls take.
        Instant now = Instant.now();
        Instant seekTime =
                !startPositionApplied && startPosition.requiresSeek()
                        ? seekTimeFor(startPosition, now)
                        : null;
        if (seekTime != null && restoredState != null) {
            LOG.warn(
                    "The restored checkpoint was taken before start position {} took effect, so it"
                            + " is applied again. No subtask held a split at that point, so nothing"
                            + " already emitted is affected{}",
                    startPosition,
                    startPosition.getMode() == PubSubStartPosition.Mode.LATEST
                            // The only mode whose second application is not the first one repeated.
                            ? ", but LATEST resolves against the clock again, discarding whatever"
                                    + " was published in the meantime."
                            : ", and this position resolves to the same instant as before.");
        }
        boolean backwardsSeek = seekTime != null && seekTime.isBefore(now);

        LOG.info(
                "Verifying {} Pub/Sub subscription(s) before assigning splits{}; readers that"
                        + " register meanwhile wait for the check to finish.",
                subscriptions.size(),
                seekTime == null ? "" : ", seeking them to " + seekTime);
        context.callAsync(
                () -> runStartupCheck(seekTime, backwardsSeek), this::onStartupCheckComplete);
    }

    /**
     * Verifies every subscription and applies the start position.
     *
     * <p>Runs on a worker thread, so it touches nothing the coordinator thread owns — its inputs
     * arrive as arguments and the immutable configuration, and its only effect on the enumerator is
     * whether it throws.
     */
    private Void runStartupCheck(@Nullable Instant seekTime, boolean backwardsSeek)
            throws IOException {
        // Nothing is created or sought until every subscription that already exists has passed.
        // The rejections are deterministic, so a rejection reached part-way through a
        // describe-create-verify loop would leave the job crash-looping over an orphan
        // subscription — one bound to its topic and accumulating a full copy of the stream — or
        // over subscriptions it had already rewound. Which subscriptions those are is only knowable
        // here: whether an existing one has exactly-once delivery, or the ordering the mode needs,
        // is what `GetSubscription` is for, and the builder cannot see it.
        Set<SubscriptionDestination> existing = new HashSet<>();
        for (SubscriptionDestination subscription : config.getSubscriptions()) {
            SubscriptionInfo info = admin.describe(subscription);
            if (info != null) {
                verify(subscription, info, backwardsSeek);
                existing.add(subscription);
            }
        }
        for (SubscriptionDestination subscription : config.getSubscriptions()) {
            if (!existing.contains(subscription)) {
                // Verified too, cheaply: the builder rejects settings the check would refuse, so
                // this only fires when a concurrent creator won the race with different ones.
                verify(subscription, createSubscription(subscription), backwardsSeek);
            }
        }
        if (seekTime != null) {
            for (SubscriptionDestination subscription : config.getSubscriptions()) {
                admin.seek(subscription, seekTime);
            }
        }
        return null;
    }

    /** Creates a missing subscription, if settings were supplied authorising that. */
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
        return admin.create(subscription, options);
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
        if (config.getDeserializationFailurePolicy().requiresDeadLetterPolicy()
                && !info.isDeadLetterPolicyConfigured()) {
            throw new IOException(
                    // The constant name, not toString(): this sentence spells builder calls, while
                    // toString() is the enum's DDL spelling for the table layer.
                    "deserializationFailurePolicy("
                            + config.getDeserializationFailurePolicy().name()
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
     * Resolves the instant a start position seeks to.
     *
     * <p>{@link PubSubStartPosition.Mode#EARLIEST_RETAINED} resolves to the epoch: Pub/Sub
     * documents a seek target older than the retention window as marking every <em>retained</em>
     * message unacknowledged, which is exactly "as far back as this subscription goes" without
     * having to ask how far that is.
     *
     * @param startPosition a position that requires a seek
     * @param now the moment the check started, which {@link PubSubStartPosition.Mode#LATEST}
     *     resolves against
     */
    @VisibleForTesting
    static Instant seekTimeFor(PubSubStartPosition startPosition, Instant now) {
        switch (startPosition.getMode()) {
            case EARLIEST_RETAINED:
                return Instant.EPOCH;
            case LATEST:
                return now;
            case TIMESTAMP:
                return startPosition.getTimestamp();
            default:
                throw new IllegalStateException(
                        "Start position " + startPosition + " requires no seek.");
        }
    }

    /**
     * Runs on the coordinator thread once the check finishes, so it needs no synchronization.
     * Throwing is how a split enumerator fails the job from an asynchronous call.
     */
    private void onStartupCheckComplete(@Nullable Void ignored, @Nullable Throwable error) {
        if (closed) {
            // The job is being torn down; failing it now would turn a clean cancellation into a
            // failure whose cause is our own shutdown.
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to verify the Pub/Sub subscriptions "
                            + config.getSubscriptions()
                            + " for consumption; the source cannot start.",
                    error);
        }
        startPositionApplied = true;
        startupCheckComplete = true;
        if (!awaitedUploads.isEmpty()) {
            LOG.info(
                    "Pub/Sub subscriptions verified; {} waiting subtask(s) also wait for the"
                            + " deduplication ledger.",
                    pendingReaders.size());
            return;
        }
        LOG.info(
                "Pub/Sub subscriptions verified; assigning splits to {} waiting subtask(s).",
                pendingReaders.size());
        assignPendingReaders();
    }

    /** Assigns every parked subtask, once both the startup check and the ledger wait are done. */
    private void assignPendingReaders() {
        if (!startupCheckComplete || !awaitedUploads.isEmpty()) {
            return;
        }
        for (int subtaskId : pendingReaders) {
            assign(subtaskId);
        }
        pendingReaders.clear();
        ledgerToForward = null;
    }

    private boolean deduplicates() {
        return config.getRestoreDeduplicationTtl() != null;
    }

    /** Arms the timeout of the open wait, once, when its first reader registers. */
    private void armLedgerTimeout() {
        if (ledgerTimeoutArmed) {
            return;
        }
        ledgerTimeoutArmed = true;
        int generation = ledgerWaitGeneration;
        long timeoutMillis = LEDGER_UPLOAD_TIMEOUT.toMillis();
        // callAsync only offers a periodic schedule; an effectively infinite period makes it
        // one-shot, and the generation check keeps a late run from ending a later wait.
        context.callAsync(
                () -> null,
                (ignored, error) -> onLedgerUploadTimeout(generation),
                timeoutMillis,
                Long.MAX_VALUE);
    }

    private void onLedgerUploadTimeout(int generation) {
        if (closed || generation != ledgerWaitGeneration || awaitedUploads.isEmpty()) {
            return;
        }
        LOG.warn(
                "Source subtask(s) {} did not upload their deduplication ledger within {};"
                        + " assigning splits without it. Messages their chunks covered may be"
                        + " emitted again.",
                awaitedUploads,
                LEDGER_UPLOAD_TIMEOUT);
        endLedgerWait();
    }

    /** Groups the uploaded chunks by subscription and releases the parked assignments. */
    private void endLedgerWait() {
        awaitedUploads.clear();
        ledgerTimeoutArmed = false;
        for (SubscriptionSplit chunk : uploadedChunks.values()) {
            forwardedChunks.putIfAbsent(chunk.splitId(), chunk);
        }
        uploadedChunks.clear();
        Map<SubscriptionDestination, List<SubscriptionSplit>> bySubscription =
                new LinkedHashMap<>();
        for (SubscriptionSplit chunk : forwardedChunks.values()) {
            bySubscription
                    .computeIfAbsent(chunk.getSubscription(), key -> new ArrayList<>())
                    .add(chunk);
        }
        ledgerToForward = bySubscription;
        LOG.info(
                "Deduplication ledger collected: {} chunk(s) across {} subscription(s).",
                bySubscription.values().stream().mapToInt(List::size).sum(),
                bySubscription.size());
        assignPendingReaders();
    }

    /**
     * Registers the enumerator gauges. Both are derived from which subtasks are registered rather
     * than accumulated, so they settle once every reader has registered and stay settled across
     * failovers; a value that stays below the plan's split count means a subtask never came back.
     */
    private void registerMetrics() {
        metrics = EnumeratorMetrics.of(context.metricGroup());
        metrics.gauge(PubSubMetricNames.ASSIGNED_SPLITS, (Gauge<Integer>) this::assignedSplitCount);
        metrics.gauge(
                PubSubMetricNames.UNASSIGNED_READERS, (Gauge<Integer>) subtasksWithoutSplits::size);
        metrics.unassignedSplitsGauge(() -> (long) (plan.splitCount() - assignedSplitCount()));
        if (closed) {
            // close() ran first, on Flink's closer thread, and closed the view this replaced: let
            // go
            // of what was just bound rather than leave it reading a closed enumerator.
            metrics.close();
        }
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
        if (!awaitedUploads.isEmpty()) {
            armLedgerTimeout();
            LOG.info(
                    "Source subtask {} registered while the deduplication ledger is collected; it"
                            + " waits for the upload to finish.",
                    subtaskId);
            pendingReaders.add(subtaskId);
            return;
        }
        if (!startupCheckComplete) {
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
        forwardLedger(subtaskId, splits);
        LOG.info("Assigning splits {} to source subtask {}.", splits, subtaskId);
        subtasksWithSplits.add(subtaskId);
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    /**
     * Sends a subtask the ledger chunks of every subscription it is about to consume. Under {@link
     * OrderingMode#NONE} several subtasks consume one subscription and Pub/Sub may redeliver to any
     * of them, so each receives that subscription's whole ledger. The chunks precede the assignment
     * on the same ordered gateway, so the ledger is in place before any subscriber starts.
     */
    private void forwardLedger(int subtaskId, List<SubscriptionSplit> splits) {
        if (ledgerToForward == null || ledgerToForward.isEmpty()) {
            return;
        }
        Set<SubscriptionDestination> consumed = new LinkedHashSet<>();
        for (SubscriptionSplit split : splits) {
            consumed.add(split.getSubscription());
        }
        int sent = 0;
        for (SubscriptionDestination subscription : consumed) {
            for (SubscriptionSplit chunk :
                    ledgerToForward.getOrDefault(subscription, Collections.emptyList())) {
                context.sendEventToSourceReader(subtaskId, new LedgerChunkEvent(chunk));
                sent++;
            }
        }
        if (sent > 0) {
            LOG.info(
                    "Forwarded {} deduplication ledger chunk(s) to source subtask {}.",
                    sent,
                    subtaskId);
        }
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
        if (deduplicates()) {
            // The subtask restores the checkpoint, ledger chunks included, while this enumerator
            // keeps running; wait for its upload as a whole restore would.
            if (awaitedUploads.isEmpty()) {
                ledgerWaitGeneration++;
                ledgerTimeoutArmed = false;
            }
            awaitedUploads.add(subtaskId);
        }
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
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof LedgerChunkEvent) {
            if (awaitedUploads.contains(subtaskId)) {
                SubscriptionSplit chunk = ((LedgerChunkEvent) sourceEvent).chunk();
                uploadedChunks.putIfAbsent(chunk.splitId(), chunk);
            }
            // Otherwise the wait already ended without this subtask, and its chunks reach no one;
            // that costs their duplicates only.
            return;
        }
        if (sourceEvent instanceof LedgerUploadedEvent) {
            if (awaitedUploads.remove(subtaskId) && awaitedUploads.isEmpty()) {
                endLedgerWait();
            }
            return;
        }
        if (!(sourceEvent instanceof SubscriberBufferLimitExceededEvent)) {
            SplitEnumerator.super.handleSourceEvent(subtaskId, sourceEvent);
            return;
        }
        SubscriberBufferLimitExceededEvent exceeded =
                (SubscriberBufferLimitExceededEvent) sourceEvent;
        throw new FlinkRuntimeException(
                "Pub/Sub source reader subtask "
                        + subtaskId
                        + " could not admit a delivery for split "
                        + exceeded.splitId()
                        + ": the attempted aggregate subscriber buffer was "
                        + exceeded.attemptedMessages()
                        + " messages and "
                        + exceeded.attemptedBytes()
                        + " bytes, past the reader's budget of "
                        + exceeded.maxMessages()
                        + " messages or "
                        + exceeded.maxBytes()
                        + " bytes (subscriberBufferMaxMessages / subscriberBufferMaxBytes; Table"
                        + " API: 'scan.subscriber-buffer.max-messages' and"
                        + " 'scan.subscriber-buffer.max-bytes'). Left unset, the budget is twice"
                        + " the flow-control limits, or the paused-split bounds where larger, for"
                        + " every subscriber the reader holds, and is"
                        + " crossed only when downstream stops consuming for longer than"
                        + " maxAckExtensionPeriod, after which the client lets leases lapse and"
                        + " keeps receiving; remove that stall. An explicit budget below that"
                        + " share is crossed by ordinary backpressure: leave it unset, or lower"
                        + " the flow-control limits to hold less in memory.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (awaitedUploads.isEmpty()) {
            // A completed checkpoint is the restore point from now on; it carries its own ledger.
            forwardedChunks.clear();
        }
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
        metrics.close();
        try {
            admin.close();
        } catch (Exception e) {
            throw new IOException("Failed to close the Pub/Sub subscription admin", e);
        }
    }
}
