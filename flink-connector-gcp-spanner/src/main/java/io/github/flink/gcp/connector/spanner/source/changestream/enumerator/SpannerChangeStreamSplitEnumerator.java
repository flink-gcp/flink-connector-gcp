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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.metrics.MetricValues;
import io.github.flink.gcp.connector.base.source.EnumeratorMetrics;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.base.source.StartPositionResolver.RestoreExpiry;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedAckEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamInitializationEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarkEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Coordinates the checkpointed parent-child lifecycle of Spanner Change Streams partitions.
 *
 * <h2>The compact lineage is the only record</h2>
 *
 * <p>Spanner's change stream hands out partitions as a lineage: reading one partition discovers its
 * children, and a child may only be read once <em>every</em> parent that names it has finished.
 * Beam keeps that lineage in a metadata table in the user's database. This connector keeps it in
 * {@code ledger} and checkpoints it, so the source needs no table of its own and no write
 * permission. A lost checkpoint is a lost lineage because there is no second copy to reconcile
 * against.
 *
 * <p>An entry moves {@code CREATED} → {@code SCHEDULED} → {@code RUNNING} → {@code FINISHED}.
 * {@code CREATED} is the state that carries the dependency: a discovered child sits there until
 * {@link #allParentsFinished} holds, which is why {@code childrenByParent} exists and why finishing
 * a partition is the event that promotes children rather than discovering them. A finished split
 * leaves the ledger once its finish is final (below). Its id remains in {@code
 * finishedParentProofs} only while a created child still needs that durable fact after restore.
 *
 * <h2>A finish is final only once a completed checkpoint covers it</h2>
 *
 * <p>Region failover restarts the failed tasks from the last completed checkpoint but keeps this
 * coordinator, and Flink returns the splits it assigned since that checkpoint through {@link
 * #addSplitsBack}. Every finished partition therefore stays in the ledger as a pending {@code
 * FINISHED} entry in {@code pendingFinishes} until a checkpoint covering the finish completes, as a
 * Debezium partition is {@code FINISHED} only once its offsets are committed. A returned split
 * restarts where Flink assigned it, because its downstream rolled back with it, and a returned or
 * restored pending finish becomes unfinished again; {@link #reconcileAfterRollback} then sends its
 * scheduled children back to waiting. A snapshot leaves pending finishes out: the checkpoint it
 * belongs to already covers them.
 *
 * <p><b>Child discovery does not finish a parent.</b> A reader forwards child-partitions records as
 * it meets them and sends a separate completion event only when its query ends successfully, so a
 * parent that failed part-way through cannot leave its children schedulable.
 *
 * <p><b>Every finish is acknowledged, and applying one is idempotent.</b> A reader keeps a finished
 * split, with the child reports it forwarded, in its checkpointed state until the {@link
 * PartitionFinishedAckEvent} arrives, and a restored reader sends both again. A restore from a
 * checkpoint whose coordinator snapshot missed a finish therefore still receives it. A finish for a
 * partition this ledger no longer holds is acknowledged and otherwise ignored, and so is a child
 * report for one, including a child report for a compacted parent whose child has since finished
 * too; a pending finish stays in the ledger, so a child whose finish is not final is found there.
 * Only finishes are acknowledged; a reader sends its child reports before the finish and, after a
 * restore, again before the repeated finish.
 *
 * <h2>A child waits for a checkpoint, not only for its parents</h2>
 *
 * <p>A parent's finish is its reader's report that the query ended, and the parent's last records
 * can still be on their way downstream at that moment. If another subtask reads the child, the
 * child's records take a different path and can reach a keyed operator first, so one key's changes
 * arrive out of commit order (#2193). With {@code holdChildrenUntilCheckpoint}, a finished parent
 * stays in {@code heldParents} until a checkpoint whose {@link #snapshotState} ran after the finish
 * has completed. The reader emitted those records before it reported the finish, so they precede
 * its barrier for that checkpoint, and a completed checkpoint means downstream processed them.
 * Flink CDC's MySQL source waits the same way between its snapshot and binlog phases.
 *
 * <p>The hold is runtime state only: a restored checkpoint has completed, so every finish it
 * records is already released. Nothing is held at parallelism 1, where every record takes one path,
 * or after a parent whose reader emitted no record, such as the initial query, since nothing of it
 * is downstream to overtake. The coordinator cannot read the job's checkpoint configuration, so it
 * measures a job without checkpointing: a child held for {@code firstCheckpointTimeoutNanos} before
 * any checkpoint completed releases every hold with a warning, and later finishes are not held
 * until one does. The evidence is a completed checkpoint rather than a {@link #snapshotState} call:
 * Flink snapshots the coordinator for every savepoint but reports completion only of a checkpoint
 * or a synchronous savepoint, so a savepoint taken while the job runs must not end the measurement.
 *
 * <h2>The watermark frontier</h2>
 *
 * <p>The source-wide watermark is the minimum watermark across every ledger entry, a pending finish
 * included, and it is the enumerator's rather than each reader's because a partition that is
 * scheduled but not yet assigned belongs to nobody — a per-reader minimum would silently omit it
 * and let the source advance past work it has not started. Running entries carry the positions that
 * completed checkpoints recorded, which readers report from {@code notifyCheckpointComplete}, so
 * the frontier never passes a record a region failover reads again. Readers' live positions apply
 * only once no checkpoint completed within {@code firstCheckpointTimeoutNanos} of {@link #start},
 * and every finish is final at once while they do.
 *
 * <p>{@code ledgerWatermarkCounts} is a counted ordered index maintained incrementally, rather than
 * a scan of the ledger on every progress report, because a report names every split a reader holds.
 * {@link #replacePartition} keeps it in step and skips both halves of the update when an entry's
 * indexed watermark has not moved. It is rebuilt from the compact ledger on restore rather than
 * checkpointed beside it, so the two can never disagree.
 *
 * <p>A transition that would move the frontier backwards is rejected rather than absorbed: readers
 * have already emitted the old value, and Flink treats records at or below a watermark as late, so
 * a frontier that retreated would silently reclassify records that were on time. Once live
 * positions have moved the ledger, a rollback can legitimately fall behind them, so the frontier
 * then holds instead.
 *
 * <h2>Initialization happens off the coordinator thread</h2>
 *
 * <p>{@link #start} hands the metadata read — dialect, stream scope, retention, partition mode — to
 * {@code callAsync}, because it is an RPC and the coordinator thread must not block on one. Reader
 * events that arrive in that window have no ledger to apply to yet, so they are parked in {@code
 * deferredActions} and replayed in order once initialization completes; {@link #snapshotState}
 * refuses to run before that point rather than checkpointing a ledger those events have not reached
 * yet.
 *
 * <p>On restore the checkpointed ledger wins and never seeds a second null-token query. If an
 * unfinished position has fallen outside retention the default is to fail, because the alternative
 * silently loses the unavailable interval; an explicit fallback discards the <em>whole</em> ledger
 * and starts one null-token query, since advancing a single stale token could skip the terminal
 * child record that would have completed the lineage.
 *
 * <p>A bounded source's bound travels with the ledger: the initial split carries it, every child
 * inherits its parent's, and the checkpoint records the bound even once no partition is left to
 * carry it. A restore whose configured bound differs therefore fails rather than reading to the
 * checkpointed one, and a start that resolves past the bound fails rather than seeding a query with
 * nothing to read.
 *
 * <p>See {@code docs/adr/0099} for the protocol's evidence and {@code docs/adr/0094} for the start
 * position and restore-expiry rules.
 */
@Internal
public final class SpannerChangeStreamSplitEnumerator
        implements SplitEnumerator<ChangeStreamPartitionSplit, SpannerChangeStreamEnumeratorState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SpannerChangeStreamSplitEnumerator.class);

    private static final long UNCOVERED = -1L;

    /** The longest pause between two checks of the first-checkpoint wait. */
    private static final long MAX_FIRST_CHECKPOINT_CHECK_MILLIS = Duration.ofSeconds(30).toMillis();

    /** The shortest pause between two checks, so a short wait does not flood the coordinator. */
    private static final long MIN_FIRST_CHECKPOINT_CHECK_MILLIS = Duration.ofSeconds(1).toMillis();

    private final SplitEnumeratorContext<ChangeStreamPartitionSplit> context;
    private final SpannerChangeStreamCoordinatorClientFactory clientFactory;

    /**
     * The source's client metrics for the client {@link #initialize()} builds, or {@code null} when
     * nothing is exported to Flink. The source registers them once for all its enumerators; the
     * client records into them and never closes them.
     */
    @Nullable private final ClientMetrics clientMetrics;

    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    @Nullable private final Instant endTimestamp;
    private final long heartbeatMillis;
    private final boolean holdChildrenUntilCheckpoint;
    private final long firstCheckpointTimeoutNanos;
    @Nullable private final SpannerChangeStreamEnumeratorState restoredState;
    private final LongSupplier currentTimeMillis;
    private final LongSupplier nanoTime;

    private final Map<String, ChangeStreamPartitionSplit> ledger = new LinkedHashMap<>();
    private final Map<String, Set<String>> childrenByParent = new HashMap<>();
    private final Map<String, Integer> finishedParentProofReferences = new LinkedHashMap<>();

    /**
     * Finished parents whose children wait for a checkpoint, each with the id of the first
     * checkpoint snapshotted after its finish, or {@code UNCOVERED} before one is.
     */
    private final Map<String, Long> heldParents = new LinkedHashMap<>();

    /**
     * Partitions whose reader reported the finish, kept in the ledger as {@code FINISHED} until a
     * completed checkpoint covers the finish: each maps to the id of the first checkpoint
     * snapshotted after it, or {@code UNCOVERED} before one is. Until then a region failover can
     * hand the split back or a restored reader can read it again, so the finish is not final, as a
     * Debezium partition is not {@code FINISHED} before its offsets are committed.
     */
    private final Map<String, Long> pendingFinishes = new LinkedHashMap<>();

    private final Deque<String> scheduledPartitions = new ArrayDeque<>();
    private final TreeMap<Long, Integer> scheduledPositionCounts = new TreeMap<>();
    private final TreeMap<Long, Integer> ledgerWatermarkCounts = new TreeMap<>();
    private final Set<Integer> waitingReaders = new LinkedHashSet<>();
    private final List<DeferredAction> deferredActions = new ArrayList<>();
    private final AtomicLong scheduledCount = new AtomicLong();
    private final AtomicLong oldestScheduledPositionMillis = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong ledgerEntryCount = new AtomicLong();
    private final AtomicLong finishedParentProofCount = new AtomicLong();

    /**
     * Replaced at {@link #start()}; volatile because {@link #close()} may run on another thread.
     */
    private volatile EnumeratorMetrics metrics = EnumeratorMetrics.unregistered();

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter partitionsDiscovered = new ThreadSafeSimpleCounter();
    @Nullable private SpannerChangeStreamCoordinatorClient client;
    private boolean initialized;
    private boolean discardRestoredReaderSplits;
    private boolean boundedLedger;
    private int unfinishedPartitions;
    private long sourceWatermark = Long.MIN_VALUE;
    private boolean checkpointCompleted;
    private boolean releasingWithoutCheckpoint;

    /**
     * Whether the frontier follows the readers' live positions because no checkpoint completed
     * within {@code firstCheckpointTimeoutNanos} of {@link #start}; finishes are final at once
     * then.
     */
    private boolean frontierFromLiveProgress;

    /** Whether live positions ever moved the ledger, which can leave it ahead of a restore. */
    private boolean liveProgressApplied;

    private long startedNanos;

    /** When the oldest current hold began, on {@code nanoTime}; null while nothing is held. */
    @Nullable private Long heldSinceNanos;

    private volatile boolean closed;

    public SpannerChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant endTimestamp,
            long heartbeatMillis,
            boolean holdChildrenUntilCheckpoint,
            Duration firstCheckpointTimeout,
            @Nullable SpannerChangeStreamEnumeratorState restoredState,
            @Nullable ClientMetrics clientMetrics) {
        this(
                context,
                clientFactory,
                startPosition,
                resumeFallback,
                endTimestamp,
                heartbeatMillis,
                holdChildrenUntilCheckpoint,
                firstCheckpointTimeout,
                restoredState,
                clientMetrics,
                System::currentTimeMillis,
                System::nanoTime);
    }

    @VisibleForTesting
    SpannerChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant endTimestamp,
            long heartbeatMillis,
            boolean holdChildrenUntilCheckpoint,
            Duration firstCheckpointTimeout,
            @Nullable SpannerChangeStreamEnumeratorState restoredState,
            @Nullable ClientMetrics clientMetrics,
            LongSupplier currentTimeMillis,
            LongSupplier nanoTime) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.clientFactory =
                Preconditions.checkNotNull(clientFactory, "clientFactory must not be null");
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback = resumeFallback;
        this.endTimestamp = endTimestamp;
        Preconditions.checkArgument(
                heartbeatMillis > 0,
                "heartbeatMillis must be positive, but was %s",
                heartbeatMillis);
        this.heartbeatMillis = heartbeatMillis;
        this.holdChildrenUntilCheckpoint = holdChildrenUntilCheckpoint;
        Preconditions.checkNotNull(
                firstCheckpointTimeout, "firstCheckpointTimeout must not be null");
        Preconditions.checkArgument(
                !firstCheckpointTimeout.isNegative(),
                "firstCheckpointTimeout must not be negative, but was %s",
                firstCheckpointTimeout);
        this.firstCheckpointTimeoutNanos = firstCheckpointTimeout.toNanos();
        this.restoredState = restoredState;
        this.clientMetrics = clientMetrics;
        this.currentTimeMillis =
                Preconditions.checkNotNull(currentTimeMillis, "currentTimeMillis must not be null");
        this.nanoTime = Preconditions.checkNotNull(nanoTime, "nanoTime must not be null");
    }

    @Override
    public void start() {
        registerMetrics();
        startedNanos = nanoTime.getAsLong();
        context.callAsync(this::initialize, this::onInitialized);
        if (firstCheckpointTimeoutNanos > 0) {
            long period =
                    Math.max(
                            MIN_FIRST_CHECKPOINT_CHECK_MILLIS,
                            Math.min(
                                    Duration.ofNanos(firstCheckpointTimeoutNanos).toMillis() / 4,
                                    MAX_FIRST_CHECKPOINT_CHECK_MILLIS));
            context.callAsync(
                    () -> null,
                    (ignored, error) -> {
                        checkFirstCheckpointWait();
                        checkFirstCheckpointForFrontier();
                    },
                    period,
                    period);
        }
    }

    private Initialization initialize() throws Exception {
        SpannerChangeStreamCoordinatorClient created = clientFactory.create(clientMetrics);
        if (!installClient(created)) {
            throw new IllegalStateException(
                    "Spanner Change Streams enumerator was already closed.");
        }
        Duration retention = created.initialize();
        StartPositionResolver resolver = StartPositionResolver.create(getClass(), () -> retention);
        if (restoredState == null) {
            Instant start = resolver.resolve(startPosition);
            checkWithinBound("start position", startPosition, start);
            return Initialization.fresh(
                    ChangeStreamPartitionSplit.initial(start, endTimestamp, heartbeatMillis),
                    false,
                    Long.MIN_VALUE);
        }

        List<RestoreExpiry> expiries = new ArrayList<>();
        for (ChangeStreamPartitionSplit partition : restoredState.getPartitions()) {
            if (partition.getLifecycleState() == PartitionLifecycleState.FINISHED) {
                continue;
            }
            resolver.inspectRestored(partition.splitId(), partition.getCurrentPosition())
                    .ifPresent(expiries::add);
        }
        if (expiries.isEmpty()) {
            checkRestoredBound(restoredState);
            return Initialization.restored(
                    restoredState.getPartitions(),
                    restoredState.getFinishedParentProofs(),
                    restoredState.isBounded(),
                    restoredState.getSourceWatermark());
        }
        if (resumeFallback == null) {
            FlinkRuntimeException expiry = expiries.get(0).asFailure();
            throw new FlinkRuntimeException(
                    expiry.getMessage()
                            + " No restore fallback was configured; set"
                            + " SpannerChangeStreamSourceBuilder.resumeFallback(...) to opt into"
                            + " restarting from a retained position.",
                    expiry);
        }

        StartPosition requestedFallback = resumeFallback;
        Instant resolvedFallback = resolver.resolveFallback(requestedFallback);
        checkWithinBound("resume fallback", requestedFallback, resolvedFallback);
        RestoreExpiry oldest = oldest(expiries);
        LOG.warn(
                "{} unfinished Spanner Change Streams partition(s) have expired restored state;"
                        + " the oldest position {} predates the computed earliest {} by {}."
                        + " Discarding the checkpointed partition topology and restarting one"
                        + " initial null-token query from fallback {} resolved to {}. Records in"
                        + " the unavailable range are lost and records at or after the fallback"
                        + " may be delivered again.",
                expiries.size(),
                oldest.getRestoredPosition(),
                oldest.getComputedEarliest(),
                oldest.getUnavailableRange(),
                requestedFallback,
                resolvedFallback);
        return Initialization.fresh(
                ChangeStreamPartitionSplit.initial(resolvedFallback, endTimestamp, heartbeatMillis),
                true,
                Long.MIN_VALUE);
    }

    private void checkWithinBound(String role, StartPosition requested, Instant resolved) {
        if (endTimestamp != null && resolved.isAfter(endTimestamp)) {
            throw new FlinkRuntimeException(
                    "The Spanner Change Streams "
                            + role
                            + " "
                            + requested
                            + " resolved to "
                            + resolved
                            + ", after the bounded timestamp "
                            + endTimestamp
                            + "; there is nothing to read up to the bound. Set the bound through"
                            + " SpannerChangeStreamSourceBuilder.boundedTimestamp(...) or"
                            + " scan.bounded.timestamp-millis at or after the position the source"
                            + " starts from.");
        }
    }

    /**
     * Refuses a restore whose checkpointed bound differs from the configured one. Every partition
     * carries the bound it reads to and a child inherits its parent's, so the ledger cannot follow
     * a changed configuration; reading on to the checkpointed bound would silently ignore it.
     */
    private void checkRestoredBound(SpannerChangeStreamEnumeratorState restored) {
        // A completed version-3 ledger cannot name its bound, so only its boundedness is compared.
        Instant restoredBound = restored.getBoundedTimestamp();
        boolean matches =
                restored.isBounded() == (endTimestamp != null)
                        && (restoredBound == null || restoredBound.equals(endTimestamp));
        if (!matches) {
            throw new FlinkRuntimeException(
                    "The restored Spanner Change Streams partition ledger is "
                            + (restored.isBounded()
                                    ? "bounded"
                                            + (restoredBound == null ? "" : " at " + restoredBound)
                                    : "unbounded")
                            + ", but the source is configured "
                            + (endTimestamp == null
                                    ? "unbounded"
                                    : "with the bounded timestamp " + endTimestamp)
                            + ". A restore reads to the bound it was checkpointed with; configure"
                            + " the same bound through"
                            + " SpannerChangeStreamSourceBuilder.boundedTimestamp(...) or"
                            + " scan.bounded.timestamp-millis, or start without the job's state to"
                            + " read to a different bound.");
        }
    }

    private synchronized boolean installClient(SpannerChangeStreamCoordinatorClient created)
            throws Exception {
        if (closed) {
            created.close();
            return false;
        }
        client = created;
        return true;
    }

    private static RestoreExpiry oldest(List<RestoreExpiry> expiries) {
        RestoreExpiry oldest = expiries.get(0);
        for (RestoreExpiry expiry : expiries) {
            if (expiry.getRestoredPosition().isBefore(oldest.getRestoredPosition())) {
                oldest = expiry;
            }
        }
        return oldest;
    }

    private void onInitialized(@Nullable Initialization result, @Nullable Throwable error) {
        if (closed) {
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to initialize Spanner Change Streams partition state.", error);
        }
        Initialization initialization =
                Preconditions.checkNotNull(result, "initialization result must not be null");
        for (ChangeStreamPartitionSplit partition : initialization.partitions) {
            ledger.put(partition.splitId(), partition);
        }
        for (String proof : initialization.finishedParentProofs) {
            finishedParentProofReferences.put(proof, 0);
        }
        boundedLedger = initialization.bounded;
        rebuildRuntimeIndexes();
        sourceWatermark = initialization.sourceWatermark;
        initializeSourceWatermark();
        discardRestoredReaderSplits = initialization.discardRestoredReaderSplits;
        initialized = true;
        for (Integer subtaskId : context.registeredReaders().keySet()) {
            sendInitializationEvent(subtaskId);
        }
        for (DeferredAction deferred : deferredActions) {
            deferred.replay(this);
        }
        deferredActions.clear();
        serveWaitingReaders();
        LOG.info(
                "Initialized Spanner Change Streams with {} unfinished partition ledger entries"
                        + " ({} created, {} scheduled, {} running) and {} finished-parent proofs.",
                ledger.size(),
                count(PartitionLifecycleState.CREATED),
                count(PartitionLifecycleState.SCHEDULED),
                count(PartitionLifecycleState.RUNNING),
                finishedParentProofReferences.size());
    }

    private long count(PartitionLifecycleState state) {
        return ledger.values().stream()
                .filter(partition -> partition.getLifecycleState() == state)
                .count();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (closed) {
            return;
        }
        if (!initialized) {
            waitingReaders.add(subtaskId);
            return;
        }
        assignOrWait(subtaskId);
    }

    private void assignOrWait(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            waitingReaders.remove(subtaskId);
            return;
        }
        ChangeStreamPartitionSplit scheduled = firstScheduled();
        if (scheduled == null) {
            if (boundedLedgerFinished()) {
                waitingReaders.remove(subtaskId);
                context.signalNoMoreSplits(subtaskId);
                return;
            }
            waitingReaders.add(subtaskId);
            return;
        }
        ChangeStreamPartitionSplit running =
                scheduled.withLifecycleState(PartitionLifecycleState.RUNNING);
        ledger.put(running.splitId(), running);
        scheduledPartitions.removeFirst();
        scheduledCount.decrementAndGet();
        untrackScheduledPosition(scheduled.getCurrentPosition().toEpochMilli());
        waitingReaders.remove(subtaskId);
        splitsAssigned.inc();
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(running))));
    }

    @Nullable
    private ChangeStreamPartitionSplit firstScheduled() {
        while (!scheduledPartitions.isEmpty()) {
            ChangeStreamPartitionSplit partition = ledger.get(scheduledPartitions.peekFirst());
            if (partition != null
                    && partition.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                return partition;
            }
            scheduledPartitions.removeFirst();
        }
        return null;
    }

    @Override
    public void addSplitsBack(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        if (closed) {
            return;
        }
        if (!initialized) {
            deferredActions.add(new DeferredSplitsBack(splits));
            return;
        }
        addSplitsBackInitialized(splits);
    }

    /**
     * Takes back what a region failover returns: the splits Flink assigned since the checkpoint the
     * failed subtask restores. That subtask's downstream rolled back to the checkpoint too, so each
     * split restarts from where it was assigned, not from progress its reader reported since, and a
     * split whose finish no completed checkpoint covers yet is read again.
     */
    private void addSplitsBackInitialized(List<ChangeStreamPartitionSplit> splits) {
        boolean rolledBack = false;
        for (ChangeStreamPartitionSplit returned : splits) {
            ChangeStreamPartitionSplit current = ledger.get(returned.splitId());
            if (current == null) {
                // A finish leaves the ledger once a completed checkpoint covers it, and Flink
                // returns only assignments no completed checkpoint covers; while the frontier
                // follows live progress, though, every finish leaves at once.
                LOG.warn(
                        "Ignoring returned split {}, which is no longer in the ledger: either a"
                                + " completed checkpoint covers its finish, or it finished while no"
                                + " checkpoint completed.",
                        returned.splitId());
                continue;
            }
            if (current.getLifecycleState() == PartitionLifecycleState.CREATED
                    || current.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                continue;
            }
            Preconditions.checkArgument(
                    current.samePartitionDefinition(returned),
                    "returned split %s does not match the checkpointed partition definition",
                    returned.splitId());
            withdrawFinish(current.splitId());
            ledger.put(
                    current.splitId(),
                    current.withProgress(returned.getCurrentPosition(), returned.getWatermark())
                            .withLifecycleState(PartitionLifecycleState.SCHEDULED));
            rolledBack = true;
        }
        if (rolledBack) {
            reconcileAfterRollback();
        }
        refreshSourceWatermark();
        splitsReturned.inc(splits.size());
        serveWaitingReaders();
    }

    /**
     * Takes back the finish of every restored split the ledger still holds as a pending finish: its
     * reader reads it again from the restored checkpoint, so its children must wait again.
     */
    private void acceptRestoredPartitions(PartitionsRestoredEvent restored) {
        if (discardRestoredReaderSplits) {
            // The reader restored the discarded ledger's splits and is about to drop them.
            return;
        }
        boolean rolledBack = false;
        for (String splitId : restored.getSplitIds()) {
            ChangeStreamPartitionSplit current = ledger.get(splitId);
            if (current == null
                    || current.getLifecycleState() != PartitionLifecycleState.FINISHED) {
                continue;
            }
            withdrawFinish(splitId);
            ledger.put(splitId, current.withLifecycleState(PartitionLifecycleState.RUNNING));
            rolledBack = true;
        }
        if (rolledBack) {
            reconcileAfterRollback();
            refreshSourceWatermark();
            serveWaitingReaders();
        }
    }

    /** Forgets a pending finish's bookkeeping; the caller puts the entry back as unfinished. */
    private void withdrawFinish(String splitId) {
        pendingFinishes.remove(splitId);
        heldParents.remove(splitId);
        if (heldParents.isEmpty()) {
            heldSinceNanos = null;
        }
        finishedParentProofReferences.remove(splitId);
    }

    /**
     * Restores the lineage invariants after partitions became unfinished again: a scheduled child
     * of an unfinished parent goes back to {@code CREATED}, a finished parent it still names is
     * proven again, and the runtime indexes are rebuilt from the ledger. A child a live reader is
     * already reading keeps running; its order relative to the re-read parent is not restored.
     */
    private void reconcileAfterRollback() {
        for (ChangeStreamPartitionSplit partition : new ArrayList<>(ledger.values())) {
            if (partition.getLifecycleState() != PartitionLifecycleState.SCHEDULED
                    || !hasUnfinishedParent(partition)) {
                continue;
            }
            ledger.put(
                    partition.splitId(),
                    partition.withLifecycleState(PartitionLifecycleState.CREATED));
            for (String parentId : partition.getParentPartitionIds()) {
                ChangeStreamPartitionSplit parent = ledger.get(parentId);
                if (parent == null
                        || parent.getLifecycleState() == PartitionLifecycleState.FINISHED) {
                    finishedParentProofReferences.putIfAbsent(parentId, 0);
                }
            }
        }
        rebuildRuntimeIndexes();
    }

    private boolean hasUnfinishedParent(ChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            ChangeStreamPartitionSplit parent = ledger.get(parentId);
            if (parent != null && parent.getLifecycleState() != PartitionLifecycleState.FINISHED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addReader(int subtaskId) {
        if (initialized) {
            sendInitializationEvent(subtaskId);
        }
    }

    private void sendInitializationEvent(int subtaskId) {
        context.sendEventToSourceReader(
                subtaskId,
                new SpannerChangeStreamInitializationEvent(
                        discardRestoredReaderSplits, sourceWatermark));
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (closed) {
            return;
        }
        if (!initialized) {
            deferredActions.add(new DeferredSourceEvent(subtaskId, sourceEvent));
            return;
        }
        handleSourceEventInitialized(subtaskId, sourceEvent);
    }

    private void handleSourceEventInitialized(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionProgressEvent) {
            updateProgress((PartitionProgressEvent) sourceEvent);
            return;
        }
        if (sourceEvent instanceof PartitionsRestoredEvent) {
            acceptRestoredPartitions((PartitionsRestoredEvent) sourceEvent);
            return;
        }
        if (sourceEvent instanceof ChildPartitionsEvent) {
            acceptChildren(subtaskId, (ChildPartitionsEvent) sourceEvent);
            return;
        }
        if (sourceEvent instanceof PartitionFinishedEvent) {
            finishPartition(subtaskId, (PartitionFinishedEvent) sourceEvent);
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported Spanner Change Streams source event " + sourceEvent + ".");
    }

    /**
     * Moves running and pending entries to the reported positions. Committed reports always apply;
     * live ones only while the frontier follows live progress, because a frontier built from live
     * positions can run ahead of what a restore re-reads.
     */
    private void updateProgress(PartitionProgressEvent progress) {
        if (!progress.isCommitted() && !frontierFromLiveProgress) {
            return;
        }
        liveProgressApplied |= !progress.isCommitted();
        for (PartitionProgressEvent.Progress reported : progress.getPartitions()) {
            ChangeStreamPartitionSplit current = ledger.get(reported.getSplitId());
            if (current == null
                    || (current.getLifecycleState() != PartitionLifecycleState.RUNNING
                            && current.getLifecycleState() != PartitionLifecycleState.FINISHED)) {
                continue;
            }
            replacePartition(
                    current,
                    current.withProgress(
                            later(current.getCurrentPosition(), reported.getCurrentPosition()),
                            later(current.getWatermark(), reported.getWatermark())));
        }
        refreshSourceWatermark();
    }

    private void acceptChildren(int subtaskId, ChildPartitionsEvent event) {
        ChangeStreamPartitionSplit parent = ledger.get(event.getParentSplitId());
        boolean reportingParentIsProof =
                (parent == null
                                && finishedParentProofReferences.containsKey(
                                        event.getParentSplitId()))
                        || (parent != null
                                && parent.getLifecycleState() == PartitionLifecycleState.FINISHED);
        if (parent == null && !reportingParentIsProof) {
            // A restored reader repeats the child reports of a finish this ledger applied and
            // compacted already.
            LOG.debug(
                    "Ignoring child partitions for split {} from subtask {}, which is not in the"
                            + " ledger.",
                    event.getParentSplitId(),
                    subtaskId);
            return;
        }
        if (parent != null
                && parent.getLifecycleState() != PartitionLifecycleState.RUNNING
                && parent.getLifecycleState() != PartitionLifecycleState.FINISHED) {
            throw new IllegalStateException(
                    "Child partitions arrived for "
                            + parent.splitId()
                            + " while it was "
                            + parent.getLifecycleState()
                            + ".");
        }
        for (ChildPartitionsEvent.ChildPartition child : event.getChildren()) {
            Preconditions.checkArgument(
                    child.getParentPartitionIds().contains(event.getParentSplitId()),
                    "child token %s does not name reporting parent %s",
                    child.getToken(),
                    event.getParentSplitId());
            String childId = ChangeStreamPartitionSplit.idForToken(child.getToken());
            ChangeStreamPartitionSplit existing = ledger.get(childId);
            if (reportingParentIsProof) {
                if (existing == null) {
                    // A restored reader repeats the reports of a finish this ledger applied
                    // already; the child has been released and finished since, and a completed
                    // checkpoint covers that finish, or the child would still be pending here.
                    continue;
                }
                Preconditions.checkState(
                        sameCompactedChildReport(existing, child, event.getStartTimestamp()),
                        "child token %s was reported with conflicting partition metadata",
                        child.getToken());
                promoteIfReady(existing);
                continue;
            }
            // A known child was checked when first reported; a parent read again after a region
            // failover repeats it while another of its parents may have left the ledger since.
            for (String parentId : child.getParentPartitionIds()) {
                Preconditions.checkArgument(
                        existing != null || ledger.containsKey(parentId),
                        "child token %s names unknown parent %s",
                        child.getToken(),
                        parentId);
            }
            ChangeStreamPartitionSplit discovered =
                    new ChangeStreamPartitionSplit(
                            child.getToken(),
                            child.getParentPartitionIds(),
                            event.getStartTimestamp(),
                            parent.getEndTimestamp(),
                            parent.getHeartbeatMillis(),
                            event.getStartTimestamp(),
                            PartitionLifecycleState.CREATED,
                            event.getStartTimestamp());
            if (existing == null) {
                ledger.put(childId, discovered);
                trackLedgerWatermark(discovered);
                unfinishedPartitions++;
                partitionsDiscovered.inc();
                indexCreatedPartition(discovered);
                promoteIfReady(discovered);
            } else {
                Preconditions.checkState(
                        existing.samePartitionDefinition(discovered),
                        "child token %s was reported with conflicting partition metadata",
                        child.getToken());
                promoteIfReady(existing);
            }
        }
        refreshLineageMetricCounts();
        refreshSourceWatermark();
        serveWaitingReaders();
    }

    private static boolean sameCompactedChildReport(
            ChangeStreamPartitionSplit existing,
            ChildPartitionsEvent.ChildPartition reported,
            Instant startTimestamp) {
        return existing.splitId().equals(ChangeStreamPartitionSplit.idForToken(reported.getToken()))
                && existing.getParentPartitionIds().equals(reported.getParentPartitionIds())
                && existing.getStartTimestamp().equals(startTimestamp);
    }

    private void finishPartition(int subtaskId, PartitionFinishedEvent event) {
        applyFinish(subtaskId, event);
        acknowledgeFinish(subtaskId, event.getSplitId());
    }

    /**
     * Lets the reader drop the finished split from its state. A reader that failed since sending
     * the finish has no gateway; its restored attempt reports the finish again.
     */
    private void acknowledgeFinish(int subtaskId, String splitId) {
        if (context.registeredReaders().containsKey(subtaskId)) {
            context.sendEventToSourceReader(subtaskId, new PartitionFinishedAckEvent(splitId));
        }
    }

    private void applyFinish(int subtaskId, PartitionFinishedEvent event) {
        ChangeStreamPartitionSplit current = ledger.get(event.getSplitId());
        if (current == null || current.getLifecycleState() == PartitionLifecycleState.FINISHED) {
            // A restored reader repeats a finish this ledger applied already.
            LOG.debug(
                    "Ignoring completion for split {} from subtask {}, which is not in the ledger.",
                    event.getSplitId(),
                    subtaskId);
            return;
        }
        if (current.getLifecycleState() != PartitionLifecycleState.RUNNING) {
            throw new IllegalStateException(
                    "Partition "
                            + current.splitId()
                            + " finished while it was "
                            + current.getLifecycleState()
                            + ".");
        }
        unfinishedPartitions--;
        if (!frontierFromLiveProgress) {
            // Its records count downstream only once a checkpoint covering the finish completes,
            // and a parent read again reports it as a child again, which must find it here.
            replacePartition(current, current.withLifecycleState(PartitionLifecycleState.FINISHED));
            pendingFinishes.put(current.splitId(), UNCOVERED);
        } else {
            untrackLedgerWatermark(current);
            ledger.remove(current.splitId());
        }
        Set<String> children = childrenByParent.remove(current.splitId());
        if (children != null && !children.isEmpty()) {
            finishedParentProofReferences.put(current.splitId(), children.size());
            if (holdsChildrenOf(event)) {
                heldParents.put(current.splitId(), UNCOVERED);
                if (heldSinceNanos == null) {
                    heldSinceNanos = nanoTime.getAsLong();
                }
            }
        }
        for (String childId : children == null ? Collections.<String>emptySet() : children) {
            ChangeStreamPartitionSplit child = ledger.get(childId);
            if (child != null) {
                promoteIfReady(child);
            }
        }
        refreshLineageMetricCounts();
        refreshSourceWatermark();
        serveWaitingReaders();
    }

    private void rebuildRuntimeIndexes() {
        childrenByParent.clear();
        finishedParentProofReferences.replaceAll((ignored, references) -> 0);
        scheduledPartitions.clear();
        scheduledPositionCounts.clear();
        ledgerWatermarkCounts.clear();
        scheduledCount.set(0);
        unfinishedPartitions = 0;
        for (ChangeStreamPartitionSplit partition : ledger.values()) {
            trackLedgerWatermark(partition);
            if (partition.getLifecycleState() == PartitionLifecycleState.FINISHED) {
                continue;
            }
            unfinishedPartitions++;
            if (partition.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                scheduledPartitions.addLast(partition.splitId());
                scheduledCount.incrementAndGet();
                trackScheduledPosition(partition.getCurrentPosition().toEpochMilli());
            } else if (partition.getLifecycleState() == PartitionLifecycleState.CREATED) {
                indexCreatedPartition(partition);
            }
        }
        for (ChangeStreamPartitionSplit partition : new ArrayList<>(ledger.values())) {
            promoteIfReady(partition);
        }
        refreshOldestScheduledPosition();
        refreshLineageMetricCounts();
    }

    private void indexCreatedPartition(ChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            if (finishedParentProofReferences.containsKey(parentId)) {
                finishedParentProofReferences.merge(parentId, 1, Integer::sum);
                continue;
            }
            childrenByParent
                    .computeIfAbsent(parentId, ignored -> new LinkedHashSet<>())
                    .add(partition.splitId());
        }
    }

    @VisibleForTesting
    int pendingParentDependencyCount() {
        return childrenByParent.values().stream().mapToInt(Set::size).sum();
    }

    private void promoteIfReady(ChangeStreamPartitionSplit partition) {
        if (partition.getLifecycleState() == PartitionLifecycleState.CREATED
                && allParentsFinished(partition)) {
            releaseParentDependencies(partition);
            schedule(partition);
        }
    }

    private void schedule(ChangeStreamPartitionSplit partition) {
        ChangeStreamPartitionSplit scheduled =
                partition.withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ChangeStreamPartitionSplit current = ledger.get(scheduled.splitId());
        if (current == null) {
            ledger.put(scheduled.splitId(), scheduled);
            trackLedgerWatermark(scheduled);
        } else {
            replacePartition(current, scheduled);
        }
        scheduledPartitions.addLast(scheduled.splitId());
        scheduledCount.incrementAndGet();
        trackScheduledPosition(scheduled.getCurrentPosition().toEpochMilli());
        refreshOldestScheduledPosition();
        refreshLineageMetricCounts();
    }

    private void releaseParentDependencies(ChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            Set<String> waitingChildren = childrenByParent.get(parentId);
            if (waitingChildren != null) {
                waitingChildren.remove(partition.splitId());
                if (waitingChildren.isEmpty()) {
                    childrenByParent.remove(parentId);
                }
            }
            Integer references = finishedParentProofReferences.get(parentId);
            if (references == null) {
                continue;
            }
            if (references == 1) {
                finishedParentProofReferences.remove(parentId);
            } else {
                finishedParentProofReferences.put(parentId, references - 1);
            }
        }
    }

    private boolean allParentsFinished(ChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            if (!finishedParentProofReferences.containsKey(parentId)
                    || heldParents.containsKey(parentId)) {
                return false;
            }
        }
        return true;
    }

    private void serveWaitingReaders() {
        Iterator<Integer> waiting = waitingReaders.iterator();
        while (firstScheduled() != null && waiting.hasNext()) {
            int subtaskId = waiting.next();
            waiting.remove();
            assignOrWait(subtaskId);
        }
        if (boundedLedgerFinished()) {
            while (waiting.hasNext()) {
                int subtaskId = waiting.next();
                waiting.remove();
                if (context.registeredReaders().containsKey(subtaskId)) {
                    context.signalNoMoreSplits(subtaskId);
                }
            }
        }
    }

    private boolean boundedLedgerFinished() {
        return boundedLedger && unfinishedPartitions == 0;
    }

    private static Instant later(Instant left, Instant right) {
        return right.isAfter(left) ? right : left;
    }

    private void replacePartition(
            ChangeStreamPartitionSplit current, ChangeStreamPartitionSplit replacement) {
        boolean sameIndexedWatermark =
                SpannerChangeStreamWatermarks.beforeInstant(current.getWatermark())
                        == SpannerChangeStreamWatermarks.beforeInstant(replacement.getWatermark());
        if (!sameIndexedWatermark) {
            untrackLedgerWatermark(current);
        }
        ledger.put(replacement.splitId(), replacement);
        if (!sameIndexedWatermark) {
            trackLedgerWatermark(replacement);
        }
    }

    /** Every ledger entry counts, a pending finish included: its records are not committed yet. */
    private void trackLedgerWatermark(ChangeStreamPartitionSplit partition) {
        ledgerWatermarkCounts.merge(
                SpannerChangeStreamWatermarks.beforeInstant(partition.getWatermark()),
                1,
                Integer::sum);
    }

    private void untrackLedgerWatermark(ChangeStreamPartitionSplit partition) {
        long watermark = SpannerChangeStreamWatermarks.beforeInstant(partition.getWatermark());
        Integer count = ledgerWatermarkCounts.get(watermark);
        Preconditions.checkState(count != null, "Unfinished watermark index is inconsistent.");
        if (count == 1) {
            ledgerWatermarkCounts.remove(watermark);
        } else {
            ledgerWatermarkCounts.put(watermark, count - 1);
        }
    }

    private void refreshSourceWatermark() {
        if (ledgerWatermarkCounts.isEmpty()) {
            return;
        }
        long next = ledgerWatermarkCounts.firstKey();
        if (next < sourceWatermark && liveProgressApplied) {
            // Live positions moved the frontier past what a region failover re-reads; readers
            // emitted it already, so it holds rather than move back.
            return;
        }
        Preconditions.checkState(
                next >= sourceWatermark,
                "Spanner Change Streams ledger watermark moved backwards from %s to %s.",
                sourceWatermark,
                next);
        if (next == sourceWatermark) {
            return;
        }
        sourceWatermark = next;
        SpannerChangeStreamWatermarkEvent event = new SpannerChangeStreamWatermarkEvent(next);
        for (Integer subtaskId : context.registeredReaders().keySet()) {
            context.sendEventToSourceReader(subtaskId, event);
        }
    }

    private void initializeSourceWatermark() {
        if (ledgerWatermarkCounts.isEmpty()) {
            return;
        }
        long candidate = ledgerWatermarkCounts.firstKey();
        Preconditions.checkState(
                candidate >= sourceWatermark,
                "Restored Spanner Change Streams source watermark %s is ahead of unfinished-ledger"
                        + " frontier %s.",
                sourceWatermark,
                candidate);
        sourceWatermark = candidate;
    }

    @Override
    public SpannerChangeStreamEnumeratorState snapshotState(long checkpointId) {
        Preconditions.checkState(
                initialized,
                "Spanner Change Streams initialization is still outstanding; retry the"
                        + " checkpoint after its deferred reader actions have been replayed.");
        heldParents.replaceAll(
                (ignored, covering) -> covering == UNCOVERED ? checkpointId : covering);
        pendingFinishes.replaceAll(
                (ignored, covering) -> covering == UNCOVERED ? checkpointId : covering);
        List<ChangeStreamPartitionSplit> unfinished = new ArrayList<>(ledger.size());
        for (ChangeStreamPartitionSplit partition : ledger.values()) {
            if (partition.getLifecycleState() != PartitionLifecycleState.FINISHED) {
                unfinished.add(partition);
            }
        }
        // A frontier held above a rollback after live progress must not be checkpointed ahead of
        // the ledger, which a restore validates; restored readers start from it afresh.
        long checkpointedWatermark =
                ledgerWatermarkCounts.isEmpty()
                        ? sourceWatermark
                        : Math.min(sourceWatermark, ledgerWatermarkCounts.firstKey());
        return SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                unfinished,
                finishedParentProofReferences.keySet(),
                endTimestamp,
                checkpointedWatermark);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (closed) {
            return;
        }
        if (releasingWithoutCheckpoint) {
            LOG.info(
                    "Checkpoint {} is the first to complete since this coordinator started, so"
                            + " child partitions wait for a checkpoint again.",
                    checkpointId);
            releasingWithoutCheckpoint = false;
        }
        checkpointCompleted = true;
        // A reader restores the splits of the latest completed checkpoint, so once one completes
        // after a fallback, no reader can restore the discarded ledger's splits any more.
        discardRestoredReaderSplits = false;
        if (frontierFromLiveProgress) {
            LOG.info(
                    "Checkpoint {} completed, so the source watermark follows committed reader"
                            + " positions again.",
                    checkpointId);
            frontierFromLiveProgress = false;
        }
        settleCoveredFinishes(checkpointId);
        if (heldParents.isEmpty()) {
            return;
        }
        if (heldParents
                .values()
                .removeIf(covering -> covering != UNCOVERED && covering <= checkpointId)) {
            releaseHeldChildren();
        }
    }

    private boolean holdsChildren() {
        return holdChildrenUntilCheckpoint && context.currentParallelism() > 1;
    }

    private boolean holdsChildrenOf(PartitionFinishedEvent finish) {
        // A parent whose reader emitted nothing has nothing downstream for its children to
        // overtake. That covers the initial query, which in practice returns only child
        // partitions, without assuming it never returns data.
        return holdsChildren() && finish.hasEmittedRecords() && !releasingWithoutCheckpoint;
    }

    /**
     * Releases every hold when no checkpoint has completed and a child has waited out {@code
     * firstCheckpointTimeoutNanos}, which is how a job without checkpointing looks from here.
     */
    private void checkFirstCheckpointWait() {
        if (closed
                || checkpointCompleted
                || heldSinceNanos == null
                || nanoTime.getAsLong() - heldSinceNanos < firstCheckpointTimeoutNanos) {
            return;
        }
        LOG.warn(
                "No checkpoint has completed while child partitions waited {} for one, so they"
                        + " start as soon as their parents finish until one completes. Without one,"
                        + " a key's changes can reach a keyed operator"
                        + " out of commit order across a partition split or merge. Enable"
                        + " checkpointing; raise firstCheckpointTimeout"
                        + " ('scan.first-checkpoint-timeout') for a job that checkpoints less"
                        + " often; or set partitionHandoff(IMMEDIATE)"
                        + " ('scan.change-stream.partition-handoff' = 'immediate') to start"
                        + " children at once.",
                Duration.ofNanos(firstCheckpointTimeoutNanos));
        releasingWithoutCheckpoint = true;
        heldParents.clear();
        releaseHeldChildren();
    }

    private void releaseHeldChildren() {
        if (heldParents.isEmpty()) {
            heldSinceNanos = null;
        }
        for (ChangeStreamPartitionSplit partition : new ArrayList<>(ledger.values())) {
            promoteIfReady(partition);
        }
        refreshLineageMetricCounts();
        serveWaitingReaders();
    }

    @VisibleForTesting
    int heldParentCount() {
        return heldParents.size();
    }

    @VisibleForTesting
    int pendingFinishCount() {
        return pendingFinishes.size();
    }

    /** Removes the pending finishes a completed checkpoint covers; they are final now. */
    private void settleCoveredFinishes(long checkpointId) {
        boolean settled = false;
        Iterator<Map.Entry<String, Long>> pending = pendingFinishes.entrySet().iterator();
        while (pending.hasNext()) {
            Map.Entry<String, Long> finish = pending.next();
            if (finish.getValue() == UNCOVERED || finish.getValue() > checkpointId) {
                continue;
            }
            pending.remove();
            ChangeStreamPartitionSplit partition = ledger.remove(finish.getKey());
            if (partition != null) {
                untrackLedgerWatermark(partition);
            }
            settled = true;
        }
        if (settled) {
            refreshLineageMetricCounts();
            refreshSourceWatermark();
        }
    }

    /**
     * Makes every finish final and lets the frontier follow live positions when no checkpoint
     * completed within {@code firstCheckpointTimeoutNanos} of {@link #start}: without one, a
     * pending finish would hold the frontier for good.
     */
    private void checkFirstCheckpointForFrontier() {
        if (closed
                || !initialized
                || checkpointCompleted
                || frontierFromLiveProgress
                || nanoTime.getAsLong() - startedNanos < firstCheckpointTimeoutNanos) {
            return;
        }
        LOG.warn(
                "The source watermark follows the readers' current positions, and a finished"
                        + " partition is not read again after a region failover, until a"
                        + " checkpoint completes: none has completed within {} of the source"
                        + " starting. Enable checkpointing, or raise firstCheckpointTimeout"
                        + " ('scan.first-checkpoint-timeout') for a job that checkpoints less"
                        + " often.",
                Duration.ofNanos(firstCheckpointTimeoutNanos));
        frontierFromLiveProgress = true;
        for (String splitId : pendingFinishes.keySet()) {
            ChangeStreamPartitionSplit partition = ledger.remove(splitId);
            if (partition != null) {
                untrackLedgerWatermark(partition);
            }
        }
        pendingFinishes.clear();
        refreshLineageMetricCounts();
        refreshSourceWatermark();
    }

    private void registerMetrics() {
        metrics = EnumeratorMetrics.of(context.metricGroup());
        splitsAssigned = metrics.counter(SpannerMetricNames.SPLITS_ASSIGNED);
        splitsReturned = metrics.counter(SpannerMetricNames.SPLITS_RETURNED);
        partitionsDiscovered =
                metrics.counter(SpannerMetricNames.CHANGE_STREAM_PARTITIONS_DISCOVERED);
        metrics.unassignedSplitsGauge(scheduledCount::get);
        metrics.gauge(
                SpannerMetricNames.UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::unassignedPartitionLagMillis);
        metrics.gauge(
                SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES,
                (Gauge<Long>) ledgerEntryCount::get);
        metrics.gauge(
                SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS,
                (Gauge<Long>) finishedParentProofCount::get);
        if (closed) {
            // close() ran first, on Flink's closer thread, and closed the view this replaced: let
            // go
            // of what was just bound rather than leave it reading a closed enumerator.
            metrics.close();
        }
    }

    private void refreshLineageMetricCounts() {
        ledgerEntryCount.set(ledger.size() - pendingFinishes.size());
        finishedParentProofCount.set(finishedParentProofReferences.size());
    }

    private void refreshOldestScheduledPosition() {
        oldestScheduledPositionMillis.set(
                scheduledPositionCounts.isEmpty()
                        ? Long.MAX_VALUE
                        : scheduledPositionCounts.firstKey());
    }

    private void trackScheduledPosition(long positionMillis) {
        scheduledPositionCounts.merge(positionMillis, 1, Integer::sum);
    }

    private void untrackScheduledPosition(long positionMillis) {
        Integer count = scheduledPositionCounts.get(positionMillis);
        Preconditions.checkState(count != null, "Scheduled position index is inconsistent.");
        if (count == 1) {
            scheduledPositionCounts.remove(positionMillis);
        } else {
            scheduledPositionCounts.put(positionMillis, count - 1);
        }
        refreshOldestScheduledPosition();
    }

    private long unassignedPartitionLagMillis() {
        long oldest = oldestScheduledPositionMillis.get();
        return oldest == Long.MAX_VALUE
                ? 0
                : MetricValues.elapsedMillis(currentTimeMillis.getAsLong(), oldest);
    }

    @Override
    public void close() throws IOException {
        SpannerChangeStreamCoordinatorClient closing;
        synchronized (this) {
            closed = true;
            closing = client;
            client = null;
        }
        // After the flag: a start() still registering sees it and lets go of what it bound.
        metrics.close();
        try {
            Closers.closeAll(closing);
        } catch (Exception e) {
            throw new IOException("Failed to close the Spanner Change Streams coordinator.", e);
        }
    }

    private static final class Initialization {

        private final List<ChangeStreamPartitionSplit> partitions;
        private final Set<String> finishedParentProofs;
        private final boolean bounded;
        private final boolean discardRestoredReaderSplits;
        private final long sourceWatermark;

        private Initialization(
                List<ChangeStreamPartitionSplit> partitions,
                Set<String> finishedParentProofs,
                boolean bounded,
                boolean discardRestoredReaderSplits,
                long sourceWatermark) {
            this.partitions = partitions;
            this.finishedParentProofs = finishedParentProofs;
            this.bounded = bounded;
            this.discardRestoredReaderSplits = discardRestoredReaderSplits;
            this.sourceWatermark = sourceWatermark;
        }

        private static Initialization fresh(
                ChangeStreamPartitionSplit initial,
                boolean discardRestoredReaderSplits,
                long sourceWatermark) {
            return new Initialization(
                    Collections.singletonList(initial),
                    Collections.emptySet(),
                    initial.getEndTimestamp() != null,
                    discardRestoredReaderSplits,
                    sourceWatermark);
        }

        private static Initialization restored(
                List<ChangeStreamPartitionSplit> partitions,
                Set<String> finishedParentProofs,
                boolean bounded,
                long sourceWatermark) {
            return new Initialization(
                    new ArrayList<>(partitions),
                    new LinkedHashSet<>(finishedParentProofs),
                    bounded,
                    false,
                    sourceWatermark);
        }
    }

    private interface DeferredAction {

        void replay(SpannerChangeStreamSplitEnumerator enumerator);
    }

    private static final class DeferredSourceEvent implements DeferredAction {

        private final int subtaskId;
        private final SourceEvent event;

        private DeferredSourceEvent(int subtaskId, SourceEvent event) {
            this.subtaskId = subtaskId;
            this.event = event;
        }

        @Override
        public void replay(SpannerChangeStreamSplitEnumerator enumerator) {
            enumerator.handleSourceEventInitialized(subtaskId, event);
        }
    }

    private static final class DeferredSplitsBack implements DeferredAction {

        private final List<ChangeStreamPartitionSplit> splits;

        private DeferredSplitsBack(List<ChangeStreamPartitionSplit> splits) {
            this.splits = new ArrayList<>(splits);
        }

        @Override
        public void replay(SpannerChangeStreamSplitEnumerator enumerator) {
            enumerator.addSplitsBackInitialized(splits);
        }
    }
}
