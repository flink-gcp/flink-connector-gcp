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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.MetricValues;
import io.github.flink.gcp.connector.base.source.EnumeratorMetrics;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionAckEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates Bigtable Change Streams partitions, split successors, and merge-parent tokens.
 *
 * <h2>A close is final only once a completed checkpoint covers it</h2>
 *
 * <p>Region failover restarts the failed tasks from the last completed checkpoint but keeps this
 * coordinator, and Flink returns the splits it assigned since that checkpoint through {@link
 * #addSplitsBack}. A partition whose {@code CloseStream} a reader reported therefore stays in
 * {@code pendingCloses} until a checkpoint whose {@link #snapshotState} ran after the transition
 * completes, as a Debezium partition is {@code FINISHED} only once its offsets are committed. A
 * returned split whose close is pending is read again from where Flink assigned it, because its
 * downstream rolled back with it, and so is a pending close a restored reader names in a {@link
 * PartitionsRestoredEvent}. A snapshot leaves pending closes out: the checkpoint it belongs to
 * already covers them.
 *
 * <p>The successors of a close are created when the transition arrives, so a partition read again
 * after its close must not create them twice. Its id stays in {@code rereads}, which the checkpoint
 * records, until its next close is final; that close creates nothing, and a successor a reader
 * already reads keeps running. When a close has waited {@code firstCheckpointTimeout} and no
 * checkpoint has completed since {@link #start}, which is how a job without checkpointing looks
 * from here, every close is final at once until one does.
 *
 * <h2>Successors wait for a checkpoint</h2>
 *
 * <p>A partition's last records can still be on their way downstream when its reader reports the
 * {@code CloseStream}, in a network buffer or a sink writer's batch. If another subtask reads a
 * successor, the successor's records take a different path and can reach a keyed operator first, so
 * one row key's changes arrive out of commit order (#2194, the same shape as Spanner's #2193). With
 * {@code holdSuccessorsUntilCheckpoint}, a successor stays in {@code unassigned} but is not handed
 * out while {@code heldSuccessors} names a parent close that is not final yet: one whose pending
 * close holds successors, or one being read again after a rollback. A close becomes final when a
 * completed checkpoint covers it, as above. The reader emitted the parent's records before it
 * reported the close, so they precede its barrier for that checkpoint, and a completed checkpoint
 * means downstream processed them.
 *
 * <p>A partition the reconciler restarts from a pending merge's tokens, or without a token over
 * part of its range, waits for that merge's parents in the same way. The hold is runtime state
 * only, and the checkpoint format is unchanged: a held successor is checkpointed as unassigned, and
 * a restore starts it at once. That is exact for a parent whose close the restored checkpoint
 * covers; a parent the checkpoint records as read again is replayed after the restore, and its
 * order against its successors is a failover's, which the hold does not restore. Nothing is held at
 * parallelism 1, where every record takes one path, or after a close whose reader emitted no record
 * for the partition, since nothing of it is downstream to overtake. When closes become final at
 * once for want of a checkpoint, so does the hold.
 *
 * <p><b>Every transition is acknowledged, and applying one is idempotent.</b> A reader keeps a
 * closed split, with its successors, in its checkpointed state until the {@link
 * PartitionTransitionAckEvent} arrives, and a restored reader sends the transition again. A restore
 * from a checkpoint whose coordinator snapshot missed a transition therefore still receives it. A
 * transition for a partition this ledger does not hold as assigned is acknowledged and otherwise
 * ignored: its successors already exist.
 *
 * <h2>Progress reports</h2>
 *
 * <p>Readers report the positions a completed checkpoint recorded, and their current positions
 * until one completes; an assigned split takes every report. The positions serve the reconciler,
 * never a restart or a restore check: a returned split restarts from the split Flink returns, and a
 * restored assigned split is checked against retention by the reader that restores it.
 */
@Internal
public final class BigtableChangeStreamSplitEnumerator
        implements SplitEnumerator<
                ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableChangeStreamSplitEnumerator.class);

    /**
     * How often the reconciliation scan asks the service for the current keyspace, and how long it
     * waits before the first one. The grace periods a recovery must outlast live on {@link
     * ChangeStreamPartitionReconciler}; this is only how often the ledger is compared against the
     * service at all, so it is short next to those.
     */
    private static final Duration RECONCILIATION_SCAN_INTERVAL = Duration.ofSeconds(10);

    /**
     * How long a fetched change-stream retention is reused before the scan asks for it again.
     * Retention is a table property an operator changes by hand, so re-reading it on every scan
     * would be a table-admin call every {@code RECONCILIATION_SCAN_INTERVAL} for a value that
     * almost never moves.
     */
    private static final Duration RETENTION_REFRESH_INTERVAL = Duration.ofMinutes(5);

    /**
     * How far inside the retention window a clamped low watermark is placed. The service answers no
     * request for a position it no longer retains, and the window moves: a watermark clamped
     * exactly to its edge has left it again before the read starts, so the clamp lands a minute
     * inside instead. The connector's documentation carries the user-visible half — a clamped
     * restart skips the changes between the position it tracked and the one it starts from.
     */
    private static final Duration RETENTION_CLAMP_MARGIN = Duration.ofSeconds(60);

    /** The covering checkpoint of a pending close that no snapshot has run after yet. */
    private static final long UNCOVERED = -1L;

    /** Bounds on how often a coordinator waiting for its first checkpoint looks at the clock. */
    private static final long MIN_FIRST_CHECKPOINT_CHECK_MILLIS = 1_000L;

    private static final long MAX_FIRST_CHECKPOINT_CHECK_MILLIS = 30_000L;

    private final SplitEnumeratorContext<ChangeStreamPartitionSplit> context;
    private final ChangeStreamCoordinatorClient client;
    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    private final boolean bounded;
    private final boolean reconciliationEnabled;
    private final boolean holdSuccessorsUntilCheckpoint;
    private final Duration firstCheckpointTimeout;
    private final Clock clock;
    @Nullable private final BigtableChangeStreamEnumeratorState restoredState;

    private final Deque<ChangeStreamPartitionSplit> unassigned = new ArrayDeque<>();
    private final Map<String, ChangeStreamPartitionSplit> assigned = new LinkedHashMap<>();
    private final Map<ByteStringRange, PendingMergeAccumulator> pendingMerges =
            new LinkedHashMap<>();
    private final List<MissingPartition> missingPartitions = new ArrayList<>();
    private final List<ByteStringRange> completedPartitions = new ArrayList<>();
    private final ChangeStreamPartitionReconciler reconciler =
            new ChangeStreamPartitionReconciler();
    private final Map<Integer, ReaderState> readers = new LinkedHashMap<>();

    /** Closed partitions no completed checkpoint covers yet, by split id, in close order. */
    private final Map<String, PendingClose> pendingCloses = new LinkedHashMap<>();

    /** Unassigned, assigned or pending-close splits read again after their successors exist. */
    private final Set<String> rereads = new LinkedHashSet<>();

    /**
     * Unassigned successors not handed out yet, by split id, each with the parent closes it waits
     * for. Runtime state only; every key is also in {@code unassigned}.
     */
    private final Map<String, Set<String>> heldSuccessors = new LinkedHashMap<>();

    /** Reader attempts told that no more splits are coming. */
    private final Set<Integer> toldNoMoreSplits = new HashSet<>();

    /**
     * How many reader attempts have registered for each subtask. An event deferred while the
     * enumerator initializes is acknowledged only if the attempt that sent it is still the latest.
     */
    private final Map<Integer, Integer> readerAttempts = new HashMap<>();

    private final List<DeferredAction> deferredActions = new ArrayList<>();
    private final AtomicInteger unassignedMetricCount = new AtomicInteger();
    private final AtomicLong oldestUnassignedPositionMillis = new AtomicLong(Long.MAX_VALUE);

    /**
     * Replaced at {@link #start()}; volatile because {@link #close()} may run on another thread.
     */
    private volatile EnumeratorMetrics metrics = EnumeratorMetrics.unregistered();

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter partitionsReconciled = new ThreadSafeSimpleCounter();
    private Counter tokenlessRestarts = new ThreadSafeSimpleCounter();
    private Counter partitionsDiscovered = new ThreadSafeSimpleCounter();
    private Counter partitionSplits = new ThreadSafeSimpleCounter();
    private Counter partitionMerges = new ThreadSafeSimpleCounter();
    private boolean initialized;
    private boolean boundedComplete;
    private boolean checkpointCompleted;
    private boolean closesFinalAtOnce;
    private volatile boolean closed;
    private Instant resolvedStartTime = Instant.EPOCH;
    private long nextSplitId;
    @Nullable private volatile Duration reconciliationRetention;
    @Nullable private volatile Instant reconciliationRetentionFetchedAt;

    /**
     * Creates the enumerator a job runs.
     *
     * <p>This is the whole public surface, and it is deliberately the shape production passes:
     * reconciliation is always on in a job, and the clock is always the system one.
     *
     * @param context the enumerator context Flink supplies
     * @param client the coordinator client; the enumerator takes ownership and closes it
     * @param startPosition where a partition with no stored token starts
     * @param resumeFallback where a partition whose token has expired resumes, or {@code null}
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     * @param bounded whether the stream has an end time and so finishes
     * @param holdSuccessorsUntilCheckpoint whether a successor waits until a completed checkpoint
     *     covers its parents' closes
     * @param firstCheckpointTimeout how long a close waits for a completed checkpoint before every
     *     close becomes final at once, while none has completed; zero waits indefinitely
     */
    public BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded,
            boolean holdSuccessorsUntilCheckpoint,
            Duration firstCheckpointTimeout) {
        this(
                context,
                client,
                startPosition,
                resumeFallback,
                restoredState,
                bounded,
                true,
                Clock.systemUTC(),
                holdSuccessorsUntilCheckpoint,
                firstCheckpointTimeout);
    }

    /**
     * The test seam, exposing the two values the public constructor fixes.
     *
     * <p>A unit test taking the public constructor would schedule a real periodic reconciliation
     * scan against the wall clock, which is why both are parameters here rather than knobs there.
     * The clock also measures how long a pending close has waited for a checkpoint.
     */
    @VisibleForTesting
    BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded,
            boolean reconciliationEnabled,
            Clock clock,
            boolean holdSuccessorsUntilCheckpoint,
            Duration firstCheckpointTimeout) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.client = Preconditions.checkNotNull(client, "client must not be null");
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback = resumeFallback;
        this.restoredState = restoredState;
        this.bounded = bounded;
        this.reconciliationEnabled = reconciliationEnabled;
        this.clock = Preconditions.checkNotNull(clock, "clock must not be null");
        this.holdSuccessorsUntilCheckpoint = holdSuccessorsUntilCheckpoint;
        this.firstCheckpointTimeout =
                Preconditions.checkNotNull(
                        firstCheckpointTimeout, "firstCheckpointTimeout must not be null");
    }

    @Override
    public void start() {
        registerMetrics();
        context.callAsync(this::initialize, this::onInitialized);
        if (!firstCheckpointTimeout.isZero()) {
            long period =
                    Math.max(
                            MIN_FIRST_CHECKPOINT_CHECK_MILLIS,
                            Math.min(
                                    firstCheckpointTimeout.toMillis() / 4,
                                    MAX_FIRST_CHECKPOINT_CHECK_MILLIS));
            context.callAsync(
                    () -> null, (ignored, error) -> checkFirstCheckpoint(), period, period);
        }
    }

    private Initialization initialize() throws Exception {
        client.validateSingleClusterAppProfile();
        StartPositionResolver resolver =
                StartPositionResolver.create(getClass(), client::retention);
        if (restoredState == null || !restoredState.isInitialized()) {
            Instant start = resolver.resolve(startPosition);
            List<ChangeStreamPartitionSplit> initial = new ArrayList<>();
            long id = 0;
            for (ByteStringRange partition : client.generateInitialPartitions()) {
                initial.add(
                        new ChangeStreamPartitionSplit(
                                splitId(id++), partition, Collections.emptyList(), start));
            }
            return Initialization.fresh(start, id, initial);
        }

        List<ChangeStreamPartitionSplit> restoredUnassigned =
                restartExpiredSplits(resolver, restoredState.getUnassignedSplits());
        // Every assigned split is in some reader's restored state, which that reader checks
        // against retention itself, closed ones included. The ledger's copy carries only the
        // positions of the previous completed checkpoint, so checking it here could refuse a
        // restore whose readers are still within retention.
        List<ChangeStreamPartitionSplit> restoredAssigned = restoredState.getAssignedSplits();
        List<PendingMerge> restoredMerges = new ArrayList<>();
        long restoredNextSplitId = restoredState.getNextSplitId();
        for (PendingMerge merge : restoredState.getPendingMerges()) {
            Optional<Instant> fallback =
                    resolver.resolveRestored(
                            RowRanges.format(merge.getPartition()),
                            merge.getLowWatermark(),
                            resumeFallback);
            if (fallback.isPresent()) {
                restoredUnassigned.add(
                        new ChangeStreamPartitionSplit(
                                splitId(restoredNextSplitId++),
                                merge.getPartition(),
                                Collections.emptyList(),
                                fallback.get()));
            } else {
                restoredMerges.add(merge);
            }
        }
        List<MissingPartition> restoredMissing =
                new ArrayList<>(restoredState.getMissingPartitions().size());
        for (MissingPartition missing : restoredState.getMissingPartitions()) {
            // Of the two instants a MissingPartition carries, expiry is about the low
            // watermark a tokenless restart would read from, not the grace timer.
            Optional<Instant> fallback =
                    resolver.resolveRestored(
                            RowRanges.format(missing.getPartition()),
                            missing.getLowWatermark(),
                            resumeFallback);
            restoredMissing.add(fallback.map(missing::restartAt).orElse(missing));
        }
        return Initialization.restored(
                restoredState.getStartTime(),
                restoredNextSplitId,
                restoredUnassigned,
                restoredAssigned,
                restoredMerges,
                restoredMissing,
                restoredState.getCompletedPartitions(),
                restoredState.getRereadSplitIds());
    }

    private List<ChangeStreamPartitionSplit> restartExpiredSplits(
            StartPositionResolver resolver, List<ChangeStreamPartitionSplit> splits)
            throws Exception {
        List<ChangeStreamPartitionSplit> resolved = new ArrayList<>(splits.size());
        for (ChangeStreamPartitionSplit split : splits) {
            Optional<Instant> fallback =
                    resolver.resolveRestored(
                            RowRanges.format(split.getPartition()),
                            split.getLowWatermark(),
                            resumeFallback);
            resolved.add(fallback.map(split::restartAt).orElse(split));
        }
        return resolved;
    }

    private void onInitialized(@Nullable Initialization result, @Nullable Throwable error) {
        if (closed) {
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to initialize Bigtable Change Streams partitions.", error);
        }
        Initialization initialization =
                Preconditions.checkNotNull(result, "initialization result must not be null");
        resolvedStartTime = initialization.startTime;
        nextSplitId = initialization.nextSplitId;
        unassigned.addAll(initialization.unassigned);
        refreshUnassignedMetrics();
        for (ChangeStreamPartitionSplit split : initialization.assigned) {
            assigned.put(split.splitId(), split);
        }
        for (PendingMerge merge : initialization.pendingMerges) {
            PendingMergeAccumulator restored = PendingMergeAccumulator.restore(merge);
            pendingMerges.put(restored.partitionKey(), restored);
        }
        missingPartitions.addAll(initialization.missingPartitions);
        completedPartitions.addAll(initialization.completedPartitions);
        rereads.addAll(initialization.rereads);
        if (initialization.fresh) {
            partitionsDiscovered.inc(initialization.unassigned.size());
        }
        initialized = true;
        LOG.info(
                "Initialized Bigtable Change Streams at {} with {} unassigned, {} assigned, and"
                        + " {} pending merge partition(s).",
                resolvedStartTime,
                unassigned.size(),
                assigned.size(),
                pendingMerges.size());
        for (DeferredAction deferred : deferredActions) {
            deferred.replay(this);
        }
        deferredActions.clear();
        serveAvailableReaders();
        if (reconciliationEnabled) {
            context.callAsync(
                    this::reconciliationScan,
                    this::onReconciled,
                    RECONCILIATION_SCAN_INTERVAL.toMillis(),
                    RECONCILIATION_SCAN_INTERVAL.toMillis());
        }
    }

    private ReconciliationScan reconciliationScan() throws Exception {
        Duration retention = reconciliationRetention;
        Instant fetchedAt = reconciliationRetentionFetchedAt;
        Instant now = Instant.now(clock);
        if (retention == null
                || fetchedAt == null
                || fetchedAt.plus(RETENTION_REFRESH_INTERVAL).isBefore(now)) {
            retention = client.retention();
            reconciliationRetention = retention;
            reconciliationRetentionFetchedAt = now;
        }
        return new ReconciliationScan(client.generateInitialPartitions(), retention);
    }

    private void onReconciled(@Nullable ReconciliationScan scan, @Nullable Throwable error) {
        if (closed) {
            return;
        }
        if (boundedComplete) {
            return;
        }
        if (error != null) {
            LOG.warn("Bigtable Change Streams partition reconciliation scan failed.", error);
            return;
        }
        List<ChangeStreamPartitionSplit> ledger = new ArrayList<>(unassigned);
        ledger.addAll(assigned.values());
        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Preconditions.checkNotNull(scan, "reconciliation scan must not be null")
                                .partitions,
                        ledger,
                        completedPartitions,
                        pendingMergeSnapshot(),
                        missingPartitions,
                        Instant.now(clock),
                        ledgerLowWatermark());
        missingPartitions.clear();
        missingPartitions.addAll(result.missing);
        // Rendered before the loop, because the loop adds the restarts themselves to the ledger:
        // what a reader of the warning needs is what the scan saw, not what it left behind.
        String ledgerDescription = tokenlessAmong(result.recoveries) ? describeLedger() : "";
        for (ChangeStreamPartitionReconciler.Recovery recovery : result.recoveries) {
            // A recovery continues from the parents' tokens, or restarts part of their range, so it
            // waits for those parents' closes as a completed merge would. A tokenless restart may
            // cover only part of a merge target, after the service split it again.
            Set<String> parents = new LinkedHashSet<>();
            Iterator<PendingMergeAccumulator> merges = pendingMerges.values().iterator();
            while (merges.hasNext()) {
                PendingMergeAccumulator merge = merges.next();
                boolean replaced =
                        merge.tokensAreContainedBy(recovery.tokens)
                                || (recovery.tokenless
                                        && merge.partitionKey().equals(recovery.partition));
                if (replaced
                        || merge.sharesATokenWith(recovery.tokens)
                        || (recovery.tokenless
                                && overlaps(merge.partitionKey(), recovery.partition))) {
                    parents.addAll(merge.getParentSplitIds());
                }
                if (replaced) {
                    merges.remove();
                }
            }
            Instant recoveryLowWatermark =
                    recovery.tokenless
                            ? clampToRetention(
                                    recovery.lowWatermark, scan.retention, Instant.now(clock))
                            : recovery.lowWatermark;
            ChangeStreamPartitionSplit restart =
                    new ChangeStreamPartitionSplit(
                            splitId(nextSplitId++),
                            recovery.partition,
                            recovery.tokens,
                            recoveryLowWatermark);
            unassigned.add(restart);
            holdUntilParentsSettle(restart.splitId(), parents);
            partitionsReconciled.inc();
            if (recovery.tokenless) {
                tokenlessRestarts.inc();
                LOG.warn(
                        "Restarting missing Bigtable Change Streams partition {} without a"
                                + " continuation token at low watermark {} after {}. The scan"
                                + " found it uncovered by {}.",
                        RowRanges.format(recovery.partition),
                        recoveryLowWatermark,
                        ChangeStreamPartitionReconciler.TOKENLESS_GRACE,
                        ledgerDescription);
            }
        }
        refreshUnassignedMetrics();
        serveAvailableReaders();
        // A scan that recovered the last missing partition of an otherwise drained bounded run is
        // the moment that run became complete, and no reader event is owed afterwards to notice it.
        signalBoundedCompletionIfDrained();
    }

    private static boolean overlaps(ByteStringRange left, ByteStringRange right) {
        return !RowRanges.intersect(
                        Collections.singletonList(left), Collections.singletonList(right))
                .isEmpty();
    }

    private static Instant clampToRetention(Instant lowWatermark, Duration retention, Instant now) {
        Instant earliest = now.minus(retention).plus(RETENTION_CLAMP_MARGIN);
        return lowWatermark.isBefore(earliest) ? earliest : lowWatermark;
    }

    private static boolean tokenlessAmong(
            List<ChangeStreamPartitionReconciler.Recovery> recoveries) {
        for (ChangeStreamPartitionReconciler.Recovery recovery : recoveries) {
            if (recovery.tokenless) {
                return true;
            }
        }
        return false;
    }

    /**
     * Renders the four collections a gap is computed against.
     *
     * <p>A tokenless restart says a range was uncovered; only this says by what. The three
     * explanations it separates are an incomplete merge still waiting for a parent token, a bounded
     * range already read to the end time, and a ledger that genuinely holds nothing (#951).
     *
     * @return the ledger, split by the collection each range sits in
     */
    private String describeLedger() {
        List<ByteStringRange> unassignedRanges = new ArrayList<>(unassigned.size());
        for (ChangeStreamPartitionSplit split : unassigned) {
            unassignedRanges.add(split.getPartition());
        }
        List<ByteStringRange> assignedRanges = new ArrayList<>(assigned.size());
        for (ChangeStreamPartitionSplit split : assigned.values()) {
            assignedRanges.add(split.getPartition());
        }
        return "unassigned "
                + describeRanges(unassignedRanges)
                + ", assigned "
                + describeRanges(assignedRanges)
                + ", pending merges "
                + describeRanges(new ArrayList<>(pendingMerges.keySet()))
                + ", completed "
                + describeRanges(completedPartitions);
    }

    private static String describeRanges(List<ByteStringRange> ranges) {
        if (ranges.isEmpty()) {
            return "none";
        }
        StringBuilder rendered = new StringBuilder();
        for (ByteStringRange range : ranges) {
            if (rendered.length() > 0) {
                rendered.append(' ');
            }
            rendered.append(RowRanges.format(range));
        }
        return rendered.toString();
    }

    private Instant ledgerLowWatermark() {
        Instant low = null;
        for (ChangeStreamPartitionSplit split : unassigned) {
            if (low == null || split.getLowWatermark().isBefore(low)) {
                low = split.getLowWatermark();
            }
        }
        for (ChangeStreamPartitionSplit split : assigned.values()) {
            if (low == null || split.getLowWatermark().isBefore(low)) {
                low = split.getLowWatermark();
            }
        }
        for (PendingMergeAccumulator merge : pendingMerges.values()) {
            if (low == null || merge.getLowWatermark().isBefore(low)) {
                low = merge.getLowWatermark();
            }
        }
        return low == null ? resolvedStartTime : low;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // A reader that asks for work has capacity for at least one split, whether or not it has
        // advertised a figure yet; the assignment waits for initialization, the floor does not.
        ReaderState reader = readers.computeIfAbsent(subtaskId, ignored -> new ReaderState());
        reader.freeSlots = Math.max(reader.inFlight() + 1, reader.freeSlots);
        if (!initialized) {
            return;
        }
        assignAvailable(subtaskId);
    }

    private void assignAvailable(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            readers.remove(subtaskId);
            return;
        }
        ReaderState reader = readers.get(subtaskId);
        int capacity = reader == null ? 0 : reader.free();
        if (capacity <= 0 || !hasAssignable()) {
            // Called for its effect, not its verdict: a bounded run with nothing left anywhere
            // signals no-more-splits here, and there is nothing to assign either way.
            signalBoundedCompletionIfDrained();
            return;
        }
        List<ChangeStreamPartitionSplit> splits = new ArrayList<>(capacity);
        while (splits.size() < capacity && hasAssignable()) {
            ChangeStreamPartitionSplit split = takeAssignable();
            assigned.put(split.splitId(), split);
            splits.add(split);
        }
        refreshUnassignedMetrics();
        reader.sentSplits += splits.size();
        splitsAssigned.inc(splits.size());
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    @Override
    public void addSplitsBack(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        if (!initialized) {
            // A capacity report deferred from the failed attempt would be replayed against the
            // next one, which registers under the same subtask: splits sent on it would count
            // against state the returned splits then drop, and the next attempt would receive
            // them uncounted. Its progress and transition events still describe its splits.
            deferredActions.removeIf(
                    deferred ->
                            deferred instanceof DeferredSourceEvent
                                    && ((DeferredSourceEvent) deferred).subtaskId == subtaskId
                                    && ((DeferredSourceEvent) deferred).sourceEvent
                                            instanceof ReaderCapacityEvent);
            deferredActions.add(new DeferredSplitsBack(splits, subtaskId));
            return;
        }
        addSplitsBackInitialized(splits, subtaskId);
    }

    /**
     * Takes back what a region failover returns: the splits Flink assigned since the checkpoint the
     * failed subtask restores. That subtask's downstream rolled back to the checkpoint too, so each
     * split restarts from where it was assigned, and a split whose close no completed checkpoint
     * covers yet is read again.
     */
    private void addSplitsBackInitialized(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        readers.remove(subtaskId);
        toldNoMoreSplits.remove(subtaskId);
        for (ChangeStreamPartitionSplit split : splits) {
            if (assigned.remove(split.splitId()) != null) {
                unassigned.add(split);
            } else if (pendingCloses.remove(split.splitId()) != null) {
                rereads.add(split.splitId());
                unassigned.add(split);
                // A bounded run that had drained has work again; readers that registered since
                // were told nothing is coming, and the next drain tells only the ones not told.
                boundedComplete = false;
            } else {
                LOG.warn(
                        "Ignoring returned split {}, which is no longer in the ledger: either a"
                                + " completed checkpoint covers its close, or it closed while no"
                                + " checkpoint completed.",
                        split.splitId());
            }
        }
        refreshUnassignedMetrics();
        splitsReturned.inc(splits.size());
        serveAvailableReaders();
    }

    @Override
    public void addReader(int subtaskId) {
        // The drain-time broadcast reaches only readers already registered. Flink registers a
        // later reader in the context before calling this method, so replay the terminal signal.
        // A new attempt of a subtask was told nothing yet, whatever its previous attempt was.
        toldNoMoreSplits.remove(subtaskId);
        readerAttempts.merge(subtaskId, 1, Integer::sum);
        if (boundedComplete) {
            signalNoMoreSplits(subtaskId);
            return;
        }
        // Readers request their first partition when they start.
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!initialized) {
            deferredActions.add(
                    new DeferredSourceEvent(subtaskId, readerAttempt(subtaskId), sourceEvent));
            return;
        }
        handleSourceEventInitialized(subtaskId, readerAttempt(subtaskId), sourceEvent);
    }

    private int readerAttempt(int subtaskId) {
        return readerAttempts.getOrDefault(subtaskId, 0);
    }

    /**
     * Applies a reader event.
     *
     * @param attempt the subtask's reader attempt that sent it, counted by {@link #addReader}
     */
    private void handleSourceEventInitialized(int subtaskId, int attempt, SourceEvent sourceEvent) {
        if (sourceEvent instanceof ReaderCapacityEvent) {
            ReaderCapacityEvent capacity = (ReaderCapacityEvent) sourceEvent;
            ReaderState reader = readers.computeIfAbsent(subtaskId, ignored -> new ReaderState());
            reader.freeSlots = capacity.getFreeSlots();
            reader.heldSplits = capacity.getHeldSplits();
            reader.receivedSplits = capacity.getReceivedSplits();
            reader.reported = true;
            assignAvailable(subtaskId);
            assignBeyondFreeSlots();
            return;
        }
        if (sourceEvent instanceof PartitionProgressEvent) {
            for (PartitionProgressEvent.Progress progress :
                    ((PartitionProgressEvent) sourceEvent).getPartitions()) {
                ChangeStreamPartitionSplit current = assigned.get(progress.getSplitId());
                if (current != null) {
                    assigned.put(
                            current.splitId(),
                            new ChangeStreamPartitionSplit(
                                    current.splitId(),
                                    current.getPartition(),
                                    progress.getContinuationTokens(),
                                    progress.getLowWatermark()));
                }
            }
            return;
        }
        if (sourceEvent instanceof PartitionsRestoredEvent) {
            acceptRestoredPartitions((PartitionsRestoredEvent) sourceEvent);
            return;
        }
        if (!(sourceEvent instanceof PartitionTransitionEvent)) {
            throw new IllegalArgumentException(
                    "Unsupported Bigtable Change Streams source event " + sourceEvent + ".");
        }
        PartitionTransitionEvent transition = (PartitionTransitionEvent) sourceEvent;
        String splitId = transition.getFinishedSplitId();
        acknowledgeTransition(subtaskId, attempt, splitId);
        ChangeStreamPartitionSplit finished = assigned.remove(splitId);
        if (finished == null) {
            LOG.info(
                    "Ignoring a repeated transition for closed split {} from subtask {}; its"
                            + " successors already exist.",
                    splitId,
                    subtaskId);
            return;
        }
        boolean reread = rereads.contains(splitId);
        if (closesFinalAtOnce) {
            rereads.remove(splitId);
        } else {
            pendingCloses.put(
                    splitId,
                    new PendingClose(finished, clock.millis(), holdsSuccessorsOf(transition)));
        }
        if (reread) {
            LOG.info(
                    "Split {} closed again after a rollback; its successors already exist.",
                    splitId);
        } else {
            if (bounded && transition.getSuccessors().isEmpty()) {
                // No successor means an OK CloseStream, which java-bigtable's own precondition
                // proves carries no continuation token: the service ended the stream at the end
                // time rather than at a split or a merge, so this range is the run's finished
                // business. getPartition() already hands back an independent normalised copy.
                completedPartitions.add(finished.getPartition());
            }
            for (PartitionTransitionEvent.Successor successor : transition.getSuccessors()) {
                acceptSuccessor(
                        successor.getPartition(),
                        successor.getContinuationToken(),
                        transition.getLowWatermark(),
                        splitId);
            }
        }
        if (reread && closesFinalAtOnce) {
            // A close read again after a rollback settles at once here, which frees what waited
            // for it; any other close settles only when a checkpoint completes.
            releaseSettledSuccessors();
        }
        refreshUnassignedMetrics();
        serveAvailableReaders();
        signalBoundedCompletionIfDrained();
    }

    /**
     * Acknowledges a transition to the reader attempt that sent it. An attempt that has failed
     * since has no gateway to send to, and its restored attempt reports the transition again; a
     * later attempt of the same subtask must not take the acknowledgement as its own, because it
     * may close the same split again and would then drop that close from its checkpoint. Flink
     * names the sending attempt only under concurrent execution attempts, so the enumerator counts
     * the registrations itself.
     */
    private void acknowledgeTransition(int subtaskId, int attempt, String splitId) {
        if (attempt == readerAttempt(subtaskId)
                && context.registeredReaders().containsKey(subtaskId)) {
            context.sendEventToSourceReader(subtaskId, new PartitionTransitionAckEvent(splitId));
        }
    }

    /**
     * Takes back the close of every restored split the ledger still holds as pending: its reader
     * reads it again from the restored checkpoint.
     */
    private void acceptRestoredPartitions(PartitionsRestoredEvent restored) {
        for (String splitId : restored.getSplitIds()) {
            PendingClose pending = pendingCloses.remove(splitId);
            if (pending == null) {
                continue;
            }
            rereads.add(splitId);
            assigned.put(splitId, pending.split);
        }
    }

    private void acceptSuccessor(
            ByteStringRange partition,
            ChangeStreamContinuationToken token,
            Instant lowWatermark,
            String parentSplitId) {
        ByteStringRange partitionKey = RowRanges.copyOf(partition);
        PendingMergeAccumulator merge = pendingMerges.get(partitionKey);
        if (merge == null) {
            merge = new PendingMergeAccumulator(partition, lowWatermark);
            pendingMerges.put(partitionKey, merge);
            partitionsDiscovered.inc();
        }
        merge.add(token, lowWatermark);
        merge.addParent(parentSplitId);
        if (!merge.isComplete()) {
            return;
        }
        pendingMerges.remove(partitionKey);
        PendingMerge completed = merge.toPendingMerge();
        if (completed.getContinuationTokens().size() > 1) {
            partitionMerges.inc();
        } else {
            partitionSplits.inc();
        }
        ChangeStreamPartitionSplit successor =
                new ChangeStreamPartitionSplit(
                        splitId(nextSplitId++),
                        completed.getPartition(),
                        completed.getContinuationTokens(),
                        completed.getLowWatermark());
        unassigned.add(successor);
        holdUntilParentsSettle(successor.splitId(), merge.getParentSplitIds());
    }

    /** Holds a new unassigned successor until every one of these parents' closes has settled. */
    private void holdUntilParentsSettle(String successorId, Set<String> parents) {
        if (!holdsSuccessors()) {
            return;
        }
        Set<String> waitingFor = new LinkedHashSet<>();
        for (String parent : parents) {
            if (!closeIsSettled(parent)) {
                waitingFor.add(parent);
            }
        }
        if (!waitingFor.isEmpty()) {
            heldSuccessors.put(successorId, waitingFor);
        }
    }

    /** Whether successors wait at all: at parallelism 1 every record takes the same path. */
    private boolean holdsSuccessors() {
        return holdSuccessorsUntilCheckpoint && context.currentParallelism() > 1;
    }

    private boolean holdsSuccessorsOf(PartitionTransitionEvent transition) {
        // A partition whose reader emitted nothing has nothing downstream for a successor to
        // overtake.
        return holdsSuccessors() && transition.hasEmittedRecords();
    }

    /**
     * Whether a successor no longer waits for this parent: its close holds nothing back, or it is
     * final, and it is not being read again after a rollback.
     */
    private boolean closeIsSettled(String parentSplitId) {
        if (rereads.contains(parentSplitId)) {
            return false;
        }
        PendingClose pending = pendingCloses.get(parentSplitId);
        return pending == null || !pending.holdsSuccessors;
    }

    /** Hands out the held successors whose every parent close has settled. */
    private void releaseSettledSuccessors() {
        if (heldSuccessors.isEmpty()) {
            return;
        }
        boolean released = false;
        Iterator<Set<String>> held = heldSuccessors.values().iterator();
        while (held.hasNext()) {
            Set<String> waitingFor = held.next();
            waitingFor.removeIf(this::closeIsSettled);
            if (waitingFor.isEmpty()) {
                held.remove();
                released = true;
            }
        }
        if (released) {
            serveAvailableReaders();
        }
    }

    /** Whether {@code unassigned} holds a split that no parent close holds back. */
    private boolean hasAssignable() {
        return unassigned.size() > heldSuccessors.size();
    }

    /** Removes and returns the first unassigned split that no parent close holds back. */
    private ChangeStreamPartitionSplit takeAssignable() {
        Iterator<ChangeStreamPartitionSplit> candidates = unassigned.iterator();
        while (candidates.hasNext()) {
            ChangeStreamPartitionSplit candidate = candidates.next();
            if (!heldSuccessors.containsKey(candidate.splitId())) {
                candidates.remove();
                return candidate;
            }
        }
        throw new IllegalStateException("No unassigned split is free of a hold.");
    }

    private void serveAvailableReaders() {
        for (int subtaskId : new ArrayList<>(readers.keySet())) {
            if (!hasAssignable()) {
                break;
            }
            assignAvailable(subtaskId);
        }
        assignBeyondFreeSlots();
    }

    /**
     * Hands every split still unassigned to the readers holding the fewest, one at a time.
     *
     * <p>Free slots are filled first, by {@code assignAvailable}; what this hands out goes to a
     * reader's queue, which the reader rotates through its open reads at service heartbeats.
     * Waiting for a free slot instead would leave a split unread for as long as the reader's
     * partitions stay stable, which for an unbounded source can be until the split's position
     * leaves the table's change-stream retention (#1893).
     *
     * <p>It waits until every subtask has registered and reported, so that the readers that start
     * first do not take every split. A reader that failed is unregistered until it starts again,
     * which holds the excess back for that long rather than piling it on the others.
     */
    private void assignBeyondFreeSlots() {
        if (!hasAssignable() || boundedComplete) {
            return;
        }
        Map<Integer, ?> registered = context.registeredReaders();
        if (registered.size() < context.currentParallelism()) {
            return;
        }
        for (int subtaskId : registered.keySet()) {
            ReaderState reader = readers.get(subtaskId);
            if (reader == null || !reader.reported) {
                return;
            }
        }
        Map<Integer, List<ChangeStreamPartitionSplit>> batches = new LinkedHashMap<>();
        int handedOut = 0;
        while (hasAssignable()) {
            int target = -1;
            int fewest = Integer.MAX_VALUE;
            for (int subtaskId : registered.keySet()) {
                int load = readers.get(subtaskId).load();
                if (load < fewest || (load == fewest && subtaskId < target)) {
                    target = subtaskId;
                    fewest = load;
                }
            }
            ChangeStreamPartitionSplit split = takeAssignable();
            assigned.put(split.splitId(), split);
            batches.computeIfAbsent(target, ignored -> new ArrayList<>()).add(split);
            readers.get(target).sentSplits++;
            handedOut++;
        }
        refreshUnassignedMetrics();
        splitsAssigned.inc(handedOut);
        for (Map.Entry<Integer, List<ChangeStreamPartitionSplit>> batch : batches.entrySet()) {
            context.assignSplits(
                    new SplitsAssignment<>(
                            Collections.singletonMap(batch.getKey(), batch.getValue())));
        }
    }

    private boolean signalBoundedCompletionIfDrained() {
        if (!bounded
                || boundedComplete
                || !unassigned.isEmpty()
                || !assigned.isEmpty()
                || !pendingMerges.isEmpty()
                || !missingPartitions.isEmpty()) {
            return false;
        }
        boundedComplete = true;
        for (int subtaskId : context.registeredReaders().keySet()) {
            signalNoMoreSplits(subtaskId);
        }
        readers.clear();
        return true;
    }

    private void signalNoMoreSplits(int subtaskId) {
        if (toldNoMoreSplits.add(subtaskId)) {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public BigtableChangeStreamEnumeratorState snapshotState(long checkpointId) {
        Preconditions.checkState(
                initialized,
                "Bigtable Change Streams initialization is still outstanding; retry the"
                        + " checkpoint after its deferred reader actions have been replayed.");
        for (PendingClose pending : pendingCloses.values()) {
            if (pending.covering == UNCOVERED) {
                pending.covering = checkpointId;
            }
        }
        List<String> checkpointedRereads = new ArrayList<>();
        for (String splitId : rereads) {
            if (!pendingCloses.containsKey(splitId)) {
                checkpointedRereads.add(splitId);
            }
        }
        return new BigtableChangeStreamEnumeratorState(
                initialized,
                resolvedStartTime,
                nextSplitId,
                new ArrayList<>(unassigned),
                new ArrayList<>(assigned.values()),
                pendingMergeSnapshot(),
                missingPartitions,
                completedPartitions,
                checkpointedRereads);
    }

    /** Makes final every pending close the completed checkpoint covers. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (closed) {
            return;
        }
        checkpointCompleted = true;
        if (closesFinalAtOnce) {
            LOG.info(
                    "Checkpoint {} completed, so a closed partition is final again only once a"
                            + " completed checkpoint covers it{}.",
                    checkpointId,
                    holdsSuccessors() ? ", and its successors wait for that" : "");
            closesFinalAtOnce = false;
        }
        Iterator<Map.Entry<String, PendingClose>> pending = pendingCloses.entrySet().iterator();
        while (pending.hasNext()) {
            Map.Entry<String, PendingClose> close = pending.next();
            long covering = close.getValue().covering;
            if (covering != UNCOVERED && covering <= checkpointId) {
                pending.remove();
                rereads.remove(close.getKey());
            }
        }
        releaseSettledSuccessors();
    }

    /**
     * Makes every close final at once when a close has waited {@code firstCheckpointTimeout} and no
     * checkpoint has completed: without one, pending closes would pile up for good.
     */
    private void checkFirstCheckpoint() {
        if (closed || checkpointCompleted || closesFinalAtOnce || pendingCloses.isEmpty()) {
            return;
        }
        // Insertion order: a close taken back and made again moves to the end.
        long oldest = pendingCloses.values().iterator().next().closedAtMillis;
        if (clock.millis() - oldest < firstCheckpointTimeout.toMillis()) {
            return;
        }
        boolean holding = !heldSuccessors.isEmpty();
        for (PendingClose pending : pendingCloses.values()) {
            holding |= pending.holdsSuccessors;
        }
        if (holding) {
            LOG.warn(
                    "A closed partition has waited {} for a completed checkpoint and none has"
                            + " completed, so until one does, a closed partition is final at once,"
                            + " a region failover does not read it again, and its successors start"
                            + " at once. Without a checkpoint, a row key's changes can reach a"
                            + " keyed operator out of commit order across a partition split or"
                            + " merge. Enable checkpointing; raise firstCheckpointTimeout"
                            + " ('scan.first-checkpoint-timeout') for a job that checkpoints less"
                            + " often; or set partitionHandoff(IMMEDIATE)"
                            + " ('scan.change-stream.partition-handoff' = 'immediate') to start"
                            + " successors at once.",
                    firstCheckpointTimeout);
        } else {
            LOG.warn(
                    "A closed partition has waited {} for a completed checkpoint and none has"
                            + " completed, so a closed partition is final at once, and a region"
                            + " failover does not read it again, until one does. Enable"
                            + " checkpointing, or raise firstCheckpointTimeout"
                            + " ('scan.first-checkpoint-timeout') for a job that checkpoints less"
                            + " often.",
                    firstCheckpointTimeout);
        }
        closesFinalAtOnce = true;
        for (String splitId : pendingCloses.keySet()) {
            rereads.remove(splitId);
        }
        pendingCloses.clear();
        releaseSettledSuccessors();
    }

    /** Returns how long a close waits for a completed checkpoint, as the source configured it. */
    @VisibleForTesting
    public Duration getFirstCheckpointTimeout() {
        return firstCheckpointTimeout;
    }

    @VisibleForTesting
    int pendingCloseCount() {
        return pendingCloses.size();
    }

    @VisibleForTesting
    int heldSuccessorCount() {
        return heldSuccessors.size();
    }

    private List<PendingMerge> pendingMergeSnapshot() {
        List<PendingMerge> snapshot = new ArrayList<>(pendingMerges.size());
        for (PendingMergeAccumulator merge : pendingMerges.values()) {
            snapshot.add(merge.toPendingMerge());
        }
        return snapshot;
    }

    /** Sums what the accumulators counted, so a test can hold the whole enumerator to the bound. */
    @VisibleForTesting
    long pendingMergeMaterializations() {
        long materializations = 0;
        for (PendingMergeAccumulator merge : pendingMerges.values()) {
            materializations += merge.getMaterializations();
        }
        return materializations;
    }

    private void registerMetrics() {
        metrics = EnumeratorMetrics.of(context.metricGroup());
        splitsAssigned = metrics.counter(BigtableMetricNames.SPLITS_ASSIGNED);
        splitsReturned = metrics.counter(BigtableMetricNames.SPLITS_RETURNED);
        partitionsReconciled =
                metrics.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED);
        tokenlessRestarts = metrics.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS);
        partitionsDiscovered =
                metrics.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_DISCOVERED);
        partitionSplits = metrics.counter(BigtableMetricNames.CHANGE_STREAM_PARTITION_SPLITS);
        partitionMerges = metrics.counter(BigtableMetricNames.CHANGE_STREAM_PARTITION_MERGES);
        metrics.gauge(
                BigtableMetricNames.UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::unassignedLagMillis);
        metrics.unassignedSplitsGauge(() -> (long) unassignedMetricCount.get());
        if (closed) {
            // close() ran first, on Flink's closer thread, and closed the view this replaced: let
            // go
            // of what was just bound rather than leave it reading a closed enumerator.
            metrics.close();
        }
    }

    private long unassignedLagMillis() {
        long oldest = oldestUnassignedPositionMillis.get();
        return oldest == Long.MAX_VALUE ? 0 : MetricValues.elapsedMillis(clock.millis(), oldest);
    }

    private void refreshUnassignedMetrics() {
        long oldest = Long.MAX_VALUE;
        for (ChangeStreamPartitionSplit split : unassigned) {
            oldest = Math.min(oldest, split.getLowWatermark().toEpochMilli());
        }
        unassignedMetricCount.set(unassigned.size());
        oldestUnassignedPositionMillis.set(oldest);
    }

    @Override
    public void close() throws IOException {
        // The flag first: a start() still registering sees it and lets go of what it bound.
        closed = true;
        metrics.close();
        try {
            Closers.closeAll(client);
        } catch (Exception e) {
            throw new IOException("Failed to close the Bigtable Change Streams coordinator.", e);
        }
    }

    private static String splitId(long id) {
        return "change-stream-" + id;
    }

    private static final class Initialization {

        private final Instant startTime;
        private final long nextSplitId;
        private final List<ChangeStreamPartitionSplit> unassigned;
        private final List<ChangeStreamPartitionSplit> assigned;
        private final List<PendingMerge> pendingMerges;
        private final List<MissingPartition> missingPartitions;
        private final List<ByteStringRange> completedPartitions;
        private final List<String> rereads;
        private final boolean fresh;

        private Initialization(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions,
                List<ByteStringRange> completedPartitions,
                List<String> rereads,
                boolean fresh) {
            this.startTime = startTime;
            this.nextSplitId = nextSplitId;
            this.unassigned = unassigned;
            this.assigned = assigned;
            this.pendingMerges = pendingMerges;
            this.missingPartitions = missingPartitions;
            this.completedPartitions = completedPartitions;
            this.rereads = rereads;
            this.fresh = fresh;
        }

        private static Initialization fresh(
                Instant startTime, long nextSplitId, List<ChangeStreamPartitionSplit> partitions) {
            return new Initialization(
                    startTime,
                    nextSplitId,
                    partitions,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    true);
        }

        private static Initialization restored(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions,
                List<ByteStringRange> completedPartitions,
                List<String> rereads) {
            return new Initialization(
                    startTime,
                    nextSplitId,
                    unassigned,
                    assigned,
                    pendingMerges,
                    missingPartitions,
                    completedPartitions,
                    rereads,
                    false);
        }
    }

    /**
     * A closed partition, when it closed, whether its successors wait for the close to become
     * final, and the first checkpoint whose snapshot ran after its close.
     */
    private static final class PendingClose {

        private final ChangeStreamPartitionSplit split;
        private final long closedAtMillis;
        private final boolean holdsSuccessors;
        private long covering = UNCOVERED;

        private PendingClose(
                ChangeStreamPartitionSplit split, long closedAtMillis, boolean holdsSuccessors) {
            this.split = split;
            this.closedAtMillis = closedAtMillis;
            this.holdsSuccessors = holdsSuccessors;
        }
    }

    private static final class ReconciliationScan {
        private final List<ByteStringRange> partitions;
        private final Duration retention;

        private ReconciliationScan(List<ByteStringRange> partitions, Duration retention) {
            this.partitions = partitions;
            this.retention = retention;
        }
    }

    private interface DeferredAction {

        void replay(BigtableChangeStreamSplitEnumerator enumerator);
    }

    private static final class DeferredSourceEvent implements DeferredAction {

        private final int subtaskId;
        private final int attempt;
        private final SourceEvent sourceEvent;

        private DeferredSourceEvent(int subtaskId, int attempt, SourceEvent sourceEvent) {
            this.subtaskId = subtaskId;
            this.attempt = attempt;
            this.sourceEvent = sourceEvent;
        }

        @Override
        public void replay(BigtableChangeStreamSplitEnumerator enumerator) {
            enumerator.handleSourceEventInitialized(subtaskId, attempt, sourceEvent);
        }
    }

    private static final class DeferredSplitsBack implements DeferredAction {

        private final List<ChangeStreamPartitionSplit> splits;
        private final int subtaskId;

        private DeferredSplitsBack(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
            this.splits = new ArrayList<>(splits);
            this.subtaskId = subtaskId;
        }

        @Override
        public void replay(BigtableChangeStreamSplitEnumerator enumerator) {
            enumerator.addSplitsBackInitialized(splits, subtaskId);
        }
    }

    /**
     * What one reader attempt last reported, and the splits sent to it.
     *
     * <p>A report can be older than the splits sent after it, so it counts {@code receivedSplits},
     * and the difference from {@code sentSplits} is in flight: the reader holds those already, and
     * has a slot fewer for each, whatever its report says. A failed reader's state is dropped,
     * because its next attempt counts from zero and its last report described splits it has
     * returned.
     */
    private static final class ReaderState {

        private int freeSlots;
        private int heldSplits;
        private long receivedSplits;
        private long sentSplits;
        private boolean reported;

        private int inFlight() {
            return (int) Math.max(0, sentSplits - receivedSplits);
        }

        private int free() {
            return Math.max(0, freeSlots - inFlight());
        }

        private int load() {
            return heldSplits + inFlight();
        }
    }
}
