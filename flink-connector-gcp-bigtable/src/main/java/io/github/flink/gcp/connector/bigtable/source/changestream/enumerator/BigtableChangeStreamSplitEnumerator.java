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
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.BigtableMetricValues;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Coordinates Bigtable Change Streams partitions, split successors, and merge-parent tokens. */
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

    private final SplitEnumeratorContext<ChangeStreamPartitionSplit> context;
    private final ChangeStreamCoordinatorClient client;
    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    private final boolean bounded;
    private final boolean reconciliationEnabled;
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
    private final Map<Integer, Integer> readerCapacities = new LinkedHashMap<>();
    private final List<DeferredAction> deferredActions = new ArrayList<>();
    private final AtomicInteger unassignedMetricCount = new AtomicInteger();
    private final AtomicLong oldestUnassignedPositionMillis = new AtomicLong(Long.MAX_VALUE);

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter partitionsReconciled = new ThreadSafeSimpleCounter();
    private Counter tokenlessRestarts = new ThreadSafeSimpleCounter();
    private Counter partitionsDiscovered = new ThreadSafeSimpleCounter();
    private Counter partitionSplits = new ThreadSafeSimpleCounter();
    private Counter partitionMerges = new ThreadSafeSimpleCounter();
    private boolean initialized;
    private boolean boundedComplete;
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
     */
    public BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded) {
        this(
                context,
                client,
                startPosition,
                resumeFallback,
                restoredState,
                bounded,
                true,
                Clock.systemUTC());
    }

    /**
     * The test seam, exposing the two values the public constructor fixes.
     *
     * <p>A unit test taking the public constructor would schedule a real periodic reconciliation
     * scan against the wall clock, which is why both are parameters here rather than knobs there.
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
            Clock clock) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.client = Preconditions.checkNotNull(client, "client must not be null");
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback = resumeFallback;
        this.restoredState = restoredState;
        this.bounded = bounded;
        this.reconciliationEnabled = reconciliationEnabled;
        this.clock = Preconditions.checkNotNull(clock, "clock must not be null");
    }

    @Override
    public void start() {
        registerMetrics();
        context.callAsync(this::initialize, this::onInitialized);
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
        List<ChangeStreamPartitionSplit> restoredAssigned =
                restartExpiredSplits(resolver, restoredState.getAssignedSplits());
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
                restoredState.getCompletedPartitions());
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
            pendingMerges
                    .values()
                    .removeIf(
                            merge ->
                                    merge.tokensAreContainedBy(recovery.tokens)
                                            || (recovery.tokenless
                                                    && merge.partitionKey()
                                                            .equals(recovery.partition)));
            Instant recoveryLowWatermark =
                    recovery.tokenless
                            ? clampToRetention(
                                    recovery.lowWatermark, scan.retention, Instant.now(clock))
                            : recovery.lowWatermark;
            unassigned.add(
                    new ChangeStreamPartitionSplit(
                            splitId(nextSplitId++),
                            recovery.partition,
                            recovery.tokens,
                            recoveryLowWatermark));
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
        readerCapacities.put(subtaskId, Math.max(1, readerCapacities.getOrDefault(subtaskId, 0)));
        if (!initialized) {
            return;
        }
        assignAvailable(subtaskId);
    }

    private void assignAvailable(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            readerCapacities.remove(subtaskId);
            return;
        }
        int capacity = readerCapacities.getOrDefault(subtaskId, 0);
        if (capacity <= 0 || unassigned.isEmpty()) {
            // Called for its effect, not its verdict: a bounded run with nothing left anywhere
            // signals no-more-splits here, and there is nothing to assign either way.
            signalBoundedCompletionIfDrained();
            return;
        }
        List<ChangeStreamPartitionSplit> splits = new ArrayList<>(capacity);
        while (splits.size() < capacity && !unassigned.isEmpty()) {
            ChangeStreamPartitionSplit split = unassigned.removeFirst();
            assigned.put(split.splitId(), split);
            splits.add(split);
        }
        refreshUnassignedMetrics();
        readerCapacities.put(subtaskId, capacity - splits.size());
        splitsAssigned.inc(splits.size());
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    @Override
    public void addSplitsBack(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        if (!initialized) {
            deferredActions.add(new DeferredSplitsBack(splits, subtaskId));
            return;
        }
        addSplitsBackInitialized(splits, subtaskId);
    }

    private void addSplitsBackInitialized(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        readerCapacities.remove(subtaskId);
        for (ChangeStreamPartitionSplit split : splits) {
            if (assigned.remove(split.splitId()) != null) {
                unassigned.add(split);
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
        if (boundedComplete) {
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        // Readers request their first partition when they start.
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (!initialized) {
            deferredActions.add(new DeferredSourceEvent(subtaskId, sourceEvent));
            return;
        }
        handleSourceEventInitialized(subtaskId, sourceEvent);
    }

    private void handleSourceEventInitialized(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof ReaderCapacityEvent) {
            ReaderCapacityEvent capacity = (ReaderCapacityEvent) sourceEvent;
            readerCapacities.put(subtaskId, capacity.getFreeSlots());
            assignAvailable(subtaskId);
            return;
        }
        if (sourceEvent instanceof PartitionProgressEvent) {
            PartitionProgressEvent progress = (PartitionProgressEvent) sourceEvent;
            ChangeStreamPartitionSplit current = assigned.get(progress.getSplitId());
            if (current != null && progress.getLowWatermark().isAfter(current.getLowWatermark())) {
                assigned.put(
                        current.splitId(),
                        new ChangeStreamPartitionSplit(
                                current.splitId(),
                                current.getPartition(),
                                Collections.singletonList(progress.getContinuationToken()),
                                progress.getLowWatermark()));
            }
            return;
        }
        if (!(sourceEvent instanceof PartitionTransitionEvent)) {
            throw new IllegalArgumentException(
                    "Unsupported Bigtable Change Streams source event " + sourceEvent + ".");
        }
        PartitionTransitionEvent transition = (PartitionTransitionEvent) sourceEvent;
        ChangeStreamPartitionSplit finished = assigned.remove(transition.getFinishedSplitId());
        if (finished == null) {
            LOG.warn(
                    "Ignoring duplicate transition for finished split {} from subtask {}.",
                    transition.getFinishedSplitId(),
                    subtaskId);
            return;
        }
        if (bounded && transition.getSuccessors().isEmpty()) {
            // No successor means an OK CloseStream, which java-bigtable's own precondition proves
            // carries no continuation token: the service ended the stream at the end time rather
            // than at a split or a merge, so this range is the run's finished business.
            // getPartition() already hands back an independent normalised copy.
            completedPartitions.add(finished.getPartition());
        }
        for (PartitionTransitionEvent.Successor successor : transition.getSuccessors()) {
            acceptSuccessor(
                    successor.getPartition(),
                    successor.getContinuationToken(),
                    transition.getLowWatermark());
        }
        refreshUnassignedMetrics();
        serveAvailableReaders();
        signalBoundedCompletionIfDrained();
    }

    private void acceptSuccessor(
            ByteStringRange partition, ChangeStreamContinuationToken token, Instant lowWatermark) {
        ByteStringRange partitionKey = RowRanges.copyOf(partition);
        PendingMergeAccumulator merge = pendingMerges.get(partitionKey);
        if (merge == null) {
            merge = new PendingMergeAccumulator(partition, lowWatermark);
            pendingMerges.put(partitionKey, merge);
            partitionsDiscovered.inc();
        }
        merge.add(token, lowWatermark);
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
        unassigned.add(
                new ChangeStreamPartitionSplit(
                        splitId(nextSplitId++),
                        completed.getPartition(),
                        completed.getContinuationTokens(),
                        completed.getLowWatermark()));
    }

    private void serveAvailableReaders() {
        for (int subtaskId : new ArrayList<>(readerCapacities.keySet())) {
            if (unassigned.isEmpty()) {
                break;
            }
            assignAvailable(subtaskId);
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
            context.signalNoMoreSplits(subtaskId);
        }
        readerCapacities.clear();
        return true;
    }

    @Override
    public BigtableChangeStreamEnumeratorState snapshotState(long checkpointId) {
        Preconditions.checkState(
                initialized,
                "Bigtable Change Streams initialization is still outstanding; retry the"
                        + " checkpoint after its deferred reader actions have been replayed.");
        return new BigtableChangeStreamEnumeratorState(
                initialized,
                resolvedStartTime,
                nextSplitId,
                new ArrayList<>(unassigned),
                new ArrayList<>(assigned.values()),
                pendingMergeSnapshot(),
                missingPartitions,
                completedPartitions);
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
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        splitsAssigned =
                metricGroup.counter(
                        BigtableMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter());
        splitsReturned =
                metricGroup.counter(
                        BigtableMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter());
        partitionsReconciled =
                metricGroup.counter(
                        BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED,
                        new ThreadSafeSimpleCounter());
        tokenlessRestarts =
                metricGroup.counter(
                        BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS,
                        new ThreadSafeSimpleCounter());
        partitionsDiscovered =
                metricGroup.counter(
                        BigtableMetricNames.CHANGE_STREAM_PARTITIONS_DISCOVERED,
                        new ThreadSafeSimpleCounter());
        partitionSplits =
                metricGroup.counter(
                        BigtableMetricNames.CHANGE_STREAM_PARTITION_SPLITS,
                        new ThreadSafeSimpleCounter());
        partitionMerges =
                metricGroup.counter(
                        BigtableMetricNames.CHANGE_STREAM_PARTITION_MERGES,
                        new ThreadSafeSimpleCounter());
        metricGroup.gauge(
                BigtableMetricNames.UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::unassignedLagMillis);
        metricGroup.setUnassignedSplitsGauge(() -> (long) unassignedMetricCount.get());
    }

    private long unassignedLagMillis() {
        long oldest = oldestUnassignedPositionMillis.get();
        return oldest == Long.MAX_VALUE
                ? 0
                : BigtableMetricValues.elapsedMillis(clock.millis(), oldest);
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
        closed = true;
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
        private final boolean fresh;

        private Initialization(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions,
                List<ByteStringRange> completedPartitions,
                boolean fresh) {
            this.startTime = startTime;
            this.nextSplitId = nextSplitId;
            this.unassigned = unassigned;
            this.assigned = assigned;
            this.pendingMerges = pendingMerges;
            this.missingPartitions = missingPartitions;
            this.completedPartitions = completedPartitions;
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
                    true);
        }

        private static Initialization restored(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions,
                List<ByteStringRange> completedPartitions) {
            return new Initialization(
                    startTime,
                    nextSplitId,
                    unassigned,
                    assigned,
                    pendingMerges,
                    missingPartitions,
                    completedPartitions,
                    false);
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
        private final SourceEvent sourceEvent;

        private DeferredSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            this.subtaskId = subtaskId;
            this.sourceEvent = sourceEvent;
        }

        @Override
        public void replay(BigtableChangeStreamSplitEnumerator enumerator) {
            enumerator.handleSourceEventInitialized(subtaskId, sourceEvent);
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
}
