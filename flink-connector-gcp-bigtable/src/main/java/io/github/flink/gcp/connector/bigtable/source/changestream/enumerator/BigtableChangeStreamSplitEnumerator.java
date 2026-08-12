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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Counter;
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
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Coordinates Bigtable Change Streams partitions, split successors, and merge-parent tokens. */
@Internal
public final class BigtableChangeStreamSplitEnumerator
        implements SplitEnumerator<
                ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableChangeStreamSplitEnumerator.class);

    private final SplitEnumeratorContext<ChangeStreamPartitionSplit> context;
    private final ChangeStreamCoordinatorClient client;
    private final StartPosition startPosition;
    private final Optional<StartPosition> resumeFallback;
    private final boolean bounded;
    private final boolean reconciliationEnabled;
    private final Clock clock;
    @Nullable private final BigtableChangeStreamEnumeratorState restoredState;

    private final Deque<ChangeStreamPartitionSplit> unassigned = new ArrayDeque<>();
    private final Map<String, ChangeStreamPartitionSplit> assigned = new LinkedHashMap<>();
    private final Map<ByteStringRange, PendingMergeAccumulator> pendingMerges =
            new LinkedHashMap<>();
    private final List<MissingPartition> missingPartitions = new ArrayList<>();
    private final ChangeStreamPartitionReconciler reconciler =
            new ChangeStreamPartitionReconciler();
    private final Set<Integer> waitingReaders = new LinkedHashSet<>();
    private final List<DeferredAction> deferredActions = new ArrayList<>();

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter partitionsReconciled = new ThreadSafeSimpleCounter();
    private Counter tokenlessRestarts = new ThreadSafeSimpleCounter();
    private boolean initialized;
    private boolean boundedComplete;
    private volatile boolean closed;
    private Instant resolvedStartTime = Instant.EPOCH;
    private long nextSplitId;
    @Nullable private volatile Duration reconciliationRetention;
    @Nullable private volatile Instant reconciliationRetentionFetchedAt;

    public BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState) {
        this(context, client, startPosition, resumeFallback, restoredState, false, false);
    }

    public BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded) {
        this(context, client, startPosition, resumeFallback, restoredState, bounded, false);
    }

    public BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded,
            boolean reconciliationEnabled) {
        this(
                context,
                client,
                startPosition,
                resumeFallback,
                restoredState,
                bounded,
                reconciliationEnabled,
                Clock.systemUTC());
    }

    BigtableChangeStreamSplitEnumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamCoordinatorClient client,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable BigtableChangeStreamEnumeratorState restoredState,
            boolean bounded,
            boolean reconciliationEnabled,
            Clock clock) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.client = Preconditions.checkNotNull(client, "client must not be null");
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback =
                Preconditions.checkNotNull(resumeFallback, "resumeFallback must not be null");
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
                resolveRestored(resolver, restoredState.getUnassignedSplits());
        List<ChangeStreamPartitionSplit> restoredAssigned =
                resolveRestored(resolver, restoredState.getAssignedSplits());
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
        return Initialization.restored(
                restoredState.getStartTime(),
                restoredNextSplitId,
                restoredUnassigned,
                restoredAssigned,
                restoredMerges,
                restoredState.getMissingPartitions());
    }

    private List<ChangeStreamPartitionSplit> resolveRestored(
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
        for (ChangeStreamPartitionSplit split : initialization.assigned) {
            assigned.put(split.splitId(), split);
        }
        for (PendingMerge merge : initialization.pendingMerges) {
            PendingMergeAccumulator restored = PendingMergeAccumulator.restore(merge);
            pendingMerges.put(restored.partitionKey(), restored);
        }
        missingPartitions.addAll(initialization.missingPartitions);
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
        List<Integer> waiting = new ArrayList<>(waitingReaders);
        waitingReaders.clear();
        for (int subtaskId : waiting) {
            assignOrWait(subtaskId);
        }
        if (reconciliationEnabled) {
            context.callAsync(
                    this::reconciliationScan,
                    this::onReconciled,
                    java.time.Duration.ofSeconds(10).toMillis(),
                    java.time.Duration.ofSeconds(10).toMillis());
        }
    }

    private ReconciliationScan reconciliationScan() throws Exception {
        Duration retention = reconciliationRetention;
        Instant fetchedAt = reconciliationRetentionFetchedAt;
        Instant now = Instant.now(clock);
        if (retention == null
                || fetchedAt == null
                || fetchedAt.plus(Duration.ofMinutes(5)).isBefore(now)) {
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
                        pendingMergeSnapshot(),
                        missingPartitions,
                        Instant.now(clock),
                        ledgerLowWatermark());
        missingPartitions.clear();
        missingPartitions.addAll(result.missing);
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
                                + " continuation token at low watermark {} after {}.",
                        RowRanges.format(recovery.partition),
                        recoveryLowWatermark,
                        ChangeStreamPartitionReconciler.TOKENLESS_GRACE);
            }
        }
        serveWaitingReaders();
    }

    private static Instant clampToRetention(Instant lowWatermark, Duration retention, Instant now) {
        Instant earliest = now.minus(retention).plusSeconds(60);
        return lowWatermark.isBefore(earliest) ? earliest : lowWatermark;
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
        if (!initialized) {
            waitingReaders.add(subtaskId);
            return;
        }
        assignOrWait(subtaskId);
    }

    private void assignOrWait(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            return;
        }
        ChangeStreamPartitionSplit split = unassigned.poll();
        if (split == null) {
            if (signalBoundedCompletionIfDrained()) {
                return;
            }
            waitingReaders.add(subtaskId);
            return;
        }
        assigned.put(split.splitId(), split);
        splitsAssigned.inc();
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))));
    }

    @Override
    public void addSplitsBack(List<ChangeStreamPartitionSplit> splits, int subtaskId) {
        if (!initialized) {
            deferredActions.add(new DeferredSplitsBack(splits));
            return;
        }
        addSplitsBackInitialized(splits);
    }

    private void addSplitsBackInitialized(List<ChangeStreamPartitionSplit> splits) {
        for (ChangeStreamPartitionSplit split : splits) {
            if (assigned.remove(split.splitId()) != null) {
                unassigned.add(split);
            }
        }
        splitsReturned.inc(splits.size());
        serveWaitingReaders();
    }

    @Override
    public void addReader(int subtaskId) {
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
        for (PartitionTransitionEvent.Successor successor : transition.getSuccessors()) {
            acceptSuccessor(
                    successor.getPartition(),
                    successor.getContinuationToken(),
                    transition.getLowWatermark());
        }
        serveWaitingReaders();
        signalBoundedCompletionIfDrained();
    }

    private void acceptSuccessor(
            ByteStringRange partition, ChangeStreamContinuationToken token, Instant lowWatermark) {
        ByteStringRange partitionKey = RowRanges.copyOf(partition);
        PendingMergeAccumulator merge = pendingMerges.get(partitionKey);
        if (merge == null) {
            merge = new PendingMergeAccumulator(partition, lowWatermark);
            pendingMerges.put(partitionKey, merge);
        }
        merge.add(token, lowWatermark);
        if (!merge.isComplete()) {
            return;
        }
        pendingMerges.remove(partitionKey);
        PendingMerge completed = merge.toPendingMerge();
        unassigned.add(
                new ChangeStreamPartitionSplit(
                        splitId(nextSplitId++),
                        completed.getPartition(),
                        completed.getContinuationTokens(),
                        completed.getLowWatermark()));
    }

    private void serveWaitingReaders() {
        Iterator<Integer> waiting = waitingReaders.iterator();
        while (!unassigned.isEmpty() && waiting.hasNext()) {
            int subtaskId = waiting.next();
            waiting.remove();
            assignOrWait(subtaskId);
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
        waitingReaders.clear();
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
                missingPartitions);
    }

    private List<PendingMerge> pendingMergeSnapshot() {
        List<PendingMerge> snapshot = new ArrayList<>(pendingMerges.size());
        for (PendingMergeAccumulator merge : pendingMerges.values()) {
            snapshot.add(merge.toPendingMerge());
        }
        return snapshot;
    }

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
        metricGroup.setUnassignedSplitsGauge(() -> (long) unassigned.size());
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

        private Initialization(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions) {
            this.startTime = startTime;
            this.nextSplitId = nextSplitId;
            this.unassigned = unassigned;
            this.assigned = assigned;
            this.pendingMerges = pendingMerges;
            this.missingPartitions = missingPartitions;
        }

        private static Initialization fresh(
                Instant startTime, long nextSplitId, List<ChangeStreamPartitionSplit> partitions) {
            return new Initialization(
                    startTime,
                    nextSplitId,
                    partitions,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList());
        }

        private static Initialization restored(
                Instant startTime,
                long nextSplitId,
                List<ChangeStreamPartitionSplit> unassigned,
                List<ChangeStreamPartitionSplit> assigned,
                List<PendingMerge> pendingMerges,
                List<MissingPartition> missingPartitions) {
            return new Initialization(
                    startTime, nextSplitId, unassigned, assigned, pendingMerges, missingPartitions);
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

        private DeferredSplitsBack(List<ChangeStreamPartitionSplit> splits) {
            this.splits = new ArrayList<>(splits);
        }

        @Override
        public void replay(BigtableChangeStreamSplitEnumerator enumerator) {
            enumerator.addSplitsBackInitialized(splits);
        }
    }
}
