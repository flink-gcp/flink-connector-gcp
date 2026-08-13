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
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.base.source.StartPositionResolver;
import io.github.flink.gcp.connector.base.source.StartPositionResolver.RestoreExpiry;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.SpannerMetricValues;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamInitializationEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Coordinates the checkpointed parent-child lifecycle of Spanner Change Streams partitions. */
@Internal
public final class SpannerChangeStreamSplitEnumerator
        implements SplitEnumerator<
                SpannerChangeStreamPartitionSplit, SpannerChangeStreamEnumeratorState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SpannerChangeStreamSplitEnumerator.class);

    private final SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context;
    private final SpannerChangeStreamCoordinatorClientFactory clientFactory;
    private final StartPosition startPosition;
    private final Optional<StartPosition> resumeFallback;
    @Nullable private final Instant endTimestamp;
    private final long heartbeatMillis;
    @Nullable private final SpannerChangeStreamEnumeratorState restoredState;
    private final LongSupplier currentTimeMillis;

    private final Map<String, SpannerChangeStreamPartitionSplit> ledger = new LinkedHashMap<>();
    private final Map<String, Set<String>> childrenByParent = new HashMap<>();
    private final Deque<String> scheduledPartitions = new ArrayDeque<>();
    private final TreeMap<Long, Integer> scheduledPositionCounts = new TreeMap<>();
    private final Set<Integer> waitingReaders = new LinkedHashSet<>();
    private final List<DeferredAction> deferredActions = new ArrayList<>();
    private final AtomicLong scheduledCount = new AtomicLong();
    private final AtomicLong oldestScheduledPositionMillis = new AtomicLong(Long.MAX_VALUE);

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter partitionsDiscovered = new ThreadSafeSimpleCounter();
    @Nullable private SpannerChangeStreamCoordinatorClient client;
    private boolean initialized;
    private boolean discardRestoredReaderSplits;
    private boolean boundedLedger;
    private int unfinishedPartitions;
    private volatile boolean closed;

    public SpannerChangeStreamSplitEnumerator(
            SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable Instant endTimestamp,
            long heartbeatMillis,
            @Nullable SpannerChangeStreamEnumeratorState restoredState) {
        this(
                context,
                clientFactory,
                startPosition,
                resumeFallback,
                endTimestamp,
                heartbeatMillis,
                restoredState,
                System::currentTimeMillis);
    }

    @VisibleForTesting
    SpannerChangeStreamSplitEnumerator(
            SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            StartPosition startPosition,
            Optional<StartPosition> resumeFallback,
            @Nullable Instant endTimestamp,
            long heartbeatMillis,
            @Nullable SpannerChangeStreamEnumeratorState restoredState,
            LongSupplier currentTimeMillis) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.clientFactory =
                Preconditions.checkNotNull(clientFactory, "clientFactory must not be null");
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback =
                Preconditions.checkNotNull(resumeFallback, "resumeFallback must not be null");
        this.endTimestamp = endTimestamp;
        Preconditions.checkArgument(
                heartbeatMillis > 0,
                "heartbeatMillis must be positive, but was %s",
                heartbeatMillis);
        this.heartbeatMillis = heartbeatMillis;
        this.restoredState = restoredState;
        this.currentTimeMillis =
                Preconditions.checkNotNull(currentTimeMillis, "currentTimeMillis must not be null");
    }

    @Override
    public void start() {
        registerMetrics();
        context.callAsync(this::initialize, this::onInitialized);
    }

    private Initialization initialize() throws Exception {
        SpannerChangeStreamCoordinatorClient created = clientFactory.create();
        if (!installClient(created)) {
            throw new IllegalStateException(
                    "Spanner Change Streams enumerator was already closed.");
        }
        Duration retention = created.initialize();
        StartPositionResolver resolver = StartPositionResolver.create(getClass(), () -> retention);
        if (restoredState == null) {
            return Initialization.fresh(
                    SpannerChangeStreamPartitionSplit.initial(
                            resolver.resolve(startPosition), endTimestamp, heartbeatMillis),
                    false);
        }

        List<RestoreExpiry> expiries = new ArrayList<>();
        for (SpannerChangeStreamPartitionSplit partition : restoredState.getPartitions()) {
            if (partition.getLifecycleState() == PartitionLifecycleState.FINISHED) {
                continue;
            }
            resolver.inspectRestored(partition.splitId(), partition.getCurrentPosition())
                    .ifPresent(expiries::add);
        }
        if (expiries.isEmpty()) {
            return Initialization.restored(restoredState.getPartitions());
        }
        if (!resumeFallback.isPresent()) {
            FlinkRuntimeException expiry = expiries.get(0).asFailure();
            throw new FlinkRuntimeException(
                    expiry.getMessage()
                            + " No restore fallback was configured; set"
                            + " SpannerChangeStreamSourceBuilder.resumeFallback(...) to opt into"
                            + " restarting from a retained position.",
                    expiry);
        }

        StartPosition requestedFallback = resumeFallback.get();
        Instant resolvedFallback = resolver.resolveFallback(requestedFallback);
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
                SpannerChangeStreamPartitionSplit.initial(
                        resolvedFallback, endTimestamp, heartbeatMillis),
                true);
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
        for (SpannerChangeStreamPartitionSplit partition : initialization.partitions) {
            ledger.put(partition.splitId(), partition);
        }
        rebuildRuntimeIndexes();
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
                "Initialized Spanner Change Streams with {} partition ledger entries ({}"
                        + " scheduled, {} running, {} finished).",
                ledger.size(),
                count(PartitionLifecycleState.SCHEDULED),
                count(PartitionLifecycleState.RUNNING),
                count(PartitionLifecycleState.FINISHED));
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
        SpannerChangeStreamPartitionSplit scheduled = firstScheduled();
        if (scheduled == null) {
            if (boundedLedgerFinished()) {
                waitingReaders.remove(subtaskId);
                context.signalNoMoreSplits(subtaskId);
                return;
            }
            waitingReaders.add(subtaskId);
            return;
        }
        SpannerChangeStreamPartitionSplit running =
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
    private SpannerChangeStreamPartitionSplit firstScheduled() {
        while (!scheduledPartitions.isEmpty()) {
            SpannerChangeStreamPartitionSplit partition =
                    ledger.get(scheduledPartitions.peekFirst());
            if (partition != null
                    && partition.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                return partition;
            }
            scheduledPartitions.removeFirst();
        }
        return null;
    }

    @Override
    public void addSplitsBack(List<SpannerChangeStreamPartitionSplit> splits, int subtaskId) {
        if (closed) {
            return;
        }
        if (!initialized) {
            deferredActions.add(new DeferredSplitsBack(splits));
            return;
        }
        addSplitsBackInitialized(splits);
    }

    private void addSplitsBackInitialized(List<SpannerChangeStreamPartitionSplit> splits) {
        for (SpannerChangeStreamPartitionSplit returned : splits) {
            SpannerChangeStreamPartitionSplit current = ledger.get(returned.splitId());
            if (current == null
                    || current.getLifecycleState() == PartitionLifecycleState.FINISHED
                    || current.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                continue;
            }
            Preconditions.checkArgument(
                    current.samePartitionDefinition(returned),
                    "returned split %s does not match the checkpointed partition definition",
                    returned.splitId());
            SpannerChangeStreamPartitionSplit merged =
                    current.withProgress(
                            later(current.getCurrentPosition(), returned.getCurrentPosition()),
                            later(current.getWatermark(), returned.getWatermark()));
            schedule(merged);
        }
        splitsReturned.inc(splits.size());
        serveWaitingReaders();
    }

    @Override
    public void addReader(int subtaskId) {
        if (initialized) {
            sendInitializationEvent(subtaskId);
        }
    }

    private void sendInitializationEvent(int subtaskId) {
        context.sendEventToSourceReader(
                subtaskId, new SpannerChangeStreamInitializationEvent(discardRestoredReaderSplits));
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

    private void updateProgress(PartitionProgressEvent progress) {
        SpannerChangeStreamPartitionSplit current = ledger.get(progress.getSplitId());
        if (current == null || current.getLifecycleState() != PartitionLifecycleState.RUNNING) {
            return;
        }
        ledger.put(
                current.splitId(),
                current.withProgress(
                        later(current.getCurrentPosition(), progress.getCurrentPosition()),
                        later(current.getWatermark(), progress.getWatermark())));
    }

    private void acceptChildren(int subtaskId, ChildPartitionsEvent event) {
        SpannerChangeStreamPartitionSplit parent = ledger.get(event.getParentSplitId());
        if (parent == null) {
            LOG.warn(
                    "Ignoring child partitions for unknown split {} from subtask {}.",
                    event.getParentSplitId(),
                    subtaskId);
            return;
        }
        if (parent.getLifecycleState() != PartitionLifecycleState.RUNNING
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
                    child.getParentPartitionIds().contains(parent.splitId()),
                    "child token %s does not name reporting parent %s",
                    child.getToken(),
                    parent.splitId());
            for (String parentId : child.getParentPartitionIds()) {
                Preconditions.checkArgument(
                        ledger.containsKey(parentId),
                        "child token %s names unknown parent %s",
                        child.getToken(),
                        parentId);
            }
            String childId = SpannerChangeStreamPartitionSplit.idForToken(child.getToken());
            SpannerChangeStreamPartitionSplit discovered =
                    new SpannerChangeStreamPartitionSplit(
                            child.getToken(),
                            child.getParentPartitionIds(),
                            event.getStartTimestamp(),
                            parent.getEndTimestamp(),
                            parent.getHeartbeatMillis(),
                            event.getStartTimestamp(),
                            PartitionLifecycleState.CREATED,
                            event.getStartTimestamp());
            SpannerChangeStreamPartitionSplit existing = ledger.get(childId);
            if (existing == null) {
                ledger.put(childId, discovered);
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
        serveWaitingReaders();
    }

    private void finishPartition(int subtaskId, PartitionFinishedEvent event) {
        SpannerChangeStreamPartitionSplit current = ledger.get(event.getSplitId());
        if (current == null) {
            LOG.warn(
                    "Ignoring completion for unknown split {} from subtask {}.",
                    event.getSplitId(),
                    subtaskId);
            return;
        }
        if (current.getLifecycleState() == PartitionLifecycleState.FINISHED) {
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
        SpannerChangeStreamPartitionSplit finished =
                current.withProgress(
                                later(current.getCurrentPosition(), event.getCurrentPosition()),
                                later(current.getWatermark(), event.getWatermark()))
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ledger.put(finished.splitId(), finished);
        unfinishedPartitions--;
        Set<String> children = childrenByParent.remove(finished.splitId());
        for (String childId : children == null ? Collections.<String>emptySet() : children) {
            SpannerChangeStreamPartitionSplit child = ledger.get(childId);
            if (child != null) {
                promoteIfReady(child);
            }
        }
        serveWaitingReaders();
    }

    private void rebuildRuntimeIndexes() {
        childrenByParent.clear();
        scheduledPartitions.clear();
        scheduledPositionCounts.clear();
        scheduledCount.set(0);
        unfinishedPartitions = 0;
        boundedLedger = false;
        for (SpannerChangeStreamPartitionSplit partition : ledger.values()) {
            boundedLedger |= partition.getEndTimestamp() != null;
            if (partition.getLifecycleState() != PartitionLifecycleState.FINISHED) {
                unfinishedPartitions++;
            }
            if (partition.getLifecycleState() == PartitionLifecycleState.SCHEDULED) {
                scheduledPartitions.addLast(partition.splitId());
                scheduledCount.incrementAndGet();
                trackScheduledPosition(partition.getCurrentPosition().toEpochMilli());
            } else if (partition.getLifecycleState() == PartitionLifecycleState.CREATED) {
                indexCreatedPartition(partition);
            }
        }
        for (SpannerChangeStreamPartitionSplit partition : new ArrayList<>(ledger.values())) {
            promoteIfReady(partition);
        }
        refreshOldestScheduledPosition();
    }

    private void indexCreatedPartition(SpannerChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            SpannerChangeStreamPartitionSplit parent = ledger.get(parentId);
            if (parent != null && parent.getLifecycleState() == PartitionLifecycleState.FINISHED) {
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

    private void promoteIfReady(SpannerChangeStreamPartitionSplit partition) {
        if (partition.getLifecycleState() == PartitionLifecycleState.CREATED
                && allParentsFinished(partition)) {
            schedule(partition);
        }
    }

    private void schedule(SpannerChangeStreamPartitionSplit partition) {
        SpannerChangeStreamPartitionSplit scheduled =
                partition.withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ledger.put(scheduled.splitId(), scheduled);
        scheduledPartitions.addLast(scheduled.splitId());
        scheduledCount.incrementAndGet();
        trackScheduledPosition(scheduled.getCurrentPosition().toEpochMilli());
        refreshOldestScheduledPosition();
    }

    private boolean allParentsFinished(SpannerChangeStreamPartitionSplit partition) {
        for (String parentId : partition.getParentPartitionIds()) {
            SpannerChangeStreamPartitionSplit parent = ledger.get(parentId);
            if (parent == null || parent.getLifecycleState() != PartitionLifecycleState.FINISHED) {
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

    @Override
    public SpannerChangeStreamEnumeratorState snapshotState(long checkpointId) {
        Preconditions.checkState(
                initialized,
                "Spanner Change Streams initialization is still outstanding; retry the"
                        + " checkpoint after its deferred reader actions have been replayed.");
        return SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(ledger.values());
    }

    private void registerMetrics() {
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        splitsAssigned =
                metricGroup.counter(
                        SpannerMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter());
        splitsReturned =
                metricGroup.counter(
                        SpannerMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter());
        partitionsDiscovered =
                metricGroup.counter(
                        SpannerMetricNames.CHANGE_STREAM_PARTITIONS_DISCOVERED,
                        new ThreadSafeSimpleCounter());
        metricGroup.setUnassignedSplitsGauge(scheduledCount::get);
        metricGroup.gauge(
                SpannerMetricNames.UNASSIGNED_CHANGE_STREAM_PARTITION_LAG_MILLIS,
                (Gauge<Long>) this::unassignedPartitionLagMillis);
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
                : SpannerMetricValues.elapsedMillis(currentTimeMillis.getAsLong(), oldest);
    }

    @Override
    public void close() throws IOException {
        SpannerChangeStreamCoordinatorClient closing;
        synchronized (this) {
            closed = true;
            closing = client;
            client = null;
        }
        try {
            Closers.closeAll(closing);
        } catch (Exception e) {
            throw new IOException("Failed to close the Spanner Change Streams coordinator.", e);
        }
    }

    private static final class Initialization {

        private final List<SpannerChangeStreamPartitionSplit> partitions;
        private final boolean discardRestoredReaderSplits;

        private Initialization(
                List<SpannerChangeStreamPartitionSplit> partitions,
                boolean discardRestoredReaderSplits) {
            this.partitions = partitions;
            this.discardRestoredReaderSplits = discardRestoredReaderSplits;
        }

        private static Initialization fresh(
                SpannerChangeStreamPartitionSplit initial, boolean discardRestoredReaderSplits) {
            return new Initialization(
                    Collections.singletonList(initial), discardRestoredReaderSplits);
        }

        private static Initialization restored(List<SpannerChangeStreamPartitionSplit> partitions) {
            return new Initialization(new ArrayList<>(partitions), false);
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

        private final List<SpannerChangeStreamPartitionSplit> splits;

        private DeferredSplitsBack(List<SpannerChangeStreamPartitionSplit> splits) {
            this.splits = new ArrayList<>(splits);
        }

        @Override
        public void replay(SpannerChangeStreamSplitEnumerator enumerator) {
            enumerator.addSplitsBackInitialized(splits);
        }
    }
}
