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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionAckEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Reads a bounded number of Bigtable Change Streams partitions concurrently.
 *
 * <h2>A closed split stays checkpointed until the coordinator acknowledges it</h2>
 *
 * <p>Flink restores reader-to-coordinator events sent between the coordinator's snapshot and this
 * reader's barrier into neither side, so a transition sent there would leave the partition assigned
 * in the restored ledger with no reader holding it. A split whose {@code CloseStream} this reader
 * has seen therefore stays in {@link #snapshotState}, with the successors the record named, until a
 * {@link PartitionTransitionAckEvent} arrives, and a restored reader sends its transition again
 * from {@link #start}. The coordinator applies it only if it has not already. Flink holds the
 * acknowledgement back until this reader's barrier, so the split is in the snapshot of any
 * checkpoint whose coordinator snapshot missed the transition. This is Flink CDC's {@code
 * finishedUnackedSplits}.
 *
 * <p>A restored closed split is checked against retention like an open one. One that has expired is
 * read again from the resume fallback rather than reported, because its successors' tokens are as
 * expired as its own position. Flink creates an output for every restored split, so the reader
 * releases the outputs of the closed ones it keeps, which nothing emits to and which would
 * otherwise hold the source watermark back.
 *
 * <h2>Progress reports</h2>
 *
 * <p>The coordinator learns positions from batched reports, never from one event per heartbeat.
 * When a checkpoint completes, the reader sends the positions it snapshotted for it, as Flink CDC's
 * {@code IncrementalSourceReaderWithCommit} commits its offsets. Until a checkpoint completes, it
 * also sends its current positions at most once a second. From {@link #start} it names the unclosed
 * splits it restored, so that a coordinator a region failover kept can take back a close its ledger
 * still holds as pending.
 *
 * <p>See {@code docs/adr/0097} for the coordinator protocol these events belong to.
 */
@Internal
public final class BigtableChangeStreamReader<T>
        implements SourceReader<T, ChangeStreamPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamReader.class);

    /** How often, at most, the reader reports its current positions. */
    private static final long LIVE_PROGRESS_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    /** How many snapshots' positions, at most, wait for a completion to report them. */
    private static final int MAX_RECORDED_CHECKPOINTS = 16;

    private final SourceReaderContext context;
    private final TableDestination table;
    private final ChangeStreamOpener opener;
    private final ChangeStreamRestoreResolver restoreResolver;
    @Nullable private final StartPosition resumeFallback;
    @Nullable private final Instant boundedTimestamp;
    private final int maximumStreams;
    private final BigtableChangeStreamRecordEmitter<T> emitter;
    private final BigtableChangeStreamReaderMetrics metrics;
    private final Deque<ChangeStreamPartitionSplit> queued = new ArrayDeque<>();
    private final Map<String, ActiveRead> active = new LinkedHashMap<>();

    /** Closed splits the coordinator has not acknowledged, in close order. */
    private final Map<String, ChangeStreamPartitionSplit> closedUnacknowledged =
            new LinkedHashMap<>();

    /**
     * Queued splits a rotation took out of an active read after it emitted a record for them, so
     * that the transition the split reports when it closes still says so.
     */
    private final Set<String> rotatedWithOutput = new HashSet<>();

    /** Restored closed splits whose outputs Flink created and this reader has not released. */
    private final List<String> outputsToRelease = new ArrayList<>();

    /** Positions each checkpoint's snapshot recorded, until a completion reports them. */
    private final TreeMap<Long, List<PartitionProgressEvent.Progress>> checkpointProgress =
            new TreeMap<>();

    private final LongSupplier nanoTime;
    private final ArrayBlockingQueue<Delivery<T>> handover;
    private final Object availabilityLock = new Object();
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    private boolean started;
    private boolean noMoreSplits;
    private volatile boolean closed;
    private int advertisedFreeSlots = -1;
    private int advertisedHeldSplits = -1;
    private long receivedSplits;
    private long advertisedReceivedSplits = -1;
    private long lastLiveReportNanos;
    private long maxCompletedCheckpointId = Long.MIN_VALUE;

    public BigtableChangeStreamReader(
            SourceReaderContext context, BigtableChangeStreamSourceConfig<T> config) {
        this(
                context,
                config.getTable(),
                config.getDeserializer(),
                config.getOpener(),
                config.getRestoreResolver(),
                config.getResumeFallback(),
                config.getBoundedTimestamp(),
                config.getMaxConcurrentStreamsPerSubtask(),
                config.getMutationFilter(),
                new BigtableChangeStreamReaderMetrics(context.metricGroup()),
                System::nanoTime);
    }

    @VisibleForTesting
    BigtableChangeStreamReader(
            SourceReaderContext context,
            TableDestination table,
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            ChangeStreamOpener opener,
            ChangeStreamRestoreResolver restoreResolver,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant boundedTimestamp,
            int maximumStreams,
            BigtableChangeStreamReaderMetrics metrics) {
        this(
                context,
                table,
                deserializer,
                opener,
                restoreResolver,
                resumeFallback,
                boundedTimestamp,
                maximumStreams,
                BigtableChangeStreamMutationFilter.none(),
                metrics,
                System::nanoTime);
    }

    @VisibleForTesting
    BigtableChangeStreamReader(
            SourceReaderContext context,
            TableDestination table,
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            ChangeStreamOpener opener,
            ChangeStreamRestoreResolver restoreResolver,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant boundedTimestamp,
            int maximumStreams,
            BigtableChangeStreamMutationFilter mutationFilter,
            BigtableChangeStreamReaderMetrics metrics,
            LongSupplier nanoTime) {
        this.nanoTime = Preconditions.checkNotNull(nanoTime, "nanoTime must not be null");
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        this.restoreResolver =
                Preconditions.checkNotNull(restoreResolver, "restoreResolver must not be null");
        this.resumeFallback = resumeFallback;
        this.boundedTimestamp = boundedTimestamp;
        Preconditions.checkArgument(maximumStreams > 0, "maximumStreams must be positive");
        this.maximumStreams = maximumStreams;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
        emitter = new BigtableChangeStreamRecordEmitter<>(deserializer, mutationFilter, metrics);
        handover = new ArrayBlockingQueue<>(maximumStreams);
    }

    @Override
    public void start() {
        started = true;
        lastLiveReportNanos = nanoTime.getAsLong();
        if (!queued.isEmpty()) {
            List<String> restored = new ArrayList<>(queued.size());
            for (ChangeStreamPartitionSplit split : queued) {
                restored.add(split.splitId());
            }
            context.sendSourceEventToCoordinator(new PartitionsRestoredEvent(restored));
        }
        reportRestoredTransitions();
        startQueuedReads();
        advertiseCapacity();
    }

    /**
     * Sends the transition of every restored closed split again. Flink adds a reader's restored
     * splits before {@link #start}, and nothing closes before it, so every closed split this reader
     * holds then is a restored one. The coordinator defers events until it is initialized, applies
     * the transitions its restored ledger is missing, and acknowledges every one. Each says it
     * emitted nothing: what the earlier attempt emitted preceded the restored checkpoint, so none
     * of it is downstream for a successor to overtake.
     */
    private void reportRestoredTransitions() {
        if (closedUnacknowledged.isEmpty()) {
            return;
        }
        LOG.info(
                "Reporting the transition of {} restored Bigtable Change Streams partition(s)"
                        + " again; the coordinator had not acknowledged it before the checkpoint.",
                closedUnacknowledged.size());
        for (ChangeStreamPartitionSplit split : closedUnacknowledged.values()) {
            context.sendSourceEventToCoordinator(split.toTransitionEvent(false));
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionTransitionAckEvent) {
            closedUnacknowledged.remove(((PartitionTransitionAckEvent) sourceEvent).getSplitId());
            signalAvailable();
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported Bigtable Change Streams reader event " + sourceEvent + ".");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (!outputsToRelease.isEmpty()) {
            // An output nothing emits to would hold the source's combined watermark back.
            for (String splitId : outputsToRelease) {
                output.releaseOutputForSplit(splitId);
            }
            outputsToRelease.clear();
        }
        Delivery<T> delivery = handover.poll();
        if (delivery != null) {
            consume(delivery, output);
            reportLiveProgressIfDue();
            return nextStatus();
        }

        ActiveRead terminal = firstTerminal();
        if (terminal != null) {
            processTerminal(terminal, output);
            return nextStatus();
        }

        return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    private void consume(Delivery<T> delivery, ReaderOutput<T> output) throws Exception {
        ActiveRead read = active.get(delivery.read.splitId());
        if (read != delivery.read || read.closeSeen) {
            return;
        }
        ChangeStreamRecord record = delivery.record;
        SourceOutput<T> splitOutput = output.createOutputForSplit(read.splitId());
        if (record instanceof CloseStream) {
            consumeCloseStream((CloseStream) record, splitOutput, read);
            return;
        }
        emitter.emitRecord(record, splitOutput, read.emittedState);
        metrics.progress(read.splitId(), read.emittedState.getLowWatermark());

        if (record instanceof Heartbeat && !queued.isEmpty()) {
            read.cancel(CancellationReason.ROTATION);
        } else {
            read.requestOne();
        }
    }

    private void consumeCloseStream(CloseStream close, SourceOutput<T> splitOutput, ActiveRead read)
            throws Exception {
        @Nullable StreamController controllerToCancel;
        boolean terminalAlreadyPublished;
        synchronized (read) {
            Terminal terminal = read.terminal.get();
            if (terminal != null && terminal.error != null) {
                return;
            }
            emitter.emitRecord(close, splitOutput, read.emittedState);
            metrics.progress(read.splitId(), read.emittedState.getLowWatermark());
            metrics.closeStream();
            ChangeStreamPartitionSplit closed = read.emittedState.toSplit();
            closedUnacknowledged.put(closed.splitId(), closed);
            context.sendSourceEventToCoordinator(
                    closed.toTransitionEvent(read.emittedState.hasEmittedRecords()));
            read.closeSeen = true;
            read.cancellation = CancellationReason.COMPLETION;
            terminalAlreadyPublished = terminal != null;
            controllerToCancel = terminalAlreadyPublished ? null : read.controller;
        }
        if (terminalAlreadyPublished) {
            return;
        }
        if (controllerToCancel == null) {
            read.publishTerminal(Terminal.completed());
        } else {
            controllerToCancel.cancel();
        }
    }

    private void processTerminal(ActiveRead read, ReaderOutput<T> output) throws IOException {
        Terminal terminal = read.terminal.getAndSet(null);
        Preconditions.checkNotNull(terminal, "terminal read must have a result");
        CancellationReason cancellation = read.cancellation;
        if (terminal.error != null
                && (cancellation == null
                        || !ExceptionUtils.findThrowable(
                                        terminal.error, CancellationException.class)
                                .isPresent())) {
            throw new IOException(
                    "Failed to read Bigtable Change Streams split "
                            + read.splitId()
                            + " from "
                            + table
                            + ".",
                    terminal.error);
        }
        if (cancellation == null) {
            throw new IOException(
                    "Bigtable Change Streams ended without a CloseStream record for split "
                            + read.splitId()
                            + ".");
        }

        active.remove(read.splitId());
        metrics.terminated(read.timing);
        if (cancellation == CancellationReason.ROTATION) {
            if (read.emittedState.hasEmittedRecords()) {
                rotatedWithOutput.add(read.splitId());
            }
            queued.addLast(read.emittedState.toSplit());
        } else if (cancellation == CancellationReason.COMPLETION) {
            output.releaseOutputForSplit(read.splitId());
        }
        startQueuedReads();
        advertiseCapacity();
    }

    @Override
    public List<ChangeStreamPartitionSplit> snapshotState(long checkpointId) {
        List<ChangeStreamPartitionSplit> state = activeSplits();
        state.addAll(queued);
        checkpointProgress.put(checkpointId, PartitionProgressEvent.Progress.of(state));
        // Savepoints and failing checkpoints are never reported; an older entry only matters as a
        // floor for a completion, which a newer one serves as well.
        while (checkpointProgress.size() > MAX_RECORDED_CHECKPOINTS) {
            checkpointProgress.pollFirstEntry();
        }
        state.addAll(closedUnacknowledged.values());
        return state;
    }

    /**
     * Reports the positions the completed checkpoint recorded. A completion can stand for earlier
     * checkpoints whose own notification never came, so the latest recorded at or before it is the
     * one reported.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (checkpointId <= maxCompletedCheckpointId) {
            return;
        }
        maxCompletedCheckpointId = checkpointId;
        Map.Entry<Long, List<PartitionProgressEvent.Progress>> covered =
                checkpointProgress.floorEntry(checkpointId);
        checkpointProgress.headMap(checkpointId, true).clear();
        if (covered != null && !covered.getValue().isEmpty()) {
            context.sendSourceEventToCoordinator(new PartitionProgressEvent(covered.getValue()));
        }
    }

    /** Forgets what an aborted checkpoint's snapshot recorded; no completion may report it. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        checkpointProgress.remove(checkpointId);
    }

    /**
     * Sends the active splits' current positions at most once per {@code
     * LIVE_PROGRESS_INTERVAL_NANOS}, until a checkpoint completes.
     */
    private void reportLiveProgressIfDue() {
        if (maxCompletedCheckpointId != Long.MIN_VALUE) {
            return;
        }
        long now = nanoTime.getAsLong();
        if (now - lastLiveReportNanos < LIVE_PROGRESS_INTERVAL_NANOS) {
            return;
        }
        List<ChangeStreamPartitionSplit> splits = activeSplits();
        if (splits.isEmpty()) {
            return;
        }
        lastLiveReportNanos = now;
        context.sendSourceEventToCoordinator(
                new PartitionProgressEvent(PartitionProgressEvent.Progress.of(splits)));
    }

    /**
     * Returns the splits the active reads still hold, each at the position it has emitted to.
     *
     * <p>One method rather than the same loop twice, because the rule it applies is not obvious: a
     * read that has seen {@code CloseStream} has finished its partition and is no longer assigned
     * anything, even though it is still in {@code active} until the reader drains it; until the
     * coordinator acknowledges the transition, the checkpoint holds it as a closed split instead.
     * The checkpoint, the progress reports and the {@code partitionLowWatermarkMillis} gauge all
     * answer with this, so a copy that drifted would make them disagree about the same reader.
     */
    private List<ChangeStreamPartitionSplit> activeSplits() {
        List<ChangeStreamPartitionSplit> splits = new ArrayList<>(active.size());
        for (ActiveRead read : active.values()) {
            if (!read.closeSeen) {
                splits.add(read.emittedState.toSplit());
            }
        }
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (availabilityLock) {
            if (!handover.isEmpty()
                    || firstTerminal() != null
                    || finished()
                    || !outputsToRelease.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }
            return availability;
        }
    }

    @Override
    public void addSplits(List<ChangeStreamPartitionSplit> splits) {
        List<ChangeStreamPartitionSplit> additions = splits;
        if (!started) {
            additions = new ArrayList<>(splits.size());
            for (ChangeStreamPartitionSplit split : splits) {
                ChangeStreamPartitionSplit open = split.isClosed() ? split.reopened() : split;
                ChangeStreamPartitionSplit resolved;
                try {
                    resolved = restoreResolver.resolve(open, resumeFallback);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to validate restored Bigtable Change Streams split "
                                    + split.splitId()
                                    + ".",
                            e);
                }
                if (split.isClosed() && resolved.equals(open)) {
                    // Only a restore hands this reader a closed split: the coordinator assigns
                    // open ones. Flink created an output for it, which nothing will emit to.
                    closedUnacknowledged.put(split.splitId(), split);
                    outputsToRelease.add(split.splitId());
                    continue;
                }
                if (split.isClosed()) {
                    // Its successors' tokens are as expired as its own position, so it is read
                    // again from the fallback rather than reported.
                    LOG.warn(
                            "Restored Bigtable Change Streams split {} had closed, but its position"
                                    + " has expired; it is read again from the resume fallback.",
                            split.splitId());
                }
                additions.add(resolved);
            }
        }
        queued.addAll(additions);
        if (started) {
            // Splits restored before start were assigned to an earlier attempt, so only these count
            // toward what the enumerator has sent this one.
            receivedSplits += additions.size();
            startQueuedReads();
            advertiseCapacity();
        } else {
            updateAssignedMetrics();
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        signalAvailable();
    }

    private void startQueuedReads() {
        while (!closed && active.size() < maximumStreams && !queued.isEmpty()) {
            ChangeStreamPartitionSplit split = queued.removeFirst();
            ActiveRead read = new ActiveRead(split);
            active.put(split.splitId(), read);
            try {
                opener.open(table, split, boundedTimestamp, read);
            } catch (Exception e) {
                read.cancel(CancellationReason.CLOSE);
                active.remove(split.splitId());
                metrics.terminated(read.timing);
                queued.addFirst(split);
                updateAssignedMetrics();
                throw new FlinkRuntimeException(
                        "Failed to open Bigtable Change Streams split "
                                + split.splitId()
                                + " from "
                                + table
                                + ".",
                        e);
            }
        }
        updateAssignedMetrics();
    }

    private void advertiseCapacity() {
        if (!started || closed || noMoreSplits) {
            return;
        }
        int freeSlots = Math.max(0, maximumStreams - active.size() - queued.size());
        int heldSplits = queued.size();
        for (ActiveRead read : active.values()) {
            if (!read.closeSeen) {
                heldSplits++;
            }
        }
        // Each count is worth an event on its own. A CloseStream lowers the held count without
        // freeing a slot while the reader still has a queue, and a split that arrives as another
        // read closes changes only the received count, which tells the enumerator that split is
        // no longer in flight.
        if (freeSlots != advertisedFreeSlots
                || heldSplits != advertisedHeldSplits
                || receivedSplits != advertisedReceivedSplits) {
            advertisedFreeSlots = freeSlots;
            advertisedHeldSplits = heldSplits;
            advertisedReceivedSplits = receivedSplits;
            context.sendSourceEventToCoordinator(
                    new ReaderCapacityEvent(freeSlots, heldSplits, receivedSplits));
        }
    }

    private void updateAssignedMetrics() {
        metrics.assigned(activeSplits(), queued);
    }

    @Nullable
    private ActiveRead firstTerminal() {
        for (ActiveRead read : active.values()) {
            if (read.terminal.get() != null) {
                return read;
            }
        }
        return null;
    }

    private InputStatus nextStatus() {
        if (!handover.isEmpty() || firstTerminal() != null) {
            return InputStatus.MORE_AVAILABLE;
        }
        return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    private boolean finished() {
        return noMoreSplits
                && active.isEmpty()
                && queued.isEmpty()
                && handover.isEmpty()
                && closedUnacknowledged.isEmpty();
    }

    private void signalAvailable() {
        synchronized (availabilityLock) {
            availability.complete(null);
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        for (ActiveRead read : active.values()) {
            read.cancel(CancellationReason.CLOSE);
        }
        active.clear();
        queued.clear();
        handover.clear();
        closedUnacknowledged.clear();
        rotatedWithOutput.clear();
        outputsToRelease.clear();
        checkpointProgress.clear();
        metrics.closed();
        signalAvailable();
        Closers.closeAll(opener);
    }

    private final class ActiveRead implements ResponseObserver<ChangeStreamRecord> {

        private final ChangeStreamPartitionSplitState emittedState;
        private final String splitId;
        private final BigtableChangeStreamReaderMetrics.ReadTiming timing;
        private final AtomicReference<Terminal> terminal = new AtomicReference<>();
        @Nullable private volatile StreamController controller;
        @Nullable private volatile CancellationReason cancellation;
        private boolean responseOutstanding;
        private boolean closeSeen;

        private ActiveRead(ChangeStreamPartitionSplit split) {
            emittedState =
                    new ChangeStreamPartitionSplitState(
                            split, rotatedWithOutput.remove(split.splitId()));
            splitId = split.splitId();
            timing = metrics.opening();
        }

        private String splitId() {
            return splitId;
        }

        @Override
        public void onStart(StreamController controller) {
            StreamController checked =
                    Preconditions.checkNotNull(controller, "controller must not be null");
            boolean cancelImmediately;
            synchronized (this) {
                this.controller = checked;
                cancelImmediately = closed || cancellation != null || terminal.get() != null;
                if (!cancelImmediately) {
                    metrics.started(timing);
                }
            }
            checked.disableAutoInboundFlowControl();
            if (cancelImmediately) {
                checked.cancel();
                return;
            }
            requestOne();
        }

        private void requestOne() {
            StreamController current;
            synchronized (this) {
                current = controller;
                if (closed
                        || cancellation != null
                        || current == null
                        || responseOutstanding
                        || terminal.get() != null) {
                    return;
                }
                responseOutstanding = true;
            }
            try {
                current.request(1);
            } catch (RuntimeException e) {
                synchronized (this) {
                    responseOutstanding = false;
                }
                publishTerminal(Terminal.failed(e));
            }
        }

        @Override
        public void onResponse(ChangeStreamRecord record) {
            synchronized (this) {
                if (!responseOutstanding) {
                    publishTerminal(
                            Terminal.failed(
                                    new IllegalStateException(
                                            "Bigtable Change Streams delivered an unrequested"
                                                    + " response for "
                                                    + splitId()
                                                    + ".")));
                    return;
                }
                responseOutstanding = false;
            }
            if (record instanceof ChangeStreamMutation || record instanceof Heartbeat) {
                metrics.recordReturned(timing);
            }
            if (closed) {
                return;
            }
            if (!handover.offer(new Delivery<>(this, record))) {
                publishTerminal(
                        Terminal.failed(
                                new IllegalStateException(
                                        "The bounded Bigtable Change Streams handover overflowed.")));
                return;
            }
            signalAvailable();
        }

        @Override
        public void onError(Throwable error) {
            publishTerminal(Terminal.failed(error));
        }

        @Override
        public void onComplete() {
            publishTerminal(Terminal.completed());
        }

        private void publishTerminal(Terminal result) {
            boolean published;
            synchronized (this) {
                responseOutstanding = false;
                published = terminal.compareAndSet(null, result);
            }
            if (published) {
                metrics.terminated(timing);
                signalAvailable();
            }
        }

        private void cancel(CancellationReason reason) {
            StreamController current;
            synchronized (this) {
                if (cancellation != null || terminal.get() != null) {
                    return;
                }
                cancellation = reason;
                current = controller;
            }
            if (current == null) {
                publishTerminal(Terminal.completed());
            } else {
                current.cancel();
            }
        }
    }

    private enum CancellationReason {
        ROTATION,
        COMPLETION,
        CLOSE
    }

    /**
     * One record handed from a read's gRPC thread to the task thread.
     *
     * <p>The type parameter is only what naming the enclosing reader's inner {@code ActiveRead}
     * costs; nothing in here reads a {@code T}.
     */
    private static final class Delivery<T> {
        private final BigtableChangeStreamReader<T>.ActiveRead read;
        private final ChangeStreamRecord record;

        private Delivery(BigtableChangeStreamReader<T>.ActiveRead read, ChangeStreamRecord record) {
            this.read = read;
            this.record = record;
        }
    }

    private static final class Terminal {
        @Nullable private final Throwable error;

        private Terminal(@Nullable Throwable error) {
            this.error = error;
        }

        private static Terminal failed(Throwable error) {
            return new Terminal(error);
        }

        private static Terminal completed() {
            return new Terminal(null);
        }
    }
}
