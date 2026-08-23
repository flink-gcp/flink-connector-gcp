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
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Reads a bounded number of Bigtable Change Streams partitions concurrently. */
@Internal
public final class BigtableChangeStreamReader<T>
        implements SourceReader<T, ChangeStreamPartitionSplit> {

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
    private final ArrayBlockingQueue<Delivery<T>> handover;
    private final Object availabilityLock = new Object();
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    private boolean started;
    private boolean noMoreSplits;
    private volatile boolean closed;
    private int advertisedFreeSlots = -1;

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
                new BigtableChangeStreamReaderMetrics(context.metricGroup()));
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
                metrics);
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
            BigtableChangeStreamReaderMetrics metrics) {
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
        emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        deserializer, mutationFilter, context, metrics);
        handover = new ArrayBlockingQueue<>(maximumStreams);
    }

    @Override
    public void start() {
        started = true;
        startQueuedReads();
        advertiseCapacity();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        Delivery<T> delivery = handover.poll();
        if (delivery != null) {
            consume(delivery, output);
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
        return state;
    }

    /**
     * Returns the splits the active reads still hold, each at the position it has emitted to.
     *
     * <p>One method rather than the same loop twice, because the rule it applies is not obvious: a
     * read that has seen {@code CloseStream} has finished its partition and is no longer assigned
     * anything, even though it is still in {@code active} until the reader drains it. The
     * checkpoint and the {@code partitionLowWatermarkMillis} gauge both answer with this, so a copy
     * that drifted would make the two disagree about the same reader.
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
            if (!handover.isEmpty() || firstTerminal() != null || finished()) {
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
                try {
                    additions.add(restoreResolver.resolve(split, resumeFallback));
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to validate restored Bigtable Change Streams split "
                                    + split.splitId()
                                    + ".",
                            e);
                }
            }
        }
        queued.addAll(additions);
        if (started) {
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
        if (freeSlots != advertisedFreeSlots) {
            advertisedFreeSlots = freeSlots;
            context.sendSourceEventToCoordinator(new ReaderCapacityEvent(freeSlots));
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
        return noMoreSplits && active.isEmpty() && queued.isEmpty() && handover.isEmpty();
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
            emittedState = new ChangeStreamPartitionSplitState(split);
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
