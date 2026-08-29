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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceConfig;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamInitializationEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarkEvent;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reads several Spanner Change Streams partition queries concurrently in one source subtask.
 *
 * <h2>Threading, and the one-slot handover</h2>
 *
 * <p>Two threads meet here. The client's callback thread delivers a record, a completion or a
 * failure; the task thread drains it in {@link #pollNext}. They meet at exactly one place per query
 * — {@code ActiveQuery.handover}, an {@code AtomicReference} holding <b>at most one undrained
 * result</b>. The callback publishes with a compare-and-set and throws if it finds the slot
 * occupied, because a second undrained result would mean the query had resumed before the mailbox
 * consumed the first, and the memory this reader holds would stop being bounded.
 *
 * <p>That bound is what the callback's {@code PAUSE} return buys, and it is why {@code resume()} is
 * called from {@code pollNext} <em>after</em> the result has been emitted, never from the callback.
 * Everything Flink-facing — collecting to the output, sending coordinator events, advancing split
 * progress — happens on the task thread as a result, so no user code and no Flink output is ever
 * reached from a client thread.
 *
 * <p>{@link #isAvailable()} is the other crossing: the callback completes the future and {@code
 * pollNext} replaces it once it has drained everything. Both sides hold {@code availabilityLock}
 * for that, which is the only lock in this class.
 *
 * <h2>Capacity</h2>
 *
 * <p>At most {@code maxConcurrentQueriesPerSubtask} queries are open at once. Splits beyond that
 * sit in a FIFO that is part of {@link #snapshotState}, so a restored reader that was over capacity
 * stays over capacity rather than dropping the excess. A split is requested from the enumerator
 * only when active and queued together leave room, and only one request is ever outstanding.
 *
 * <h2>What ends a query, and what that reports</h2>
 *
 * <p>A query that fails <b>fails the task and keeps its split</b>: the split is still in {@link
 * #snapshotState}, so recovery re-opens it at the checkpointed position. Only a query that ended
 * successfully sends a {@link PartitionFinishedEvent}, because that event is what lets the
 * coordinator schedule the partition's children — sending it for a failed query would advance the
 * lineage past a partition nobody finished reading.
 *
 * <h2>Time</h2>
 *
 * <p>A data record is emitted at its own commit timestamp. The watermark is not this reader's to
 * compute: the coordinator owns the unfinished-ledger frontier and broadcasts it, and this reader
 * emits it through the <em>main</em> source output rather than a per-split one, so a partition no
 * reader currently holds still counts toward Flink's minimum. Heartbeats never reach the user
 * deserializer; they advance the position this reader reports back to the coordinator.
 *
 * <p>Queries do not open until the coordinator's initialization event arrives, because a restored
 * split must not reach Spanner before the coordinator has decided whether its position is still
 * within retention.
 *
 * <p>See {@code docs/adr/0101} for the evidence behind these choices, and {@code docs/adr/0099} for
 * the coordinator protocol these events belong to.
 */
@Internal
public final class SpannerChangeStreamReader<T>
        implements SourceReader<T, ChangeStreamPartitionSplit> {

    private final SourceReaderContext context;
    private final DatabaseDestination database;
    private final int maximumQueries;
    private final SpannerChangeStreamQueryClient client;
    private final SpannerChangeStreamReaderMetrics metrics;
    private final SpannerChangeStreamRecordEmitter<T> emitter;
    private final Deque<ChangeStreamPartitionSplit> queued = new ArrayDeque<>();
    private final Map<String, ActiveQuery> active = new LinkedHashMap<>();
    private final Object availabilityLock = new Object();
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    private boolean started;
    private boolean coordinatorInitialized;
    private boolean requestOutstanding;
    private boolean noMoreSplits;
    private long pendingSourceWatermark = Long.MIN_VALUE;
    private long emittedSourceWatermark = Long.MIN_VALUE;
    private boolean sourceWatermarkPending;
    private volatile boolean closed;

    public SpannerChangeStreamReader(
            SourceReaderContext context, SpannerChangeStreamSourceConfig<T> config)
            throws Exception {
        this(
                context,
                config.getDatabase(),
                config.getDeserializer(),
                config.getRecordFilter(),
                config.getMaxConcurrentQueriesPerSubtask(),
                config.getQueryClientFactory().create());
    }

    @VisibleForTesting
    SpannerChangeStreamReader(
            SourceReaderContext context,
            DatabaseDestination database,
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            int maximumQueries,
            SpannerChangeStreamQueryClient client) {
        this(
                context,
                database,
                deserializer,
                SpannerChangeStreamRecordFilter.none(),
                maximumQueries,
                client);
    }

    @VisibleForTesting
    SpannerChangeStreamReader(
            SourceReaderContext context,
            DatabaseDestination database,
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            SpannerChangeStreamRecordFilter recordFilter,
            int maximumQueries,
            SpannerChangeStreamQueryClient client) {
        this(
                context,
                database,
                deserializer,
                recordFilter,
                Preconditions.checkNotNull(recordFilter, "recordFilter must not be null")
                        .hasFilters(),
                maximumQueries,
                client);
    }

    @VisibleForTesting
    SpannerChangeStreamReader(
            SourceReaderContext context,
            DatabaseDestination database,
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            SpannerChangeStreamRecordFilter recordFilter,
            boolean filtersActive,
            int maximumQueries,
            SpannerChangeStreamQueryClient client) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        Preconditions.checkArgument(maximumQueries > 0, "maximumQueries must be positive");
        this.maximumQueries = maximumQueries;
        this.client = Preconditions.checkNotNull(client, "client must not be null");
        this.metrics = new SpannerChangeStreamReaderMetrics(context.metricGroup());
        this.emitter =
                new SpannerChangeStreamRecordEmitter<>(
                        deserializer, recordFilter, filtersActive, context, metrics);
    }

    @Override
    public void start() {
        started = true;
        startAfterCoordinatorInitialization();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SpannerChangeStreamInitializationEvent) {
            SpannerChangeStreamInitializationEvent initialization =
                    (SpannerChangeStreamInitializationEvent) sourceEvent;
            Preconditions.checkState(
                    !coordinatorInitialized,
                    "Spanner Change Streams reader received initialization more than once.");
            if (initialization.shouldDiscardRestoredSplits()) {
                queued.clear();
                metrics.queued(queued);
            }
            acceptSourceWatermark(initialization.getSourceWatermark());
            coordinatorInitialized = true;
            startAfterCoordinatorInitialization();
            return;
        }
        if (sourceEvent instanceof SpannerChangeStreamWatermarkEvent) {
            Preconditions.checkState(
                    coordinatorInitialized,
                    "Spanner Change Streams reader received a watermark before initialization.");
            acceptSourceWatermark(
                    ((SpannerChangeStreamWatermarkEvent) sourceEvent).getSourceWatermark());
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported Spanner Change Streams reader event " + sourceEvent + ".");
    }

    private void startAfterCoordinatorInitialization() {
        if (!started || !coordinatorInitialized) {
            return;
        }
        startQueuedQueries();
        requestIfCapacity();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (sourceWatermarkPending) {
            output.emitWatermark(new Watermark(pendingSourceWatermark));
            emittedSourceWatermark = pendingSourceWatermark;
            sourceWatermarkPending = false;
            resetAvailability();
            if (sourceWatermarkPending || firstAvailable() != null) {
                return InputStatus.MORE_AVAILABLE;
            }
            return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }
        ActiveQuery query = firstAvailable();
        if (query == null) {
            resetAvailability();
            return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        QueryResult result = query.handover.getAndSet(null);
        Preconditions.checkNotNull(result, "available query must have a result");
        if (result.error != null) {
            resetAvailability();
            throw new IOException(queryFailureMessage("read", query.state.toSplit()), result.error);
        }
        if (result.finished) {
            finishQuery(query, output);
        } else {
            emit(result.record, query, output);
            metrics.resumed(query.timing);
            query.handle.resume();
        }
        resetAvailability();
        if (firstAvailable() != null) {
            return InputStatus.MORE_AVAILABLE;
        }
        return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    private void emit(SpannerChangeStreamRecord record, ActiveQuery query, ReaderOutput<T> output)
            throws Exception {
        SourceOutput<T> splitOutput = output.createOutputForSplit(query.splitId);
        emitter.emitRecord(record, splitOutput, query.state);
    }

    private void finishQuery(ActiveQuery query, ReaderOutput<T> output) {
        ChangeStreamPartitionSplit split = query.state.toSplit();
        context.sendSourceEventToCoordinator(
                new PartitionFinishedEvent(
                        split.splitId(), split.getCurrentPosition(), split.getWatermark()));
        query.handle.close();
        active.remove(split.splitId());
        output.releaseOutputForSplit(split.splitId());
        startQueuedQueries();
        requestIfCapacity();
    }

    @Override
    public List<ChangeStreamPartitionSplit> snapshotState(long checkpointId) {
        List<ChangeStreamPartitionSplit> state = new ArrayList<>();
        for (ActiveQuery query : active.values()) {
            state.add(query.state.toSplit());
        }
        state.addAll(queued);
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (availabilityLock) {
            if (sourceWatermarkPending || firstAvailable() != null || finished()) {
                return CompletableFuture.completedFuture(null);
            }
            return availability;
        }
    }

    @Override
    public void addSplits(List<ChangeStreamPartitionSplit> splits) {
        requestOutstanding = false;
        queued.addAll(splits);
        metrics.queued(queued);
        if (started && coordinatorInitialized) {
            startQueuedQueries();
            requestIfCapacity();
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        requestOutstanding = false;
        noMoreSplits = true;
        signalAvailable();
    }

    private void startQueuedQueries() {
        while (!closed && active.size() < maximumQueries && !queued.isEmpty()) {
            ChangeStreamPartitionSplit split = queued.removeFirst();
            ActiveQuery query = new ActiveQuery(split);
            active.put(split.splitId(), query);
            try {
                query.handle = client.open(split, query);
                metrics.opened(query.timing);
            } catch (Exception e) {
                metrics.openFailed(query.timing);
                active.remove(split.splitId());
                queued.addFirst(split);
                metrics.queued(queued);
                throw new FlinkRuntimeException(queryFailureMessage("open", split), e);
            }
        }
        metrics.queued(queued);
    }

    private void requestIfCapacity() {
        if (!closed
                && started
                && coordinatorInitialized
                && !noMoreSplits
                && !requestOutstanding
                && active.size() + queued.size() < maximumQueries) {
            requestOutstanding = true;
            context.sendSplitRequest();
        }
    }

    private ActiveQuery firstAvailable() {
        for (ActiveQuery query : active.values()) {
            if (query.handover.get() != null) {
                return query;
            }
        }
        return null;
    }

    private boolean finished() {
        return noMoreSplits && active.isEmpty() && queued.isEmpty();
    }

    private void signalAvailable() {
        synchronized (availabilityLock) {
            availability.complete(null);
        }
    }

    private void resetAvailability() {
        synchronized (availabilityLock) {
            if (!sourceWatermarkPending
                    && firstAvailable() == null
                    && !finished()
                    && availability.isDone()) {
                availability = new CompletableFuture<>();
            }
        }
    }

    private void acceptSourceWatermark(long sourceWatermark) {
        Preconditions.checkArgument(
                sourceWatermark >= pendingSourceWatermark,
                "Spanner Change Streams source watermark moved backwards from %s to %s.",
                pendingSourceWatermark,
                sourceWatermark);
        if (sourceWatermark <= emittedSourceWatermark
                || sourceWatermark == pendingSourceWatermark) {
            return;
        }
        pendingSourceWatermark = sourceWatermark;
        sourceWatermarkPending = true;
        signalAvailable();
    }

    private String queryFailureMessage(String operation, ChangeStreamPartitionSplit split) {
        String message =
                "Failed to "
                        + operation
                        + " Spanner Change Streams split "
                        + split.splitId()
                        + " from "
                        + database
                        + ".";
        if (split.getPartitionToken() != null) {
            return message;
        }
        return message
                + " This initial query starts at "
                + split.getStartTimestamp()
                + ". Spanner requires the initial start timestamp to be within retention, not in"
                + " the future, and at or after the change stream was created. If the stream was"
                + " created recently, use StartPosition.latest() or StartPosition.at(...) with a"
                + " timestamp after its DDL completed.";
    }

    @Override
    public void close() throws Exception {
        closed = true;
        List<AutoCloseable> closing = new ArrayList<>();
        for (ActiveQuery query : active.values()) {
            if (query.handle != null) {
                closing.add(query.handle);
            }
        }
        closing.add(client);
        active.clear();
        queued.clear();
        metrics.closed();
        signalAvailable();
        Closers.closeAll(closing);
    }

    private final class ActiveQuery
            implements SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener {

        private final ChangeStreamPartitionSplitState state;
        private final String splitId;
        private final AtomicReference<QueryResult> handover = new AtomicReference<>();
        private final SpannerChangeStreamReaderMetrics.QueryTiming timing;
        private SpannerChangeStreamQueryClient.QueryHandle handle;

        private ActiveQuery(ChangeStreamPartitionSplit split) {
            this.state = new ChangeStreamPartitionSplitState(split);
            this.splitId = split.splitId();
            this.timing = metrics.opening(split);
        }

        @Override
        public void record(SpannerChangeStreamRecord record) {
            metrics.recordReturned(timing, record instanceof SpannerChangeStreamRecord.Heartbeat);
            publish(QueryResult.record(record));
        }

        @Override
        public void finished() {
            metrics.terminated(timing);
            publish(QueryResult.finished());
        }

        @Override
        public void failed(Throwable error) {
            metrics.terminated(timing);
            if (!closed) {
                publish(QueryResult.failed(error));
            }
        }

        private void publish(QueryResult result) {
            if (closed) {
                return;
            }
            if (!handover.compareAndSet(null, result)) {
                throw new IllegalStateException(
                        "Spanner Change Streams query produced more than one undrained result for "
                                + splitId
                                + ".");
            }
            signalAvailable();
        }
    }

    private static final class QueryResult {

        private final SpannerChangeStreamRecord record;
        private final boolean finished;
        private final Throwable error;

        private QueryResult(SpannerChangeStreamRecord record, boolean finished, Throwable error) {
            this.record = record;
            this.finished = finished;
            this.error = error;
        }

        private static QueryResult record(SpannerChangeStreamRecord record) {
            return new QueryResult(record, false, null);
        }

        private static QueryResult finished() {
            return new QueryResult(null, true, null);
        }

        private static QueryResult failed(Throwable error) {
            return new QueryResult(null, false, error);
        }
    }
}
