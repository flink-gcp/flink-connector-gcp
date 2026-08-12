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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceConfig;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Reads several Spanner Change Streams partition queries concurrently in one source subtask. */
@Internal
public final class SpannerChangeStreamReader<T>
        implements SourceReader<T, SpannerChangeStreamPartitionSplit> {

    private final SourceReaderContext context;
    private final SpannerDatabase database;
    private final SpannerChangeStreamDeserializationSchema<T> deserializer;
    private final int maximumQueries;
    private final SpannerChangeStreamQueryClient client;
    private final Counter recordsSkipped;

    private final Deque<SpannerChangeStreamPartitionSplit> queued = new ArrayDeque<>();
    private final Map<String, ActiveQuery> active = new LinkedHashMap<>();
    private final Object availabilityLock = new Object();
    private CompletableFuture<Void> availability = new CompletableFuture<>();

    private boolean started;
    private boolean requestOutstanding;
    private boolean noMoreSplits;
    private volatile boolean closed;

    public SpannerChangeStreamReader(
            SourceReaderContext context, SpannerChangeStreamSourceConfig<T> config)
            throws Exception {
        this(
                context,
                config.getDatabase(),
                config.getDeserializer(),
                config.getMaxConcurrentQueriesPerSubtask(),
                config.getQueryClientFactory().create());
    }

    @VisibleForTesting
    SpannerChangeStreamReader(
            SourceReaderContext context,
            SpannerDatabase database,
            SpannerChangeStreamDeserializationSchema<T> deserializer,
            int maximumQueries,
            SpannerChangeStreamQueryClient client) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        Preconditions.checkArgument(maximumQueries > 0, "maximumQueries must be positive");
        this.maximumQueries = maximumQueries;
        this.client = Preconditions.checkNotNull(client, "client must not be null");
        this.recordsSkipped = context.metricGroup().counter(SpannerMetricNames.RECORDS_SKIPPED);
    }

    @Override
    public void start() {
        started = true;
        startQueuedQueries();
        requestIfCapacity();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        ActiveQuery query = firstAvailable();
        if (query == null) {
            resetAvailability();
            return finished() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        QueryResult result = query.handover.getAndSet(null);
        Preconditions.checkNotNull(result, "available query must have a result");
        if (result.error != null) {
            resetAvailability();
            throw new IOException(
                    "Failed to read Spanner Change Streams split "
                            + query.split.splitId()
                            + " from "
                            + database
                            + ".",
                    result.error);
        }
        if (result.finished) {
            finishQuery(query, output);
        } else {
            emit(result.record, query, output);
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
        Instant position = later(query.split.getCurrentPosition(), record.position());
        Instant watermark = query.split.getWatermark();
        if (record instanceof SpannerChangeStreamRecord.Data) {
            SpannerChangeStreamRecord.Data data = (SpannerChangeStreamRecord.Data) record;
            T deserialized = deserializer.deserialize(data.record);
            if (deserialized == null) {
                recordsSkipped.inc();
            } else {
                output.createOutputForSplit(query.split.splitId())
                        .collect(deserialized, data.record.getCommitTimestamp().toEpochMilli());
            }
        } else if (record instanceof SpannerChangeStreamRecord.Heartbeat) {
            watermark = later(watermark, record.position());
            SourceOutput<T> splitOutput = output.createOutputForSplit(query.split.splitId());
            splitOutput.emitWatermark(new Watermark(watermark.toEpochMilli()));
        } else if (record instanceof SpannerChangeStreamRecord.Children) {
            SpannerChangeStreamRecord.Children children =
                    (SpannerChangeStreamRecord.Children) record;
            context.sendSourceEventToCoordinator(childrenEvent(query.split, children));
        } else {
            throw new IllegalArgumentException("Unsupported Spanner Change Streams record.");
        }
        query.split = query.split.withProgress(position, watermark);
        context.sendSourceEventToCoordinator(
                new PartitionProgressEvent(query.split.splitId(), position, watermark));
    }

    private static ChildPartitionsEvent childrenEvent(
            SpannerChangeStreamPartitionSplit parent, SpannerChangeStreamRecord.Children record) {
        List<ChildPartitionsEvent.ChildPartition> children = new ArrayList<>();
        for (SpannerChangeStreamRecord.Child child : record.children) {
            List<String> parentIds = new ArrayList<>();
            if (child.initialParent) {
                parentIds.add(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
            }
            for (String token : child.parentTokens) {
                parentIds.add(SpannerChangeStreamPartitionSplit.idForToken(token));
            }
            if (parentIds.isEmpty() && parent.getPartitionToken() == null) {
                parentIds.add(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
            }
            children.add(new ChildPartitionsEvent.ChildPartition(child.token, parentIds));
        }
        return new ChildPartitionsEvent(parent.splitId(), record.startTimestamp, children);
    }

    private void finishQuery(ActiveQuery query, ReaderOutput<T> output) {
        context.sendSourceEventToCoordinator(
                new PartitionFinishedEvent(
                        query.split.splitId(),
                        query.split.getCurrentPosition(),
                        query.split.getWatermark()));
        query.handle.close();
        active.remove(query.split.splitId());
        output.releaseOutputForSplit(query.split.splitId());
        startQueuedQueries();
        requestIfCapacity();
    }

    @Override
    public List<SpannerChangeStreamPartitionSplit> snapshotState(long checkpointId) {
        List<SpannerChangeStreamPartitionSplit> state = new ArrayList<>();
        for (ActiveQuery query : active.values()) {
            state.add(query.split);
        }
        state.addAll(queued);
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (availabilityLock) {
            if (firstAvailable() != null || finished()) {
                return CompletableFuture.completedFuture(null);
            }
            return availability;
        }
    }

    @Override
    public void addSplits(List<SpannerChangeStreamPartitionSplit> splits) {
        requestOutstanding = false;
        queued.addAll(splits);
        if (started) {
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
            SpannerChangeStreamPartitionSplit split = queued.removeFirst();
            ActiveQuery query = new ActiveQuery(split);
            active.put(split.splitId(), query);
            try {
                query.handle = client.open(split, query);
            } catch (Exception e) {
                active.remove(split.splitId());
                queued.addFirst(split);
                throw new FlinkRuntimeException(
                        "Failed to open Spanner Change Streams split "
                                + split.splitId()
                                + " from "
                                + database
                                + ".",
                        e);
            }
        }
    }

    private void requestIfCapacity() {
        if (!closed
                && started
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
            if (firstAvailable() == null && !finished() && availability.isDone()) {
                availability = new CompletableFuture<>();
            }
        }
    }

    private static Instant later(Instant left, Instant right) {
        return right.isAfter(left) ? right : left;
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
        signalAvailable();
        Closers.closeAll(closing);
    }

    private final class ActiveQuery
            implements SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener {

        private SpannerChangeStreamPartitionSplit split;
        private final AtomicReference<QueryResult> handover = new AtomicReference<>();
        private SpannerChangeStreamQueryClient.QueryHandle handle;

        private ActiveQuery(SpannerChangeStreamPartitionSplit split) {
            this.split = split;
        }

        @Override
        public void record(SpannerChangeStreamRecord record) {
            publish(QueryResult.record(record));
        }

        @Override
        public void finished() {
            publish(QueryResult.finished());
        }

        @Override
        public void failed(Throwable error) {
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
                                + split.splitId()
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
