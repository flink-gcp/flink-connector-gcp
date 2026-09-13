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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.DefaultMutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.MutationBatcher;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.MutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Loopback-only instrumentation; Stage 2 supplies separate, explicitly authorized transports. */
class LocalStagedHarness implements AutoCloseable {
    static final Map<String, LocalStagedHarness> RUNS = new ConcurrentHashMap<>();
    static final String PROFILE = "single-cluster";
    final String id = UUID.randomUUID().toString();
    final TableDestination table;
    final String endpoint;
    final StagedMutationTestSink.Probe store = new StagedMutationTestSink.Probe();
    final Map<Long, Long> admissions = new ConcurrentHashMap<>();
    final Map<Long, Long> acknowledgements = new ConcurrentHashMap<>();
    final List<CheckAndMutateRowRequest> attempts = Collections.synchronizedList(new ArrayList<>());
    final List<ApiFuture<Boolean>> originalFutures =
            Collections.synchronizedList(new ArrayList<>());
    final List<Long> clientCompletionNanos = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger deduplicated = new AtomicInteger();
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger peakActive = new AtomicInteger();
    final AtomicInteger committersClosed = new AtomicInteger();
    final java.util.concurrent.atomic.AtomicLong peakWriterBytes =
            new java.util.concurrent.atomic.AtomicLong();
    final AtomicInteger peakWriterEntries = new AtomicInteger();
    final AtomicInteger staged = new AtomicInteger();
    final AtomicBoolean allowInputs = new AtomicBoolean(true);
    final CompletableFuture<Void> inputsAllowed = new CompletableFuture<>();
    final AtomicInteger readersStarted = new AtomicInteger();
    final AtomicBoolean finishSource = new AtomicBoolean();
    final CompletableFuture<Void> sourceReleased = new CompletableFuture<>();
    final AtomicBoolean failWriterClose = new AtomicBoolean();
    final ScheduledExecutorService receiver = Executors.newSingleThreadScheduledExecutor();
    final int payloadBytes;
    final boolean hot;
    final boolean aggregate;
    final int inFlight;
    volatile long delayMillis;
    volatile boolean hang;
    volatile int loseAnswerAt = -1;
    volatile int maxEntries = 100_000;
    volatile long maxBytes = 64L * 1024 * 1024;
    volatile long firstAdmission = Long.MAX_VALUE;
    volatile long lastAck;

    LocalStagedHarness(int payloadBytes, boolean hot, boolean aggregate, int inFlight) {
        this(
                TableDestination.of("local-project", "local-instance", "local-table"),
                "127.0.0.1:1",
                payloadBytes,
                hot,
                aggregate,
                inFlight);
    }

    LocalStagedHarness(
            TableDestination table,
            String endpoint,
            int payloadBytes,
            boolean hot,
            boolean aggregate,
            int inFlight) {
        // An explicit loopback literal prevents a typo from selecting ADC or a cloud endpoint.
        if (endpoint == null || !endpoint.matches("127\\.0\\.0\\.1:[0-9]+")) {
            throw new IllegalArgumentException("A literal 127.0.0.1 emulator endpoint is required");
        }
        EmulatorEndpoint.parse(endpoint, "endpoint");
        if (payloadBytes < Long.BYTES || inFlight < 1) {
            throw new IllegalArgumentException("payloadBytes >= 8 and inFlight >= 1 are required");
        }
        this.table = table;
        this.endpoint = endpoint;
        this.payloadBytes = payloadBytes;
        this.hot = hot;
        this.aggregate = aggregate;
        this.inFlight = inFlight;
        RUNS.put(id, this);
    }

    org.apache.flink.api.connector.source.Source<Long, ?, ?> source(long records, boolean hold) {
        return hold
                ? new LocalStagedSource(id, records)
                : new org.apache.flink.api.connector.source.lib.NumberSequenceSource(
                        0, records - 1);
    }

    long checkpointOperationTimeoutMillis() {
        return 40_000;
    }

    long checkpointTimeoutMillis() {
        return 30_000;
    }

    void commitStarted(Object committer, int entries) {}

    void commitFinished(Object committer, boolean successful) {}

    void beforeSend(CheckAndMutateRowRequest wire) throws IOException {}

    void prepared(Collection<CheckAndMutateRowRequest> requests) throws IOException {}

    void beforeBulk(MutateRowsRequest.Entry entry) {}

    void writerStaged(Object writer, int entries, long bytes) {}

    SimpleVersionedSerializer<CheckAndMutateRowRequest> serializer() {
        return new StagedMutationTestSink(id, 1, 1).getCommittableSerializer();
    }

    void trace(CheckAndMutateRowRequest wire, ApiFuture<Boolean> future) {
        attempts.add(wire);
        originalFutures.add(future);
    }

    void committerWaited(long nanos) {}

    void clientCompleted(long sequence, long nanos, long completedAt) {
        clientCompletionNanos.add(nanos);
    }

    synchronized void commitAcknowledged(CheckAndMutateRowRequest wire, long now)
            throws IOException {
        if (loseAnswerAt == acknowledgedCount()) {
            loseAnswerAt = -1;
            throw new IOException("Applied row; injected response loss");
        }
        acknowledged(sequence(wire.getFalseMutationsList()), now);
    }

    int acknowledgedCount() {
        return acknowledgements.size();
    }

    DefaultSingleRowClientFactory singleRowFactory() throws IOException {
        return new DefaultSingleRowClientFactory(
                PROFILE,
                BigtableRequestOptions.builder().build(),
                EmulatorEndpoint.parse(endpoint, "endpoint"),
                null);
    }

    MutationBatcherFactory batcherFactory() throws IOException {
        return new DefaultMutationBatcherFactory(
                PROFILE,
                BigtableWriterOptions.builder().maxInFlightEntries(inFlight).build(),
                EmulatorEndpoint.parse(endpoint, "endpoint"),
                null);
    }

    static String tableName(TableDestination table) {
        return "projects/"
                + table.getProject()
                + "/instances/"
                + table.getInstance()
                + "/tables/"
                + table.getTable();
    }

    static LocalStagedHarness run(String id) {
        LocalStagedHarness run = RUNS.get(id);
        if (run == null) {
            throw new IllegalStateException("Local run is closed: " + id);
        }
        return run;
    }

    StagedMutationTestSink.Input input(long sequence) {
        byte[] payload = new byte[payloadBytes];
        new java.util.Random(sequence).nextBytes(payload);
        ByteBuffer.wrap(payload).putLong(sequence);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(
                Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setFamilyName("cf")
                                        .setColumnQualifier(ByteString.copyFromUtf8("q"))
                                        .setTimestampMicros(1000)
                                        .setValue(ByteString.copyFrom(payload)))
                        .build());
        if (aggregate) {
            mutations.add(
                    Mutation.newBuilder()
                            .setAddToCell(
                                    Mutation.AddToCell.newBuilder()
                                            .setFamilyName("agg")
                                            .setColumnQualifier(
                                                    com.google.bigtable.v2.Value.newBuilder()
                                                            .setRawValue(
                                                                    ByteString.copyFromUtf8(
                                                                            "count")))
                                            .setTimestamp(
                                                    com.google.bigtable.v2.Value.newBuilder()
                                                            .setRawTimestampMicros(1000))
                                            .setInput(
                                                    com.google.bigtable.v2.Value.newBuilder()
                                                            .setIntValue(1)))
                            .build());
        }
        String row =
                hot && sequence % 10 != 0
                        ? "hot"
                        : Long.toHexString(Long.rotateLeft(sequence * 0x9e3779b97f4a7c15L, 17));
        return new StagedMutationTestSink.Input(
                tableName(table), ByteString.copyFromUtf8(row), mutations);
    }

    synchronized void admitted(long sequence) {
        long now = System.nanoTime();
        admissions.putIfAbsent(sequence, now);
        firstAdmission = Math.min(firstAdmission, now);
    }

    synchronized void acknowledged(long sequence, long now) {
        acknowledgements.putIfAbsent(sequence, now);
        lastAck = Math.max(lastAck, now);
    }

    static long sequence(List<Mutation> mutations) {
        return mutations.get(0).getSetCell().getValue().asReadOnlyByteBuffer().getLong();
    }

    long percentileNanos(double quantile) {
        List<Long> latencies = new ArrayList<>();
        acknowledgements.forEach(
                (sequence, time) -> latencies.add(time - admissions.get(sequence)));
        Collections.sort(latencies);
        if (latencies.isEmpty()) {
            throw new IllegalStateException("No acknowledgements");
        }
        return latencies.get(Math.max(0, (int) Math.ceil(quantile * latencies.size()) - 1));
    }

    double throughput() {
        return acknowledgements.size() * 1_000_000_000.0 / (lastAck - firstAdmission);
    }

    void allowInputs() {
        allowInputs.set(true);
        inputsAllowed.complete(null);
    }

    void releaseSource() {
        allowInputs();
        finishSource.set(true);
        sourceReleased.complete(null);
    }

    @Override
    public void close() throws InterruptedException {
        releaseSource();
        receiver.shutdownNow();
        try {
            if (!receiver.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Local receiver did not terminate");
            }
        } finally {
            RUNS.remove(id, this);
        }
    }

    /** Test-only async receiver; the scheduler delay is an instrument, not service latency. */
    ApiFuture<Boolean> fake(CheckAndMutateRowRequest request) {
        SettableApiFuture<Boolean> future = SettableApiFuture.create();
        if (!hang) {
            receiver.schedule(
                    () -> {
                        if (!future.isCancelled()) {
                            try {
                                future.set(store.apply(request));
                            } catch (Exception failure) {
                                future.setException(failure);
                            }
                        }
                    },
                    delayMillis,
                    TimeUnit.MILLISECONDS);
        }
        return future;
    }

    /** A Long input avoids Kryo/protobuf serialization in the source-to-writer edge. */
    static final class StagedSink
            implements CrossVersionSink<Long>,
                    SupportsCommitter<CheckAndMutateRowRequest>,
                    SupportsPreCommitTopology<CheckAndMutateRowRequest, CheckAndMutateRowRequest> {
        private static final long serialVersionUID = 1L;
        final String runId;
        final boolean emulator;

        StagedSink(LocalStagedHarness run, boolean emulator) {
            this.runId = run.id;
            this.emulator = emulator;
        }

        @Override
        public CommittingSinkWriter<Long, CheckAndMutateRowRequest> createWriter(
                WriterInitContext context) {
            LocalStagedHarness run = run(runId);
            StagedMutationTestSink.Writer delegate =
                    new StagedMutationTestSink.Writer(run.maxEntries, run.maxBytes);
            return new CommittingSinkWriter<>() {

                @Override
                public void write(Long value, Context context) throws IOException {
                    run.admitted(value);
                    delegate.write(run.input(value), context);
                    run.staged.incrementAndGet();
                    run.writerStaged(delegate, delegate.stagedEntries(), delegate.stagedBytes());
                    run.peakWriterBytes.accumulateAndGet(delegate.stagedBytes(), Math::max);
                    run.peakWriterEntries.accumulateAndGet(delegate.stagedEntries(), Math::max);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public Collection<CheckAndMutateRowRequest> prepareCommit() throws IOException {
                    Collection<CheckAndMutateRowRequest> requests = delegate.prepareCommit();
                    run.writerStaged(delegate, 0, 0);
                    run.prepared(requests);
                    return requests;
                }

                @Override
                public void close() throws Exception {
                    delegate.close();
                    run.writerStaged(delegate, 0, 0);
                    if (run.failWriterClose.compareAndSet(true, false)) {
                        throw new IOException("Injected writer close failure after stop");
                    }
                }
            };
        }

        @Override
        public Committer<CheckAndMutateRowRequest> createCommitter(CommitterInitContext context) {
            return new LocalCommitter(run(runId), emulator);
        }

        @Override
        public SimpleVersionedSerializer<CheckAndMutateRowRequest> getCommittableSerializer() {
            return run(runId).serializer();
        }

        @Override
        public SimpleVersionedSerializer<CheckAndMutateRowRequest> getWriteResultSerializer() {
            return getCommittableSerializer();
        }

        @Override
        public DataStream<CommittableMessage<CheckAndMutateRowRequest>> addPreCommitTopology(
                DataStream<CommittableMessage<CheckAndMutateRowRequest>> stream) {
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env =
                    stream.getExecutionEnvironment();
            if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                            != RuntimeExecutionMode.STREAMING
                    || !env.getCheckpointConfig().isCheckpointingEnabled()
                    || env.getCheckpointConfig().getCheckpointingConsistencyMode()
                            != CheckpointingMode.EXACTLY_ONCE
                    || !env.getConfiguration()
                            .get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)) {
                throw new IllegalStateException(
                        "Local staged sink requires STREAMING, exactly-once checkpoints and checkpoints after tasks finish");
            }
            return stream;
        }
    }

    /** Uses bounded original futures, including during Flink's restored initializeState commit. */
    static final class LocalCommitter implements Committer<CheckAndMutateRowRequest> {
        final LocalStagedHarness run;
        final boolean emulator;
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        DefaultSingleRowClientFactory factory;
        SingleRowClient client;

        LocalCommitter(LocalStagedHarness run, boolean emulator) {
            this.run = run;
            this.emulator = emulator;
        }

        @Override
        public void commit(Collection<CommitRequest<CheckAndMutateRowRequest>> requests)
                throws IOException, InterruptedException {
            run.commitStarted(this, requests.size());
            boolean successful = false;
            try {
                for (CommitRequest<CheckAndMutateRowRequest> request : requests) {
                    if (pending.size() == run.inFlight) {
                        finishFirst();
                    }
                    CheckAndMutateRowRequest wire = request.getCommittable();
                    if (!wire.getTableName().equals(tableName(run.table))
                            || !wire.getAppProfileId().equals(PROFILE)) {
                        throw new IOException(
                                "Restored destination/profile differs from the local run");
                    }
                    run.beforeSend(wire);
                    long started;
                    ApiFuture<Boolean> future;
                    if (emulator) {
                        if (client == null) {
                            factory = run.singleRowFactory();
                            client = factory.create(run.table);
                        }
                        ConditionalRowMutation mutation = ConditionalRowMutation.fromProto(wire);
                        // The SDK overwrites these fields; prove the frozen request survives it.
                        if (!wire.equals(
                                mutation.toProto(
                                        RequestContext.create(
                                                run.table.getProject(),
                                                run.table.getInstance(),
                                                PROFILE)))) {
                            throw new IOException(
                                    "SDK reconstruction changed the persisted request");
                        }
                        started = System.nanoTime();
                        future = client.checkAndMutateRow(mutation);
                    } else {
                        started = System.nanoTime();
                        future = run.fake(wire);
                    }
                    run.trace(wire, future);
                    run.peakActive.accumulateAndGet(run.active.incrementAndGet(), Math::max);
                    pending.addLast(new Pending(request, future, started, run));
                }
                while (!pending.isEmpty()) {
                    finishFirst();
                }
                successful = true;
            } finally {
                try {
                    cancelPending();
                } finally {
                    run.commitFinished(this, successful);
                }
            }
        }

        void finishFirst() throws IOException, InterruptedException {
            Pending first = pending.getFirst();
            long waitStarted = System.nanoTime();
            try {
                boolean matched = first.observed.get(30, TimeUnit.SECONDS);
                CheckAndMutateRowRequest wire = first.request.getCommittable();
                run.commitAcknowledged(wire, first.completedAt);
                if (matched) {
                    run.deduplicated.incrementAndGet();
                    first.request.signalAlreadyCommitted();
                }
                pending.removeFirst();
                run.active.decrementAndGet();
            } catch (java.util.concurrent.ExecutionException
                    | java.util.concurrent.TimeoutException failure) {
                throw new IOException("Local conditional request failed", failure);
            } finally {
                run.committerWaited(System.nanoTime() - waitStarted);
            }
        }

        void cancelPending() {
            while (!pending.isEmpty()) {
                pending.removeFirst().future.cancel(true);
                run.active.decrementAndGet();
            }
        }

        @Override
        public void close() throws Exception {
            cancelPending();
            try {
                if (factory != null) {
                    factory.close();
                }
            } finally {
                run.committersClosed.incrementAndGet();
            }
        }
    }

    static final class Pending {
        final Committer.CommitRequest<CheckAndMutateRowRequest> request;
        final ApiFuture<Boolean> future;
        final ApiFuture<Boolean> observed;
        volatile long completedAt;

        Pending(
                Committer.CommitRequest<CheckAndMutateRowRequest> request,
                ApiFuture<Boolean> future,
                long started,
                LocalStagedHarness run) {
            this.request = request;
            this.future = future;
            this.observed =
                    ApiFutures.transform(
                            future,
                            matched -> {
                                completedAt = System.nanoTime();
                                run.clientCompleted(
                                        sequence(request.getCommittable().getFalseMutationsList()),
                                        completedAt - started,
                                        completedAt);
                                return matched;
                            },
                            Runnable::run);
        }
    }

    /** Instruments the production bulk writer through its existing package-private factory seam. */
    static final class BulkSink extends BigtableMutateRowsSink<Long> {
        private static final long serialVersionUID = 1L;
        final String runId;
        final boolean emulator;

        BulkSink(LocalStagedHarness run, boolean emulator) {
            super(
                    ((BigtableMutateRowsSink<Long>)
                                    BigtableSink.<Long>builder()
                                            .table(run.table)
                                            .emulatorEndpoint(run.endpoint)
                                            .writerOptions(
                                                    BigtableWriterOptions.builder()
                                                            .maxInFlightEntries(run.inFlight)
                                                            .build())
                                            .serializer(new Serializer(run.id))
                                            .build())
                            .getConfig());
            this.runId = run.id;
            this.emulator = emulator;
        }

        @Override
        public SinkWriter<Long> createWriter(WriterInitContext context) throws IOException {
            return createWriter(context, new ObservedBatchers(run(runId), emulator));
        }
    }

    static final class Serializer
            implements io.github.flink.gcp.connector.bigtable.sink.serializer
                            .BigtableSerializationSchema<
                    Long> {
        private static final long serialVersionUID = 1L;
        final String runId;

        Serializer(String runId) {
            this.runId = runId;
        }

        @Override
        public RowMutationEntry serialize(Long value, SinkWriter.Context context) {
            LocalStagedHarness run = run(runId);
            run.admitted(value);
            StagedMutationTestSink.Input input = run.input(value);
            if (run.aggregate) {
                throw new IllegalStateException("SUM correctness uses the staged arm only");
            }
            Mutation.SetCell cell = input.mutations().get(0).getSetCell();
            return RowMutationEntry.create(input.row())
                    .setCell(
                            cell.getFamilyName(),
                            cell.getColumnQualifier(),
                            cell.getTimestampMicros(),
                            cell.getValue());
        }
    }

    static final class ObservedBatchers implements MutationBatcherFactory {
        private static final long serialVersionUID = 1L;
        final transient LocalStagedHarness run;
        final transient MutationBatcherFactory delegate;

        ObservedBatchers(LocalStagedHarness run, boolean emulator) throws IOException {
            this.run = run;
            this.delegate = emulator ? run.batcherFactory() : null;
        }

        @Override
        public MutationBatcher create(TableDestination table)
                throws IOException, InterruptedException {
            MutationBatcher batcher = delegate == null ? null : delegate.create(table);
            return new MutationBatcher() {

                @Override
                public ApiFuture<Void> add(RowMutationEntry entry) {
                    long started = System.nanoTime();
                    MutateRowsRequest.Entry wire = entry.toProto();
                    run.beforeBulk(wire);
                    ApiFuture<Void> future;
                    if (batcher == null) {
                        SettableApiFuture<Void> answer = SettableApiFuture.create();
                        run.receiver.schedule(
                                () -> answer.set(null), run.delayMillis, TimeUnit.MILLISECONDS);
                        future = answer;
                    } else {
                        future = batcher.add(entry);
                    }
                    run.peakActive.accumulateAndGet(run.active.incrementAndGet(), Math::max);
                    ApiFutures.addCallback(
                            future,
                            new ApiFutureCallback<Void>() {

                                @Override
                                public void onSuccess(Void ignored) {
                                    long now = System.nanoTime();
                                    run.clientCompleted(
                                            sequence(wire.getMutationsList()), now - started, now);
                                    run.acknowledged(sequence(wire.getMutationsList()), now);
                                    run.active.decrementAndGet();
                                }

                                @Override
                                public void onFailure(Throwable failure) {
                                    run.active.decrementAndGet();
                                }
                            },
                            Runnable::run);
                    return future;
                }

                @Override
                public void sendOutstanding() {
                    if (batcher != null) {
                        batcher.sendOutstanding();
                    }
                }

                @Override
                public void shutdown() {
                    if (batcher != null) {
                        batcher.shutdown();
                    }
                }

                @Override
                public void close() throws Exception {
                    if (batcher != null) {
                        batcher.close();
                    }
                }
            };
        }

        @Override
        public void release(TableDestination table) throws Exception {
            if (delegate != null) {
                delegate.release(table);
            }
        }

        @Override
        public void close() throws Exception {
            if (delegate != null) {
                delegate.close();
            }
        }
    }
}
