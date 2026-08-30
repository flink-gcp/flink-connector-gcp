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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.BigQueryLoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.CopyJobSpec;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.DestinationCommitExecutor;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.FakeLoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.FakeTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobSpec;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.QueryJobSpec;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsCommitter} against recording fakes. */
class FileLoadsCommitterTest {

    @TempDir Path tempDir;

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /** A serializer only used for its schema. */
    private static class SchemaOnlySerializer extends BigQueryProtoSerializationSchema<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(Object element) {
            return ByteString.EMPTY;
        }
    }

    private static final class TrackingSchemaSerializer extends SchemaOnlySerializer {
        private static final long serialVersionUID = 1L;

        private final CallbackConcurrency concurrency;

        private TrackingSchemaSerializer(CallbackConcurrency concurrency) {
            this.concurrency = concurrency;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return concurrency.invoke(() -> SCHEMA);
        }
    }

    /** A minimal {@link CommitRequest}; signals are irrelevant to these tests. */
    private static final class TestCommitRequest implements CommitRequest<FileLoadsCommittable> {

        private final FileLoadsCommittable committable;

        TestCommitRequest(FileLoadsCommittable committable) {
            this.committable = committable;
        }

        @Override
        public FileLoadsCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(FileLoadsCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {}
    }

    /** Everything one committer test touches. */
    private static final class Harness {
        final FakeLoadJobRunner runner = new FakeLoadJobRunner();
        final FakeTableAdmin tableAdmin = new FakeTableAdmin();
        final InMemoryStagingStorage storage = new InMemoryStagingStorage();
        final TestSinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();
        final FileLoadsCommitter committer;

        Harness() {
            this(FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(), null);
        }

        Harness(FileLoadsOptions options, DestinationCommitExecutor.WorkerFactory workerFactory) {
            BigQuerySinkConfig<Object> config =
                    ((BigQueryFileLoadsSink<Object>)
                                    BigQuerySink.builder()
                                            .writeMethod(WriteMethod.FILE_LOADS)
                                            .table(T1)
                                            .serializer(new SchemaOnlySerializer())
                                            .fileLoadsOptions(options)
                                            .build())
                            .getConfig();
            this.committer =
                    workerFactory == null
                            ? new FileLoadsCommitter(
                                    config,
                                    options,
                                    storage,
                                    metrics,
                                    () -> runner,
                                    () -> tableAdmin)
                            : new FileLoadsCommitter(
                                    config, options, storage, metrics, workerFactory);
        }

        void commit(FileLoadsCommittable... committables) throws IOException {
            committer.commit(
                    Arrays.stream(committables)
                            .map(TestCommitRequest::new)
                            .collect(Collectors.toList()));
        }
    }

    @Test
    void closeClosesTheStagingStorageAndDestinationWorkers() throws Exception {
        // #820: this close() was an empty body while the committer held a staging storage of its
        // own. The committer sits behind addPreCommitTopology's global exchange, in a vertex of
        // its own, so the writer closing its copy of the sink's storage releases nothing here.
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                        .build();
        Harness harness =
                new Harness(
                        options,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FakeLoadJobRunner(), new FakeTableAdmin()));
        Set<Thread> threadsBefore = commitThreads();
        Set<Thread> startedThreads = Set.of();
        boolean closed = false;

        try {
            harness.commit(file(T1, "a"), file(TableDestination.of("p", "d", "t2"), "b"));
            startedThreads = commitThreads();
            startedThreads.removeAll(threadsBefore);
            assertThat(startedThreads).isNotEmpty();

            harness.committer.close();
            closed = true;

            for (Thread thread : startedThreads) {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            }
            assertThat(startedThreads).noneMatch(Thread::isAlive);
        } finally {
            if (!closed) {
                harness.committer.close();
            }
        }

        assertThat(harness.storage.getCloseCount()).isEqualTo(1);
    }

    @Test
    void theDefaultAdminFactoryWrapsForTheCreationRetry() throws IOException {
        // Every case above injects its own factory, so a public constructor that stopped wrapping
        // would leave them all green — and the only thing to notice would be a commit failing a
        // creation race it could have waited out (#383). The reconcile budget and not a knob of
        // its own: it is already this write method's budget for contention on the same per-table
        // metadata quota, so the attempt count is what names which schedule was taken.
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .schemaReconcileMaxAttempts(6)
                        .build();
        String keyFile = ServiceAccountKeyFiles.create(tempDir).toString();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .serviceAccountKeyFile(keyFile)
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        FileLoadsCommitter committer =
                new FileLoadsCommitter(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        TestSinkCommitterMetricGroup.create());

        TableAdmin admin = committer.tableAdmin();

        assertThat(admin).isInstanceOf(RetryingTableAdmin.class);
        assertThat(((RetryingTableAdmin) admin).getSchedule().maxAttempts()).isEqualTo(6);
    }

    @Test
    void eachCommitWorkerOwnsOneCredentialledClientAndSharesSubmittedJobs() throws Exception {
        String keyFile = ServiceAccountKeyFiles.create(tempDir).toString();
        FileLoadsOptions options =
                FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .serviceAccountKeyFile(keyFile)
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        DestinationCommitExecutor.WorkerFactory factory =
                FileLoadsCommitter.productionWorkerFactory(config, options);
        DestinationCommitExecutor.Worker first = factory.create();
        DestinationCommitExecutor.Worker second = factory.create();
        BigQueryLoadJobRunner firstRunner = (BigQueryLoadJobRunner) first.runner();
        BigQueryLoadJobRunner secondRunner = (BigQueryLoadJobRunner) second.runner();

        assertThat(firstRunner.getClient()).isNotNull();
        assertThat(first.tableAdmin())
                .isInstanceOf(RetryingTableAdmin.class)
                .asInstanceOf(
                        org.assertj.core.api.InstanceOfAssertFactories.type(
                                RetryingTableAdmin.class))
                .extracting(RetryingTableAdmin::getDelegate)
                .isInstanceOf(BigQueryTableAdmin.class)
                .asInstanceOf(
                        org.assertj.core.api.InstanceOfAssertFactories.type(
                                BigQueryTableAdmin.class))
                .extracting(BigQueryTableAdmin::getClient)
                .isSameAs(firstRunner.getClient());
        assertThat(secondRunner).isNotSameAs(firstRunner);
        assertThat(second.tableAdmin()).isNotSameAs(first.tableAdmin());
        assertThat(secondRunner.getClient()).isNotSameAs(firstRunner.getClient());
        Field sharedJobs = BigQueryLoadJobRunner.class.getDeclaredField("sharedJobs");
        assertThat(sharedJobs.trySetAccessible()).isTrue();
        assertThat(sharedJobs.get(secondRunner)).isSameAs(sharedJobs.get(firstRunner));
    }

    private static FileLoadsCommittable file(String name) {
        return file(T1, name);
    }

    private static Set<Thread> commitThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("bigquery-file-loads-commit-"))
                .collect(Collectors.toCollection(HashSet::new));
    }

    private static FileLoadsCommittable file(TableDestination destination, String name) {
        return file(destination, name, StagingFormat.AVRO);
    }

    private static FileLoadsCommittable file(
            TableDestination destination, String name, StagingFormat format) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID,
                destination,
                "gs://bucket/prefix/"
                        + name
                        + (format == StagingFormat.AVRO ? ".avro" : ".parquet"),
                10,
                5,
                format);
    }

    @Test
    void batchCommittablesLoadWithoutACheckpointSegment() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("b"), file("a"));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id ->
                                assertThat(id)
                                        .matches(
                                                "flink-bq-load-" + FLINK_JOB_ID + "-[0-9a-f]{16}"));
        assertThat(harness.runner.loads.values().iterator().next().getSourceUris())
                .containsExactly("gs://bucket/prefix/a.avro", "gs://bucket/prefix/b.avro");
        assertThat(harness.storage.getDeleted())
                .containsExactlyInAnyOrder(
                        "gs://bucket/prefix/a.avro", "gs://bucket/prefix/b.avro");
    }

    @Test
    void stampedCommittablesLoadWithTheirCheckpointSegment() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a").withCheckpointId(7), file("b").withCheckpointId(7));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(id -> assertThat(id).contains("-c7-"));
        assertThat(harness.storage.getDeleted()).hasSize(2);
    }

    @Test
    void mixedCommitBatchesAreRejected() {
        // The framework commits one checkpoint at a time; a mixed batch would break the
        // per-checkpoint job-id attribution, so the invariant fails loudly.
        Harness harness = new Harness();

        assertThatThrownBy(
                        () ->
                                harness.commit(
                                        file("a").withCheckpointId(1),
                                        file("b").withCheckpointId(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mixes");
        assertThat(harness.runner.loads).isEmpty();
    }

    @Test
    void jobIdsFollowTheCommittablesOriginatingFlinkJobId() throws IOException {
        // Committer state restored under a NEW Flink job id must reproduce the ORIGINAL job's
        // deterministic BigQuery job ids so the runner re-attaches instead of double-loading;
        // the id therefore comes from the committable, not from the runtime.
        Harness harness = new Harness();
        String originalJobId = "fedcba9876543210fedcba9876543210";

        harness.commit(
                new FileLoadsCommittable(
                                originalJobId,
                                T1,
                                "gs://bucket/prefix/a.avro",
                                10,
                                5,
                                StagingFormat.AVRO)
                        .withCheckpointId(3));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id -> assertThat(id).startsWith("flink-bq-load-" + originalJobId + "-c3-"));
    }

    @Test
    void loadFailurePropagatesAndLeavesFilesInPlace() {
        Harness harness = new Harness();
        harness.runner.failAllAwaits = true;

        assertThatThrownBy(() -> harness.commit(file("a").withCheckpointId(3)))
                .isInstanceOf(IOException.class);

        assertThat(harness.storage.getDeleted()).isEmpty();
    }

    @Test
    void eachCommitReconcilesTheDestinationAgain() throws IOException {
        // The committer builds an orchestrator and destination plan per commit. Reusing a
        // reconciled schema across those plans would silently skip the second commit's live-table
        // read, and every other test in this suite would stay green.
        Harness harness = new Harness();
        // Pre-populating the table makes each commit owe exactly one read, and checking after each
        // commit locates which commit skipped or repeated reconciliation.
        harness.tableAdmin.tables.put(T1, SCHEMA);

        harness.commit(file("a"));
        assertThat(harness.tableAdmin.schemaReads).isEqualTo(1);

        harness.commit(file("b").withCheckpointId(1));
        assertThat(harness.tableAdmin.schemaReads).isEqualTo(2);
    }

    @Test
    void countsEveryLoadJobSubmitted() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a"));
        harness.commit(file("b").withCheckpointId(1));

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.metrics.counterValue("loadJobsSubmitted")).isEqualTo(2);
    }

    @Test
    void failedProductionOrchestrationClearsLiveCommitMetrics() throws IOException {
        Harness harness = new Harness();
        harness.runner.failAllAwaits = true;
        harness.runner.awaitDelayMillis = 10;

        assertThatThrownBy(() -> harness.commit(file("a")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("failed (scripted)");

        assertThat(
                        harness.metrics.<Integer>gaugeValue(
                                BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(
                        harness.metrics.<Integer>gaugeValue(
                                BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(
                        harness.metrics.<Long>gaugeValue(
                                BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                .isZero();
    }

    @Test
    void productionOrchestrationCommitsFiftyDestinationsWithinTheConfiguredBound()
            throws Exception {
        int maximumConcurrency = 4;
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        AtomicInteger started = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicBoolean cleanupObservedDuringCommit = new AtomicBoolean();
        CallbackConcurrency schemaCallbacks = new CallbackConcurrency();
        CallbackConcurrency tableOptionsCallbacks = new CallbackConcurrency();
        CyclicBarrier workersReady = new CyclicBarrier(maximumConcurrency);
        CyclicBarrier schemaReadsReady = new CyclicBarrier(maximumConcurrency);
        CyclicBarrier firstWave = new CyclicBarrier(maximumConcurrency);
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(maximumConcurrency)
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new TrackingSchemaSerializer(schemaCallbacks))
                                        .tableCreateOptionsProvider(
                                                destination ->
                                                        tableOptionsCallbacks.invoke(
                                                                TableCreateOptions::defaults))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        TestSinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();
        FileLoadsCommitter committer =
                new FileLoadsCommitter(
                        config,
                        options,
                        storage,
                        metrics,
                        () -> {
                            try {
                                workersReady.await(5, TimeUnit.SECONDS);
                            } catch (Exception failure) {
                                throw new IOException(
                                        "Timed out waiting for committer workers", failure);
                            }
                            FakeTableAdmin tableAdmin = new FakeTableAdmin();
                            tableAdmin.firstSchemaReadBarrier = schemaReadsReady;
                            return new DestinationCommitExecutor.Worker(
                                    new TrackingRunner(
                                            maximumConcurrency,
                                            active,
                                            maximumActive,
                                            started,
                                            completed,
                                            firstWave,
                                            metrics,
                                            cleanupObservedDuringCommit),
                                    tableAdmin);
                        });
        FileLoadsCommittable[] files =
                IntStream.range(0, 50)
                        .boxed()
                        .flatMap(
                                index ->
                                        Stream.of(
                                                file(
                                                        TableDestination.of("p", "d", "t" + index),
                                                        "file-" + index + "-avro",
                                                        StagingFormat.AVRO),
                                                file(
                                                        TableDestination.of("p", "d", "t" + index),
                                                        "file-" + index + "-parquet",
                                                        StagingFormat.PARQUET)))
                        .toArray(FileLoadsCommittable[]::new);

        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                committer.commit(
                                        Arrays.stream(files)
                                                .map(TestCommitRequest::new)
                                                .collect(Collectors.toList()));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitSerializedCallback(schemaCallbacks);
            schemaCallbacks.releaseFirst();
            awaitSerializedCallback(tableOptionsCallbacks);
            tableOptionsCallbacks.releaseFirst();
            coordinator.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            schemaCallbacks.releaseFirst();
            tableOptionsCallbacks.releaseFirst();
            coordinator.join(TimeUnit.SECONDS.toMillis(10));
            committer.close();
        }

        assertThat(coordinator.isAlive()).isFalse();
        assertThat(observed.get()).isNull();
        assertThat(completed).hasValue(150);
        assertThat(maximumActive).hasValue(maximumConcurrency);
        assertThat(storage.getDeleted()).hasSize(100);
        assertThat(metrics.counterValue("loadJobsSubmitted")).isEqualTo(100);
        assertThat(schemaCallbacks.maximumActive).hasValue(1);
        assertThat(tableOptionsCallbacks.maximumActive).hasValue(1);
        assertThat(cleanupObservedDuringCommit).isTrue();
    }

    @Test
    void streamingCommitsArrivingTooFastAreReported() throws IOException {
        // The runtime backstop for the graph-construction guard, which cannot see cluster-side
        // checkpoint configuration. Both commits have to carry a checkpoint id: the check runs
        // only for a streaming commit and stamps unconditionally, so a first commit without one
        // leaves the stamp at zero and the second finds nothing to compare against - which is why
        // countsEveryLoadJobSubmitted above has never reached this branch (#323).
        Harness harness = new Harness();

        harness.commit(file("a").withCheckpointId(1));

        try (LogCapture capture = LogCapture.of(FileLoadsCommitter.class)) {
            harness.commit(file("b").withCheckpointId(2));

            // Nothing else reports it: both commits succeed and both load jobs are counted.
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("1,500 modifications")
                    .contains(
                            String.valueOf(
                                    FileLoadsOptions.DEFAULT_MIN_CHECKPOINT_INTERVAL.toMillis()));
        }

        assertThat(harness.metrics.counterValue("loadJobsSubmitted")).isEqualTo(2);
    }

    @Test
    void countsNoLoadJobForAnEmptyCommit() throws IOException {
        Harness harness = new Harness();

        harness.commit();

        assertThat(harness.metrics.counterValue("loadJobsSubmitted")).isZero();
    }

    @Test
    void streamingCommitsAppendWithTheConfiguredDispositions() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a").withCheckpointId(1));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getDestination()).isEqualTo(T1);
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
                        });
    }

    private static final class CallbackConcurrency {
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maximumActive = new AtomicInteger();
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);

        private <T> T invoke(Supplier<T> callback) {
            int now = active.incrementAndGet();
            maximumActive.accumulateAndGet(now, Math::max);
            try {
                if (calls.incrementAndGet() == 1) {
                    firstEntered.countDown();
                    if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release the first callback");
                    }
                }
                return callback.get();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Callback concurrency test was interrupted", failure);
            } finally {
                active.decrementAndGet();
            }
        }

        private void releaseFirst() {
            releaseFirst.countDown();
        }
    }

    private static void awaitSerializedCallback(CallbackConcurrency callbacks) throws Exception {
        assertThat(callbacks.firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean blocked = false;
        while (callbacks.maximumActive.get() == 1 && !blocked && System.nanoTime() < deadline) {
            blocked =
                    Thread.getAllStackTraces().keySet().stream()
                            .anyMatch(
                                    thread ->
                                            thread.getName()
                                                            .startsWith(
                                                                    "bigquery-file-loads-commit-")
                                                    && thread.getState() == Thread.State.BLOCKED);
            Thread.onSpinWait();
        }
        assertThat(callbacks.maximumActive).hasValue(1);
        assertThat(blocked).isTrue();
    }

    private static final class TrackingRunner implements LoadJobRunner {
        private final int maximumConcurrency;
        private final AtomicInteger active;
        private final AtomicInteger maximumActive;
        private final AtomicInteger started;
        private final AtomicInteger completed;
        private final CyclicBarrier firstWave;
        private final TestSinkCommitterMetricGroup metrics;
        private final AtomicBoolean cleanupObservedDuringCommit;

        private TrackingRunner(
                int maximumConcurrency,
                AtomicInteger active,
                AtomicInteger maximumActive,
                AtomicInteger started,
                AtomicInteger completed,
                CyclicBarrier firstWave,
                TestSinkCommitterMetricGroup metrics,
                AtomicBoolean cleanupObservedDuringCommit) {
            this.maximumConcurrency = maximumConcurrency;
            this.active = active;
            this.maximumActive = maximumActive;
            this.started = started;
            this.completed = completed;
            this.firstWave = firstWave;
            this.metrics = metrics;
            this.cleanupObservedDuringCommit = cleanupObservedDuringCommit;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) throws IOException {
            int now = active.incrementAndGet();
            maximumActive.accumulateAndGet(now, Math::max);
            try {
                if (started.incrementAndGet() <= maximumConcurrency) {
                    firstWave.await(5, TimeUnit.SECONDS);
                }
                completed.incrementAndGet();
            } catch (Exception failure) {
                throw new IOException("Timed out waiting for committer test workers", failure);
            } finally {
                active.decrementAndGet();
            }
        }

        @Override
        public void deleteTable(TableDestination table) {
            if (metrics.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS) > 0
                    && metrics.<Long>gaugeValue(BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS)
                            > 0) {
                cleanupObservedDuringCommit.set(true);
            }
        }
    }
}
