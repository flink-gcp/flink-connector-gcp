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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.metrics.SimpleCounter;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Failure-graph regressions whose coordination helpers are kept separate from the main suite. */
class LoadJobOrchestratorFailureAggregationTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");
    private static final TableDestination T2 = TableDestination.of("p", "d", "t2");
    private static final TableDestination T3 = TableDestination.of("p", "d", "t3");
    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    @Test
    void terminalQueryWaveIsCappedAtThePublishedInteractiveQueryLimit() throws Exception {
        int destinations = 1_001;
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .maxConcurrentDestinations(8)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        AtomicInteger pendingQueries = new AtomicInteger();
        AtomicInteger maximumPendingQueries = new AtomicInteger();
        AtomicInteger querySubmissions = new AtomicInteger();
        ConcurrentMap<String, Boolean> queryJobs = new ConcurrentHashMap<>();
        CountDownLatch firstQueryAwait = new CountDownLatch(1);
        CountDownLatch releaseQueryAwaits = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        8,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new QueryPendingJobRunner(
                                                pendingQueries,
                                                maximumPendingQueries,
                                                querySubmissions,
                                                queryJobs,
                                                firstQueryAwait,
                                                releaseQueryAwaits),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        Limits.BIGQUERY,
                        executor);
        List<FileLoadsCommittable> files = new ArrayList<>(destinations * 2);
        for (int index = 0; index < destinations; index++) {
            TableDestination destination = TableDestination.of("p", "d", "t" + index);
            files.add(file(destination, "avro", 10, StagingFormat.AVRO));
            files.add(file(destination, "parquet", 10, StagingFormat.PARQUET));
        }
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(files);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(firstQueryAwait.await(30, TimeUnit.SECONDS)).isTrue();

            assertThat(querySubmissions).hasValue(1_000);
            assertThat(pendingQueries).hasValue(1_000);
            assertThat(maximumPendingQueries).hasValue(1_000);
            assertThat(coordinator.isAlive()).isTrue();

            releaseQueryAwaits.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isNull();
            assertThat(querySubmissions).hasValue(destinations);
            assertThat(pendingQueries).hasValue(0);
            assertThat(maximumPendingQueries).hasValue(1_000);
        } finally {
            releaseQueryAwaits.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void awaitInterruptionAndAnEarlierBatchFailureAreEachReportedOnce() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(1)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException ordinaryFailure = new IOException("scripted ordinary await failure");
        AtomicReference<IOException> interruptionFailure = new AtomicReference<>();
        CountDownLatch secondAwaitStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondAwait = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new OrdinaryThenBlockingAwaitRunner(
                                                ordinaryFailure,
                                                interruptionFailure,
                                                secondAwaitStarted,
                                                releaseSecondAwait),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file("part-1", 8L << 40),
                                                file("part-2", 8L << 40)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(secondAwaitStarted);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(interruptionFailure.get());
            assertThat(failureReferences(observed.get(), interruptionFailure.get())).isOne();
            assertThat(failureReferences(observed.get(), ordinaryFailure)).isOne();
        } finally {
            releaseSecondAwait.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void overlappingSubmissionAwaitAndInterruptionFailuresAreEachReportedOnce() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(1)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException submissionFailure = new IOException("scripted submission failure");
        IOException awaitFailure = new IOException("scripted ordinary await failure");
        AtomicReference<IOException> interruptionFailure = new AtomicReference<>();
        CountDownLatch secondAwaitStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondAwait = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new SubmissionThenOrdinaryThenBlockingAwaitRunner(
                                                submissionFailure,
                                                awaitFailure,
                                                interruptionFailure,
                                                secondAwaitStarted,
                                                releaseSecondAwait),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 3),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file("part-1", 8L << 40),
                                                file("part-2", 8L << 40),
                                                file("part-3", 8L << 40)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(secondAwaitStarted);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(interruptionFailure.get());
            assertThat(failureReferences(observed.get(), interruptionFailure.get())).isOne();
            assertThat(failureReferences(observed.get(), submissionFailure)).isOne();
            assertThat(failureReferences(observed.get(), awaitFailure)).isOne();
        } finally {
            releaseSecondAwait.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void serialDrainInterruptionPrecedesAwaitWorkerCreationFailure() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException workerFailure = new IOException("scripted await worker creation failure");
        AtomicReference<IOException> interruptionFailure = new AtomicReference<>();
        ExecutorService workers = new PerTaskThreadExecutor("serial-drain-worker");
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> {
                            if (Thread.currentThread().getName().endsWith("-5")) {
                                throw workerFailure;
                            }
                            return new DestinationCommitExecutor.Worker(
                                    new InterruptingSerialAwaitRunner(interruptionFailure),
                                    new FakeTableAdmin());
                        },
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()),
                        workers);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(file(T1, "part-1", 10), file(T2, "part-2", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        },
                        "serial-drain-coordinator");

        try {
            coordinator.start();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(interruptionFailure.get());
            assertThat(failureReferences(observed.get(), workerFailure)).isOne();
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void fatalAwaitFailureKeepsEarlierSubmissionFailuresInPlanOrder() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(3)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException recordedFailure = new IOException("scripted recorded submission");
        IOException executorOnlyFailure = new IOException("scripted interrupted submission");
        OutOfMemoryError fatalFailure = new OutOfMemoryError("scripted fatal await");
        CountDownLatch submissionsStarted = new CountDownLatch(3);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        3,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new InterruptedSubmissionThenFatalAwaitRunner(
                                                recordedFailure,
                                                executorOnlyFailure,
                                                fatalFailure,
                                                submissionsStarted),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 3),
                        executor);

        try {
            try {
                orchestrator.run(
                        List.of(
                                file(T1, "part-1", 10),
                                file(T2, "part-2", 10),
                                file(T3, "part-3", 10)));
                throw new AssertionError("The scripted await should fail fatally");
            } catch (Throwable observed) {
                assertThat(observed).isSameAs(fatalFailure);
                assertThat(observed.getSuppressed())
                        .containsExactly(recordedFailure, executorOnlyFailure);
            }
        } finally {
            executor.close();
        }
    }

    @Test
    void coordinatorInterruptDuringFatalDrainRemainsSuppressedAfterBatchAggregation()
            throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        OutOfMemoryError fatalFailure = new OutOfMemoryError("scripted fatal await");
        CountDownLatch awaitsStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        FatalAndBlockingAwaitRunner runner =
                new FatalAndBlockingAwaitRunner(fatalFailure, awaitsStarted, releasePeer);
        TrackingTerminationExecutor workerPool = new TrackingTerminationExecutor();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()),
                        workerPool);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(file(T1, "part-1", 10), file(T2, "part-2", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(awaitsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(workerPool.drainStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            assertThat(workerPool.interruptObserved.await(5, TimeUnit.SECONDS)).isTrue();
            releasePeer.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(fatalFailure);
            assertThat(observed.get().getSuppressed())
                    .extracting(Throwable::getMessage)
                    .containsExactly(
                            "Interrupted while draining FILE_LOADS destinations after a JVM-fatal failure");
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    private static FileLoadsCommittable file(String name, long bytes) {
        return file(T1, name, bytes);
    }

    private static FileLoadsCommittable file(
            TableDestination destination, String name, long bytes) {
        return file(destination, name, bytes, StagingFormat.AVRO);
    }

    private static FileLoadsCommittable file(
            TableDestination destination, String name, long bytes, StagingFormat format) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID,
                destination,
                "gs://bucket/prefix/" + name + format.getExtension(),
                bytes,
                10,
                format);
    }

    private static void awaitLatch(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for orchestrator test coordination");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Orchestrator test coordination was interrupted", failure);
        }
    }

    private static int failureReferences(Throwable failure, Throwable expected) {
        List<Throwable> pending = new ArrayList<>();
        int references = 0;
        pending.add(failure);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            if (candidate == expected) {
                references++;
            }
            pending.addAll(List.of(candidate.getSuppressed()));
        }
        return references;
    }

    private static final class SchemaOnlySerializer
            extends BigQueryProtoSerializationSchema<Object> {
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

    private static final class QueryPendingJobRunner implements LoadJobRunner {
        private final AtomicInteger pendingQueries;
        private final AtomicInteger maximumPendingQueries;
        private final AtomicInteger querySubmissions;
        private final ConcurrentMap<String, Boolean> queryJobs;
        private final CountDownLatch firstQueryAwait;
        private final CountDownLatch releaseQueryAwaits;

        private QueryPendingJobRunner(
                AtomicInteger pendingQueries,
                AtomicInteger maximumPendingQueries,
                AtomicInteger querySubmissions,
                ConcurrentMap<String, Boolean> queryJobs,
                CountDownLatch firstQueryAwait,
                CountDownLatch releaseQueryAwaits) {
            this.pendingQueries = pendingQueries;
            this.maximumPendingQueries = maximumPendingQueries;
            this.querySubmissions = querySubmissions;
            this.queryJobs = queryJobs;
            this.firstQueryAwait = firstQueryAwait;
            this.releaseQueryAwaits = releaseQueryAwaits;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            queryJobs.put(jobId, true);
            querySubmissions.incrementAndGet();
            int pending = pendingQueries.incrementAndGet();
            maximumPendingQueries.accumulateAndGet(pending, Math::max);
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            if (queryJobs.remove(jobId) == null) {
                return;
            }
            firstQueryAwait.countDown();
            try {
                if (!releaseQueryAwaits.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting to release terminal-query jobs");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException("Terminal-query await was interrupted", failure);
            }
            pendingQueries.decrementAndGet();
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static class OrdinaryThenBlockingAwaitRunner implements LoadJobRunner {
        private final IOException ordinaryFailure;
        private final AtomicReference<IOException> interruptionFailure;
        private final CountDownLatch secondAwaitStarted;
        private final CountDownLatch releaseSecondAwait;
        private int awaits;

        private OrdinaryThenBlockingAwaitRunner(
                IOException ordinaryFailure,
                AtomicReference<IOException> interruptionFailure,
                CountDownLatch secondAwaitStarted,
                CountDownLatch releaseSecondAwait) {
            this.ordinaryFailure = ordinaryFailure;
            this.interruptionFailure = interruptionFailure;
            this.secondAwaitStarted = secondAwaitStarted;
            this.releaseSecondAwait = releaseSecondAwait;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The await-interruption test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The await-interruption test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            if (awaits++ == 0) {
                throw ordinaryFailure;
            }
            secondAwaitStarted.countDown();
            try {
                if (releaseSecondAwait.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("The second await was released without interruption");
                }
                throw new IOException("Timed out waiting for the coordinator interruption");
            } catch (InterruptedException cause) {
                Thread.currentThread().interrupt();
                IOException failure =
                        new IOException("scripted await interruption after a failure", cause);
                interruptionFailure.set(failure);
                throw failure;
            }
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class SubmissionThenOrdinaryThenBlockingAwaitRunner
            extends OrdinaryThenBlockingAwaitRunner {
        private final IOException submissionFailure;
        private int submissions;

        private SubmissionThenOrdinaryThenBlockingAwaitRunner(
                IOException submissionFailure,
                IOException ordinaryFailure,
                AtomicReference<IOException> interruptionFailure,
                CountDownLatch secondAwaitStarted,
                CountDownLatch releaseSecondAwait) {
            super(ordinaryFailure, interruptionFailure, secondAwaitStarted, releaseSecondAwait);
            this.submissionFailure = submissionFailure;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            if (submissions++ == 2) {
                throw submissionFailure;
            }
        }
    }

    private static final class InterruptedSubmissionThenFatalAwaitRunner implements LoadJobRunner {
        private final IOException recordedFailure;
        private final IOException executorOnlyFailure;
        private final OutOfMemoryError fatalFailure;
        private final CountDownLatch submissionsStarted;

        private InterruptedSubmissionThenFatalAwaitRunner(
                IOException recordedFailure,
                IOException executorOnlyFailure,
                OutOfMemoryError fatalFailure,
                CountDownLatch submissionsStarted) {
            this.recordedFailure = recordedFailure;
            this.executorOnlyFailure = executorOnlyFailure;
            this.fatalFailure = fatalFailure;
            this.submissionsStarted = submissionsStarted;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissionsStarted.countDown();
            awaitLatch(submissionsStarted);
            if (spec.getDestination().equals(T1)) {
                throw recordedFailure;
            }
            if (spec.getDestination().equals(T2)) {
                Thread.currentThread().interrupt();
                throw executorOnlyFailure;
            }
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The fatal-await test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The fatal-await test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            throw fatalFailure;
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class FatalAndBlockingAwaitRunner implements LoadJobRunner {
        private final OutOfMemoryError fatalFailure;
        private final CountDownLatch awaitsStarted;
        private final CountDownLatch releasePeer;
        private final AtomicBoolean fatalClaimed = new AtomicBoolean();

        private FatalAndBlockingAwaitRunner(
                OutOfMemoryError fatalFailure,
                CountDownLatch awaitsStarted,
                CountDownLatch releasePeer) {
            this.fatalFailure = fatalFailure;
            this.awaitsStarted = awaitsStarted;
            this.releasePeer = releasePeer;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The fatal-drain test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The fatal-drain test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            awaitsStarted.countDown();
            awaitIgnoringInterrupt(awaitsStarted);
            if (fatalClaimed.compareAndSet(false, true)) {
                throw fatalFailure;
            }
            awaitIgnoringInterrupt(releasePeer);
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class InterruptingSerialAwaitRunner implements LoadJobRunner {
        private final AtomicReference<IOException> interruptionFailure;

        private InterruptingSerialAwaitRunner(AtomicReference<IOException> interruptionFailure) {
            this.interruptionFailure = interruptionFailure;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The serial-drain test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The serial-drain test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            if (Thread.currentThread().getName().endsWith("-7")) {
                IOException failure = new IOException("scripted serial-drain interruption");
                interruptionFailure.set(failure);
                Thread.currentThread().interrupt();
                throw failure;
            }
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class PerTaskThreadExecutor extends AbstractExecutorService {
        private final String threadPrefix;
        private final AtomicInteger threadNumber = new AtomicInteger();
        private final List<Thread> threads = new ArrayList<>();
        private boolean shutdown;

        private PerTaskThreadExecutor(String threadPrefix) {
            this.threadPrefix = threadPrefix;
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("The test executor is shut down");
            }
            Thread thread =
                    new Thread(command, threadPrefix + "-" + threadNumber.incrementAndGet());
            threads.add(thread);
            thread.start();
        }

        @Override
        public synchronized void shutdown() {
            shutdown = true;
        }

        @Override
        public synchronized List<Runnable> shutdownNow() {
            shutdown = true;
            for (Thread thread : threads) {
                thread.interrupt();
            }
            return List.of();
        }

        @Override
        public synchronized boolean isShutdown() {
            return shutdown;
        }

        @Override
        public synchronized boolean isTerminated() {
            return shutdown && threads.stream().noneMatch(Thread::isAlive);
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            for (Thread thread : threadSnapshot()) {
                while (thread.isAlive()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
            }
            return true;
        }

        private synchronized List<Thread> threadSnapshot() {
            return List.copyOf(threads);
        }
    }

    private static final class TrackingTerminationExecutor extends AbstractExecutorService {
        private final ExecutorService delegate = Executors.newFixedThreadPool(2);
        private final CountDownLatch drainStarted = new CountDownLatch(1);
        private final CountDownLatch interruptObserved = new CountDownLatch(1);

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            drainStarted.countDown();
            try {
                return delegate.awaitTermination(timeout, unit);
            } catch (InterruptedException failure) {
                interruptObserved.countDown();
                throw failure;
            }
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
    }
}
