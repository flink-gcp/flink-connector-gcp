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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Runs the staged files of one run — a whole batch job, or one checkpoint of a streaming job — into
 * their destination tables: submits every load, copy and terminal query through a {@link
 * LoadJobRunner}, then deletes the temporary tables and the staged objects.
 *
 * <p><b>The whole job graph is planned before any of it happens.</b> The {@code CommitPlanner} this
 * class builds in its constructor turns the committables into a {@link CommitPlan} — every job, its
 * deterministic id, its destination and its dispositions — and validates the plan's size, all
 * without touching BigQuery ({@code docs/adr/0018}). Why the plan has the shape it has, and what
 * makes it reproducible across retries, lives on that class rather than here.
 *
 * <p><b>Every load consults the live table first</b>, through the thread-confined {@code
 * FileLoadsSchemaReconciler} each destination task creates: one reconciliation per destination per
 * commit, shared by direct loads and the temp-table path alike, so whether a run fits one partition
 * cannot decide whether its records load. The policy and its rationale live on that class, not
 * here.
 *
 * <p><b>Concurrency.</b> The committer-lifetime {@link DestinationCommitExecutor} runs independent
 * destination actions concurrently, while every job for one destination retains its load,
 * copy-level, final-action order. Each deterministic wave is submitted across all destinations
 * before its jobs are awaited, preserving BigQuery's server-side parallelism. A wave holds at most
 * 50,000 pending jobs, or 1,000 pending interactive terminal queries. A copy level remains a
 * barrier before a job that reads its tables is submitted.
 *
 * <p><b>Retries.</b> Because the plan is a function of the committables alone, a retried run
 * submits the same job ids and the {@link LoadJobRunner} re-attaches instead of double-loading. On
 * destination execution failure, temporary tables and staged files are deliberately left in place
 * for the retry. After every destination succeeds, temporary-table cleanup begins and can be
 * partial if interrupted; staged files are deleted only after that cleanup completes normally.
 * Orphans rely on the temporary dataset's expiration and the staging bucket's lifecycle rule.
 */
@Internal
public final class LoadJobOrchestrator {

    // BigQuery admits at most 1,000 queued interactive queries per project and region.
    private static final int MAX_QUERY_SUBMISSIONS_PER_WAVE = 1_000;

    private static final Logger LOG = LoggerFactory.getLogger(LoadJobOrchestrator.class);

    private final CommitPlanner planner;
    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final DestinationCommitExecutor executor;
    private final StagingStorage storage;
    @Nullable private final Long checkpointId;
    private final Counter loadJobsSubmitted;
    private final Limits limits;
    private final Object userCallbackLock = new Object();

    /**
     * Creates an orchestrator.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param runner the job runner used by this serial compatibility constructor
     * @param tableAdmin the table admin used by this serial compatibility constructor
     * @param storage the staging storage (post-load cleanup)
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     * @param checkpointId the checkpoint whose files this run loads, or {@code null} for a batch
     *     run; a non-null id scopes streaming job ids and temporary-table names
     * @param loadJobsSubmitted the committer's load-job counter. Passed as the counter rather than
     *     as a metric group because this type is constructed once per commit, while the metric it
     *     feeds is registered once per committer
     */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted) {
        this(
                config,
                options,
                storage,
                flinkJobId,
                checkpointId,
                loadJobsSubmitted,
                Limits.BIGQUERY,
                new DestinationCommitExecutor(
                        1,
                        () -> new DestinationCommitExecutor.Worker(runner, tableAdmin),
                        FileLoadsCommitterMetrics.unregistered(loadJobsSubmitted)));
    }

    @VisibleForTesting
    LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted,
            Limits limits) {
        this(
                config,
                options,
                storage,
                flinkJobId,
                checkpointId,
                loadJobsSubmitted,
                limits,
                new DestinationCommitExecutor(
                        1,
                        () -> new DestinationCommitExecutor.Worker(runner, tableAdmin),
                        FileLoadsCommitterMetrics.unregistered(loadJobsSubmitted)));
    }

    /** Creates an orchestrator over the committer-lifetime destination executor. */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted,
            DestinationCommitExecutor executor) {
        this(
                config,
                options,
                storage,
                flinkJobId,
                checkpointId,
                loadJobsSubmitted,
                Limits.BIGQUERY,
                executor);
    }

    @VisibleForTesting
    LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted,
            Limits limits,
            DestinationCommitExecutor executor) {
        this.planner = new CommitPlanner(config, options, flinkJobId, checkpointId, limits);
        this.config = config;
        this.options = options;
        this.executor = executor;
        this.storage = storage;
        this.loadJobsSubmitted = loadJobsSubmitted;
        this.checkpointId = checkpointId;
        this.limits = limits;
    }

    /**
     * Loads all staged files of the run into their destination tables.
     *
     * @param committables the staged files
     * @throws IOException if any load, copy, or terminal query fails, or if temporary-table cleanup
     *     is interrupted; staged files are left in place
     */
    public void run(List<FileLoadsCommittable> committables) throws IOException {
        if (committables.isEmpty()) {
            LOG.info("No staged files; nothing to load");
            return;
        }
        executor.commitStarted();
        try {
            CommitPlan plan = planner.plan(committables);
            runPhase(
                    plan.destinations,
                    (destination, worker, stop) ->
                            destination.reconciledSchema =
                                    new FileLoadsSchemaReconciler(
                                                    config,
                                                    options,
                                                    worker.tableAdmin,
                                                    userCallbackLock)
                                            .finalTableSchema(destination.destination));
            runJobStage(
                    plan.destinations,
                    destination -> destination.loads,
                    limits.maxSubmissionsPerWave,
                    (destination, worker, load) ->
                            submitLoad(
                                    worker.runner,
                                    load,
                                    Preconditions.checkNotNull(destination.reconciledSchema)),
                    load -> load.jobId);

            int intermediateLevels = 0;
            for (DestinationCommitPlan destination : plan.destinations) {
                if (destination.copy != null) {
                    intermediateLevels =
                            Math.max(
                                    intermediateLevels, destination.copy.intermediateLevels.size());
                }
            }
            for (int level = 0; level < intermediateLevels; level++) {
                int currentLevel = level;
                runJobStage(
                        plan.destinations,
                        destination ->
                                destination.copy != null
                                                && currentLevel
                                                        < destination.copy.intermediateLevels.size()
                                        ? destination.copy.intermediateLevels.get(currentLevel)
                                        : List.of(),
                        limits.maxSubmissionsPerWave,
                        (destination, worker, copy) -> submitCopy(worker.runner, copy),
                        copy -> copy.jobId);
            }
            runJobStage(
                    plan.destinations,
                    destination ->
                            destination.copy == null
                                    ? List.of()
                                    : List.of(destination.copy.finalCopy),
                    limits.maxSubmissionsPerWave,
                    (destination, worker, copy) -> submitCopy(worker.runner, copy),
                    copy -> copy.jobId);
            runJobStage(
                    plan.destinations,
                    destination ->
                            destination.copy == null || destination.copy.terminalQuery == null
                                    ? List.of()
                                    : List.of(destination.copy.terminalQuery),
                    Math.min(limits.maxSubmissionsPerWave, MAX_QUERY_SUBMISSIONS_PER_WAVE),
                    (destination, worker, query) -> submitQuery(worker.runner, query),
                    query -> query.jobId);
            executor.cleanup(plan.destinations);

            List<String> uris = new ArrayList<>(committables.size());
            long rows = 0;
            for (FileLoadsCommittable committable : committables) {
                uris.add(committable.getUri());
                rows += committable.getRowCount();
            }
            storage.deleteObjects(uris);
            LOG.info(
                    "Loaded {} rows from {} staged files into {} tables{}",
                    rows,
                    committables.size(),
                    plan.destinations.size(),
                    checkpointId != null ? " for checkpoint " + checkpointId : "");
        } finally {
            executor.commitFinished();
        }
    }

    private <T> void runJobStage(
            List<DestinationCommitPlan> destinations,
            Function<DestinationCommitPlan, List<T>> jobsForDestination,
            int maximumJobsPerWave,
            JobSubmitter<T> submitter,
            Function<T, String> jobId)
            throws IOException {
        List<DestinationJobBatch<T>> wave = new ArrayList<>();
        int waveSize = 0;
        for (DestinationCommitPlan destination : destinations) {
            List<T> jobs = jobsForDestination.apply(destination);
            int next = 0;
            while (next < jobs.size()) {
                int count = Math.min(maximumJobsPerWave - waveSize, jobs.size() - next);
                wave.add(new DestinationJobBatch<>(destination, jobs.subList(next, next + count)));
                next += count;
                waveSize += count;
                if (waveSize == maximumJobsPerWave) {
                    runJobWave(wave, submitter, jobId);
                    wave = new ArrayList<>();
                    waveSize = 0;
                }
            }
        }
        if (!wave.isEmpty()) {
            runJobWave(wave, submitter, jobId);
        }
    }

    private <T> void runJobWave(
            List<DestinationJobBatch<T>> wave, JobSubmitter<T> submitter, Function<T, String> jobId)
            throws IOException {
        Throwable executorFailure = null;
        try {
            runPhase(
                    wave,
                    recordWorkerCreationFailures(
                            (batch, worker, stop) -> {
                                for (T job : batch.jobs) {
                                    if (stop.get()) {
                                        return;
                                    }
                                    try {
                                        submitter.submit(batch.destination, worker, job);
                                        batch.submitted++;
                                    } catch (Throwable failure) {
                                        stop.set(true);
                                        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                                            throwFailure(failure);
                                        }
                                        if (Thread.currentThread().isInterrupted()) {
                                            batch.recordExecutorOnlyFailure(failure);
                                            throwFailure(failure);
                                        }
                                        batch.addFailure(failure);
                                        return;
                                    }
                                }
                            }));
        } catch (Throwable failure) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                throwFailure(mergeFailuresOrRetainFatal(failure, null, wave));
            }
            executorFailure = failure;
            if (Thread.currentThread().isInterrupted()) {
                throwFailure(mergeBatchFailures(failure, wave));
            }
        }

        DestinationCommitExecutor.DestinationWork<DestinationJobBatch<T>> awaitWork =
                recordWorkerCreationFailures(
                        (batch, worker, stop) -> awaitBatch(batch, worker, jobId));
        try {
            runAllPhase(wave, awaitWork);
        } catch (Throwable failure) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                throwFailure(mergeFailuresOrRetainFatal(failure, executorFailure, wave));
            }
            executorFailure =
                    Thread.currentThread().isInterrupted()
                            ? addFailure(failure, executorFailure)
                            : addFailure(executorFailure, failure);
        }

        if (!Thread.currentThread().isInterrupted()) {
            List<DestinationJobBatch<T>> unfinished = new ArrayList<>();
            for (DestinationJobBatch<T> batch : wave) {
                if (batch.awaited < batch.submitted) {
                    unfinished.add(batch);
                }
            }
            if (!unfinished.isEmpty()) {
                try {
                    runAllSerialPhase(unfinished, awaitWork);
                } catch (Throwable failure) {
                    if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                        throwFailure(mergeFailuresOrRetainFatal(failure, executorFailure, wave));
                    }
                    executorFailure =
                            Thread.currentThread().isInterrupted()
                                    ? addFailure(failure, executorFailure)
                                    : addFailure(executorFailure, failure);
                }
            }
        }

        if (executorFailure == null) {
            Throwable batchFailure = aggregateBatchFailures(wave);
            if (batchFailure != null) {
                throwFailure(batchFailure);
            }
            return;
        }
        if (containsOnlyBatchFailures(executorFailure, wave)) {
            throwFailure(aggregateBatchFailures(wave));
        }
        throwFailure(mergeBatchFailures(executorFailure, wave));
    }

    private static <T> void awaitBatch(
            DestinationJobBatch<T> batch,
            DestinationCommitExecutor.Worker worker,
            Function<T, String> jobId)
            throws IOException {
        Throwable firstAwaitFailure = null;
        while (batch.awaited < batch.submitted) {
            try {
                worker.runner.awaitJob(jobId.apply(batch.jobs.get(batch.awaited)));
                batch.awaited++;
            } catch (Throwable failure) {
                if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                    throwFailure(failure);
                }
                if (Thread.currentThread().isInterrupted()) {
                    batch.recordExecutorOnlyFailure(failure);
                    throwFailure(failure);
                }
                batch.addFailure(failure);
                batch.awaited++;
                if (firstAwaitFailure == null) {
                    firstAwaitFailure = failure;
                }
            }
        }
        if (firstAwaitFailure != null) {
            throwFailure(firstAwaitFailure);
        }
    }

    private <T> void runPhase(List<T> tasks, DestinationCommitExecutor.DestinationWork<T> work)
            throws IOException {
        if (tasks.isEmpty()) {
            return;
        }
        executor.destinationsPlanned(tasks.size());
        executor.runWithinCommit(tasks, work);
    }

    private <T> void runAllPhase(List<T> tasks, DestinationCommitExecutor.DestinationWork<T> work)
            throws IOException {
        if (tasks.isEmpty()) {
            return;
        }
        executor.destinationsPlanned(tasks.size());
        executor.runAllWithinCommit(tasks, work);
    }

    private <T> void runAllSerialPhase(
            List<T> tasks, DestinationCommitExecutor.DestinationWork<T> work) throws IOException {
        executor.destinationsPlanned(tasks.size());
        executor.runAllSerialWithinCommit(tasks, work);
    }

    private void submitLoad(LoadJobRunner runner, PlannedLoad load, Schema schema)
            throws IOException {
        // Reconciliation completed before this phase; count only when submission is attempted.
        loadJobsSubmitted.inc();
        runner.submitLoad(
                load.jobId,
                new LoadJobSpec(
                        load.jobDestination,
                        load.uris,
                        schema,
                        load.createDisposition,
                        load.writeDisposition,
                        load.schemaUpdateOptions,
                        load.format));
    }

    private static void submitCopy(LoadJobRunner runner, PlannedCopy copy) throws IOException {
        runner.submitCopy(copy.jobId, copy.spec);
    }

    private static void submitQuery(LoadJobRunner runner, PlannedQuery query) throws IOException {
        runner.submitQuery(query.jobId, query.spec);
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("BigQuery job execution failed", failure);
    }

    private static Throwable addFailure(Throwable primary, Throwable failure) {
        FailureAccumulator failures = new FailureAccumulator(primary);
        failures.add(failure);
        return failures.failure();
    }

    private static <T> Throwable aggregateBatchFailures(List<DestinationJobBatch<T>> wave) {
        FailureAccumulator failures = new FailureAccumulator(null);
        for (DestinationJobBatch<T> batch : wave) {
            for (Throwable batchFailure : batch.failureSnapshot()) {
                failures.add(batchFailure);
            }
        }
        return failures.failure();
    }

    private static <T> Throwable mergeBatchFailures(
            Throwable failure, List<DestinationJobBatch<T>> wave) {
        FailureAccumulator failures = new FailureAccumulator(failure);
        for (DestinationJobBatch<T> batch : wave) {
            for (Throwable batchFailure : batch.failureSnapshot()) {
                failures.add(batchFailure);
            }
        }
        return failures.failure();
    }

    private static <T> Throwable mergeFailuresOrRetainFatal(
            Throwable fatalFailure,
            @Nullable Throwable executorFailure,
            List<DestinationJobBatch<T>> wave) {
        try {
            FailureAccumulator failures = new FailureAccumulator(fatalFailure);
            for (DestinationJobBatch<T> batch : wave) {
                for (Throwable batchFailure : batch.failureSnapshotForFatalAggregation()) {
                    failures.add(batchFailure);
                }
            }
            failures.add(executorFailure);
            return failures.failure();
        } catch (OutOfMemoryError aggregationFailure) {
            return fatalFailure;
        }
    }

    private static <T> boolean containsOnlyBatchFailures(
            Throwable failures, List<DestinationJobBatch<T>> wave) {
        Set<Throwable> batchFailures = Collections.newSetFromMap(new IdentityHashMap<>());
        for (DestinationJobBatch<T> batch : wave) {
            for (Throwable batchFailure : batch.failureSnapshot()) {
                collectFailures(batchFailure, batchFailures);
            }
        }
        List<Throwable> pending = new ArrayList<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(failures);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            if (!visited.add(candidate)) {
                continue;
            }
            if (!batchFailures.contains(candidate)) {
                return false;
            }
            Collections.addAll(pending, candidate.getSuppressed());
        }
        return true;
    }

    private static boolean containsFailure(Throwable failures, Throwable expected) {
        if (failures == null) {
            return false;
        }
        List<Throwable> pending = new ArrayList<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(failures);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            if (candidate == expected) {
                return true;
            }
            if (visited.add(candidate)) {
                Collections.addAll(pending, candidate.getSuppressed());
            }
        }
        return false;
    }

    private static void collectFailures(@Nullable Throwable failures, Set<Throwable> destination) {
        if (failures == null) {
            return;
        }
        List<Throwable> pending = new ArrayList<>();
        pending.add(failures);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            if (destination.add(candidate)) {
                Collections.addAll(pending, candidate.getSuppressed());
            }
        }
    }

    private static final class FailureAccumulator {
        private final Set<Throwable> recorded = Collections.newSetFromMap(new IdentityHashMap<>());
        @Nullable private Throwable primary;

        private FailureAccumulator(@Nullable Throwable initial) {
            primary = initial;
            collectFailures(initial, recorded);
        }

        private void add(@Nullable Throwable failure) {
            if (failure == null || recorded.contains(failure)) {
                return;
            }
            if (primary == null) {
                primary = failure;
                collectFailures(failure, recorded);
                return;
            }
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)
                    && !ExceptionUtils.isJvmFatalOrOutOfMemoryError(primary)) {
                if (!containsFailure(failure, primary)) {
                    failure.addSuppressed(primary);
                }
                primary = failure;
            } else if (containsFailure(failure, primary)) {
                primary = failure;
            } else {
                primary.addSuppressed(failure);
            }
            collectFailures(failure, recorded);
        }

        @Nullable
        private Throwable failure() {
            return primary;
        }
    }

    private static <T>
            DestinationCommitExecutor.DestinationWork<DestinationJobBatch<T>>
                    recordWorkerCreationFailures(
                            DestinationCommitExecutor.DestinationWork<DestinationJobBatch<T>>
                                    delegate) {
        return new DestinationCommitExecutor.DestinationWork<>() {
            @Override
            public void run(
                    DestinationJobBatch<T> batch,
                    DestinationCommitExecutor.Worker worker,
                    AtomicBoolean stop)
                    throws IOException {
                delegate.run(batch, worker, stop);
            }

            @Override
            public void workerCreationFailed(DestinationJobBatch<T> batch, Throwable failure) {
                if (!ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                    batch.addFailure(failure);
                }
            }

            @Override
            public void taskFailureObserved(DestinationJobBatch<T> batch, Throwable failure) {
                if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                    batch.recordFatalFailure(failure);
                }
            }

            @Override
            public boolean deferOrdinaryFailureAggregationUnderFatal() {
                return true;
            }
        };
    }

    @FunctionalInterface
    interface JobSubmitter<T> {

        void submit(
                DestinationCommitPlan destination, DestinationCommitExecutor.Worker worker, T job)
                throws IOException;
    }

    private static final class DestinationJobBatch<T> {
        private final DestinationCommitPlan destination;
        private final List<T> jobs;
        private final List<Throwable> failures = new ArrayList<>();
        @Nullable private volatile Throwable executorOnlyFailure;
        @Nullable private volatile Throwable fatalFailure;
        private int submitted;
        private int awaited;

        private DestinationJobBatch(DestinationCommitPlan destination, List<T> jobs) {
            this.destination = destination;
            this.jobs = jobs;
        }

        private synchronized void addFailure(Throwable failure) {
            failures.add(failure);
        }

        private synchronized List<Throwable> failureSnapshot() {
            return failures.isEmpty() ? List.of() : new ArrayList<>(failures);
        }

        private synchronized List<Throwable> failureSnapshotForFatalAggregation() {
            if (failures.isEmpty() && executorOnlyFailure == null && fatalFailure == null) {
                return List.of();
            }
            List<Throwable> snapshot = new ArrayList<>(failures);
            if (executorOnlyFailure != null) {
                snapshot.add(executorOnlyFailure);
            }
            if (fatalFailure != null) {
                snapshot.add(fatalFailure);
            }
            return snapshot;
        }

        private void recordExecutorOnlyFailure(Throwable failure) {
            executorOnlyFailure = failure;
        }

        private void recordFatalFailure(Throwable failure) {
            fatalFailure = failure;
        }
    }
}
