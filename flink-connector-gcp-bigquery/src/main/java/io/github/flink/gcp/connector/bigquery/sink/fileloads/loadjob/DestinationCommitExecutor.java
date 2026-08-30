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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/** Runs independent destination actions with a fixed, committer-lifetime concurrency bound. */
@Internal
public final class DestinationCommitExecutor implements AutoCloseable {

    private static final long CLOSE_TIMEOUT_SECONDS = 30;
    private static final Runnable NOOP = () -> {};
    private static final LongSupplier SYSTEM_NANO_TIME = System::nanoTime;
    private static final Logger LOG = LoggerFactory.getLogger(DestinationCommitExecutor.class);

    /** One worker's thread-confined BigQuery collaborators. */
    public static final class Worker {
        final LoadJobRunner runner;
        final TableAdmin tableAdmin;

        /** Creates a worker over thread-confined collaborators. */
        public Worker(LoadJobRunner runner, TableAdmin tableAdmin) {
            this.runner = runner;
            this.tableAdmin = tableAdmin;
        }

        /** Returns this worker's thread-confined job runner. */
        public LoadJobRunner runner() {
            return runner;
        }

        /** Returns this worker's thread-confined table administrator. */
        public TableAdmin tableAdmin() {
            return tableAdmin;
        }
    }

    /** Builds one worker context, lazily on the thread that owns it. */
    @FunctionalInterface
    public interface WorkerFactory {
        Worker create() throws IOException;
    }

    @FunctionalInterface
    interface DestinationWork<T> {
        void run(T task, Worker worker, AtomicBoolean stop) throws IOException;

        /** Records a worker-creation failure against the task whose action could not start. */
        default void workerCreationFailed(T task, Throwable failure) {}

        /** Records that the coordinator actually observed a task failure. */
        default void taskFailureObserved(T task, Throwable failure) {}

        /** Lets a caller that records task failures preserve its own cross-phase ordering. */
        default boolean deferOrdinaryFailureAggregationUnderFatal() {
            return false;
        }
    }

    private final int maximumConcurrency;
    private final WorkerFactory workerFactory;
    private final FileLoadsCommitterMetrics metrics;
    private final Runnable fatalRecordedHook;
    private final ThreadLocal<Worker> workers = new ThreadLocal<>();
    private final AtomicBoolean workerDrainAbandoned = new AtomicBoolean();
    private final LateFatalFailure[] fatalRecords;

    private boolean fatalDrainInterrupted;
    private Worker inlineWorker;
    private ExecutorService executor;
    private long metricAttempt;

    /** Creates an executor with the configured destination bound. */
    public DestinationCommitExecutor(
            int maximumConcurrency,
            WorkerFactory workerFactory,
            FileLoadsCommitterMetrics metrics) {
        this(maximumConcurrency, workerFactory, metrics, null, NOOP);
    }

    @VisibleForTesting
    DestinationCommitExecutor(
            int maximumConcurrency,
            WorkerFactory workerFactory,
            FileLoadsCommitterMetrics metrics,
            ExecutorService executor) {
        this(maximumConcurrency, workerFactory, metrics, executor, NOOP);
    }

    @VisibleForTesting
    DestinationCommitExecutor(
            int maximumConcurrency,
            WorkerFactory workerFactory,
            FileLoadsCommitterMetrics metrics,
            ExecutorService executor,
            Runnable fatalRecordedHook) {
        Preconditions.checkArgument(
                maximumConcurrency > 0,
                "maximumConcurrency must be positive: %s",
                maximumConcurrency);
        this.maximumConcurrency = maximumConcurrency;
        this.workerFactory = workerFactory;
        this.metrics = metrics;
        this.executor = executor;
        this.fatalRecordedHook = fatalRecordedHook;
        this.fatalRecords = new LateFatalFailure[maximumConcurrency];
        for (int index = 0; index < maximumConcurrency; index++) {
            fatalRecords[index] = new LateFatalFailure();
        }
    }

    @VisibleForTesting
    void run(List<DestinationCommitPlan> plans, DestinationWork<DestinationCommitPlan> work)
            throws IOException {
        if (plans.isEmpty()) {
            return;
        }
        commitStarted();
        try {
            destinationsPlanned(plans.size());
            runWithinCommit(plans, work);
        } finally {
            commitFinished();
        }
    }

    <T> void runWithinCommit(List<T> tasks, DestinationWork<T> work) throws IOException {
        runWithinCommit(tasks, work, true);
    }

    <T> void runAllWithinCommit(List<T> tasks, DestinationWork<T> work) throws IOException {
        runWithinCommit(tasks, work, false);
    }

    <T> void runAllSerialWithinCommit(List<T> tasks, DestinationWork<T> work) throws IOException {
        if (executor == null) {
            runInline(tasks, work, false);
        } else {
            runConcurrent(tasks, work, false, 1);
        }
    }

    private <T> void runWithinCommit(List<T> tasks, DestinationWork<T> work, boolean stopOnFailure)
            throws IOException {
        if (maximumConcurrency == 1 || (tasks.size() == 1 && executor == null)) {
            runInline(tasks, work, stopOnFailure);
        } else {
            runConcurrent(tasks, work, stopOnFailure);
        }
    }

    void commitStarted() {
        metricAttempt = metrics.commitStarted();
    }

    void destinationsPlanned(int destinations) {
        metrics.destinationsPlanned(metricAttempt, destinations);
    }

    void commitFinished() {
        metrics.commitFinished(metricAttempt);
        metricAttempt = 0;
    }

    void cleanup(List<DestinationCommitPlan> plans) throws IOException {
        List<DestinationCommitPlan> withTables = new ArrayList<>();
        for (DestinationCommitPlan plan : plans) {
            if (plan.copy != null && !plan.copy.cleanupTables.isEmpty()) {
                withTables.add(plan);
            }
        }
        if (withTables.isEmpty()) {
            return;
        }
        Failure fatalDrainInterruption =
                fatalDrainInterruption(
                        "Interrupted while draining FILE_LOADS cleanup after a JVM-fatal failure");
        destinationsPlanned(withTables.size());
        long attempt = metricAttempt;
        if (maximumConcurrency == 1 || executor == null) {
            Worker worker;
            try {
                worker = inlineWorker();
            } catch (IOException failure) {
                // The production factory can fail only while creating its client. Cleanup remains
                // best-effort, under the same contract as LoadJobRunner.deleteTable.
                LOG.warn("Could not create the FILE_LOADS cleanup worker", failure);
                return;
            }
            for (DestinationCommitPlan plan : withTables) {
                metrics.cleanupStarted(attempt);
                try {
                    deleteTables(plan, worker);
                } finally {
                    metrics.cleanupFinished(attempt);
                }
            }
            return;
        }
        CompletionService<Result> completions = new ExecutorCompletionService<>(executor());
        int next = 0;
        int running = 0;
        int initial = Math.min(maximumConcurrency, withTables.size());
        while (next < initial) {
            submitCleanup(completions, withTables.get(next), next, attempt);
            next++;
            running++;
        }
        boolean interrupted = false;
        List<Failure> fatalFailures = new ArrayList<>(maximumConcurrency + 1);
        while (running > 0) {
            while (true) {
                try {
                    recordCleanupResult(completions.take().get(), fatalFailures);
                    running--;
                    if (!fatalFailures.isEmpty()) {
                        boolean interruptedDuringDrain = stopExecutorAfterFatalFailure();
                        drainCompletedCleanup(completions, fatalFailures, true);
                        if (interruptedDuringDrain) {
                            fatalFailures.add(fatalDrainInterruption);
                        }
                        throwFailuresAfterReportingUnobserved(fatalFailures);
                        throw new AssertionError(
                                "throwFailures returned for a non-empty failure list");
                    }
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                    stopExecutorAfterInterrupt();
                    running = 0;
                    break;
                } catch (ExecutionException failure) {
                    recordCleanupFailure(
                            new Failure(Integer.MAX_VALUE, failure.getCause()), fatalFailures);
                    running--;
                    if (!fatalFailures.isEmpty()) {
                        boolean interruptedDuringDrain = stopExecutorAfterFatalFailure();
                        drainCompletedCleanup(completions, fatalFailures, true);
                        if (interruptedDuringDrain) {
                            fatalFailures.add(fatalDrainInterruption);
                        }
                        throwFailuresAfterReportingUnobserved(fatalFailures);
                        throw new AssertionError(
                                "throwFailures returned for a non-empty failure list");
                    }
                    break;
                }
            }
            if (!interrupted && fatalFailures.isEmpty() && next < withTables.size()) {
                submitCleanup(completions, withTables.get(next), next, attempt);
                next++;
                running++;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            try {
                drainCompletedCleanup(completions, fatalFailures, false);
                IOException interruption =
                        new IOException(
                                "Interrupted while cleaning up FILE_LOADS temporary tables");
                if (fatalFailures.isEmpty()) {
                    throw interruption;
                }
                fatalFailures.add(new Failure(Integer.MIN_VALUE, interruption));
                throwFailures(fatalFailures);
                throw new AssertionError("throwFailures returned for a non-empty failure list");
            } finally {
                // Completed Futures claim their recorded fatal failures before any abandoned
                // worker is reported. The finally also closes allocation and diagnostic-failure
                // gaps between the bounded drain and the final throw.
                reportUnobservedFatalFailures();
            }
        }
        if (!fatalFailures.isEmpty()) {
            throwFailures(fatalFailures);
        }
    }

    private void submitCleanup(
            CompletionService<Result> completions,
            DestinationCommitPlan plan,
            int index,
            long attempt) {
        LateFatalFailure fatalRecord = registerFatalRecord();
        Result result = new Result(index);
        try {
            completions.submit(
                    () -> {
                        try {
                            metrics.cleanupStarted(attempt);
                            try {
                                deleteTables(plan, worker());
                            } catch (IOException failure) {
                                // Best-effort cleanup: a dataset expiration and staging-bucket
                                // lifecycle are the durable fallback.
                                LOG.warn(
                                        "FILE_LOADS temporary-table cleanup did not finish for {}",
                                        plan.destination,
                                        failure);
                            } finally {
                                metrics.cleanupFinished(attempt);
                            }
                            return result;
                        } catch (Error failure) {
                            recordPotentiallyUnobservedFatalFailure(fatalRecord, failure);
                            fatalRecordedHook.run();
                            completeResultOrReportAbandonedFatal(fatalRecord, result, failure);
                            return result;
                        } finally {
                            if (fatalRecord.failure == null) {
                                releaseFatalRecord(fatalRecord);
                            }
                        }
                    });
        } catch (Throwable failure) {
            releaseFatalRecord(fatalRecord);
            throw failure;
        }
    }

    private void recordCleanupResult(Result result, List<Failure> fatalFailures) {
        if (result.failure == null) {
            return;
        }
        recordCleanupFailure(result.failureRecord, fatalFailures);
    }

    private void recordCleanupFailure(Failure failure, List<Failure> fatalFailures) {
        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure.failure)) {
            fatalFailures.add(failure);
        } else {
            LOG.warn("FILE_LOADS temporary-table cleanup failed", failure.failure);
        }
    }

    private void drainCompletedCleanup(
            CompletionService<Result> completions,
            List<Failure> fatalFailures,
            boolean retainPrimaryFatal) {
        if (retainPrimaryFatal) {
            try {
                drainCompletedCleanup(completions, fatalFailures, false);
            } catch (Throwable ignored) {
                // Diagnostic allocation must not replace the fatal failure already observed.
            }
            return;
        }
        Future<Result> completion;
        while ((completion = completions.poll()) != null) {
            try {
                recordCleanupResult(completion.get(), fatalFailures);
            } catch (InterruptedException ignored) {
                // The coordinator already stopped and bounded the worker drain.
            } catch (ExecutionException failure) {
                recordCleanupFailure(
                        new Failure(Integer.MAX_VALUE, failure.getCause()), fatalFailures);
            }
        }
    }

    private <T> void runInline(List<T> tasks, DestinationWork<T> work, boolean stopOnFailure)
            throws IOException {
        long attempt = metricAttempt;
        List<Failure> failures = new ArrayList<>(tasks.size());
        Worker worker;
        try {
            worker = inlineWorker();
        } catch (IOException | RuntimeException | Error failure) {
            work.workerCreationFailed(tasks.get(0), failure);
            work.taskFailureObserved(tasks.get(0), failure);
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                throwFailure(failure);
            }
            failures.add(new Failure(0, failure));
            throwFailures(failures, work.deferOrdinaryFailureAggregationUnderFatal());
            throw new AssertionError("throwFailures returned for a non-empty failure list");
        }
        AtomicBoolean stop = new AtomicBoolean();
        for (int index = 0; index < tasks.size(); index++) {
            metrics.destinationStarted(attempt);
            try {
                work.run(tasks.get(index), worker, stop);
            } catch (Throwable failure) {
                work.taskFailureObserved(tasks.get(index), failure);
                if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                    throwFailure(failure);
                }
                failures.add(new Failure(index, failure));
                if (stopOnFailure || Thread.currentThread().isInterrupted()) {
                    stop.set(true);
                    break;
                }
            } finally {
                metrics.destinationFinished(attempt);
            }
        }
        if (!failures.isEmpty()) {
            throwFailures(failures, work.deferOrdinaryFailureAggregationUnderFatal());
        }
    }

    private <T> void runConcurrent(List<T> tasks, DestinationWork<T> work, boolean stopOnFailure)
            throws IOException {
        runConcurrent(tasks, work, stopOnFailure, maximumConcurrency);
    }

    private <T> void runConcurrent(
            List<T> tasks, DestinationWork<T> work, boolean stopOnFailure, int concurrentTasks)
            throws IOException {
        Failure fatalDrainInterruption =
                fatalDrainInterruption(
                        "Interrupted while draining FILE_LOADS destinations after a JVM-fatal failure");
        // Once the pool is needed, singleton phases reuse its thread-confined workers. Release the
        // coordinator-owned worker so the committer retains at most the configured number.
        inlineWorker = null;
        AtomicBoolean stop = new AtomicBoolean();
        CompletionService<Result> completions = new ExecutorCompletionService<>(executor());
        int next = 0;
        int running = 0;
        int initial = Math.min(concurrentTasks, tasks.size());
        while (next < initial) {
            submit(completions, tasks, next++, work, stop, stopOnFailure);
            running++;
        }

        List<Failure> failures = new ArrayList<>(tasks.size() + 1);
        while (running > 0) {
            Result result;
            try {
                result = completions.take().get();
            } catch (InterruptedException error) {
                stop.set(true);
                stopExecutorAfterInterrupt();
                Thread.currentThread().interrupt();
                try {
                    failures.add(
                            new Failure(
                                    Integer.MIN_VALUE,
                                    new IOException(
                                            "Interrupted while waiting for FILE_LOADS destinations",
                                            error)));
                    drainCompletedDestinations(completions, tasks, work, failures, false);
                    throwFailures(failures, work.deferOrdinaryFailureAggregationUnderFatal());
                    throw new AssertionError("throwFailures returned for a non-empty failure list");
                } finally {
                    // See the cleanup counterpart: consume completed Futures first, but report
                    // every fatal record if allocation or diagnostics abort that consumption.
                    reportUnobservedFatalFailures();
                }
            } catch (ExecutionException error) {
                boolean fatal = ExceptionUtils.isJvmFatalOrOutOfMemoryError(error.getCause());
                if (stopOnFailure || fatal) {
                    stop.set(true);
                }
                failures.add(new Failure(Integer.MAX_VALUE, error.getCause()));
                running--;
                if (fatal) {
                    boolean interruptedDuringDrain = stopExecutorAfterFatalFailure();
                    drainCompletedDestinations(completions, tasks, work, failures, true);
                    if (interruptedDuringDrain) {
                        failures.add(fatalDrainInterruption);
                    }
                    throwFailuresAfterReportingUnobserved(
                            failures, work.deferOrdinaryFailureAggregationUnderFatal());
                    throw new AssertionError("throwFailures returned for a non-empty failure list");
                }
                if (!stop.get() && next < tasks.size()) {
                    submit(completions, tasks, next++, work, stop, stopOnFailure);
                    running++;
                }
                continue;
            }
            running--;
            if (concurrentTasks == 1 && result.workerInterrupted) {
                stop.set(true);
                Thread.currentThread().interrupt();
            }
            if (result.failure != null) {
                work.taskFailureObserved(tasks.get(result.failureRecord.index), result.failure);
                boolean fatal = ExceptionUtils.isJvmFatalOrOutOfMemoryError(result.failure);
                if (stopOnFailure || fatal) {
                    stop.set(true);
                }
                failures.add(result.failureRecord);
                if (fatal) {
                    boolean interruptedDuringDrain = stopExecutorAfterFatalFailure();
                    drainCompletedDestinations(completions, tasks, work, failures, true);
                    if (interruptedDuringDrain) {
                        failures.add(fatalDrainInterruption);
                    }
                    throwFailuresAfterReportingUnobserved(
                            failures, work.deferOrdinaryFailureAggregationUnderFatal());
                    throw new AssertionError("throwFailures returned for a non-empty failure list");
                }
            }
            if (!stop.get() && next < tasks.size()) {
                submit(completions, tasks, next++, work, stop, stopOnFailure);
                running++;
            }
        }

        if (!failures.isEmpty()) {
            throwFailures(failures, work.deferOrdinaryFailureAggregationUnderFatal());
        }
    }

    private <T> void drainCompletedDestinations(
            CompletionService<Result> completions,
            List<T> tasks,
            DestinationWork<T> work,
            List<Failure> failures,
            boolean retainPrimaryFatal) {
        if (retainPrimaryFatal) {
            try {
                drainCompletedDestinations(completions, tasks, work, failures, false);
            } catch (Throwable ignored) {
                // Diagnostic allocation must not replace the fatal failure already observed.
            }
            return;
        }
        Future<Result> completion;
        while ((completion = completions.poll()) != null) {
            try {
                Result result = completion.get();
                if (result.failure != null) {
                    work.taskFailureObserved(tasks.get(result.failureRecord.index), result.failure);
                    failures.add(result.failureRecord);
                }
            } catch (InterruptedException ignored) {
                // The first interrupt already stopped and bounded the worker drain.
            } catch (ExecutionException failure) {
                failures.add(new Failure(Integer.MAX_VALUE, failure.getCause()));
            }
        }
    }

    private <T> void submit(
            CompletionService<Result> completions,
            List<T> tasks,
            int index,
            DestinationWork<T> work,
            AtomicBoolean stop,
            boolean stopOnFailure) {
        T task = tasks.get(index);
        long attempt = metricAttempt;
        LateFatalFailure fatalRecord = registerFatalRecord();
        Result result = new Result(index);
        try {
            completions.submit(
                    () -> {
                        try {
                            metrics.destinationStarted(attempt);
                            try {
                                if (!stop.get()) {
                                    Worker worker;
                                    try {
                                        worker = worker();
                                    } catch (IOException | RuntimeException | Error failure) {
                                        work.workerCreationFailed(task, failure);
                                        throw failure;
                                    }
                                    work.run(task, worker, stop);
                                }
                                return result;
                            } finally {
                                metrics.destinationFinished(attempt);
                            }
                        } catch (Throwable failure) {
                            result.workerInterrupted = Thread.currentThread().isInterrupted();
                            if (stopOnFailure
                                    || ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                                stop.set(true);
                            }
                            recordPotentiallyUnobservedFatalFailure(fatalRecord, failure);
                            fatalRecordedHook.run();
                            completeResultOrReportAbandonedFatal(fatalRecord, result, failure);
                            return result;
                        } finally {
                            if (fatalRecord.failure == null) {
                                releaseFatalRecord(fatalRecord);
                            }
                        }
                    });
        } catch (Throwable failure) {
            releaseFatalRecord(fatalRecord);
            throw failure;
        }
    }

    private Worker worker() throws IOException {
        Worker worker = workers.get();
        if (worker == null) {
            worker = workerFactory.create();
            workers.set(worker);
        }
        return worker;
    }

    private Worker inlineWorker() throws IOException {
        if (inlineWorker == null) {
            inlineWorker = workerFactory.create();
        }
        return inlineWorker;
    }

    @VisibleForTesting
    boolean hasInlineWorker() {
        return inlineWorker != null;
    }

    private ExecutorService executor() {
        if (executor == null) {
            executor =
                    Executors.newFixedThreadPool(
                            maximumConcurrency,
                            new ExecutorThreadFactory("bigquery-file-loads-commit"));
        }
        return executor;
    }

    private void stopExecutorAfterInterrupt() throws IOException {
        try {
            executor.shutdownNow();
        } catch (Throwable failure) {
            // ThreadPoolExecutor may fail while allocating the returned queue snapshot. The
            // coordinator is already interrupted, but a JVM-fatal shutdown failure remains
            // primary. In either case, make every recorded or subsequent JVM-fatal worker failure
            // visible outside its Future.
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                abandonWorkerDrain();
                Thread.currentThread().interrupt();
                throwFailure(failure);
            }
            workerDrainAbandoned.set(true);
            return;
        }
        boolean terminated;
        try {
            terminated = awaitTerminationAfterInterrupt(executor);
        } catch (Throwable failure) {
            // Logging or another diagnostic action must not strand a worker fatal failure in an
            // unconsumed Future after the coordinator returns.
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                abandonWorkerDrain();
                Thread.currentThread().interrupt();
                throwFailure(failure);
            }
            workerDrainAbandoned.set(true);
            return;
        }
        if (!terminated) {
            // The coordinator must return after the bounded drain. A worker that ignores its
            // interrupt can still fail later, after no Future consumer remains to surface a
            // JVM-fatal error. Let that worker report such an error through its inherited
            // uncaught-exception path.
            workerDrainAbandoned.set(true);
        }
    }

    private void abandonWorkerDrain() {
        workerDrainAbandoned.set(true);
        // Close the publication race with a worker that recorded its failure immediately before
        // abandonment became visible but has not yet published its Future result.
        reportUnobservedFatalFailures();
    }

    private boolean stopExecutorAfterFatalFailure() {
        fatalDrainInterrupted = false;
        try {
            executor.shutdownNow();
        } catch (Throwable ignored) {
            // ThreadPoolExecutor drains queued tasks into a new list after stopping workers.
            // Allocation failure there must not replace the fatal failure already observed.
            workerDrainAbandoned.set(true);
            return Thread.currentThread().isInterrupted();
        }
        if (!awaitTerminationAfterFatalFailure(SYSTEM_NANO_TIME)) {
            workerDrainAbandoned.set(true);
            try {
                LOG.warn(
                        "FILE_LOADS destination workers did not stop within {} seconds after a JVM-fatal failure",
                        CLOSE_TIMEOUT_SECONDS);
            } catch (Throwable ignored) {
                // Warning allocation must not replace the original fatal failure.
            }
        }
        if (fatalDrainInterrupted) {
            Thread.currentThread().interrupt();
        }
        return fatalDrainInterrupted;
    }

    static boolean awaitTerminationAfterInterrupt(ExecutorService executor) {
        return awaitTerminationAfterInterrupt(executor, SYSTEM_NANO_TIME);
    }

    @VisibleForTesting
    static boolean awaitTerminationAfterInterrupt(ExecutorService executor, LongSupplier nanoTime) {
        long deadline = nanoTime.getAsLong() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
        while (true) {
            long remainingNanos = deadline - nanoTime.getAsLong();
            if (remainingNanos <= 0) {
                LOG.warn(
                        "FILE_LOADS destination workers did not stop within {} seconds after an interrupt",
                        CLOSE_TIMEOUT_SECONDS);
                return false;
            }
            try {
                if (executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    return true;
                }
                LOG.warn(
                        "FILE_LOADS destination workers did not stop within {} seconds after an interrupt",
                        CLOSE_TIMEOUT_SECONDS);
                return false;
            } catch (InterruptedException ignored) {
                // Keep draining until the original deadline. The caller restores the interrupt.
            }
        }
    }

    private boolean awaitTerminationAfterFatalFailure(LongSupplier nanoTime) {
        long deadline = nanoTime.getAsLong() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
        while (true) {
            long remainingNanos = deadline - nanoTime.getAsLong();
            if (remainingNanos <= 0) {
                return false;
            }
            try {
                if (executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    return true;
                }
                return false;
            } catch (InterruptedException ignored) {
                fatalDrainInterrupted = true;
            }
        }
    }

    private synchronized LateFatalFailure registerFatalRecord() {
        for (LateFatalFailure record : fatalRecords) {
            if (!record.inUse) {
                record.inUse = true;
                record.worker = null;
                record.failure = null;
                return record;
            }
        }
        throw new IllegalStateException("No FILE_LOADS fatal-failure record is available");
    }

    private void recordPotentiallyUnobservedFatalFailure(
            LateFatalFailure recorded, Throwable failure) {
        if (!ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
            return;
        }
        Thread worker = Thread.currentThread();
        recorded.worker = worker;
        recorded.failure = failure;
    }

    private void completeResultOrReportAbandonedFatal(
            LateFatalFailure recorded, Result result, Throwable failure) {
        Thread workerToReport = null;
        synchronized (this) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
                if (!recorded.inUse) {
                    return;
                }
                if (workerDrainAbandoned.get()) {
                    recorded.inUse = false;
                    workerToReport = recorded.worker;
                } else {
                    result.failed(failure);
                }
            } else {
                result.failed(failure);
            }
        }
        if (workerToReport != null) {
            reportLateFatalFailure(workerToReport, failure);
        }
    }

    private void forgetRecordedFatalFailure(Throwable failure) {
        if (failure == null) {
            return;
        }
        for (LateFatalFailure recorded : fatalRecords) {
            if (claimFatalRecord(recorded, failure)) {
                return;
            }
        }
    }

    private void reportUnobservedFatalFailures() {
        for (LateFatalFailure recorded : fatalRecords) {
            Throwable failure = recorded.failure;
            Thread worker = recorded.worker;
            if (failure != null && claimFatalRecord(recorded, failure)) {
                reportLateFatalFailure(worker, failure);
            }
        }
    }

    private synchronized boolean claimFatalRecord(
            LateFatalFailure recorded, Throwable expectedFailure) {
        if (!recorded.inUse || recorded.failure != expectedFailure) {
            return false;
        }
        recorded.inUse = false;
        return true;
    }

    private synchronized void releaseFatalRecord(LateFatalFailure recorded) {
        recorded.inUse = false;
    }

    private static void reportLateFatalFailure(Thread worker, Throwable failure) {
        Thread.UncaughtExceptionHandler handler = worker.getUncaughtExceptionHandler();
        if (handler != null) {
            try {
                handler.uncaughtException(worker, failure);
            } catch (Throwable ignored) {
                // The JVM also ignores an exception thrown by an uncaught-exception handler.
            }
        }
        try {
            LOG.error(
                    "A FILE_LOADS destination worker failed fatally after its coordinator abandoned the worker drain",
                    failure);
        } catch (Throwable ignored) {
            // Fatal reporting must not replace the original failure under memory pressure.
        }
    }

    private static void deleteTables(DestinationCommitPlan plan, Worker worker) throws IOException {
        for (TableDestination table : plan.copy.cleanupTables) {
            throwIfCleanupInterrupted();
            worker.runner.deleteTable(table);
            throwIfCleanupInterrupted();
        }
    }

    private static void throwIfCleanupInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Interrupted while cleaning up FILE_LOADS temporary tables");
        }
    }

    private void throwFailures(List<Failure> failures) throws IOException {
        throwFailures(failures, false);
    }

    private void throwFailures(List<Failure> failures, boolean deferOrdinaryUnderFatal)
            throws IOException {
        Throwable primary = prepareFailuresOrRetainFatal(failures, deferOrdinaryUnderFatal);
        forgetRecordedFailures(failures);
        throwFailure(primary);
    }

    private void throwFailuresAfterReportingUnobserved(List<Failure> failures) throws IOException {
        throwFailuresAfterReportingUnobserved(failures, false);
    }

    private void throwFailuresAfterReportingUnobserved(
            List<Failure> failures, boolean deferOrdinaryUnderFatal) throws IOException {
        Throwable primary = prepareFailuresOrRetainFatal(failures, deferOrdinaryUnderFatal);
        forgetRecordedFailures(failures);
        reportUnobservedFatalFailures();
        throwFailure(primary);
    }

    private static Failure fatalDrainInterruption(String message) {
        // Reserve the diagnostic before workers start. Allocation after an OutOfMemoryError could
        // otherwise replace the fatal failure that this diagnostic is meant to accompany.
        return new Failure(Integer.MIN_VALUE, new DrainInterruptedException(message));
    }

    private static Throwable prepareFailures(
            List<Failure> failures, boolean deferOrdinaryUnderFatal) {
        failures.sort(Comparator.comparingInt(failure -> failure.index));
        int primaryIndex = 0;
        for (int i = 0; i < failures.size(); i++) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failures.get(i).failure)) {
                primaryIndex = i;
                break;
            }
        }
        Throwable primary = failures.get(primaryIndex).failure;
        if (deferOrdinaryUnderFatal && ExceptionUtils.isJvmFatalOrOutOfMemoryError(primary)) {
            for (Failure failure : failures) {
                if (failure.index == Integer.MIN_VALUE && failure.failure != primary) {
                    primary.addSuppressed(failure.failure);
                }
            }
            return primary;
        }
        for (int i = 0; i < failures.size(); i++) {
            if (i == primaryIndex) {
                continue;
            }
            Throwable suppressed = failures.get(i).failure;
            if (suppressed != primary) {
                primary.addSuppressed(suppressed);
            }
        }
        return primary;
    }

    private Throwable prepareFailuresOrRetainFatal(
            List<Failure> failures, boolean deferOrdinaryUnderFatal) {
        try {
            return prepareFailures(failures, deferOrdinaryUnderFatal);
        } catch (OutOfMemoryError aggregationFailure) {
            Throwable originalFatal = firstFatalFailure(failures);
            forgetRecordedFailures(failures);
            if (originalFatal != null) {
                return originalFatal;
            }
            throw aggregationFailure;
        }
    }

    private static Throwable firstFatalFailure(List<Failure> failures) {
        Failure first = null;
        for (Failure failure : failures) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure.failure)
                    && (first == null || failure.index < first.index)) {
                first = failure;
            }
        }
        return first == null ? null : first.failure;
    }

    private void forgetRecordedFailures(List<Failure> failures) {
        for (Failure failure : failures) {
            forgetRecordedFatalFailure(failure.failure);
        }
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
        throw new IOException("FILE_LOADS destination commit failed", failure);
    }

    @Override
    public void close() throws IOException {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IOException("FILE_LOADS destination executor did not terminate");
                }
            }
        } catch (InterruptedException error) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while closing FILE_LOADS destination executor", error);
        }
    }

    private static final class Result {
        private final int index;
        private final Failure failureRecord;
        private Throwable failure;
        private boolean workerInterrupted;

        private Result(int index) {
            this.index = index;
            this.failureRecord = new Failure(index, null);
        }

        private void failed(Throwable failure) {
            this.failure = failure;
            this.failureRecord.failure = failure;
        }
    }

    private static final class LateFatalFailure {
        private boolean inUse;
        private Thread worker;
        private volatile Throwable failure;
    }

    private static final class Failure {
        private final int index;
        private Throwable failure;

        private Failure(int index, Throwable failure) {
            this.index = index;
            this.failure = failure;
        }
    }

    private static final class DrainInterruptedException extends IOException {
        private static final long serialVersionUID = 1L;

        private DrainInterruptedException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            // This exception is reserved before workers start, not created where the drain is
            // interrupted. An empty trace avoids attributing the later interrupt to this site.
            return this;
        }
    }
}
