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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/** Finalizes independent staging files with deterministic result and failure ordering. */
@Internal
final class StagedFileFinalizer {

    private static final long FATAL_FAILURE_POLL_MILLIS = 100;

    private enum OwnershipState {
        QUEUED,
        STARTED,
        SUCCEEDED,
        ABORTED
    }

    private StagedFileFinalizer() {}

    /**
     * Requires a concurrent batch of at least two files. After validating that precondition, takes
     * ownership of and finalizes the files, reporting successful committables in input order on the
     * calling thread.
     *
     * <p>The method drains every submitted finalization before returning or throwing, except when a
     * worker reports a JVM-fatal failure. That path cancels its peers so the fatal failure cannot
     * remain hidden behind an earlier stalled finalization. An interrupt received while the files
     * drain is remembered, restored on the calling thread, and reported after any file failures.
     * This keeps no upload channel running after the writer advances to close or a later checkpoint
     * in every non-fatal outcome.
     *
     * @param files two or more staging files; the writer retains the serial path for smaller inputs
     */
    static void finish(
            List<StagedFileWriter> files,
            int maximumConcurrency,
            Consumer<FileLoadsCommittable> onFinished)
            throws IOException {
        finish(
                files,
                maximumConcurrency,
                onFinished,
                workers ->
                        Executors.newFixedThreadPool(
                                workers,
                                new ExecutorThreadFactory("bigquery-file-loads-finalizer")));
    }

    static void finish(
            List<StagedFileWriter> files,
            int maximumConcurrency,
            Consumer<FileLoadsCommittable> onFinished,
            IntFunction<ExecutorService> executorFactory)
            throws IOException {
        Preconditions.checkArgument(
                files.size() >= 2, "Concurrent finalization requires at least two staging files");
        int workers = boundedWorkerCount(files.size(), maximumConcurrency);
        // A flag that predates this drain belongs to the caller. Clear it before creating the
        // executor so every exit, including an executor-creation failure, restores it.
        boolean initiallyInterrupted = Thread.interrupted();
        ExecutorService executor;
        try {
            executor = executorFactory.apply(workers);
        } catch (Throwable creationFailure) {
            try {
                abortAll(files, 0);
            } finally {
                if (initiallyInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            rethrow(creationFailure);
            throw new AssertionError("rethrow returned");
        }
        List<Future<FileLoadsCommittable>> futures = new ArrayList<>(files.size());
        List<AtomicReference<OwnershipState>> ownership = new ArrayList<>(files.size());
        AtomicReference<Throwable> fatalWorkerFailure = new AtomicReference<>();
        Throwable primaryFailure = null;
        try {
            for (StagedFileWriter file : files) {
                AtomicReference<OwnershipState> fileOwnership =
                        new AtomicReference<>(OwnershipState.QUEUED);
                try {
                    futures.add(
                            executor.submit(
                                    () -> finishFile(file, fileOwnership, fatalWorkerFailure)));
                    ownership.add(fileOwnership);
                } catch (RuntimeException | Error submissionFailure) {
                    primaryFailure = submissionFailure;
                    break;
                }
            }

            List<FileLoadsCommittable> finished = new ArrayList<>(futures.size());
            boolean interrupted = false;
            boolean abortForFatal =
                    primaryFailure != null
                            && ExceptionUtils.isJvmFatalOrOutOfMemoryError(primaryFailure);
            drain:
            for (int index = 0; index < futures.size(); index++) {
                Future<FileLoadsCommittable> future = futures.get(index);
                if (Thread.interrupted()) {
                    interrupted = true;
                }
                if (abortForFatal) {
                    break;
                }
                while (true) {
                    Throwable signalledFatal = fatalWorkerFailure.get();
                    if (signalledFatal != null) {
                        primaryFailure =
                                recordFailurePreservingFatalPriority(
                                        primaryFailure, signalledFatal);
                        abortForFatal = true;
                        break drain;
                    }
                    try {
                        finished.add(future.get(FATAL_FAILURE_POLL_MILLIS, TimeUnit.MILLISECONDS));
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    } catch (TimeoutException e) {
                        // Polling lets a later worker's fatal failure overtake an earlier stalled
                        // file without using caller interrupts as an ambiguous wake-up signal.
                    } catch (ExecutionException e) {
                        primaryFailure =
                                recordFailurePreservingFatalPriority(primaryFailure, e.getCause());
                        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(e.getCause())) {
                            abortForFatal = true;
                            break drain;
                        }
                        break;
                    }
                }
            }
            if (abortForFatal) {
                cancelAll(futures);
                // Claim submitted files that no worker started before cancellation. A worker and
                // this caller race through the same atomic transition, so an interrupt-ignoring
                // finish() is never aborted concurrently.
                abortQueued(files, ownership);
                abortAll(files, futures.size());
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            } else {
                // Every submitted worker has completed. A failed worker aborted its own file;
                // unsubmitted files are still owned by this caller.
                abortAll(files, futures.size());
                for (FileLoadsCommittable committable : finished) {
                    onFinished.accept(committable);
                }

                // Future.get() can return an already-complete result without observing an interrupt
                // that raced with completion. Consume that pending flag before deciding the
                // outcome.
                if (Thread.interrupted()) {
                    interrupted = true;
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                    primaryFailure =
                            recordFailure(
                                    primaryFailure,
                                    new IOException("Interrupted while finalizing staging files"));
                }
            }
        } catch (Throwable failure) {
            // Preserve any file or submission failure already selected above. In particular, an
            // allocation failure while recording results must not bypass executor shutdown.
            primaryFailure = recordFailurePreservingFatalPriority(primaryFailure, failure);
        } finally {
            // Every successfully submitted future above is complete in non-fatal outcomes. A fatal
            // worker failure cancels its peers before this final shutdown.
            primaryFailure = shutdownExecutor(executor, primaryFailure);
            if (initiallyInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (primaryFailure != null) {
            rethrow(primaryFailure);
        }
    }

    static int boundedWorkerCount(int fileCount, int maximumConcurrency) {
        return Math.min(maximumConcurrency, fileCount);
    }

    /** Shuts down the pool without replacing the failure that caused the checkpoint to fail. */
    static Throwable shutdownExecutor(ExecutorService executor, Throwable primaryFailure) {
        try {
            executor.shutdownNow();
        } catch (Throwable shutdownFailure) {
            return recordFailurePreservingFatalPriority(primaryFailure, shutdownFailure);
        }
        return primaryFailure;
    }

    private static FileLoadsCommittable finishFile(
            StagedFileWriter file,
            AtomicReference<OwnershipState> ownership,
            AtomicReference<Throwable> fatalWorkerFailure)
            throws IOException {
        if (!ownership.compareAndSet(OwnershipState.QUEUED, OwnershipState.STARTED)) {
            throw new IllegalStateException("Staging file was aborted before finalization started");
        }
        try {
            FileLoadsCommittable committable = file.finish();
            ownership.set(OwnershipState.SUCCEEDED);
            return committable;
        } catch (Throwable failure) {
            boolean fatalFailure = ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure);
            if (fatalFailure) {
                fatalWorkerFailure.compareAndSet(null, failure);
            }
            // The worker owns a file once it starts. Abort on that same worker so a failure that
            // races a fatal peer cannot fall between a caller-side scan and cancellation.
            Throwable primaryFailure = failure;
            try {
                file.abort();
            } catch (Throwable abortFailure) {
                // A fatal failure may already be visible to the caller. Do not mutate that
                // published Throwable; it already dominates any cleanup failure.
                if (!fatalFailure) {
                    primaryFailure = recordFailurePreservingFatalPriority(failure, abortFailure);
                }
            } finally {
                ownership.set(OwnershipState.ABORTED);
            }
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(primaryFailure)) {
                fatalWorkerFailure.compareAndSet(null, primaryFailure);
            }
            rethrow(primaryFailure);
            throw new AssertionError("rethrow returned");
        }
    }

    private static void cancelAll(List<? extends Future<?>> futures) {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private static void abortAll(List<StagedFileWriter> files, int fromIndex) {
        for (int index = fromIndex; index < files.size(); index++) {
            files.get(index).abort();
        }
    }

    private static void abortQueued(
            List<StagedFileWriter> files, List<AtomicReference<OwnershipState>> ownership) {
        for (int index = 0; index < ownership.size(); index++) {
            if (ownership.get(index).compareAndSet(OwnershipState.QUEUED, OwnershipState.ABORTED)) {
                files.get(index).abort();
            }
        }
    }

    private static Throwable recordFailurePreservingFatalPriority(
            Throwable primary, Throwable failure) {
        if (primary != null
                && primary != failure
                && ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)
                && !ExceptionUtils.isJvmFatalOrOutOfMemoryError(primary)) {
            failure.addSuppressed(primary);
            return failure;
        }
        return recordFailure(primary, failure);
    }

    private static Throwable recordFailure(Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Failed to finalize staging files", failure);
    }
}
