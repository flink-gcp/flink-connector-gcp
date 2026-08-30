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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StagedFileFinalizerTest {

    @Test
    void workerCountIsBoundedByTheOptionAndFileCount() {
        assertThat(StagedFileFinalizer.boundedWorkerCount(3, 2)).isEqualTo(2);
        assertThat(StagedFileFinalizer.boundedWorkerCount(3, 8)).isEqualTo(3);
    }

    @Test
    void concurrentFinalizationRequiresAtLeastTwoFiles() {
        assertThatThrownBy(() -> StagedFileFinalizer.finish(List.of(), 2, ignored -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Concurrent finalization requires at least two staging files");
        assertThatThrownBy(
                        () ->
                                StagedFileFinalizer.finish(
                                        List.of(file("only", () -> {})), 2, ignored -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Concurrent finalization requires at least two staging files");
    }

    @Test
    void boundsConcurrencyAndReportsEveryResult() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        CountDownLatch twoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        List<String> finishedUris = new ArrayList<>();
        List<StagedFileWriter> files =
                Arrays.asList(
                        blockingFile("first", active, maximumActive, twoStarted, release),
                        blockingFile("second", active, maximumActive, twoStarted, release),
                        blockingFile("third", active, maximumActive, twoStarted, release));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                StagedFileFinalizer.finish(
                                        files,
                                        2,
                                        committable -> finishedUris.add(committable.getUri()));
                                return null;
                            });

            assertThat(twoStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(maximumActive).hasValue(2);
            release.countDown();
            finalization.get(5, TimeUnit.SECONDS);

            assertThat(maximumActive).hasValue(2);
            assertThat(finishedUris)
                    .containsExactly(
                            "gs://bucket/first", "gs://bucket/second", "gs://bucket/third");
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void reportsResultsInInputOrderAfterOutOfOrderCompletion() throws IOException {
        CountDownLatch laterFilesFinished = new CountDownLatch(2);
        List<String> finishedUris = new ArrayList<>();
        List<StagedFileWriter> files =
                Arrays.asList(
                        file("first", () -> await(laterFilesFinished, "later files")),
                        file("second", laterFilesFinished::countDown),
                        file("third", laterFilesFinished::countDown));

        StagedFileFinalizer.finish(files, 2, committable -> finishedUris.add(committable.getUri()));

        assertThat(finishedUris)
                .containsExactly("gs://bucket/first", "gs://bucket/second", "gs://bucket/third");
    }

    @Test
    void drainsEveryFileAndAggregatesFailuresInInputOrder() {
        AtomicInteger finishCalls = new AtomicInteger();
        AtomicInteger firstAbortCalls = new AtomicInteger();
        AtomicInteger secondAbortCalls = new AtomicInteger();
        AtomicInteger thirdAbortCalls = new AtomicInteger();
        CountDownLatch laterFailureFinished = new CountDownLatch(1);
        List<String> finishedUris = new ArrayList<>();
        List<StagedFileWriter> files =
                Arrays.asList(
                        failingFile(
                                "first",
                                finishCalls,
                                new IOException("first failure"),
                                () -> await(laterFailureFinished, "later failure"),
                                firstAbortCalls::incrementAndGet),
                        failingFile(
                                "second",
                                finishCalls,
                                null,
                                () -> {},
                                secondAbortCalls::incrementAndGet),
                        failingFile(
                                "third",
                                finishCalls,
                                new IOException("third failure"),
                                laterFailureFinished::countDown,
                                thirdAbortCalls::incrementAndGet));

        assertThatThrownBy(
                        () ->
                                StagedFileFinalizer.finish(
                                        files,
                                        3,
                                        committable -> finishedUris.add(committable.getUri())))
                .isInstanceOf(IOException.class)
                .hasMessage("first failure")
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .singleElement()
                                        .isInstanceOfSatisfying(
                                                IOException.class,
                                                suppressed ->
                                                        assertThat(suppressed)
                                                                .hasMessage("third failure")));

        assertThat(finishCalls).hasValue(3);
        assertThat(finishedUris).containsExactly("gs://bucket/second");
        assertThat(firstAbortCalls).hasValue(1);
        assertThat(secondAbortCalls).hasValue(0);
        assertThat(thirdAbortCalls).hasValue(1);
    }

    @Test
    void fatalFailureAbortsAnOrdinaryFailureThatCompletedBehindAStalledFile() throws Exception {
        CountDownLatch stalledFileStarted = new CountDownLatch(1);
        CountDownLatch releaseStalledFile = new CountDownLatch(1);
        CountDownLatch stalledFileInterrupted = new CountDownLatch(1);
        CountDownLatch stalledFileFinished = new CountDownLatch(1);
        CountDownLatch ordinaryFailureFinished = new CountDownLatch(1);
        CountDownLatch ordinaryAbortFinished = new CountDownLatch(1);
        AtomicInteger ordinaryAbortCalls = new AtomicInteger();
        OutOfMemoryError fatalFailure = new OutOfMemoryError("fatal failure");
        List<StagedFileWriter> files =
                Arrays.asList(
                        interruptIgnoringFile(
                                "first",
                                stalledFileStarted,
                                releaseStalledFile,
                                stalledFileInterrupted,
                                stalledFileFinished),
                        failingFile(
                                "second",
                                new AtomicInteger(),
                                new IOException("ordinary failure"),
                                ordinaryFailureFinished::countDown,
                                () -> {
                                    ordinaryAbortCalls.incrementAndGet();
                                    ordinaryAbortFinished.countDown();
                                }),
                        failingFile(
                                "third",
                                new AtomicInteger(),
                                fatalFailure,
                                () -> await(ordinaryFailureFinished, "ordinary failure")));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                StagedFileFinalizer.finish(files, 3, ignored -> {});
                                return null;
                            });

            assertThat(stalledFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> finalization.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(fatalFailure);
            assertThat(ordinaryAbortFinished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(ordinaryAbortCalls).hasValue(1);
        } finally {
            releaseStalledFile.countDown();
            caller.shutdownNow();
            assertThat(stalledFileFinished.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void publishedFatalFailureIsNotMutatedWhenItsAbortFails() throws Exception {
        CountDownLatch stalledFileStarted = new CountDownLatch(1);
        CountDownLatch releaseStalledFile = new CountDownLatch(1);
        CountDownLatch stalledFileInterrupted = new CountDownLatch(1);
        CountDownLatch stalledFileFinished = new CountDownLatch(1);
        CountDownLatch fatalAbortStarted = new CountDownLatch(1);
        CountDownLatch releaseFatalAbort = new CountDownLatch(1);
        AtomicReference<Thread> fatalWorker = new AtomicReference<>();
        OutOfMemoryError fatalFailure = new OutOfMemoryError("fatal failure");
        AssertionError abortFailure = new AssertionError("abort failure");
        List<StagedFileWriter> files =
                Arrays.asList(
                        interruptIgnoringFile(
                                "first",
                                stalledFileStarted,
                                releaseStalledFile,
                                stalledFileInterrupted,
                                stalledFileFinished),
                        failingFile(
                                "second",
                                new AtomicInteger(),
                                fatalFailure,
                                () -> await(stalledFileStarted, "stalled file"),
                                () -> {
                                    fatalWorker.set(Thread.currentThread());
                                    fatalAbortStarted.countDown();
                                    while (releaseFatalAbort.getCount() != 0) {
                                        try {
                                            releaseFatalAbort.await();
                                        } catch (InterruptedException ignored) {
                                            // A fatal drain cancels this task while abort is
                                            // running.
                                        }
                                    }
                                    throw abortFailure;
                                }));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                StagedFileFinalizer.finish(files, 2, ignored -> {});
                                return null;
                            });

            assertThat(stalledFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fatalAbortStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> finalization.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(fatalFailure);
            assertThat(fatalFailure.getSuppressed()).isEmpty();

            releaseFatalAbort.countDown();
            Thread worker = fatalWorker.get();
            worker.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(worker.isAlive()).isFalse();
            assertThat(fatalFailure.getSuppressed()).isEmpty();
        } finally {
            releaseFatalAbort.countDown();
            releaseStalledFile.countDown();
            caller.shutdownNow();
            assertThat(stalledFileFinished.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void fatalAbortFailureOvertakesAnOrdinaryFinishFailureAndAStalledPeer() throws Exception {
        CountDownLatch stalledFileStarted = new CountDownLatch(1);
        CountDownLatch releaseStalledFile = new CountDownLatch(1);
        CountDownLatch stalledFileInterrupted = new CountDownLatch(1);
        CountDownLatch stalledFileFinished = new CountDownLatch(1);
        IOException finishFailure = new IOException("finish failure");
        OutOfMemoryError abortFailure = new OutOfMemoryError("abort failure");
        List<StagedFileWriter> files =
                Arrays.asList(
                        interruptIgnoringFile(
                                "first",
                                stalledFileStarted,
                                releaseStalledFile,
                                stalledFileInterrupted,
                                stalledFileFinished),
                        failingFile(
                                "second",
                                new AtomicInteger(),
                                finishFailure,
                                () -> await(stalledFileStarted, "stalled file"),
                                () -> {
                                    throw abortFailure;
                                }));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                StagedFileFinalizer.finish(files, 2, ignored -> {});
                                return null;
                            });

            assertThat(stalledFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> finalization.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(abortFailure);
            assertThat(abortFailure.getSuppressed()).containsExactly(finishFailure);
        } finally {
            releaseStalledFile.countDown();
            caller.shutdownNow();
            assertThat(stalledFileFinished.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void submissionFailureDrainsAcceptedFilesAndAbortsFilesThatWereNotSubmitted() throws Exception {
        AtomicInteger firstAbortCalls = new AtomicInteger();
        AtomicInteger secondAbortCalls = new AtomicInteger();
        AtomicInteger thirdAbortCalls = new AtomicInteger();
        CountDownLatch firstFileStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstFile = new CountDownLatch(1);
        List<String> finishedUris = new ArrayList<>();
        List<StagedFileWriter> files =
                Arrays.asList(
                        file(
                                "first",
                                () -> {
                                    firstFileStarted.countDown();
                                    await(releaseFirstFile, "first file release");
                                },
                                firstAbortCalls::incrementAndGet),
                        file("second", () -> {}, secondAbortCalls::incrementAndGet),
                        file("third", () -> {}, thirdAbortCalls::incrementAndGet));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                StagedFileFinalizer.finish(
                                        files,
                                        3,
                                        committable -> finishedUris.add(committable.getUri()),
                                        ignored -> executorRejectingAfter(1));
                                return null;
                            });

            assertThat(firstFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(finalization.isDone()).isFalse();
            releaseFirstFile.countDown();
            assertThatThrownBy(() -> finalization.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class)
                    .cause()
                    .hasMessage("submission failure");

            assertThat(finishedUris).containsExactly("gs://bucket/first");
            assertThat(firstAbortCalls).hasValue(0);
            assertThat(secondAbortCalls).hasValue(1);
            assertThat(thirdAbortCalls).hasValue(1);
        } finally {
            releaseFirstFile.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void executorCreationFailureAbortsEveryFile() {
        AtomicInteger abortCalls = new AtomicInteger();
        List<StagedFileWriter> files =
                Arrays.asList(
                        file("first", () -> {}, abortCalls::incrementAndGet),
                        file("second", () -> {}, abortCalls::incrementAndGet));

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    StagedFileFinalizer.finish(
                                            files,
                                            2,
                                            ignored -> {},
                                            ignored -> {
                                                throw new IllegalStateException("creation failure");
                                            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("creation failure");

            assertThat(abortCalls).hasValue(2);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void jvmFatalWorkerFailureDoesNotWaitForAnEarlierStalledFile() throws Exception {
        CountDownLatch stalledFileStarted = new CountDownLatch(1);
        CountDownLatch releaseStalledFile = new CountDownLatch(1);
        CountDownLatch stalledFileInterrupted = new CountDownLatch(1);
        CountDownLatch stalledFileFinished = new CountDownLatch(1);
        AtomicInteger stalledFileAbortCalls = new AtomicInteger();
        CountDownLatch thirdFileStarted = new CountDownLatch(1);
        CountDownLatch thirdFileInterrupted = new CountDownLatch(1);
        CountDownLatch thirdFileFinished = new CountDownLatch(1);
        OutOfMemoryError fatalFailure = new OutOfMemoryError("fatal failure");
        AtomicBoolean callerInterruptedAfterFailure = new AtomicBoolean();
        AtomicInteger queuedFileFinishCalls = new AtomicInteger();
        AtomicInteger queuedFileAbortCalls = new AtomicInteger();
        List<StagedFileWriter> files =
                Arrays.asList(
                        interruptIgnoringFile(
                                "first",
                                stalledFileStarted,
                                releaseStalledFile,
                                stalledFileInterrupted,
                                stalledFileFinished,
                                stalledFileAbortCalls::incrementAndGet),
                        failingFile(
                                "second",
                                new AtomicInteger(),
                                fatalFailure,
                                () -> await(stalledFileStarted, "stalled file")),
                        interruptIgnoringFile(
                                "third",
                                thirdFileStarted,
                                releaseStalledFile,
                                thirdFileInterrupted,
                                thirdFileFinished),
                        failingFile(
                                "fourth",
                                queuedFileFinishCalls,
                                null,
                                () -> {},
                                queuedFileAbortCalls::incrementAndGet));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> finalization =
                    caller.submit(
                            () -> {
                                try {
                                    StagedFileFinalizer.finish(files, 2, ignored -> {});
                                    return null;
                                } finally {
                                    callerInterruptedAfterFailure.set(
                                            Thread.currentThread().isInterrupted());
                                }
                            });

            assertThat(stalledFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> finalization.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(fatalFailure);
            assertThat(stalledFileInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(stalledFileFinished.getCount()).isEqualTo(1);
            assertThat(stalledFileAbortCalls).hasValue(0);
            assertThat(callerInterruptedAfterFailure).isFalse();
            assertThat(queuedFileFinishCalls).hasValue(0);
            assertThat(queuedFileAbortCalls).hasValue(1);
        } finally {
            releaseStalledFile.countDown();
            caller.shutdownNow();
            assertThat(stalledFileFinished.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void jvmFatalWorkerFailurePreservesACallerCancellationInterrupt() throws Exception {
        CountDownLatch stalledFileStarted = new CountDownLatch(1);
        CountDownLatch releaseStalledFile = new CountDownLatch(1);
        CountDownLatch stalledFileInterrupted = new CountDownLatch(1);
        CountDownLatch stalledFileFinished = new CountDownLatch(1);
        CountDownLatch releaseFatalFailure = new CountDownLatch(1);
        OutOfMemoryError fatalFailure = new OutOfMemoryError("fatal failure");
        AtomicReference<Throwable> observedFailure = new AtomicReference<>();
        AtomicBoolean callerInterruptedAfterFailure = new AtomicBoolean();
        List<StagedFileWriter> files =
                Arrays.asList(
                        interruptIgnoringFile(
                                "first",
                                stalledFileStarted,
                                releaseStalledFile,
                                stalledFileInterrupted,
                                stalledFileFinished),
                        failingFile(
                                "second",
                                new AtomicInteger(),
                                fatalFailure,
                                () -> await(releaseFatalFailure, "fatal failure release")));
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                StagedFileFinalizer.finish(files, 2, ignored -> {});
                            } catch (Throwable failure) {
                                observedFailure.set(failure);
                            } finally {
                                callerInterruptedAfterFailure.set(
                                        Thread.currentThread().isInterrupted());
                            }
                        },
                        "fatal-finalizer-test-caller");
        try {
            caller.start();
            assertThat(stalledFileStarted.await(5, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            releaseFatalFailure.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(caller.isAlive()).isFalse();
            assertThat(observedFailure.get()).isSameAs(fatalFailure);
            assertThat(callerInterruptedAfterFailure).isTrue();
            assertThat(stalledFileInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseFatalFailure.countDown();
            releaseStalledFile.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(stalledFileFinished.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void interruptionWaitsForStartedFilesAndRestoresTheCallingThreadFlag() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        CountDownLatch twoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        List<StagedFileWriter> files =
                Arrays.asList(
                        blockingFile("first", active, maximumActive, twoStarted, release),
                        blockingFile("second", active, maximumActive, twoStarted, release));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                StagedFileFinalizer.finish(files, 2, ignored -> {});
                            } catch (Throwable t) {
                                failure.set(t);
                            } finally {
                                interrupted.set(Thread.currentThread().isInterrupted());
                            }
                        },
                        "finalizer-test-caller");

        caller.start();
        assertThat(twoStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitWaiting(caller);
        caller.interrupt();
        release.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(caller.isAlive()).isFalse();
        assertThat(failure.get())
                .isInstanceOf(IOException.class)
                .hasMessage("Interrupted while finalizing staging files");
        assertThat(interrupted).isTrue();
        assertThat(active).hasValue(0);
    }

    @Test
    void preexistingCallerInterruptIsPreservedWithoutFailingSuccessfulFinalization()
            throws IOException {
        List<String> finishedUris = new ArrayList<>();
        List<StagedFileWriter> files =
                Arrays.asList(file("first", () -> {}), file("second", () -> {}));

        Thread.currentThread().interrupt();
        try {
            StagedFileFinalizer.finish(
                    files, 2, committable -> finishedUris.add(committable.getUri()));

            assertThat(finishedUris).containsExactly("gs://bucket/first", "gs://bucket/second");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void shutdownFailurePreservesOrdinaryAndFatalFailurePriority() {
        IOException fileFailure = new IOException("file failure");
        IllegalStateException shutdownFailure = new IllegalStateException("shutdown failure");
        assertThat(
                        StagedFileFinalizer.shutdownExecutor(
                                executorWhoseShutdownFailsWith(shutdownFailure), fileFailure))
                .isSameAs(fileFailure);
        assertThat(fileFailure.getSuppressed()).containsExactly(shutdownFailure);

        IOException otherFileFailure = new IOException("other file failure");
        OutOfMemoryError fatalShutdownFailure = new OutOfMemoryError("fatal shutdown failure");
        assertThat(
                        StagedFileFinalizer.shutdownExecutor(
                                executorWhoseShutdownFailsWith(fatalShutdownFailure),
                                otherFileFailure))
                .isSameAs(fatalShutdownFailure);
        assertThat(fatalShutdownFailure.getSuppressed()).containsExactly(otherFileFailure);
    }

    private static ExecutorService executorWhoseShutdownFailsWith(Throwable failure) {
        return new AbstractExecutorService() {
            @Override
            public void shutdown() {}

            @Override
            public List<Runnable> shutdownNow() {
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                throw (Error) failure;
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return false;
            }

            @Override
            public void execute(Runnable command) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ExecutorService executorRejectingAfter(int acceptedTasks) {
        return new AbstractExecutorService() {
            private int submittedTasks;

            @Override
            public void shutdown() {}

            @Override
            public List<Runnable> shutdownNow() {
                return List.of();
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return true;
            }

            @Override
            public void execute(Runnable command) {
                if (submittedTasks++ == acceptedTasks) {
                    throw new RejectedExecutionException("submission failure");
                }
                Thread worker = new Thread(command, "accepted-finalizer-test-worker");
                worker.setDaemon(true);
                worker.start();
            }
        };
    }

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive()
                && thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            Thread.onSpinWait();
        }
        assertThat(thread.getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
    }

    private static StagedFileWriter file(String name, FinishAction action) {
        return file(name, action, () -> {});
    }

    private static StagedFileWriter file(
            String name, FinishAction action, Runnable abortOperation) {
        return new TestStagedFileWriter(
                () -> {
                    action.run();
                    return committable(name);
                },
                abortOperation);
    }

    private static StagedFileWriter failingFile(
            String name, AtomicInteger finishCalls, Throwable failure, FinishAction beforeFailure) {
        return failingFile(name, finishCalls, failure, beforeFailure, () -> {});
    }

    private static StagedFileWriter failingFile(
            String name,
            AtomicInteger finishCalls,
            Throwable failure,
            FinishAction beforeFailure,
            Runnable abortOperation) {
        return new TestStagedFileWriter(
                () -> {
                    finishCalls.incrementAndGet();
                    beforeFailure.run();
                    if (failure instanceof IOException) {
                        throw (IOException) failure;
                    }
                    if (failure instanceof RuntimeException) {
                        throw (RuntimeException) failure;
                    }
                    if (failure instanceof Error) {
                        throw (Error) failure;
                    }
                    return committable(name);
                },
                abortOperation);
    }

    private static void await(CountDownLatch latch, String description) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for " + description);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + description, e);
        }
    }

    private static StagedFileWriter interruptIgnoringFile(
            String name,
            CountDownLatch started,
            CountDownLatch release,
            CountDownLatch interrupted,
            CountDownLatch finished) {
        return interruptIgnoringFile(name, started, release, interrupted, finished, () -> {});
    }

    private static StagedFileWriter interruptIgnoringFile(
            String name,
            CountDownLatch started,
            CountDownLatch release,
            CountDownLatch interrupted,
            CountDownLatch finished,
            Runnable abortOperation) {
        return new TestStagedFileWriter(
                () -> {
                    started.countDown();
                    try {
                        while (release.getCount() != 0) {
                            try {
                                release.await();
                            } catch (InterruptedException ignored) {
                                // This fake models a close that cannot be interrupted.
                                interrupted.countDown();
                            }
                        }
                        return committable(name);
                    } finally {
                        finished.countDown();
                    }
                },
                abortOperation);
    }

    private static StagedFileWriter blockingFile(
            String name,
            AtomicInteger active,
            AtomicInteger maximumActive,
            CountDownLatch started,
            CountDownLatch release) {
        return new TestStagedFileWriter(
                () -> {
                    int nowActive = active.incrementAndGet();
                    maximumActive.accumulateAndGet(nowActive, Math::max);
                    started.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting to release " + name);
                        }
                        return committable(name);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Worker interrupted", e);
                    } finally {
                        active.decrementAndGet();
                    }
                });
    }

    private static FileLoadsCommittable committable(String name) {
        return new FileLoadsCommittable(
                "job",
                TableDestination.of("project", "dataset", name),
                "gs://bucket/" + name,
                1,
                1,
                StagingFormat.AVRO);
    }

    @FunctionalInterface
    private interface FinishAction {
        void run() throws IOException;
    }

    private static final class TestStagedFileWriter implements StagedFileWriter {
        private final FinishOperation finishOperation;
        private final Runnable abortOperation;

        private TestStagedFileWriter(FinishOperation finishOperation) {
            this(finishOperation, () -> {});
        }

        private TestStagedFileWriter(FinishOperation finishOperation, Runnable abortOperation) {
            this.finishOperation = finishOperation;
            this.abortOperation = abortOperation;
        }

        @Override
        public void append(GenericRecord record) {}

        @Override
        public long bytesWritten() {
            return 1;
        }

        @Override
        public FileLoadsCommittable finish() throws IOException {
            return finishOperation.finish();
        }

        @Override
        public void abort() {
            abortOperation.run();
        }
    }

    @FunctionalInterface
    private interface FinishOperation {
        FileLoadsCommittable finish() throws IOException;
    }
}
