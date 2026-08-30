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
import org.apache.flink.util.FatalExitExceptionHandler;

import com.google.cloud.bigquery.JobInfo;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DestinationCommitExecutor}. */
class DestinationCommitExecutorTest {

    @Test
    void poolTransitionReleasesInlineWorkerAndLaterSingletonReusesThePool() throws Exception {
        AtomicInteger workers = new AtomicInteger();
        CyclicBarrier firstWave = new CyclicBarrier(2);
        DestinationCommitExecutor executor = executor(2, workers, unregisteredMetrics());

        try {
            executor.run(plans(1), (plan, worker, stop) -> {});
            assertThat(workers).hasValue(1);
            assertThat(executor.hasInlineWorker()).isTrue();

            executor.run(plans(2), (plan, worker, stop) -> await(firstWave));
            assertThat(workers).hasValue(3);
            assertThat(executor.hasInlineWorker()).isFalse();

            executor.run(plans(1), (plan, worker, stop) -> {});

            assertThat(workers).hasValue(3);
            assertThat(executor.hasInlineWorker()).isFalse();

            executor.runAllSerialWithinCommit(List.of(0, 1), (task, worker, stop) -> {});

            assertThat(workers).hasValue(3);
            assertThat(executor.hasInlineWorker()).isFalse();
        } finally {
            executor.close();
        }
    }

    @Test
    void postPoolSerialRunSubmitsOnlyOneTaskAtATime() throws Exception {
        ManualExecutorService workerPool = new ManualExecutorService();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        List<Integer> visited = new ArrayList<>();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.runAllSerialWithinCommit(
                                        List.of(0, 1), (task, worker, stop) -> visited.add(task));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitWaiting(coordinator);
            assertThat(workerPool.queuedTaskCount()).isOne();

            workerPool.runNext();
            workerPool.runNext();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isNull();
            assertThat(visited).containsExactly(0, 1);
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void repeatedCoordinatorInterruptDoesNotShortenTheWorkerDrain() {
        InterruptingTerminationExecutor executor = new InterruptingTerminationExecutor();
        long second = TimeUnit.SECONDS.toNanos(1);
        List<Long> clockValues = List.of(0L, 0L, 10 * second, 20 * second, 29 * second);
        AtomicInteger clockReads = new AtomicInteger();
        LongSupplier nanoTime =
                () -> {
                    int read = clockReads.getAndIncrement();
                    return clockValues.get(Math.min(read, clockValues.size() - 1));
                };

        assertThat(DestinationCommitExecutor.awaitTerminationAfterInterrupt(executor, nanoTime))
                .isTrue();

        assertThat(executor.awaitCalls).hasValue(4);
        assertThat(executor.timeoutNanos)
                .containsExactly(30 * second, 20 * second, 10 * second, second);
        assertThat(clockReads).hasValue(5);
    }

    @Test
    void reportsDestinationJvmFatalFailureThatFinishesAfterTheCoordinatorAbandonsItsDrain()
            throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(2);
        CountDownLatch releaseFatal = new CountDownLatch(1);
        CountDownLatch fatalRecorded = new CountDownLatch(1);
        CountDownLatch letWorkerPublish = new CountDownLatch(1);
        CountDownLatch fatalReported = new CountDownLatch(1);
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AbandoningTerminationExecutor workerPool =
                new AbandoningTerminationExecutor(
                        (thread, failure) -> {
                            reportedFailure.set(failure);
                            fatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                executor(
                        2,
                        new AtomicInteger(),
                        unregisteredMetrics(),
                        workerPool,
                        () -> {
                            fatalRecorded.countDown();
                            awaitIgnoringInterrupt(letWorkerPublish);
                        });
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            workersStarted.countDown();
                                            if (plan.destination.getTable().equals("t0")) {
                                                awaitIgnoringInterrupt(releaseFatal);
                                                throw new OutOfMemoryError("late-fatal");
                                            }
                                            try {
                                                new CountDownLatch(1).await();
                                            } catch (InterruptedException failure) {
                                                Thread.currentThread().interrupt();
                                                throw new IOException(
                                                        "worker interrupted", failure);
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(workersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseFatal.countDown();
            assertThat(fatalRecorded.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for FILE_LOADS destinations");

            assertThat(fatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reportedFailure.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("late-fatal");
        } finally {
            releaseFatal.countDown();
            letWorkerPublish.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void boundsFiftyAndTwoHundredDestinationsAndReusesOneWorkerPerThread() throws Exception {
        int maximumConcurrency = 4;
        AtomicInteger workers = new AtomicInteger();
        DestinationCommitExecutor executor =
                executor(maximumConcurrency, workers, unregisteredMetrics());

        try {
            for (int destinationCount : List.of(50, 200)) {
                AtomicInteger active = new AtomicInteger();
                AtomicInteger maximumActive = new AtomicInteger();
                AtomicInteger completed = new AtomicInteger();
                AtomicInteger started = new AtomicInteger();
                CyclicBarrier firstWave = new CyclicBarrier(maximumConcurrency);
                executor.run(
                        plans(destinationCount),
                        (plan, worker, stop) -> {
                            int now = active.incrementAndGet();
                            maximumActive.accumulateAndGet(now, Math::max);
                            try {
                                if (started.incrementAndGet() <= maximumConcurrency) {
                                    await(firstWave);
                                }
                                completed.incrementAndGet();
                            } finally {
                                active.decrementAndGet();
                            }
                        });

                assertThat(completed).hasValue(destinationCount);
                assertThat(maximumActive).hasValue(maximumConcurrency);
            }
        } finally {
            executor.close();
        }

        assertThat(workers).hasValue(maximumConcurrency);
    }

    @Test
    void productionWorkersUseFlinksFatalExitHandler() throws Exception {
        CyclicBarrier workersStarted = new CyclicBarrier(2);
        Set<Thread.UncaughtExceptionHandler> handlers = ConcurrentHashMap.newKeySet();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());

        try {
            executor.run(
                    plans(2),
                    (plan, worker, stop) -> {
                        handlers.add(Thread.currentThread().getUncaughtExceptionHandler());
                        await(workersStarted);
                    });
        } finally {
            executor.close();
        }

        assertThat(handlers).containsExactly(FatalExitExceptionHandler.INSTANCE);
    }

    @Test
    void oneDestinationRunsInlineWithoutStartingACommitThread() throws Exception {
        AtomicInteger workers = new AtomicInteger();
        String caller = Thread.currentThread().getName();
        AtomicReference<String> executingThread = new AtomicReference<>();
        DestinationCommitExecutor executor = executor(8, workers, unregisteredMetrics());

        try {
            executor.run(
                    plans(1),
                    (plan, worker, stop) -> executingThread.set(Thread.currentThread().getName()));
        } finally {
            executor.close();
        }

        assertThat(executingThread).hasValue(caller);
        assertThat(workers).hasValue(1);
    }

    @Test
    void inlineRunAllStopsAfterAnInterruptedTask() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        List<Integer> visited = new ArrayList<>();
        DestinationCommitExecutor executor =
                executor(1, new AtomicInteger(), unregisteredMetrics());
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.runAllWithinCommit(
                                        List.of(0, 1),
                                        (task, worker, stop) -> {
                                            visited.add(task);
                                            if (task == 0) {
                                                firstStarted.countDown();
                                                await(new CountDownLatch(1));
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for executor test workers");
            assertThat(visited).containsExactly(0);
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void failureStopsDispatchDrainsRunningDestinationsAndOrdersSuppressedFailures()
            throws Exception {
        Set<String> started = ConcurrentHashMap.newKeySet();
        Set<String> finished = ConcurrentHashMap.newKeySet();
        CountDownLatch firstWaveStarted = new CountDownLatch(3);
        DestinationCommitExecutor executor =
                executor(3, new AtomicInteger(), unregisteredMetrics());

        try {
            assertThatThrownBy(
                            () ->
                                    executor.run(
                                            plans(5),
                                            (plan, worker, stop) -> {
                                                String table = plan.destination.getTable();
                                                started.add(table);
                                                firstWaveStarted.countDown();
                                                await(firstWaveStarted);
                                                if (table.equals("t2")) {
                                                    throw new IOException("failure-2");
                                                }
                                                awaitStop(stop);
                                                if (table.equals("t0")) {
                                                    throw new IOException("failure-0");
                                                }
                                                finished.add(table);
                                            }))
                    .isInstanceOf(IOException.class)
                    .hasMessage("failure-0")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly("failure-2"));
        } finally {
            executor.close();
        }

        assertThat(started).containsExactlyInAnyOrder("t0", "t1", "t2");
        assertThat(finished).containsExactly("t1");
    }

    @Test
    void interruptingTheCoordinatorInterruptsRunningWorkers() throws Exception {
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            workerStarted.countDown();
                                            try {
                                                new CountDownLatch(1).await();
                                            } catch (InterruptedException failure) {
                                                workerInterrupted.countDown();
                                                Thread.currentThread().interrupt();
                                                throw new IOException(
                                                        "worker interrupted", failure);
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(workerInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for FILE_LOADS destinations");
            assertThat(coordinator.isInterrupted()).isTrue();
        } finally {
            executor.close();
        }
    }

    @Test
    void jvmFatalFailureInterruptsAnotherRunningDestinationAndReturns() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        CountDownLatch peerReadyToInterrupt = new CountDownLatch(1);
        CountDownLatch peerInterrupted = new CountDownLatch(1);
        Set<String> started = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(3),
                                        (plan, worker, stop) -> {
                                            started.add(plan.destination.getTable());
                                            bothStarted.countDown();
                                            await(releaseWorkers);
                                            if (plan.destination.getTable().equals("t0")) {
                                                await(peerReadyToInterrupt);
                                                throw new OutOfMemoryError("fatal-failure");
                                            }
                                            peerReadyToInterrupt.countDown();
                                            try {
                                                new CountDownLatch(1).await();
                                            } catch (InterruptedException failure) {
                                                peerInterrupted.countDown();
                                                Thread.currentThread().interrupt();
                                                throw new IOException(
                                                        "worker interrupted", failure);
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseWorkers.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(peerInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(started).containsExactlyInAnyOrder("t0", "t1");
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-failure")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly("worker interrupted"));
        } finally {
            releaseWorkers.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void jvmFatalFailureReturnsAfterBoundedDrainAbandonsInterruptIgnoringPeer() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AbandoningTerminationExecutor workerPool =
                new AbandoningTerminationExecutor((thread, failure) -> {});
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            bothStarted.countDown();
                                            awaitIgnoringInterrupt(bothStarted);
                                            if (plan.destination.getTable().equals("t0")) {
                                                throw new OutOfMemoryError("fatal-failure");
                                            }
                                            awaitIgnoringInterrupt(releasePeer);
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-failure");
            assertThat(workerPool.timeoutNanos).hasSize(1);
            assertThat(workerPool.timeoutNanos.get(0).longValue())
                    .isBetween(TimeUnit.SECONDS.toNanos(29), TimeUnit.SECONDS.toNanos(30));
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void shutdownAllocationFailureCannotReplaceObservedFatalAndReportsLatePeer() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        CountDownLatch lateFatalReported = new CountDownLatch(1);
        OutOfMemoryError primary = new OutOfMemoryError("primary-fatal");
        OutOfMemoryError late = new OutOfMemoryError("late-fatal");
        OutOfMemoryError shutdownFailure = new OutOfMemoryError("shutdown-allocation-failure");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        ShutdownFailingExecutor workerPool =
                new ShutdownFailingExecutor(
                        shutdownFailure,
                        (thread, failure) -> {
                            reported.set(failure);
                            lateFatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            bothStarted.countDown();
                                            awaitIgnoringInterrupt(bothStarted);
                                            if (plan.destination.getTable().equals("t0")) {
                                                throw primary;
                                            }
                                            awaitIgnoringInterrupt(releasePeer);
                                            throw late;
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isSameAs(primary);
            releasePeer.countDown();
            assertThat(lateFatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reported.get()).isSameAs(late);
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void shutdownAllocationFailureAfterCoordinatorInterruptReportsLateWorkerFatal()
            throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        CountDownLatch lateFatalReported = new CountDownLatch(1);
        OutOfMemoryError late = new OutOfMemoryError("late-fatal");
        OutOfMemoryError shutdownFailure = new OutOfMemoryError("shutdown-allocation-failure");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        ShutdownFailingExecutor workerPool =
                new ShutdownFailingExecutor(
                        shutdownFailure,
                        (thread, failure) -> {
                            reported.set(failure);
                            lateFatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            bothStarted.countDown();
                                            awaitIgnoringInterrupt(bothStarted);
                                            awaitIgnoringInterrupt(releaseWorkers);
                                            if (plan.destination.getTable().equals("t0")) {
                                                throw late;
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(shutdownFailure);

            releaseWorkers.countDown();
            assertThat(lateFatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reported.get()).isSameAs(late);
        } finally {
            releaseWorkers.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void terminationWaitFatalAfterCoordinatorInterruptRemainsPrimary() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        CountDownLatch lateFatalReported = new CountDownLatch(1);
        OutOfMemoryError late = new OutOfMemoryError("late-fatal");
        OutOfMemoryError terminationFailure = new OutOfMemoryError("termination-wait-failure");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AwaitTerminationFailingExecutor workerPool =
                new AwaitTerminationFailingExecutor(
                        terminationFailure,
                        (thread, failure) -> {
                            reported.set(failure);
                            lateFatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            bothStarted.countDown();
                                            awaitIgnoringInterrupt(releaseWorkers);
                                            if (plan.destination.getTable().equals("t0")) {
                                                throw late;
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get()).isSameAs(terminationFailure);

            releaseWorkers.countDown();
            assertThat(lateFatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reported.get()).isSameAs(late);
        } finally {
            releaseWorkers.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void coordinatorInterruptDuringFatalDrainIsRestoredAndSuppressed() throws Exception {
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        TrackingTerminationExecutor workerPool = new TrackingTerminationExecutor();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            bothStarted.countDown();
                                            awaitIgnoringInterrupt(bothStarted);
                                            if (plan.destination.getTable().equals("t0")) {
                                                throw new OutOfMemoryError("fatal-failure");
                                            }
                                            awaitIgnoringInterrupt(releasePeer);
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(workerPool.drainStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            assertThat(workerPool.interruptObserved.await(5, TimeUnit.SECONDS)).isTrue();
            releasePeer.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-failure")
                    .satisfies(
                            failure -> {
                                assertThat(failure.getSuppressed()).hasSize(1);
                                assertThat(failure.getSuppressed()[0])
                                        .hasMessage(
                                                "Interrupted while draining FILE_LOADS destinations after a JVM-fatal failure");
                                assertThat(failure.getSuppressed()[0].getStackTrace()).isEmpty();
                            });
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void jvmFatalFailureCompletedDuringInterruptDrainTakesPriority() throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            workersStarted.countDown();
                                            try {
                                                new CountDownLatch(1).await();
                                            } catch (InterruptedException failure) {
                                                Thread.currentThread().interrupt();
                                                if (plan.destination.getTable().equals("t0")) {
                                                    throw new OutOfMemoryError(
                                                            "fatal-during-drain");
                                                }
                                                throw new IOException(
                                                        "ordinary-during-drain", failure);
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(workersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-during-drain")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly(
                                                    "Interrupted while waiting for FILE_LOADS destinations",
                                                    "ordinary-during-drain"));
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void completedFatalIsNotReportedAgainWhenInterruptDrainIsAbandoned() throws Exception {
        assertCompletedFatalIsNotReportedAgain(InterruptDrainAbandonment.RETURN_FALSE);
    }

    @Test
    void completedFatalIsNotReportedAgainWhenInterruptShutdownThrowsOrdinaryFailure()
            throws Exception {
        assertCompletedFatalIsNotReportedAgain(InterruptDrainAbandonment.SHUTDOWN_FAILURE);
    }

    @Test
    void completedFatalIsNotReportedAgainWhenInterruptTerminationWaitThrowsOrdinaryFailure()
            throws Exception {
        assertCompletedFatalIsNotReportedAgain(InterruptDrainAbandonment.AWAIT_FAILURE);
    }

    private void assertCompletedFatalIsNotReportedAgain(InterruptDrainAbandonment abandonment)
            throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(2);
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        CompletedButAbandoningTerminationExecutor workerPool =
                new CompletedButAbandoningTerminationExecutor(
                        abandonment, (thread, failure) -> reported.set(failure));
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics(), workerPool);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.run(
                                        plans(2),
                                        (plan, worker, stop) -> {
                                            workersStarted.countDown();
                                            try {
                                                new CountDownLatch(1).await();
                                            } catch (InterruptedException failure) {
                                                Thread.currentThread().interrupt();
                                                if (plan.destination.getTable().equals("t0")) {
                                                    throw new OutOfMemoryError(
                                                            "fatal-during-drain");
                                                }
                                                throw new IOException(
                                                        "ordinary-during-drain", failure);
                                            }
                                        });
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(workersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-during-drain")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly(
                                                    "Interrupted while waiting for FILE_LOADS destinations",
                                                    "ordinary-during-drain"));
            assertThat(workerPool.terminationWaitTimedOut.get()).isFalse();
            assertThat(reported.get()).isNull();
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void destinationDrainFailureStillReportsAnUnpublishedFatalRecord() throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(2);
        CountDownLatch firstTaskCompleted = new CountDownLatch(1);
        CountDownLatch fatalRecordBlocked = new CountDownLatch(1);
        CountDownLatch releaseFatalPublication = new CountDownLatch(1);
        CountDownLatch fatalReported = new CountDownLatch(1);
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        CompletionAwareAbandoningExecutor workerPool =
                new CompletionAwareAbandoningExecutor(
                        firstTaskCompleted,
                        fatalRecordBlocked,
                        false,
                        (thread, failure) -> {
                            reported.set(failure);
                            fatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                executor(
                        2,
                        new AtomicInteger(),
                        unregisteredMetrics(),
                        workerPool,
                        new Runnable() {
                            private final AtomicInteger calls = new AtomicInteger();

                            @Override
                            public void run() {
                                if (calls.incrementAndGet() > 1) {
                                    fatalRecordBlocked.countDown();
                                    awaitIgnoringInterrupt(releaseFatalPublication);
                                }
                            }
                        });
        DestinationCommitExecutor.DestinationWork<DestinationCommitPlan> work =
                new DestinationCommitExecutor.DestinationWork<>() {
                    @Override
                    public void run(
                            DestinationCommitPlan plan,
                            DestinationCommitExecutor.Worker worker,
                            AtomicBoolean stop)
                            throws IOException {
                        workersStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            if (plan.destination.getTable().equals("t0")) {
                                throw new IOException("ordinary-during-drain", failure);
                            }
                            awaitIgnoringInterrupt(firstTaskCompleted);
                            throw new OutOfMemoryError("unpublished-fatal");
                        }
                    }

                    @Override
                    public void taskFailureObserved(DestinationCommitPlan plan, Throwable failure) {
                        throw new OutOfMemoryError("drain-diagnostic-failure");
                    }
                };
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.runAllWithinCommit(plans(2), work);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(workersStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(workerPool.orderingWaitTimedOut.get()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("drain-diagnostic-failure");
            assertThat(fatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reported.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("unpublished-fatal");
        } finally {
            releaseFatalPublication.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void cleanupDrainFailureStillReportsAnUnpublishedFatalRecord() throws Exception {
        CountDownLatch cleanupsStarted = new CountDownLatch(2);
        CountDownLatch firstTaskCompleted = new CountDownLatch(1);
        CountDownLatch fatalRecordBlocked = new CountDownLatch(1);
        CountDownLatch releaseFatalPublication = new CountDownLatch(1);
        CountDownLatch fatalReported = new CountDownLatch(1);
        AtomicReference<Throwable> reported = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        CompletionAwareAbandoningExecutor workerPool =
                new CompletionAwareAbandoningExecutor(
                        firstTaskCompleted,
                        fatalRecordBlocked,
                        true,
                        (thread, failure) -> {
                            reported.set(failure);
                            fatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FatalAfterInterruptCleanupRunner(
                                                cleanupsStarted, firstTaskCompleted),
                                        new FakeTableAdmin()),
                        unregisteredMetrics(),
                        workerPool,
                        () -> {
                            fatalRecordBlocked.countDown();
                            awaitIgnoringInterrupt(releaseFatalPublication);
                        });
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(cleanupPlans(2));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(cleanupsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(workerPool.orderingWaitTimedOut.get()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("cleanup-drain-diagnostic-failure");
            assertThat(fatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reported.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("unpublished-cleanup-fatal");
        } finally {
            releaseFatalPublication.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void jvmFatalFailureTakesPriorityOverAnEarlierDestinationFailure() throws Exception {
        CyclicBarrier bothStarted = new CyclicBarrier(2);
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());

        try {
            assertThatThrownBy(
                            () ->
                                    executor.run(
                                            plans(2),
                                            (plan, worker, stop) -> {
                                                await(bothStarted);
                                                if (plan.destination.getTable().equals("t0")) {
                                                    throw new IOException("ordinary-failure");
                                                }
                                                throw new OutOfMemoryError("fatal-failure");
                                            }))
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-failure")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly("ordinary-failure"));
        } finally {
            executor.close();
        }
    }

    @Test
    void runAllStopsDispatchAfterJvmFatalFailureWithoutExhaustingFatalRecords() throws Exception {
        CyclicBarrier firstWave = new CyclicBarrier(2);
        Set<Integer> started = ConcurrentHashMap.newKeySet();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), unregisteredMetrics());

        try {
            assertThatThrownBy(
                            () ->
                                    executor.runAllWithinCommit(
                                            List.of(0, 1, 2),
                                            (task, worker, stop) -> {
                                                started.add(task);
                                                await(firstWave);
                                                if (task == 0) {
                                                    throw new OutOfMemoryError("fatal-0");
                                                }
                                                awaitStop(stop);
                                                throw new OutOfMemoryError("fatal-1");
                                            }))
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("fatal-0")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly("fatal-1"));
        } finally {
            executor.close();
        }

        assertThat(started).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void reportsLiveAndCompletedCommitStateWithoutDestinationLabels() throws Exception {
        TestSinkCommitterMetricGroup group = TestSinkCommitterMetricGroup.create();
        FileLoadsCommitterMetrics metrics = new FileLoadsCommitterMetrics(group);
        DestinationCommitExecutor executor = executor(1, new AtomicInteger(), metrics);
        AtomicInteger expectedQueued = new AtomicInteger(1);

        try {
            executor.run(
                    plans(2),
                    (plan, worker, stop) -> {
                        assertThat(
                                        group.<Integer>gaugeValue(
                                                BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                                .isEqualTo(expectedQueued.getAndDecrement());
                        assertThat(
                                        group.<Integer>gaugeValue(
                                                BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                                .isOne();
                        assertThat(
                                        group.<Long>gaugeValue(
                                                BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                                .isNotNegative();
                    });
        } finally {
            executor.close();
        }

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                .isZero();
        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.LAST_COMMIT_DURATION_MILLIS))
                .isNotNegative();
    }

    @Test
    void clearsLiveCommitMetricsAfterDestinationFailure() throws Exception {
        TestSinkCommitterMetricGroup group = TestSinkCommitterMetricGroup.create();
        DestinationCommitExecutor executor =
                executor(2, new AtomicInteger(), new FileLoadsCommitterMetrics(group));

        try {
            assertThatThrownBy(
                            () ->
                                    executor.run(
                                            plans(2),
                                            (plan, worker, stop) -> {
                                                throw new IOException("scripted failure");
                                            }))
                    .isInstanceOf(IOException.class)
                    .hasMessage("scripted failure");
        } finally {
            executor.close();
        }

        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(group.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(group.<Long>gaugeValue(BigQueryMetricNames.CURRENT_COMMIT_DURATION_MILLIS))
                .isZero();
    }

    @Test
    void serialCleanupDeletesEveryTemporaryTable() throws Exception {
        FakeLoadJobRunner runner = new FakeLoadJobRunner();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            executor.cleanup(plans);
        } finally {
            executor.close();
        }

        assertThat(runner.deletedTables)
                .containsExactly(
                        TableDestination.of("p", "d", "tmp0"),
                        TableDestination.of("p", "d", "tmp1"));
    }

    @Test
    void concurrentCleanupPropagatesJvmFatalFailures() throws Exception {
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FatalCleanupRunner(), new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);

        try {
            executor.run(plans, (plan, worker, stop) -> {});

            assertThatThrownBy(() -> executor.cleanup(plans))
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("scripted cleanup fatal");
        } finally {
            executor.close();
        }
    }

    @Test
    void concurrentCleanupJvmFatalInterruptsAnotherRunningCleanupAndReturns() throws Exception {
        CountDownLatch cleanupsStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        CountDownLatch peerInterrupted = new CountDownLatch(1);
        FatalAndBlockingCleanupRunner runner =
                new FatalAndBlockingCleanupRunner(cleanupsStarted, releasePeer, peerInterrupted);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(peerInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("scripted cleanup fatal");
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void concurrentCleanupJvmFatalReturnsAfterAbandoningInterruptIgnoringPeer() throws Exception {
        CountDownLatch cleanupsStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        FatalAndBlockingCleanupRunner runner =
                new FatalAndBlockingCleanupRunner(
                        cleanupsStarted, releasePeer, new CountDownLatch(1), true);
        AbandoningTerminationExecutor workerPool =
                new AbandoningTerminationExecutor((thread, failure) -> {});
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        unregisteredMetrics(),
                        workerPool);
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("scripted cleanup fatal");
            assertThat(workerPool.timeoutNanos).hasSize(1);
            assertThat(workerPool.timeoutNanos.get(0).longValue())
                    .isBetween(TimeUnit.SECONDS.toNanos(29), TimeUnit.SECONDS.toNanos(30));
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void coordinatorInterruptDuringCleanupFatalDrainIsRestoredAndSuppressed() throws Exception {
        CountDownLatch cleanupsStarted = new CountDownLatch(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        FatalAndBlockingCleanupRunner runner =
                new FatalAndBlockingCleanupRunner(
                        cleanupsStarted, releasePeer, new CountDownLatch(1), true);
        TrackingTerminationExecutor workerPool = new TrackingTerminationExecutor();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        unregisteredMetrics(),
                        workerPool);
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(workerPool.drainStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            assertThat(workerPool.interruptObserved.await(5, TimeUnit.SECONDS)).isTrue();
            releasePeer.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("scripted cleanup fatal")
                    .satisfies(
                            failure -> {
                                assertThat(failure.getSuppressed()).hasSize(1);
                                assertThat(failure.getSuppressed()[0])
                                        .hasMessage(
                                                "Interrupted while draining FILE_LOADS cleanup after a JVM-fatal failure");
                                assertThat(failure.getSuppressed()[0].getStackTrace()).isEmpty();
                            });
        } finally {
            releasePeer.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void concurrentCleanupOrdersJvmFatalFailuresByDestinationPlan() throws Exception {
        CountDownLatch cleanupsStarted = new CountDownLatch(2);
        CountDownLatch secondFailureReady = new CountDownLatch(1);
        CountDownLatch releaseFirstFailure = new CountDownLatch(1);
        OutOfMemoryError first = new OutOfMemoryError("first cleanup fatal");
        OutOfMemoryError second = new OutOfMemoryError("second cleanup fatal");
        OrderedFatalCleanupRunner runner =
                new OrderedFatalCleanupRunner(
                        cleanupsStarted, secondFailureReady, releaseFirstFailure, first, second);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> new DestinationCommitExecutor.Worker(runner, new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondFailureReady.await(5, TimeUnit.SECONDS)).isTrue();
            releaseFirstFailure.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isSameAs(first);
            assertThat(first.getSuppressed()).containsExactly(second);
        } finally {
            releaseFirstFailure.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void concurrentCleanupPropagatesJvmFatalFailureCompletedDuringInterruptDrain()
            throws Exception {
        CountDownLatch cleanupStarted = new CountDownLatch(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new InterruptFatalCleanupRunner(cleanupStarted),
                                        new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("cleanup-fatal-during-drain")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly(
                                                    "Interrupted while cleaning up FILE_LOADS temporary tables"));
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void reportsCleanupJvmFatalFailureThatFinishesAfterTheCoordinatorAbandonsItsDrain()
            throws Exception {
        CountDownLatch cleanupStarted = new CountDownLatch(2);
        CountDownLatch releaseFatal = new CountDownLatch(1);
        CountDownLatch fatalReported = new CountDownLatch(1);
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AbandoningTerminationExecutor workerPool =
                new AbandoningTerminationExecutor(
                        (thread, failure) -> {
                            reportedFailure.set(failure);
                            fatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new LateFatalCleanupRunner(cleanupStarted, releaseFatal),
                                        new FakeTableAdmin()),
                        unregisteredMetrics(),
                        workerPool);
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while cleaning up FILE_LOADS temporary tables");

            releaseFatal.countDown();
            assertThat(fatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reportedFailure.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("late-cleanup-fatal");
        } finally {
            releaseFatal.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void reportsCleanupJvmFatalFailureRecordedBeforeCompletionPublication() throws Exception {
        CountDownLatch cleanupStarted = new CountDownLatch(2);
        CountDownLatch releaseFatal = new CountDownLatch(1);
        CountDownLatch fatalRecorded = new CountDownLatch(1);
        CountDownLatch letWorkerPublish = new CountDownLatch(1);
        CountDownLatch fatalReported = new CountDownLatch(1);
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AbandoningTerminationExecutor workerPool =
                new AbandoningTerminationExecutor(
                        (thread, failure) -> {
                            reportedFailure.set(failure);
                            fatalReported.countDown();
                        });
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new LateFatalCleanupRunner(cleanupStarted, releaseFatal),
                                        new FakeTableAdmin()),
                        unregisteredMetrics(),
                        workerPool,
                        () -> {
                            fatalRecorded.countDown();
                            awaitIgnoringInterrupt(letWorkerPublish);
                        });
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseFatal.countDown();
            assertThat(fatalRecorded.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while cleaning up FILE_LOADS temporary tables");
            assertThat(fatalReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reportedFailure.get())
                    .isInstanceOf(OutOfMemoryError.class)
                    .hasMessage("late-cleanup-fatal");
        } finally {
            releaseFatal.countDown();
            letWorkerPublish.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void concurrentCleanupInterruptionIsNotSwallowed() throws Exception {
        CountDownLatch cleanupStarted = new CountDownLatch(2);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new InterruptFatalCleanupRunner(cleanupStarted, false),
                                        new FakeTableAdmin()),
                        unregisteredMetrics());
        List<DestinationCommitPlan> plans = cleanupPlans(2);
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                executor.cleanup(plans);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            executor.run(plans, (plan, worker, stop) -> {});
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while cleaning up FILE_LOADS temporary tables");
        } finally {
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void boundsCleanupWorkAndDeletesEveryTemporaryTable() throws Exception {
        int maximumConcurrency = 4;
        AtomicInteger workers = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        Set<TableDestination> deleted = ConcurrentHashMap.newKeySet();
        AtomicInteger started = new AtomicInteger();
        TestSinkCommitterMetricGroup metricGroup = TestSinkCommitterMetricGroup.create();
        CyclicBarrier firstWave =
                new CyclicBarrier(
                        maximumConcurrency,
                        () -> {
                            assertThat(
                                            metricGroup.<Integer>gaugeValue(
                                                    BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                                    .isEqualTo(200 - maximumConcurrency);
                            assertThat(
                                            metricGroup.<Integer>gaugeValue(
                                                    BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                                    .isEqualTo(maximumConcurrency);
                        });
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        maximumConcurrency,
                        () -> {
                            workers.incrementAndGet();
                            return new DestinationCommitExecutor.Worker(
                                    new CleanupRunner(
                                            active, maximumActive, deleted, started, firstWave),
                                    new FakeTableAdmin());
                        },
                        new FileLoadsCommitterMetrics(metricGroup));

        try {
            List<DestinationCommitPlan> plans = cleanupPlans(200);
            executor.run(plans, (plan, worker, stop) -> {});
            executor.cleanup(plans);
        } finally {
            executor.close();
        }

        assertThat(deleted)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, 200)
                                .mapToObj(index -> TableDestination.of("p", "d", "tmp" + index))
                                .toList());
        assertThat(maximumActive).hasValue(maximumConcurrency);
        assertThat(workers).hasValue(maximumConcurrency);
        assertThat(metricGroup.<Integer>gaugeValue(BigQueryMetricNames.QUEUED_COMMIT_DESTINATIONS))
                .isZero();
        assertThat(metricGroup.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS))
                .isZero();
    }

    private static DestinationCommitExecutor executor(
            int maximumConcurrency, AtomicInteger workers, FileLoadsCommitterMetrics metrics) {
        return executor(maximumConcurrency, workers, metrics, null);
    }

    private static DestinationCommitExecutor executor(
            int maximumConcurrency,
            AtomicInteger workers,
            FileLoadsCommitterMetrics metrics,
            ExecutorService executor) {
        return executor(maximumConcurrency, workers, metrics, executor, () -> {});
    }

    private static DestinationCommitExecutor executor(
            int maximumConcurrency,
            AtomicInteger workers,
            FileLoadsCommitterMetrics metrics,
            ExecutorService executor,
            Runnable fatalRecordedHook) {
        return new DestinationCommitExecutor(
                maximumConcurrency,
                () -> {
                    workers.incrementAndGet();
                    return new DestinationCommitExecutor.Worker(
                            new FakeLoadJobRunner(), new FakeTableAdmin());
                },
                metrics,
                executor,
                fatalRecordedHook);
    }

    private static FileLoadsCommitterMetrics unregisteredMetrics() {
        return FileLoadsCommitterMetrics.unregistered(new SimpleCounter());
    }

    private static List<DestinationCommitPlan> plans(int count) {
        return IntStream.range(0, count)
                .mapToObj(
                        index ->
                                new DestinationCommitPlan(
                                        TableDestination.of("p", "d", "t" + index),
                                        List.of(),
                                        null))
                .toList();
    }

    private static List<DestinationCommitPlan> cleanupPlans(int count) {
        return IntStream.range(0, count)
                .mapToObj(
                        index -> {
                            TableDestination destination =
                                    TableDestination.of("p", "d", "t" + index);
                            TableDestination temporary =
                                    TableDestination.of("p", "d", "tmp" + index);
                            DestinationCopy copy =
                                    new DestinationCopy(
                                            List.of(),
                                            new PlannedCopy(
                                                    "copy" + index,
                                                    new CopyJobSpec(
                                                            List.of(temporary),
                                                            destination,
                                                            JobInfo.CreateDisposition
                                                                    .CREATE_IF_NEEDED,
                                                            JobInfo.WriteDisposition.WRITE_APPEND)),
                                            null,
                                            List.of(temporary));
                            return new DestinationCommitPlan(destination, List.of(), copy);
                        })
                .toList();
    }

    private static void await(CyclicBarrier barrier) throws IOException {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IOException("Timed out waiting for executor test workers", failure);
        }
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for executor test workers");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for executor test workers", failure);
        }
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException ignored) {
                // This deliberately models a client call that outlives shutdownNow().
            }
        }
    }

    private static void awaitStop(AtomicBoolean stop) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!stop.get()) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("Timed out waiting for executor stop signal");
            }
            Thread.onSpinWait();
        }
    }

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.WAITING) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            Thread.onSpinWait();
        }
        assertThat(thread.getState()).isEqualTo(Thread.State.WAITING);
    }

    private static final class CleanupRunner implements LoadJobRunner {
        private final AtomicInteger active;
        private final AtomicInteger maximumActive;
        private final Set<TableDestination> deleted;
        private final AtomicInteger started;
        private final CyclicBarrier firstWave;

        private CleanupRunner(
                AtomicInteger active,
                AtomicInteger maximumActive,
                Set<TableDestination> deleted,
                AtomicInteger started,
                CyclicBarrier firstWave) {
            this.active = active;
            this.maximumActive = maximumActive;
            this.deleted = deleted;
            this.started = started;
            this.firstWave = firstWave;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            int now = active.incrementAndGet();
            maximumActive.accumulateAndGet(now, Math::max);
            try {
                if (started.incrementAndGet() <= 4) {
                    await(firstWave);
                }
                deleted.add(table);
            } catch (IOException failure) {
                throw new IllegalStateException(failure);
            } finally {
                active.decrementAndGet();
            }
        }
    }

    private static final class FatalCleanupRunner implements LoadJobRunner {
        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            if (table.getTable().equals("tmp0")) {
                throw new OutOfMemoryError("scripted cleanup fatal");
            }
        }
    }

    private static final class FatalAndBlockingCleanupRunner implements LoadJobRunner {
        private final CountDownLatch cleanupsStarted;
        private final CountDownLatch releasePeer;
        private final CountDownLatch peerInterrupted;
        private final boolean ignoreInterrupt;

        private FatalAndBlockingCleanupRunner(
                CountDownLatch cleanupsStarted,
                CountDownLatch releasePeer,
                CountDownLatch peerInterrupted) {
            this(cleanupsStarted, releasePeer, peerInterrupted, false);
        }

        private FatalAndBlockingCleanupRunner(
                CountDownLatch cleanupsStarted,
                CountDownLatch releasePeer,
                CountDownLatch peerInterrupted,
                boolean ignoreInterrupt) {
            this.cleanupsStarted = cleanupsStarted;
            this.releasePeer = releasePeer;
            this.peerInterrupted = peerInterrupted;
            this.ignoreInterrupt = ignoreInterrupt;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            cleanupsStarted.countDown();
            awaitIgnoringInterrupt(cleanupsStarted);
            if (table.getTable().equals("tmp0")) {
                throw new OutOfMemoryError("scripted cleanup fatal");
            }
            if (ignoreInterrupt) {
                awaitIgnoringInterrupt(releasePeer);
                return;
            }
            try {
                releasePeer.await();
            } catch (InterruptedException ignored) {
                peerInterrupted.countDown();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class OrderedFatalCleanupRunner implements LoadJobRunner {
        private final CountDownLatch cleanupsStarted;
        private final CountDownLatch secondFailureReady;
        private final CountDownLatch releaseFirstFailure;
        private final OutOfMemoryError first;
        private final OutOfMemoryError second;

        private OrderedFatalCleanupRunner(
                CountDownLatch cleanupsStarted,
                CountDownLatch secondFailureReady,
                CountDownLatch releaseFirstFailure,
                OutOfMemoryError first,
                OutOfMemoryError second) {
            this.cleanupsStarted = cleanupsStarted;
            this.secondFailureReady = secondFailureReady;
            this.releaseFirstFailure = releaseFirstFailure;
            this.first = first;
            this.second = second;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            cleanupsStarted.countDown();
            awaitIgnoringInterrupt(cleanupsStarted);
            if (table.getTable().equals("tmp0")) {
                awaitIgnoringInterrupt(releaseFirstFailure);
                throw first;
            }
            secondFailureReady.countDown();
            throw second;
        }
    }

    private static final class InterruptFatalCleanupRunner implements LoadJobRunner {
        private final CountDownLatch cleanupStarted;
        private final boolean failFatally;

        private InterruptFatalCleanupRunner(CountDownLatch cleanupStarted) {
            this(cleanupStarted, true);
        }

        private InterruptFatalCleanupRunner(CountDownLatch cleanupStarted, boolean failFatally) {
            this.cleanupStarted = cleanupStarted;
            this.failFatally = failFatally;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            cleanupStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                if (failFatally && table.getTable().equals("tmp0")) {
                    throw new OutOfMemoryError("cleanup-fatal-during-drain");
                }
            }
        }
    }

    private static final class LateFatalCleanupRunner implements LoadJobRunner {
        private final CountDownLatch cleanupStarted;
        private final CountDownLatch releaseFatal;

        private LateFatalCleanupRunner(CountDownLatch cleanupStarted, CountDownLatch releaseFatal) {
            this.cleanupStarted = cleanupStarted;
            this.releaseFatal = releaseFatal;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            cleanupStarted.countDown();
            if (table.getTable().equals("tmp0")) {
                awaitIgnoringInterrupt(releaseFatal);
                throw new OutOfMemoryError("late-cleanup-fatal");
            }
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class FatalAfterInterruptCleanupRunner implements LoadJobRunner {
        private final CountDownLatch cleanupsStarted;
        private final CountDownLatch firstTaskCompleted;

        private FatalAfterInterruptCleanupRunner(
                CountDownLatch cleanupsStarted, CountDownLatch firstTaskCompleted) {
            this.cleanupsStarted = cleanupsStarted;
            this.firstTaskCompleted = firstTaskCompleted;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {}

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {}

        @Override
        public void awaitJob(String jobId) {}

        @Override
        public void deleteTable(TableDestination table) {
            cleanupsStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                if (table.getTable().equals("tmp0")) {
                    awaitIgnoringInterrupt(firstTaskCompleted);
                    throw new OutOfMemoryError("unpublished-cleanup-fatal");
                }
            }
        }
    }

    private static final class ManualExecutorService extends AbstractExecutorService {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        private final AtomicBoolean shutdown = new AtomicBoolean();

        @Override
        public void shutdown() {
            shutdown.set(true);
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown.set(true);
            List<Runnable> queued = new ArrayList<>();
            tasks.drainTo(queued);
            return queued;
        }

        @Override
        public boolean isShutdown() {
            return shutdown.get();
        }

        @Override
        public boolean isTerminated() {
            return shutdown.get() && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }

        @Override
        public void execute(Runnable command) {
            if (shutdown.get()) {
                throw new RejectedExecutionException("The manual executor is shut down");
            }
            tasks.add(command);
        }

        private int queuedTaskCount() {
            return tasks.size();
        }

        private void runNext() throws InterruptedException {
            Runnable task = tasks.poll(5, TimeUnit.SECONDS);
            assertThat(task).isNotNull();
            task.run();
        }
    }

    private static final class AbandoningTerminationExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final AtomicBoolean abandonNextTerminationWait = new AtomicBoolean(true);
        private final List<Long> timeoutNanos = new ArrayList<>();

        private AbandoningTerminationExecutor(Thread.UncaughtExceptionHandler handler) {
            delegate =
                    Executors.newFixedThreadPool(
                            2,
                            runnable -> {
                                Thread worker = new Thread(runnable);
                                worker.setDaemon(true);
                                worker.setUncaughtExceptionHandler(handler);
                                return worker;
                            });
        }

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
            timeoutNanos.add(unit.toNanos(timeout));
            if (abandonNextTerminationWait.getAndSet(false)) {
                return false;
            }
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }

    private static final class CompletedButAbandoningTerminationExecutor
            extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final InterruptDrainAbandonment abandonment;
        private final AtomicBoolean abandonNextTerminationWait = new AtomicBoolean(true);
        private final AtomicBoolean terminationWaitTimedOut = new AtomicBoolean();

        private CompletedButAbandoningTerminationExecutor(
                InterruptDrainAbandonment abandonment, Thread.UncaughtExceptionHandler handler) {
            this.abandonment = abandonment;
            delegate =
                    Executors.newFixedThreadPool(
                            2,
                            runnable -> {
                                Thread worker = new Thread(runnable);
                                worker.setDaemon(true);
                                worker.setUncaughtExceptionHandler(handler);
                                return worker;
                            });
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            List<Runnable> queued = delegate.shutdownNow();
            if (abandonment == InterruptDrainAbandonment.SHUTDOWN_FAILURE) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!delegate.isTerminated()) {
                    if (System.nanoTime() >= deadline) {
                        terminationWaitTimedOut.set(true);
                        throw new AssertionError("Timed out waiting for completed test workers");
                    }
                    Thread.onSpinWait();
                }
                throw new SecurityException("scripted shutdown failure");
            }
            return queued;
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
            boolean terminated = delegate.awaitTermination(timeout, unit);
            if (abandonNextTerminationWait.getAndSet(false)) {
                if (abandonment == InterruptDrainAbandonment.AWAIT_FAILURE) {
                    throw new IllegalStateException("scripted termination-wait failure");
                }
                if (abandonment == InterruptDrainAbandonment.RETURN_FALSE) {
                    return false;
                }
            }
            return terminated;
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }

    private enum InterruptDrainAbandonment {
        RETURN_FALSE,
        SHUTDOWN_FAILURE,
        AWAIT_FAILURE
    }

    private static final class CompletionAwareAbandoningExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final CountDownLatch firstTaskCompleted;
        private final CountDownLatch abandonmentReady;
        private final boolean failFirstGet;
        private final AtomicBoolean abandonNextTerminationWait = new AtomicBoolean(true);
        private final AtomicBoolean failNextGet = new AtomicBoolean(true);
        private final AtomicBoolean orderingWaitTimedOut = new AtomicBoolean();

        private CompletionAwareAbandoningExecutor(
                CountDownLatch firstTaskCompleted,
                CountDownLatch abandonmentReady,
                boolean failFirstGet,
                Thread.UncaughtExceptionHandler handler) {
            this.firstTaskCompleted = firstTaskCompleted;
            this.abandonmentReady = abandonmentReady;
            this.failFirstGet = failFirstGet;
            delegate =
                    Executors.newFixedThreadPool(
                            2,
                            runnable -> {
                                Thread worker = new Thread(runnable);
                                worker.setDaemon(true);
                                worker.setUncaughtExceptionHandler(handler);
                                return worker;
                            });
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new FutureTask<>(callable) {
                @Override
                public T get() throws InterruptedException, ExecutionException {
                    T result = super.get();
                    if (failFirstGet && failNextGet.getAndSet(false)) {
                        throw new OutOfMemoryError("cleanup-drain-diagnostic-failure");
                    }
                    return result;
                }
            };
        }

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
            if (abandonNextTerminationWait.getAndSet(false)) {
                if (!firstTaskCompleted.await(5, TimeUnit.SECONDS)) {
                    orderingWaitTimedOut.set(true);
                    throw new AssertionError("Timed out waiting for the first completed test task");
                }
                if (!abandonmentReady.await(5, TimeUnit.SECONDS)) {
                    orderingWaitTimedOut.set(true);
                    throw new AssertionError("Timed out waiting for fatal publication to block");
                }
                return false;
            }
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(
                    () -> {
                        command.run();
                        firstTaskCompleted.countDown();
                    });
        }
    }

    private static final class ShutdownFailingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final OutOfMemoryError shutdownFailure;

        private ShutdownFailingExecutor(
                OutOfMemoryError shutdownFailure, Thread.UncaughtExceptionHandler handler) {
            this.shutdownFailure = shutdownFailure;
            delegate =
                    Executors.newFixedThreadPool(
                            2,
                            runnable -> {
                                Thread worker = new Thread(runnable);
                                worker.setDaemon(true);
                                worker.setUncaughtExceptionHandler(handler);
                                return worker;
                            });
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            delegate.shutdownNow();
            throw shutdownFailure;
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
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
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

    private static final class AwaitTerminationFailingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final OutOfMemoryError terminationFailure;
        private final AtomicBoolean failNextTerminationWait = new AtomicBoolean(true);

        private AwaitTerminationFailingExecutor(
                OutOfMemoryError terminationFailure, Thread.UncaughtExceptionHandler handler) {
            this.terminationFailure = terminationFailure;
            delegate =
                    Executors.newFixedThreadPool(
                            2,
                            runnable -> {
                                Thread worker = new Thread(runnable);
                                worker.setDaemon(true);
                                worker.setUncaughtExceptionHandler(handler);
                                return worker;
                            });
        }

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
            if (failNextTerminationWait.getAndSet(false)) {
                throw terminationFailure;
            }
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }

    private static final class InterruptingTerminationExecutor extends AbstractExecutorService {
        private final AtomicInteger awaitCalls = new AtomicInteger();
        private final List<Long> timeoutNanos = new ArrayList<>();

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return awaitCalls.get() > 3;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            timeoutNanos.add(unit.toNanos(timeout));
            if (awaitCalls.getAndIncrement() < 3) {
                throw new InterruptedException("repeated coordinator interrupt");
            }
            return true;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }
    }
}
