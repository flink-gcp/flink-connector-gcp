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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Closes Bigtable data clients away from the task thread while keeping their physical lifetime
 * bounded. Scheduling failure falls back to synchronous close so the attempted handoff cannot leak
 * the client.
 *
 * <p>The semaphore counts open <em>and closing</em> clients. The executor can run at most that many
 * closes, and receives at most that many tasks because every one still owns a permit. A synchronous
 * queue is unnecessary: the permit is the stronger bound, while a fixed pool lets releases submit
 * without depending on whether a worker has started yet.
 *
 * <p>Shared by the module's client factories — the {@code MutateRows} batcher factory and the
 * single-row request client factory — so that one bounded pool holds every client a subtask opens,
 * whichever family opened it (ADR-0145, ADR-0148). A factory owns exactly one reaper, created
 * lazily with its first client and never serialized into the job graph.
 */
@Internal
public final class BigtableClientReaper {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableClientReaper.class);

    private final int maxClients;
    private final Semaphore slots;
    private final ExecutorService executor;

    /**
     * Creates a reaper whose daemon threads inherit the calling thread's uncaught-exception
     * handler, so a fatal close error reaches the task's handler rather than vanishing.
     *
     * @param maxClients the number of clients that may be open or closing at once
     */
    public BigtableClientReaper(int maxClients) {
        this(
                maxClients,
                new ClientReaperThreadFactory(
                        Thread.currentThread().getUncaughtExceptionHandler()));
    }

    @VisibleForTesting
    public BigtableClientReaper(int maxClients, ThreadFactory threadFactory) {
        this.maxClients = maxClients;
        this.slots = new Semaphore(maxClients, true);
        this.executor = Executors.newFixedThreadPool(maxClients, threadFactory);
    }

    /** Takes a permit for a client about to be created, waiting for a closing one to finish. */
    public void acquireSlot() throws InterruptedException {
        slots.acquire();
    }

    /** Returns a permit whose client creation failed, so nothing holds it. */
    public void releaseUnusedSlot() {
        slots.release();
    }

    @VisibleForTesting
    public int availableSlots() {
        return slots.availablePermits();
    }

    /**
     * Closes a client on a reaper thread, releasing its permit when the close finishes. A close
     * failure is hygiene: it is logged, never thrown, unless it is an {@link Error}.
     */
    public void closeEventually(AutoCloseable client, String description) {
        submit(client, description, true);
    }

    private CompletableFuture<Void> submit(
            AutoCloseable client, String description, boolean hygiene) {
        try {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            executor.execute(
                    () -> {
                        try {
                            client.close();
                            completion.complete(null);
                        } catch (Throwable failure) {
                            completion.completeExceptionally(failure);
                            if (hygiene) {
                                if (failure instanceof Error) {
                                    throw (Error) failure;
                                }
                                LOG.warn(
                                        "Failed to close {}; its logical slot was removed and"
                                                + " its physical resources may remain until"
                                                + " the SDK releases them.",
                                        description,
                                        failure);
                            }
                        } finally {
                            slots.release();
                        }
                    });
            return completion;
        } catch (RuntimeException | Error schedulingFailure) {
            closeAfterSchedulingFailure(client, schedulingFailure);
            throw schedulingFailure;
        }
    }

    private void closeAfterSchedulingFailure(AutoCloseable client, Throwable schedulingFailure) {
        try {
            client.close();
        } catch (Error closeFailure) {
            Throwable chosen = prioritize(closeFailure, schedulingFailure);
            if (chosen instanceof Error) {
                throw (Error) chosen;
            }
            throw (RuntimeException) chosen;
        } catch (Exception closeFailure) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            schedulingFailure.addSuppressed(closeFailure);
        } finally {
            slots.release();
        }
    }

    /** Waits until every scheduled close has physically finished. */
    public void awaitIdle() throws InterruptedException {
        slots.acquire(maxClients);
        slots.release(maxClients);
    }

    /**
     * Closes every remaining client and then the reaper itself, starting every close before waiting
     * for any so the SDK's bounded final metric exports overlap. A refused handoff closes that
     * client synchronously before the next is attempted. Reports the highest-priority failure with
     * the others suppressed; an interruption is restored after the teardown completes.
     */
    public void closeAll(List<AutoCloseable> activeClients) throws Exception {
        List<CompletableFuture<Void>> completions = new ArrayList<>(activeClients.size());
        Throwable failure = null;
        for (int i = 0; i < activeClients.size(); i++) {
            try {
                completions.add(
                        submit(activeClients.get(i), "active Bigtable client " + (i + 1), false));
            } catch (RuntimeException | Error schedulingFailure) {
                failure = prioritize(schedulingFailure, failure);
            }
        }
        executor.shutdown();

        InterruptedException interrupted = null;
        try {
            slots.acquire(maxClients);
        } catch (InterruptedException e) {
            interrupted = e;
            failure = prioritize(e, failure);
            // Teardown remains exhaustive: wait for every already-started close before
            // carrying cancellation out of the factory.
            slots.acquireUninterruptibly(maxClients);
        }

        for (CompletableFuture<Void> completion : completions) {
            try {
                completion.join();
            } catch (CompletionException e) {
                failure = prioritize(e.getCause(), failure);
            }
        }
        boolean terminated = false;
        while (!terminated) {
            try {
                terminated = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                if (interrupted == null) {
                    interrupted = e;
                }
                failure = prioritize(e, failure);
                // Every client already returned its permit. Finish joining the daemon workers
                // before restoring cancellation below, so no factory close leaves its own
                // executor behind.
            }
        }
        if (interrupted != null) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            ExceptionUtils.rethrowException(failure);
        }
    }

    /**
     * Chooses which of two failures to report, suppressing the other into it: a JVM-fatal or
     * out-of-memory error anywhere in a chain wins, then an interruption, then any other error,
     * then everything else, with the earlier failure winning a tie.
     */
    public static Throwable prioritize(Throwable next, @Nullable Throwable previous) {
        if (previous == null || previous == next) {
            return next;
        }
        int nextPriority = failurePriority(next);
        int previousPriority = failurePriority(previous);
        if (nextPriority > previousPriority) {
            next.addSuppressed(previous);
            return next;
        }
        previous.addSuppressed(next);
        return previous;
    }

    private static int failurePriority(Throwable failure) {
        if (containsJvmFatalOrOutOfMemoryError(failure)) {
            return 3;
        }
        if (failure instanceof InterruptedException) {
            return 2;
        }
        if (failure instanceof Error) {
            return 1;
        }
        return 0;
    }

    private static boolean containsJvmFatalOrOutOfMemoryError(Throwable failure) {
        return containsJvmFatalOrOutOfMemoryError(
                failure, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static boolean containsJvmFatalOrOutOfMemoryError(
            Throwable failure, Set<Throwable> visited) {
        if (!visited.add(failure)) {
            return false;
        }
        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(failure)) {
            return true;
        }
        if (failure.getCause() != null
                && containsJvmFatalOrOutOfMemoryError(failure.getCause(), visited)) {
            return true;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            if (containsJvmFatalOrOutOfMemoryError(suppressed, visited)) {
                return true;
            }
        }
        return false;
    }

    private static final class ClientReaperThreadFactory implements ThreadFactory {

        private final AtomicInteger ids = new AtomicInteger();
        private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

        private ClientReaperThreadFactory(
                Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "bigtable-client-reaper-" + ids.incrementAndGet());
            thread.setDaemon(true);
            thread.setContextClassLoader(BigtableClientReaper.class.getClassLoader());
            thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return thread;
        }
    }
}
