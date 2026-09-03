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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the bounded close pool both client factories share: a permit outlives the logical
 * removal until the physical close finishes, a refused handoff closes synchronously, and the
 * terminal close reports the highest-priority failure of an exhaustive teardown.
 *
 * <p>{@code @Timeout} for the same reason as {@code DefaultMutationBatcherFactoryTest}: a teardown
 * parked at an interruptible point fails the build instead of hanging it.
 */
@Timeout(30)
class BigtableClientReaperTest {

    /** Read-and-clear, unconditionally: two tests here set the interrupt flag on purpose. */
    @AfterEach
    void clearAnyInterruptThisClassSet() {
        Thread.interrupted();
    }

    @Test
    void reapsOffThreadButKeepsTheSlotUntilPhysicalCloseFinishes() throws Exception {
        BigtableClientReaper reaper = new BigtableClientReaper(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountDownLatch contenderStarted = new CountDownLatch(1);
        ExecutorService contender = Executors.newSingleThreadExecutor();
        reaper.acquireSlot();
        try {
            reaper.closeEventually(
                    () -> {
                        closeStarted.countDown();
                        allowClose.await();
                    },
                    "scripted client");

            assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reaper.availableSlots()).isZero();
            Future<?> waitingForSlot =
                    contender.submit(
                            () -> {
                                contenderStarted.countDown();
                                reaper.acquireSlot();
                                reaper.releaseUnusedSlot();
                                return null;
                            });

            assertThat(contenderStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(waitingForSlot.isDone()).isFalse();
            allowClose.countDown();
            waitingForSlot.get(5, TimeUnit.SECONDS);
        } finally {
            allowClose.countDown();
            contender.shutdownNow();
            reaper.closeAll(Collections.emptyList());
        }
    }

    @Test
    void letsAFatalHygieneCloseFailureReachTheReaperUncaughtHandler() throws Exception {
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        CountDownLatch observed = new CountDownLatch(1);
        Thread taskThread = Thread.currentThread();
        Thread.UncaughtExceptionHandler original = taskThread.getUncaughtExceptionHandler();
        BigtableClientReaper reaper;
        try {
            taskThread.setUncaughtExceptionHandler(
                    (ignored, failure) -> {
                        uncaught.set(failure);
                        observed.countDown();
                    });
            reaper = new BigtableClientReaper(1);
        } finally {
            taskThread.setUncaughtExceptionHandler(original);
        }
        AssertionError fatal = new AssertionError("fatal client close");
        reaper.acquireSlot();
        try {
            reaper.closeEventually(
                    () -> {
                        throw fatal;
                    },
                    "scripted client");

            assertThat(observed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(uncaught.get()).isSameAs(fatal);
        } finally {
            reaper.closeAll(Collections.emptyList());
        }
    }

    @Test
    void closesSynchronouslyAndReturnsTheSlotWhenSchedulingFails() throws Exception {
        SecurityException rejected = new SecurityException("reaper thread rejected");
        ThreadFactory rejectingThreads =
                ignored -> {
                    throw rejected;
                };
        BigtableClientReaper reaper = new BigtableClientReaper(1, rejectingThreads);
        List<String> events = new ArrayList<>();
        reaper.acquireSlot();
        try {
            assertThatThrownBy(
                            () ->
                                    reaper.closeEventually(
                                            () -> events.add("close"), "scripted client"))
                    .isSameAs(rejected);
            assertThat(events).containsExactly("close");
        } finally {
            reaper.closeAll(Collections.emptyList());
        }
    }

    @Test
    void synchronousFallbackKeepsAFatalSchedulingFailureAboveANonfatalCloseError()
            throws Exception {
        OutOfMemoryError fatal = new OutOfMemoryError("reaper thread allocation failed");
        NoClassDefFoundError closeFailure = new NoClassDefFoundError("client close failed");
        ThreadFactory rejectingThreads =
                ignored -> {
                    throw fatal;
                };
        BigtableClientReaper reaper = new BigtableClientReaper(1, rejectingThreads);
        reaper.acquireSlot();
        try {
            assertThatThrownBy(
                            () ->
                                    reaper.closeEventually(
                                            () -> {
                                                throw closeFailure;
                                            },
                                            "scripted client"))
                    .isSameAs(fatal)
                    .satisfies(
                            failure -> assertThat(failure.getSuppressed()).contains(closeFailure));
        } finally {
            reaper.closeAll(Collections.emptyList());
        }
    }

    @Test
    void closesEveryActiveClientWhenSchedulingFails() throws Exception {
        ThreadFactory rejectingThreads =
                ignored -> {
                    throw new SecurityException("reaper thread rejected");
                };
        BigtableClientReaper reaper = new BigtableClientReaper(2, rejectingThreads);
        List<String> events = new ArrayList<>();
        reaper.acquireSlot();
        reaper.acquireSlot();

        assertThatThrownBy(
                        () ->
                                reaper.closeAll(
                                        List.of(
                                                () -> events.add("first close"),
                                                () -> events.add("second close"))))
                .isInstanceOf(SecurityException.class)
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));
        assertThat(events).containsExactly("first close", "second close");
    }

    @Test
    void terminalCloseKeepsAFatalSchedulingFailureAboveInterruption() throws Exception {
        OutOfMemoryError fatal = new OutOfMemoryError("reaper thread allocation failed");
        ThreadFactory rejectingThreads =
                ignored -> {
                    throw fatal;
                };
        BigtableClientReaper reaper = new BigtableClientReaper(1, rejectingThreads);
        reaper.acquireSlot();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> reaper.closeAll(List.of(() -> {})))
                    .isSameAs(fatal)
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .anyMatch(InterruptedException.class::isInstance));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void terminalCloseKeepsALaterFatalClientFailureAboveANonfatalError() throws Exception {
        BigtableClientReaper reaper = new BigtableClientReaper(2);
        NoClassDefFoundError first = new NoClassDefFoundError("first client close failed");
        OutOfMemoryError fatal = new OutOfMemoryError("second client close failed");
        reaper.acquireSlot();
        reaper.acquireSlot();

        assertThatThrownBy(
                        () ->
                                reaper.closeAll(
                                        List.of(
                                                () -> {
                                                    throw first;
                                                },
                                                () -> {
                                                    throw fatal;
                                                })))
                .isSameAs(fatal)
                .satisfies(failure -> assertThat(failure.getSuppressed()).contains(first));
    }

    @Test
    void terminalCloseCanPrioritizeAClientFailureWhoseSuppressedGraphIsCyclic() throws Exception {
        BigtableClientReaper reaper = new BigtableClientReaper(2);
        IOException first = new IOException("first client close failed");
        NoClassDefFoundError later = new NoClassDefFoundError("second client close failed");
        first.addSuppressed(later);
        later.addSuppressed(first);
        reaper.acquireSlot();
        reaper.acquireSlot();

        assertThatThrownBy(
                        () ->
                                reaper.closeAll(
                                        List.of(
                                                () -> {
                                                    throw first;
                                                },
                                                () -> {
                                                    throw later;
                                                })))
                .isSameAs(later);
    }
}
