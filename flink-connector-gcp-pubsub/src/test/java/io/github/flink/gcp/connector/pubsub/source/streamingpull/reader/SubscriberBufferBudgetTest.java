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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriberBufferLimitExceededEvent;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the callback-thread boundary that remains active when Flink stops polling downstream. */
class SubscriberBufferBudgetTest {

    @Test
    void successfulReservationsReuseTheAdmissionToken() {
        SubscriberBufferBudget budget =
                new SubscriberBufferBudget(Long.MAX_VALUE, Long.MAX_VALUE, event -> {});

        SubscriberBufferBudget.Admission first = budget.tryReserve("split-a", 1);
        SubscriberBufferBudget.Admission second = budget.tryReserve("split-a", 1);

        assertThat(first).isSameAs(second);
    }

    @Test
    void admitsTheExactMessageBoundaryAndFailsBeforeTheNextMessageIsRetained() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        List<String> stops = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(2, 100, events::add);
        budget.register("split-a", () -> stops.add("split-a"));

        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        assertThat(budget.tryReserve("split-a", 6).isAdmitted()).isTrue();
        SubscriberBufferBudget.Admission rejected = budget.tryReserve("split-a", 1);

        assertThat(rejected.isAdmitted()).isFalse();
        assertThat(budget.usage().messages()).isEqualTo(2);
        assertThat(budget.usage().bytes()).isEqualTo(10);

        rejected.respond();

        assertThat(stops).containsExactly("split-a");
        assertThat(events)
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.splitId()).isEqualTo("split-a");
                            assertThat(event.attemptedMessages()).isEqualTo(3);
                            assertThat(event.attemptedBytes()).isEqualTo(11);
                            assertThat(event.maxMessages()).isEqualTo(2);
                            assertThat(event.maxBytes()).isEqualTo(100);
                        });
    }

    @Test
    void admitsTheExactByteBoundaryAndFailsBeforeTheNextMessageIsRetained() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        List<String> stops = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(100, 10, events::add);
        budget.register("split-a", () -> stops.add("split-a"));

        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        assertThat(budget.tryReserve("split-a", 6).isAdmitted()).isTrue();
        SubscriberBufferBudget.Admission rejected = budget.tryReserve("split-a", 1);

        assertThat(rejected.isAdmitted()).isFalse();
        assertThat(budget.usage().messages()).isEqualTo(2);
        assertThat(budget.usage().bytes()).isEqualTo(10);

        rejected.respond();

        assertThat(stops).containsExactly("split-a");
        assertThat(events)
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.attemptedMessages()).isEqualTo(3);
                            assertThat(event.attemptedBytes()).isEqualTo(11);
                            assertThat(event.maxMessages()).isEqualTo(100);
                            assertThat(event.maxBytes()).isEqualTo(10);
                        });
    }

    @Test
    void parksInsteadOfFailingWhenEveryAssignedSubscriberIsPaused() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        List<String> stops = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 10, events::add);
        budget.register("split-a", () -> stops.add("split-a"));
        budget.register("split-b", () -> stops.add("split-b"));
        budget.setPaused("split-a", true);
        budget.setPaused("split-b", true);

        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        SubscriberBufferBudget.Admission rejected = budget.tryReserve("split-b", 4);
        rejected.respond();

        assertThat(rejected.isAdmitted()).isFalse();
        assertThat(budget.parkingRequested()).isTrue();
        assertThat(stops).containsExactly("split-a", "split-b");
        assertThat(events).isEmpty();

        budget.unregister("split-a");
        budget.unregister("split-b");
        budget.release(1, 4);

        assertThat(budget.parkingRequested()).isFalse();
        assertThat(budget.tryReserve("split-a", 10).isAdmitted()).isTrue();
    }

    @Test
    void aPauseGroupBecomesVisibleToConcurrentCallbacksAtomically() throws Exception {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 10, events::add);
        budget.register("split-a", () -> {});
        budget.register("split-b", () -> {});
        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        AtomicBoolean callbackBlockedByBatch = new AtomicBoolean();
        Thread callback =
                new Thread(
                        () -> {
                            callbackStarted.countDown();
                            budget.tryReserve("split-b", 4).respond();
                        },
                        "subscriber-budget-pause-callback");
        List<String> splitIds = List.of("split-a", "split-b");

        budget.setPaused(
                new AbstractCollection<>() {
                    @Override
                    public Iterator<String> iterator() {
                        Iterator<String> delegate = splitIds.iterator();
                        return new Iterator<>() {
                            @Override
                            public boolean hasNext() {
                                return delegate.hasNext();
                            }

                            @Override
                            public String next() {
                                String splitId = delegate.next();
                                if (splitId.equals("split-b")) {
                                    callback.start();
                                    await(callbackStarted);
                                    callbackBlockedByBatch.set(
                                            waitUntilBlocked(
                                                    callback, Thread.currentThread().getId()));
                                }
                                return splitId;
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return splitIds.size();
                    }
                },
                true);
        callback.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(callback.isAlive()).isFalse();
        assertThat(callbackBlockedByBatch)
                .as("the callback waits until every pause flag is published")
                .isTrue();
        assertThat(budget.parkingRequested()).isTrue();
        assertThat(events).isEmpty();
    }

    @Test
    void oneUnpausedSubscriberMakesTheReaderFail() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 10, events::add);
        budget.register("split-a", () -> {});
        budget.register("split-b", () -> {});
        budget.setPaused("split-a", true);

        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        budget.tryReserve("split-a", 4).respond();

        assertThat(budget.parkingRequested()).isFalse();
        assertThat(events).hasSize(1);
    }

    @Test
    void registrationAfterFailureStopsTheNewSubscriberWithoutReportingAgain() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        List<String> stops = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 10, events::add);
        budget.register("split-a", () -> stops.add("split-a"));
        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        budget.tryReserve("split-a", 4).respond();

        budget.register("split-b", () -> stops.add("split-b")).respond();

        assertThat(stops).containsExactly("split-a", "split-b");
        assertThat(budget.parkingRequested()).isFalse();
        assertThat(events).hasSize(1);
        budget.unregister("split-a");
        budget.unregister("split-b");
        budget.release(1, 4);
        assertThat(budget.tryReserve("split-b", 1).isAdmitted()).isFalse();
    }

    @Test
    void aResumeRacingReaderWideParkPromotesTheResponseToFailure() {
        List<SubscriberBufferLimitExceededEvent> events = new ArrayList<>();
        SubscriberBufferBudget budget = new SubscriberBufferBudget(1, 10, events::add);
        budget.register("split-a", () -> {});
        budget.register("split-b", () -> {});
        budget.setPaused("split-a", true);
        budget.setPaused("split-b", true);

        assertThat(budget.tryReserve("split-a", 4).isAdmitted()).isTrue();
        budget.tryReserve("split-b", 4).respond();
        budget.setPaused("split-b", false);

        assertThat(budget.parkingRequested()).isFalse();
        assertThat(events)
                .singleElement()
                .satisfies(event -> assertThat(event.splitId()).isEqualTo("split-b"));
        budget.tryReserve("split-b", 1).respond();
        assertThat(events).hasSize(1);
    }

    @Test
    void concurrentCallbacksNeverReservePastTheCountCap() throws Exception {
        int callbacks = 64;
        int cap = 17;
        SubscriberBufferBudget budget = new SubscriberBufferBudget(cap, 1_000, event -> {});
        budget.register("split-a", () -> {});
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Boolean> admitted = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        for (int index = 0; index < callbacks; index++) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(e);
                                }
                                admitted.add(budget.tryReserve("split-a", 1).isAdmitted());
                            });
            threads.add(thread);
            thread.start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(admitted).filteredOn(Boolean::booleanValue).hasSize(cap);
        assertThat(budget.usage().messages()).isEqualTo(cap);
        assertThat(budget.usage().bytes()).isEqualTo(cap);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static boolean waitUntilBlocked(Thread thread, long expectedOwnerId) {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            ThreadInfo info = threads.getThreadInfo(thread.getId());
            if (info == null || info.getThreadState() == Thread.State.TERMINATED) {
                return false;
            }
            if (info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockOwnerId() == expectedOwnerId) {
                return true;
            }
            Thread.yield();
        }
        return false;
    }
}
