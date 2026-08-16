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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Deadline-bounded draining of a running job's collect iterator. */
@Internal
public final class Drains {

    /**
     * Drains the iterator until {@code count} <em>distinct</em> elements have arrived, the job
     * behind it ends, or {@code timeout} passes, returning whatever did arrive in arrival order.
     *
     * <p>Distinct, and returning a shortfall rather than blocking until the class timeout, for two
     * reasons: the connectors are at-least-once, so a redelivery is legitimate and counting total
     * elements would let one duplicate crowd out an original; and a shortfall must fail the
     * assertion that asked for the elements, with the ones that did arrive in its message, rather
     * than consume the build's whole budget. Callers should therefore assert with {@code
     * containsAll} or an in-any-order variant, not an exact multiset.
     *
     * <p>The iteration runs on its own thread because a deadline consulted between elements is not
     * enough: {@code hasNext()} on a collect iterator blocks until an element arrives or the job
     * ends, so a loop over it parks exactly when fewer elements than asked for ever arrive — and
     * JUnit's timeout interrupt is ignored by the blocking iterator. One such shortfall cost a
     * build 38 minutes before this helper existed (issue #150).
     *
     * <p>The iterator is <em>not</em> closed here: every caller owns it in try-with-resources, and
     * that close is what cancels the job and lets the drain thread (a daemon, so it cannot park the
     * JVM either) unwind after a shortfall.
     *
     * <p>A job failure surfaces here as it did from a bare {@code hasNext()} loop: if the job ends
     * exceptionally before enough elements have arrived, the failure is rethrown as-is.
     *
     * @param iterator the running job's output
     * @param count how many distinct elements to wait for
     * @param timeout how long to wait for them
     * @param distinguisher what makes an element distinct — normally the part a redelivery repeats
     */
    public static <T> List<T> drainDistinct(
            CloseableIterator<T> iterator,
            int count,
            Duration timeout,
            Function<? super T, ?> distinguisher)
            throws Exception {
        BlockingQueue<Object> arrived = new LinkedBlockingQueue<>();
        Object endOfJob = new Object();
        AtomicReference<Throwable> jobFailure = new AtomicReference<>();
        Thread drain =
                new Thread(
                        () -> {
                            try {
                                while (iterator.hasNext()) {
                                    arrived.put(iterator.next());
                                }
                            } catch (Throwable t) {
                                jobFailure.set(t);
                            } finally {
                                arrived.add(endOfJob);
                            }
                        },
                        "it-collect-drain");
        drain.setDaemon(true);
        drain.start();

        Map<Object, T> elements = new LinkedHashMap<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (elements.size() < count) {
            // A non-positive remainder still polls: whatever arrived before the deadline is
            // returned, only the waiting stops.
            long remaining = deadline - System.nanoTime();
            Object element = arrived.poll(Math.max(remaining, 0), TimeUnit.NANOSECONDS);
            if (element == null || element == endOfJob) {
                break;
            }
            @SuppressWarnings("unchecked")
            T value = (T) element;
            elements.putIfAbsent(distinguisher.apply(value), value);
        }
        Throwable failure = jobFailure.get();
        if (elements.size() < count && failure != null) {
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw (Error) failure;
        }
        return new ArrayList<>(elements.values());
    }

    private Drains() {}
}
