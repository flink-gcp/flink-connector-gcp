/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;

import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * In-memory {@link SplitEnumeratorContext} for enumerator tests.
 *
 * <p>Unlike the Pub/Sub source's, this one records assignments and no-more-splits signals in
 * <em>one ordered list</em> as well as per subtask: with pull assignment the order matters — a
 * subtask told there are no more splits and then handed one is the reference implementation's
 * data-loss bug expressed as a sequence.
 */
final class FakeSplitEnumeratorContext implements SplitEnumeratorContext<BigQueryReadStreamSplit> {

    private final int parallelism;
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<BigQueryReadStreamSplit>> assignments = new HashMap<>();
    private final Set<Integer> readersToldNoMoreSplits = new LinkedHashSet<>();
    private final List<String> events = new ArrayList<>();
    private final Deque<Runnable> asyncCalls = new ArrayDeque<>();

    private final MetricListener metricListener = new MetricListener();

    private final SplitEnumeratorMetricGroup metricGroup =
            new InternalSplitEnumeratorMetricGroup(metricListener.getMetricGroup());

    FakeSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    /** Registers a reader, as the coordinator does before calling {@code addReader}. */
    void registerReader(int subtaskId) {
        registeredReaders.put(subtaskId, new ReaderInfo(subtaskId, "localhost"));
    }

    /** Drops a reader, as the coordinator does when its last attempt goes away. */
    void unregisterReader(int subtaskId) {
        registeredReaders.remove(subtaskId);
    }

    /** Returns a copy: a caller that fed this straight back in would otherwise mutate it. */
    List<BigQueryReadStreamSplit> assignedSplits(int subtaskId) {
        return new ArrayList<>(assignments.getOrDefault(subtaskId, Collections.emptyList()));
    }

    Set<Integer> readersToldNoMoreSplits() {
        return new LinkedHashSet<>(readersToldNoMoreSplits);
    }

    /** Returns every assignment and signal in the order it happened, as {@code "verb:subtask"}. */
    List<String> events() {
        return new ArrayList<>(events);
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException("The BigQuery enumerator sends no source events.");
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        return registeredReaders;
    }

    @Override
    public void assignSplits(SplitsAssignment<BigQueryReadStreamSplit> newSplitAssignments) {
        newSplitAssignments
                .assignment()
                .forEach(
                        (subtask, splits) -> {
                            checkRegistered(subtask, "assign splits to");
                            assignments
                                    .computeIfAbsent(subtask, key -> new ArrayList<>())
                                    .addAll(splits);
                            events.add("assign:" + subtask);
                        });
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        checkRegistered(subtask, "signal no more splits to");
        readersToldNoMoreSplits.add(subtask);
        events.add("noMoreSplits:" + subtask);
    }

    private void checkRegistered(int subtaskId, String action) {
        if (!registeredReaders.containsKey(subtaskId)) {
            throw new IllegalArgumentException(
                    "Cannot "
                            + action
                            + " subtask "
                            + subtaskId
                            + " because it is not registered.");
        }
    }

    /**
     * Records the call instead of running it, so a test decides when session creation completes.
     * Mirrors {@code ExecutorNotifier}: the callable runs off the coordinator thread and the
     * handler then runs on it with exactly one of (result, error) set.
     */
    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        asyncCalls.add(
                () -> {
                    T result = null;
                    Throwable error = null;
                    try {
                        result = callable.call();
                    } catch (Throwable t) {
                        error = t;
                    }
                    handler.accept(result, error);
                });
    }

    /** Runs every async call recorded so far, including any a handler enqueues. */
    void runAsyncCalls() {
        while (!asyncCalls.isEmpty()) {
            asyncCalls.poll().run();
        }
    }

    // Session creation is the enumerator's only asynchronous step, and it is one-shot; these throw
    // rather than silently accepting work, so the next asynchronous step added has to revisit this
    // fake.

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis) {
        throw new UnsupportedOperationException(
                "The BigQuery enumerator makes no periodic async calls.");
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        throw new UnsupportedOperationException(
                "The BigQuery enumerator runs on the coordinator thread already.");
    }

    /**
     * Returns a counter the enumerator registered. The extra path element is Flink's own: {@link
     * InternalSplitEnumeratorMetricGroup} registers under an {@code "enumerator"} subgroup.
     */
    long counter(String name) {
        return metricListener
                .getCounter("enumerator", name)
                .orElseThrow(() -> new AssertionError("No counter named " + name + " registered."))
                .getCount();
    }

    /** Returns the value of a gauge the enumerator registered. */
    <T> T gauge(String name) {
        return metricListener
                .<T>getGauge("enumerator", name)
                .orElseThrow(() -> new AssertionError("No gauge named " + name + " registered."))
                .getValue();
    }
}
