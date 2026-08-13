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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

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
 * In-memory {@link SplitEnumeratorContext} for the tests of a <b>pull-assignment</b> enumerator —
 * one whose readers ask for work through {@code handleSplitRequest}.
 *
 * <p>Assignments and no-more-splits signals are recorded in <em>one ordered list</em> as well as
 * per subtask, because with pull assignment the order is the thing that can go wrong: a subtask
 * told there are no more splits and then handed one is a lost-split bug expressed as a sequence.
 *
 * <p>A <b>push</b>-assigned source's enumerator wants its own fake rather than this one, and the
 * Pub/Sub source keeps one for that reason: its coordinator-facing methods throw where these
 * record, which is what makes a test fail if that source ever starts requesting splits ({@code
 * docs/adr/0050}).
 *
 * <p>The unsupported operations throw rather than silently accepting work, so the first enumerator
 * that needs one has to come here and decide what it should do. Reader-directed source events are
 * recorded because restore validation may have to release or replace reader-owned splits.
 *
 * @param <SplitT> the split type the enumerator assigns
 */
@Internal
public final class FakeSplitEnumeratorContext<SplitT extends SourceSplit>
        implements SplitEnumeratorContext<SplitT> {

    private final int parallelism;
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<SplitT>> assignments = new HashMap<>();
    private final Set<Integer> readersToldNoMoreSplits = new LinkedHashSet<>();
    private final List<String> events = new ArrayList<>();
    private final Map<Integer, List<SourceEvent>> sourceEvents = new HashMap<>();
    private final Deque<Runnable> asyncCalls = new ArrayDeque<>();
    private final List<Runnable> periodicAsyncCalls = new ArrayList<>();

    private final MetricListener metricListener = new MetricListener();

    private final Map<String, String> metricVariables = new HashMap<>();

    private final SplitEnumeratorMetricGroup metricGroup =
            new VariablesCarryingGroup(
                    new InternalSplitEnumeratorMetricGroup(metricListener.getMetricGroup()),
                    metricVariables);

    public FakeSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    /** Registers a reader, as the coordinator does before calling {@code addReader}. */
    public void registerReader(int subtaskId) {
        registeredReaders.put(subtaskId, new ReaderInfo(subtaskId, "localhost"));
    }

    /** Drops a reader, as the coordinator does when its last attempt goes away. */
    public void unregisterReader(int subtaskId) {
        registeredReaders.remove(subtaskId);
    }

    /** Returns a copy: a caller that fed this straight back in would otherwise mutate it. */
    public List<SplitT> assignedSplits(int subtaskId) {
        return new ArrayList<>(assignments.getOrDefault(subtaskId, Collections.emptyList()));
    }

    public Set<Integer> readersToldNoMoreSplits() {
        return new LinkedHashSet<>(readersToldNoMoreSplits);
    }

    /** Returns every assignment and signal in the order it happened, as {@code "verb:subtask"}. */
    public List<String> events() {
        return new ArrayList<>(events);
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return metricGroup;
    }

    /**
     * Puts a variable into what the metric group's {@code getAllVariables()} answers.
     *
     * <p>By default the map is empty — {@code MetricListener}'s root group has no parent, so
     * nothing populates the variables Flink's own hierarchy would carry ({@code <job_name>} and its
     * siblings). A test of code reading them injects what its job would have had.
     *
     * @param key the variable key, in Flink's bracketed spelling, for example {@code <job_name>}
     * @param value the value
     */
    public void putMetricVariable(String key, String value) {
        metricVariables.put(key, value);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        checkRegistered(subtaskId, "send a source event to");
        sourceEvents.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).add(event);
    }

    /** Returns the coordinator events sent to one source reader, in order. */
    public List<SourceEvent> sourceEvents(int subtaskId) {
        return new ArrayList<>(sourceEvents.getOrDefault(subtaskId, Collections.emptyList()));
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
    public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
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
     * Records the call instead of running it, so a test decides when the enumerator's planning step
     * completes. Mirrors {@code ExecutorNotifier}: the callable runs off the coordinator thread and
     * the handler then runs on it with exactly one of (result, error) set.
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
    public void runAsyncCalls() {
        while (!asyncCalls.isEmpty()) {
            asyncCalls.poll().run();
        }
    }

    // Planning is the one asynchronous step both current enumerators take, and it is one-shot.

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis) {
        periodicAsyncCalls.add(asyncCall(callable, handler));
    }

    /** Runs each registered periodic async callback once, without waiting for wall-clock time. */
    public void runPeriodicAsyncCalls() {
        for (Runnable call : new ArrayList<>(periodicAsyncCalls)) {
            call.run();
        }
    }

    private static <T> Runnable asyncCall(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        return () -> {
            T result = null;
            Throwable error = null;
            try {
                result = callable.call();
            } catch (Throwable t) {
                error = t;
            }
            handler.accept(result, error);
        };
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        throw new UnsupportedOperationException(
                "A pull-assignment enumerator runs on the coordinator thread already.");
    }

    /**
     * Returns a counter the enumerator registered. The extra path element is Flink's own: {@link
     * InternalSplitEnumeratorMetricGroup} registers under an {@code "enumerator"} subgroup.
     */
    public long counter(String name) {
        return metricListener
                .getCounter("enumerator", name)
                .orElseThrow(() -> new AssertionError("No counter named " + name + " registered."))
                .getCount();
    }

    /** Returns the value of a gauge the enumerator registered. */
    public <T> T gauge(String name) {
        return metricListener
                .<T>getGauge("enumerator", name)
                .orElseThrow(() -> new AssertionError("No gauge named " + name + " registered."))
                .getValue();
    }

    /**
     * Carries the injected variables over the real {@code InternalSplitEnumeratorMetricGroup},
     * which cannot be handed them directly: its constructor calls {@code addGroup} on whatever it
     * wraps, so a variables-answering wrapper underneath it is unwrapped before the proxying
     * starts. Everything else — the counters and gauges a test reads back — still forwards to the
     * {@code MetricListener}-backed group.
     */
    private static final class VariablesCarryingGroup
            extends ProxyMetricGroup<SplitEnumeratorMetricGroup>
            implements SplitEnumeratorMetricGroup {

        private final Map<String, String> variables;

        VariablesCarryingGroup(SplitEnumeratorMetricGroup parent, Map<String, String> variables) {
            super(parent);
            this.variables = variables;
        }

        @Override
        public Map<String, String> getAllVariables() {
            return new HashMap<>(variables);
        }

        @Override
        public <G extends Gauge<Long>> G setUnassignedSplitsGauge(G unassignedSplitsGauge) {
            return parentMetricGroup.setUnassignedSplitsGauge(unassignedSplitsGauge);
        }
    }
}
