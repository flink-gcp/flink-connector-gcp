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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * In-memory {@link SplitEnumeratorContext} recording assignments, for enumerator tests. Async calls
 * run inline so tests stay deterministic.
 */
final class FakeSplitEnumeratorContext implements SplitEnumeratorContext<SubscriptionSplit> {

    private final int parallelism;
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<SubscriptionSplit>> assignments = new HashMap<>();
    private final Set<Integer> readersToldNoMoreSplits = new LinkedHashSet<>();

    FakeSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    /** Registers a reader, as the coordinator does before calling {@code addReader}. */
    void registerReader(int subtaskId) {
        registeredReaders.put(subtaskId, new ReaderInfo(subtaskId, "localhost"));
    }

    /** Returns the splits assigned to the given subtask so far, across all assignment calls. */
    List<SubscriptionSplit> assignedSplits(int subtaskId) {
        return assignments.getOrDefault(subtaskId, new ArrayList<>());
    }

    Set<Integer> readersToldNoMoreSplits() {
        return readersToldNoMoreSplits;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return null;
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException("The Pub/Sub enumerator sends no source events.");
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
    public void assignSplits(SplitsAssignment<SubscriptionSplit> newSplitAssignments) {
        newSplitAssignments
                .assignment()
                .forEach(
                        (subtask, splits) ->
                                assignments
                                        .computeIfAbsent(subtask, key -> new ArrayList<>())
                                        .addAll(splits));
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        readersToldNoMoreSplits.add(subtask);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        runInline(callable, handler);
    }

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis) {
        runInline(callable, handler);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        runnable.run();
    }

    private static <T> void runInline(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        try {
            handler.accept(callable.call(), null);
        } catch (Throwable t) {
            handler.accept(null, t);
        }
    }
}
