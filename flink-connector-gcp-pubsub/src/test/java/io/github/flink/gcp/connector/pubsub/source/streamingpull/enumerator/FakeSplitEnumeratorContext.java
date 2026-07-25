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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/** In-memory {@link SplitEnumeratorContext} recording assignments, for enumerator tests. */
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

    /**
     * Returns the splits assigned to the given subtask since the last {@link #forgetAssignments}.
     */
    List<SubscriptionSplit> assignedSplits(int subtaskId) {
        return assignments.getOrDefault(subtaskId, Collections.emptyList());
    }

    /** Drops the recorded assignments, so a later one can be asserted on its own. */
    void forgetAssignments() {
        assignments.clear();
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

    // The enumerator is entirely synchronous today; these throw rather than silently accepting
    // work, so the first asynchronous step added to it has to revisit this fake.

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        throw new UnsupportedOperationException("The Pub/Sub enumerator makes no async calls.");
    }

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis) {
        throw new UnsupportedOperationException("The Pub/Sub enumerator makes no async calls.");
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        throw new UnsupportedOperationException(
                "The Pub/Sub enumerator runs on the coordinator thread already.");
    }
}
