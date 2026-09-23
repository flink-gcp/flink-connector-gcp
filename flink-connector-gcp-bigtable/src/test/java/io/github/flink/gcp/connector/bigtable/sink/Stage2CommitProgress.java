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

package io.github.flink.gcp.connector.bigtable.sink;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * One live record per active commit invocation, and a bounded record of each finished one, so a
 * committer's drain rate is computed per batch rather than from the run's extremes.
 */
final class Stage2CommitProgress {
    /** Enough for a one-second interval over an hour at parallelism 16. */
    static final int MAX_FINISHED_RECORDS = 65_536;

    private final Map<Object, Batch> active = new IdentityHashMap<>();
    private final Map<Object, Integer> committerIds = new IdentityHashMap<>();
    private final List<String> finished = new ArrayList<>();
    private long droppedRecords;
    private long startedBatches;
    private long finishedBatches;
    private long failedBatches;
    private long largestBatchEntries;
    private long maxFinishedBatchNanos;

    synchronized void started(Object committer, int entries, long now) {
        started(committer, entries, now, -1);
    }

    /**
     * Records an invocation's start; {@code threadCpuNanos} is the committing thread's CPU time
     * then, or negative where the JVM cannot measure it.
     */
    synchronized void started(Object committer, int entries, long now, long threadCpuNanos) {
        if (active.containsKey(committer)) {
            throw new IllegalStateException("Commit invocation already active");
        }
        Integer id = committerIds.computeIfAbsent(committer, ignored -> committerIds.size());
        active.put(committer, new Batch(id, entries, now, threadCpuNanos));
        startedBatches++;
        largestBatchEntries = Math.max(largestBatchEntries, entries);
    }

    synchronized void finished(Object committer, boolean successful, long now) {
        finished(committer, successful, now, -1);
    }

    /** Records an invocation's end with the committing thread's CPU time then, or negative. */
    synchronized void finished(
            Object committer, boolean successful, long now, long threadCpuNanos) {
        Batch batch = active.remove(committer);
        if (batch == null) {
            throw new IllegalStateException("Commit invocation is not active");
        }
        finishedBatches++;
        if (!successful) {
            failedBatches++;
        }
        maxFinishedBatchNanos = Math.max(maxFinishedBatchNanos, now - batch.started);
        if (finished.size() < MAX_FINISHED_RECORDS) {
            finished.add(
                    "{\"committer\":"
                            + batch.committer
                            + ",\"entries\":"
                            + batch.entries
                            + ",\"startedNanos\":"
                            + batch.started
                            + ",\"durationNanos\":"
                            + (now - batch.started)
                            + ",\"threadCpuNanos\":"
                            + (batch.threadCpuNanos < 0 || threadCpuNanos < 0
                                    ? -1
                                    : threadCpuNanos - batch.threadCpuNanos)
                            + ",\"successful\":"
                            + successful
                            + "}");
        } else {
            droppedRecords++;
        }
    }

    /** One JSON object per finished invocation, in completion order. */
    synchronized List<String> finishedRecords() {
        return List.copyOf(finished);
    }

    synchronized long droppedRecords() {
        return droppedRecords;
    }

    synchronized String sample(long now) {
        long entries = 0;
        long oldest = 0;
        for (Batch batch : active.values()) {
            entries += batch.entries;
            oldest = Math.max(oldest, now - batch.started);
        }
        return "{\"activeBatches\":"
                + active.size()
                + ",\"activeBatchEntries\":"
                + entries
                + ",\"oldestBatchNanos\":"
                + oldest
                + ",\"largestBatchEntries\":"
                + largestBatchEntries
                + ",\"startedBatches\":"
                + startedBatches
                + ",\"finishedBatches\":"
                + finishedBatches
                + ",\"failedBatches\":"
                + failedBatches
                + ",\"maxFinishedBatchNanos\":"
                + maxFinishedBatchNanos
                + "}";
    }

    private static final class Batch {
        final int committer;
        final int entries;
        final long started;
        final long threadCpuNanos;

        Batch(int committer, int entries, long started, long threadCpuNanos) {
            this.committer = committer;
            this.entries = entries;
            this.threadCpuNanos = threadCpuNanos;
            this.started = started;
        }
    }
}
