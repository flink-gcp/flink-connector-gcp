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

import java.util.IdentityHashMap;
import java.util.Map;

/** One live record per active commit invocation; completed batches retain only scalar totals. */
final class Stage2CommitProgress {
    private final Map<Object, Batch> active = new IdentityHashMap<>();
    private long startedBatches;
    private long finishedBatches;
    private long failedBatches;
    private long largestBatchEntries;
    private long maxFinishedBatchNanos;

    synchronized void started(Object committer, int entries, long now) {
        if (active.containsKey(committer)) {
            throw new IllegalStateException("Commit invocation already active");
        }
        active.put(committer, new Batch(entries, now));
        startedBatches++;
        largestBatchEntries = Math.max(largestBatchEntries, entries);
    }

    synchronized void finished(Object committer, boolean successful, long now) {
        Batch batch = active.remove(committer);
        if (batch == null) {
            throw new IllegalStateException("Commit invocation is not active");
        }
        finishedBatches++;
        if (!successful) {
            failedBatches++;
        }
        maxFinishedBatchNanos = Math.max(maxFinishedBatchNanos, now - batch.started);
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
        final int entries;
        final long started;

        Batch(int entries, long started) {
            this.entries = entries;
            this.started = started;
        }
    }
}
