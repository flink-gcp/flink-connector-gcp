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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One live record per active commit invocation, and a bounded record of each finished one, so a
 * committer's drain rate is computed per batch rather than from the run's extremes.
 */
final class Stage2CommitProgress {
    /** Enough for a one-second interval over an hour at parallelism 16. */
    static final int MAX_FINISHED_RECORDS = 65_536;

    private final Map<Object, Invocation> active = new IdentityHashMap<>();
    private final Map<Object, Integer> committerIds = new IdentityHashMap<>();
    private final List<String> finished = new ArrayList<>();
    // The committer sends and waits on the thread that opened the invocation.
    private final ThreadLocal<Invocation> current = new ThreadLocal<>();
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
        Invocation batch = new Invocation(id, entries, now, threadCpuNanos, threadWaitedMillis());
        active.put(committer, batch);
        current.set(batch);
        startedBatches++;
        largestBatchEntries = Math.max(largestBatchEntries, entries);
    }

    synchronized void finished(Object committer, boolean successful, long now) {
        finished(committer, successful, now, -1);
    }

    /** Records an invocation's end with the committing thread's CPU time then, or negative. */
    synchronized void finished(
            Object committer, boolean successful, long now, long threadCpuNanos) {
        Invocation batch = active.remove(committer);
        if (batch == null) {
            throw new IllegalStateException("Commit invocation is not active");
        }
        if (current.get() == batch) {
            current.remove();
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
                            + ",\"sends\":"
                            + batch.sends
                            + ",\"sendPhaseNanos\":"
                            + ((batch.sends == 0 ? now : batch.lastSend) - batch.started)
                            + ",\"drainPhaseNanos\":"
                            + (batch.sends == 0 ? 0 : now - batch.lastSend)
                            + ",\"waitedMillis\":"
                            + waitedDelta(batch.waitedMillis, threadWaitedMillis())
                            + ",\"boundWaitNanos\":"
                            + batch.boundWaitNanos
                            + ",\"drainWaitNanos\":"
                            + batch.waitSinceSend
                            + ",\"boundBlockedWaits\":"
                            + batch.boundBlocked
                            + ",\"drainBlockedWaits\":"
                            + batch.blockedSinceSend
                            + ",\"completedBehindAtBound\":"
                            + batch.boundBehind
                            + ",\"successful\":"
                            + successful
                            + "}");
        } else {
            droppedRecords++;
        }
    }

    /**
     * Records a conditional write the calling thread's invocation sends, and returns that
     * invocation for the request to report its completion and the committer's wait on it; {@code
     * null} outside an invocation. Every wait before a send was a wait at the in-flight bound.
     */
    Invocation sent() {
        Invocation batch = current.get();
        if (batch == null) {
            return null;
        }
        batch.sends++;
        batch.lastSend = System.nanoTime();
        batch.boundWaitNanos += batch.waitSinceSend;
        batch.boundBlocked += batch.blockedSinceSend;
        batch.boundBehind += batch.behindSinceSend;
        batch.waitSinceSend = 0;
        batch.blockedSinceSend = 0;
        batch.behindSinceSend = 0;
        batch.outstanding.incrementAndGet();
        return batch;
    }

    /** Turns on the JVM's waiting-time accounting, which {@link #threadWaitedMillis()} reads. */
    static void enableWaitAccounting() {
        var bean = java.lang.management.ManagementFactory.getThreadMXBean();
        if (bean.isThreadContentionMonitoringSupported()) {
            bean.setThreadContentionMonitoringEnabled(true);
        }
    }

    /**
     * The calling thread's accumulated time in a waiting state, which covers a park on a queue as
     * well as a wait on a future, or -1 unless {@link #enableWaitAccounting()} turned it on.
     */
    static long threadWaitedMillis() {
        var bean = java.lang.management.ManagementFactory.getThreadMXBean();
        if (!bean.isThreadContentionMonitoringSupported()
                || !bean.isThreadContentionMonitoringEnabled()) {
            return -1;
        }
        var info = bean.getThreadInfo(Thread.currentThread().getId());
        return info == null ? -1 : info.getWaitedTime();
    }

    private static long waitedDelta(long before, long after) {
        return before < 0 || after < 0 ? -1 : after - before;
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
        for (Invocation batch : active.values()) {
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

    /** One commit invocation, as its requests and its committing thread report on it. */
    static final class Invocation {
        private final int committer;
        private final int entries;
        private final long started;
        private final long threadCpuNanos;
        private final long waitedMillis;
        private final AtomicInteger outstanding = new AtomicInteger();
        // Confined to the invocation's thread until finished() reads them on that same thread.
        private long lastSend;
        private long sends;
        private long reaped;
        private long waitSinceSend;
        private long blockedSinceSend;
        private long behindSinceSend;
        private long boundWaitNanos;
        private long boundBlocked;
        private long boundBehind;

        private Invocation(
                int committer, int entries, long started, long threadCpuNanos, long waitedMillis) {
            this.committer = committer;
            this.entries = entries;
            this.started = started;
            this.threadCpuNanos = threadCpuNanos;
            this.waitedMillis = waitedMillis;
            this.lastSend = started;
        }

        /** Called by a request of this invocation once its completion is published. */
        void completed() {
            outstanding.decrementAndGet();
        }

        /**
         * The requests already complete behind the oldest one as the committing thread starts
         * waiting on it: zero when the oldest is complete itself, because the wait then does not
         * block.
         */
        int completedBehindHead(boolean headDone) {
            if (headDone) {
                return 0;
            }
            return Math.max(0, (int) (sends - reaped) - outstanding.get());
        }

        /**
         * Records one wait on the oldest request; what follows it decides whether it was at the
         * bound.
         */
        void waited(long nanos, boolean blocked, int completedBehind) {
            reaped++;
            waitSinceSend += nanos;
            if (blocked) {
                blockedSinceSend++;
                behindSinceSend += completedBehind;
            }
        }
    }
}
