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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.bigtable.v2.ReadChangeStreamResponse;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.StreamContinuationToken;
import com.google.bigtable.v2.StreamPartition;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plays a scripted change stream per partition, for jobs a test restores or fails over.
 *
 * <p>A script is registered under a run id, so the opener itself stays serializable. Each partition
 * has a label, a row range and steps; a record's continuation token names the step after it, so a
 * split restored from a checkpoint resumes exactly where its last emitted record left off. A
 * mutation's row key is {@code label#n}. An await step parks the partition's stream until the test
 * opens that latch, and records that it was reached.
 */
public final class RestoreScriptedChangeStreamOpener implements ChangeStreamOpener {

    private static final long serialVersionUID = 1L;

    /** The commit time of the first scripted record; later ones are 1 ms apart. */
    public static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");

    private static final Map<String, Run> RUNS = new ConcurrentHashMap<>();

    private final String runId;

    public RestoreScriptedChangeStreamOpener(String runId) {
        this.runId = runId;
    }

    /** Registers a new script under a run id. */
    public static Run register(String runId) {
        Run run = new Run();
        RUNS.put(runId, run);
        return run;
    }

    @Override
    public void open(
            TableDestination table,
            ChangeStreamPartitionSplit split,
            @Nullable Instant boundedTimestamp,
            ResponseObserver<ChangeStreamRecord> observer) {
        Run run = RUNS.get(runId);
        String label;
        int next;
        if (split.getContinuationTokens().isEmpty()) {
            label = run.labelOf(split.getPartition());
            next = 0;
        } else {
            String token = split.getContinuationTokens().get(0).getToken();
            int at = token.lastIndexOf('@');
            label = token.substring(0, at);
            next = Integer.parseInt(token.substring(at + 1));
        }
        run.log.add("open " + label + "@" + next);
        run.opens.computeIfAbsent(label, ignored -> new AtomicInteger()).incrementAndGet();
        Stream stream = new Stream(run, label, run.partitions.get(label), next, observer);
        // Started first, so a cancel from onStart interrupts a running thread, as gax delivers a
        // cancellation; the thread delivers nothing before onStart's request(1).
        stream.start();
        observer.onStart(stream);
    }

    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {}

    @Override
    public void close() {}

    /** One test's partitions, latches and observations. */
    public static final class Run {

        private final Map<String, Partition> partitions = new ConcurrentHashMap<>();
        private final Set<String> openLatches = ConcurrentHashMap.newKeySet();

        /** Each {@code label:latch} an await step reached. */
        public final Set<String> reached = ConcurrentHashMap.newKeySet();

        /** What the streams did, in order. */
        public final List<String> log = new CopyOnWriteArrayList<>();

        private final Map<String, AtomicInteger> opens = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> closes = new ConcurrentHashMap<>();

        private Run() {}

        /** Adds a partition with its row range and steps. */
        public Run partition(String label, ByteStringRange range, Step... steps) {
            partitions.put(label, new Partition(label, range, Arrays.asList(steps)));
            return this;
        }

        /** Lets every stream waiting on, or later reaching, the latch continue. */
        public void open(String latch) {
            openLatches.add(latch);
            synchronized (this) {
                notifyAll();
            }
        }

        /** How many times a partition's stream was opened. */
        public int opens(String label) {
            AtomicInteger count = opens.get(label);
            return count == null ? 0 : count.get();
        }

        /**
         * How many times the reader took a partition's {@code CloseStream}: it cancels the stream
         * on the task thread once it has, after it sent the transition.
         */
        public int closes(String label) {
            AtomicInteger count = closes.get(label);
            return count == null ? 0 : count.get();
        }

        private String labelOf(ByteStringRange range) {
            for (Partition partition : partitions.values()) {
                if (partition.range.equals(RowRanges.copyOf(range))) {
                    return partition.label;
                }
            }
            throw new IllegalStateException("No scripted partition for " + range);
        }

        private synchronized void awaitLatch(String latch) throws InterruptedException {
            while (!openLatches.contains(latch)) {
                wait();
            }
        }
    }

    /** One scripted step of a partition's stream. */
    public static final class Step {

        private final String kind;
        private final int sequence;
        private final String latch;
        private final List<String> successors;

        private Step(String kind, int sequence, String latch, List<String> successors) {
            this.kind = kind;
            this.sequence = sequence;
            this.latch = latch;
            this.successors = successors;
        }

        /** A mutation whose row key is {@code label#sequence}. */
        public static Step data(int sequence) {
            return new Step("data", sequence, null, Collections.emptyList());
        }

        /** Waits for a latch the test opens. */
        public static Step await(String latch) {
            return new Step("await", 0, latch, Collections.emptyList());
        }

        /** A {@code CloseStream} naming the successors, or an end with none. */
        public static Step close(String... successors) {
            return new Step("close", 0, null, Arrays.asList(successors));
        }
    }

    private static final class Partition {
        private final String label;
        private final ByteStringRange range;
        private final List<Step> steps;

        private Partition(String label, ByteStringRange range, List<Step> steps) {
            this.label = label;
            this.range = RowRanges.copyOf(range);
            this.steps = steps;
        }
    }

    /** Delivers one partition's steps on its own thread, one per {@code request(1)}. */
    private static final class Stream implements StreamController, Runnable {

        private final Run run;
        private final String label;
        private final Partition partition;
        private final ResponseObserver<ChangeStreamRecord> observer;
        private final Semaphore requests = new Semaphore(0);
        private final Thread thread;
        private volatile boolean cancelled;
        private volatile boolean closeDelivered;
        private int next;

        private Stream(
                Run run,
                String label,
                Partition partition,
                int next,
                ResponseObserver<ChangeStreamRecord> observer) {
            this.run = run;
            this.label = label;
            this.partition = partition;
            this.next = next;
            this.observer = observer;
            thread = new Thread(this, "scripted-change-stream-" + label);
            thread.setDaemon(true);
        }

        private void start() {
            thread.start();
        }

        @Override
        public void cancel() {
            if (closeDelivered && !cancelled) {
                run.log.add("close " + label);
                run.closes.computeIfAbsent(label, ignored -> new AtomicInteger()).incrementAndGet();
            }
            cancelled = true;
            thread.interrupt();
        }

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {
            requests.release(count);
        }

        @Override
        public void run() {
            try {
                while (next < partition.steps.size()) {
                    Step step = partition.steps.get(next);
                    next++;
                    if (step.kind.equals("await")) {
                        run.reached.add(label + ":" + step.latch);
                        run.awaitLatch(step.latch);
                        continue;
                    }
                    requests.acquire();
                    if (cancelled) {
                        break;
                    }
                    if (step.kind.equals("data")) {
                        observer.onResponse(mutation(step.sequence));
                    } else {
                        closeDelivered = true;
                        observer.onResponse(close(step.successors));
                        // The reader cancels the stream once it has taken the CloseStream.
                        while (!cancelled) {
                            Thread.sleep(5);
                        }
                        break;
                    }
                }
            } catch (InterruptedException e) {
                // Cancelled.
            }
            observer.onError(new CancellationException("scripted"));
        }

        private ChangeStreamRecord mutation(int sequence) {
            ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                    new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
            builder.startUserMutation(
                    ByteString.copyFromUtf8(label + "#" + sequence),
                    "cluster",
                    BASE.plusMillis(sequence),
                    0);
            builder.deleteFamily("family");
            return builder.finishChangeStreamMutation(label + "@" + next, BASE);
        }

        private ChangeStreamRecord close(List<String> successors) {
            ReadChangeStreamResponse.CloseStream.Builder close =
                    ReadChangeStreamResponse.CloseStream.newBuilder();
            if (successors.isEmpty()) {
                close.setStatus(com.google.rpc.Status.newBuilder().setCode(0));
            } else {
                close.setStatus(
                        com.google.rpc.Status.newBuilder()
                                .setCode(com.google.rpc.Code.OUT_OF_RANGE_VALUE));
                for (String successor : successors) {
                    StreamPartition range = partition(run.partitions.get(successor).range);
                    close.addContinuationTokens(
                            StreamContinuationToken.newBuilder()
                                    .setPartition(range)
                                    .setToken(successor + "@0"));
                    close.addNewPartitions(range);
                }
            }
            return new DefaultChangeStreamRecordAdapter()
                    .createChangeStreamRecordBuilder()
                    .onCloseStream(close.build());
        }

        private static StreamPartition partition(ByteStringRange range) {
            RowRange.Builder rowRange = RowRange.newBuilder();
            if (range.getStartBound() == BoundType.CLOSED) {
                rowRange.setStartKeyClosed(range.getStart());
            } else if (range.getStartBound() == BoundType.OPEN) {
                rowRange.setStartKeyOpen(range.getStart());
            }
            if (range.getEndBound() == BoundType.OPEN) {
                rowRange.setEndKeyOpen(range.getEnd());
            } else if (range.getEndBound() == BoundType.CLOSED) {
                rowRange.setEndKeyClosed(range.getEnd());
            }
            return StreamPartition.newBuilder().setRowRange(rowRange).build();
        }
    }
}
