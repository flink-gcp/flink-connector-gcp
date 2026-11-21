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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A per-run script of partitions whose steps are data records, child-partitions records, latches
 * and a finish, for tests that restore a job from a checkpoint taken at a chosen step.
 *
 * <p>A query reopened at a position skips every step before it and replays the steps at it, as
 * Spanner's inclusive start does. The script lives in a static registry keyed by run id, because
 * Flink serializes the factory into the job and each attempt creates its own client.
 */
public final class RestoreScriptedQueryClientFactory
        implements SpannerChangeStreamQueryClientFactory {

    private static final long serialVersionUID = 1L;

    public static final String INITIAL = "<initial>";

    private static final Map<String, Run> RUNS = new ConcurrentHashMap<>();

    private final String run;

    public RestoreScriptedQueryClientFactory(String run) {
        this.run = run;
    }

    public static Run register(String run) {
        Run created = new Run();
        RUNS.put(run, created);
        return created;
    }

    @Override
    public SpannerChangeStreamQueryClient create(@Nullable ClientMetrics clientMetrics) {
        return new Client(RUNS.get(run));
    }

    /** The script and what the readers did with it. */
    public static final class Run {
        final Map<String, List<Step>> scripts = new ConcurrentHashMap<>();
        final Map<String, Long> pacing = new ConcurrentHashMap<>();
        public final Set<String> openLatches = ConcurrentHashMap.newKeySet();
        public final Set<String> reached = ConcurrentHashMap.newKeySet();
        public final Map<String, AtomicInteger> opens = new ConcurrentHashMap<>();
        public final Map<String, AtomicInteger> closes = new ConcurrentHashMap<>();
        public final List<String> log = Collections.synchronizedList(new ArrayList<>());

        public Run partition(String token, long pacingMillis, Step... steps) {
            scripts.put(token, List.of(steps));
            pacing.put(token, pacingMillis);
            return this;
        }

        public void open(String latch) {
            openLatches.add(latch);
        }

        public int opens(String token) {
            AtomicInteger count = opens.get(token);
            return count == null ? 0 : count.get();
        }

        public int closes(String token) {
            AtomicInteger count = closes.get(token);
            return count == null ? 0 : count.get();
        }
    }

    /** One scripted step. */
    public static final class Step {
        final String kind;
        final int offsetMillis;
        final String latch;
        final List<String> children;

        private Step(String kind, int offsetMillis, String latch, List<String> children) {
            this.kind = kind;
            this.offsetMillis = offsetMillis;
            this.latch = latch;
            this.children = children;
        }

        public static Step data(int offsetMillis) {
            return new Step("data", offsetMillis, null, null);
        }

        public static Step children(int offsetMillis, String... children) {
            return new Step("children", offsetMillis, null, List.of(children));
        }

        public static Step await(String latch) {
            return new Step("await", -1, latch, null);
        }

        public static Step finish() {
            return new Step("finish", -1, null, null);
        }
    }

    private static final class Client implements SpannerChangeStreamQueryClient {

        private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();

        private final Run run;
        private final ScheduledExecutorService callbacks =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "spanner-restore-script-"
                                                    + THREAD_NUMBER.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });

        private Client(Run run) {
            this.run = run;
        }

        @Override
        public QueryHandle open(
                ChangeStreamPartitionSplit split, SpannerChangeStreamQueryListener listener) {
            String token = split.getPartitionToken() == null ? INITIAL : split.getPartitionToken();
            run.opens.computeIfAbsent(token, ignored -> new AtomicInteger()).incrementAndGet();
            run.log.add("open " + token + " at " + split.getCurrentPosition());
            Query query = new Query(run, token, split, listener, callbacks);
            callbacks.schedule(query::next, 10, TimeUnit.MILLISECONDS);
            return query;
        }

        @Override
        public void close() {
            callbacks.shutdownNow();
        }
    }

    private static final class Query implements SpannerChangeStreamQueryClient.QueryHandle {

        private final Run run;
        private final String token;
        private final ChangeStreamPartitionSplit split;
        private final SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener;
        private final ScheduledExecutorService callbacks;
        private final List<Step> steps;
        private final long pacing;
        private int step;
        private boolean closed;

        private Query(
                Run run,
                String token,
                ChangeStreamPartitionSplit split,
                SpannerChangeStreamQueryClient.SpannerChangeStreamQueryListener listener,
                ScheduledExecutorService callbacks) {
            this.run = run;
            this.token = token;
            this.split = split;
            this.listener = listener;
            this.callbacks = callbacks;
            this.steps = run.scripts.get(token);
            this.pacing = run.pacing.get(token);
        }

        private synchronized void next() {
            if (closed) {
                return;
            }
            Instant start = split.getStartTimestamp();
            Instant position = split.getCurrentPosition();
            while (step < steps.size()) {
                Step current = steps.get(step);
                if (current.offsetMillis >= 0
                        && start.plusMillis(current.offsetMillis).isBefore(position)) {
                    step++;
                    continue;
                }
                break;
            }
            if (step >= steps.size()) {
                return;
            }
            Step current = steps.get(step);
            switch (current.kind) {
                case "await":
                    run.reached.add(token + ":" + current.latch);
                    if (!run.openLatches.contains(current.latch)) {
                        callbacks.schedule(this::next, 10, TimeUnit.MILLISECONDS);
                        return;
                    }
                    step++;
                    callbacks.execute(this::next);
                    return;
                case "data":
                    step++;
                    listener.record(
                            data(
                                    token,
                                    start.plusMillis(current.offsetMillis),
                                    current.offsetMillis));
                    return;
                case "children":
                    step++;
                    List<SpannerChangeStreamRecord.Child> children = new ArrayList<>();
                    for (String child : current.children) {
                        children.add(
                                INITIAL.equals(token)
                                        ? new SpannerChangeStreamRecord.Child(
                                                child, Collections.emptyList(), true)
                                        : new SpannerChangeStreamRecord.Child(
                                                child, Collections.singletonList(token), false));
                    }
                    run.log.add("children of " + token + ": " + current.children);
                    listener.record(
                            new SpannerChangeStreamRecord.Children(
                                    start.plusMillis(current.offsetMillis), children));
                    return;
                case "finish":
                    step++;
                    run.log.add("finish " + token);
                    listener.finished();
                    return;
                default:
                    throw new IllegalStateException(current.kind);
            }
        }

        private static SpannerChangeStreamRecord.Data data(
                String partition, Instant timestamp, int offset) {
            return new SpannerChangeStreamRecord.Data(
                    DataChangeRecord.builder()
                            .commitTimestamp(timestamp)
                            .recordSequence(partition + "#" + offset)
                            .serverTransactionId("scripted-transaction")
                            .lastRecordInTransactionInPartition(true)
                            .tableName(partition)
                            .columnTypes(Collections.emptyList())
                            .mods(Collections.emptyList())
                            .modType(ModType.UPDATE)
                            .valueCaptureType(ValueCaptureType.NEW_VALUES)
                            .numberOfRecordsInTransaction(1)
                            .numberOfPartitionsInTransaction(1)
                            .transactionTag("")
                            .systemTransaction(false)
                            .build());
        }

        @Override
        public synchronized void resume() {
            if (!closed) {
                callbacks.schedule(this::next, pacing, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void cancel() {
            close();
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                run.closes.computeIfAbsent(token, ignored -> new AtomicInteger()).incrementAndGet();
            }
            closed = true;
        }
    }
}
