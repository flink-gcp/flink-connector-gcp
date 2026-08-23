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
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Deterministic asynchronous partition script for source rescaling integration tests. */
public final class ScriptedChangeStreamOpener implements ChangeStreamOpener {

    private static final long serialVersionUID = 1L;
    private static final Map<String, Map<Integer, AtomicInteger>> ACTIVE =
            new ConcurrentHashMap<>();
    private static final Map<String, Map<Integer, AtomicInteger>> PEAK = new ConcurrentHashMap<>();
    private static final Map<String, List<String>> EVENTS = new ConcurrentHashMap<>();
    private static final AtomicInteger READER_NUMBER = new AtomicInteger();
    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();

    private final String runId;
    private transient ScheduledExecutorService callbacks;
    private transient int readerNumber;

    public ScriptedChangeStreamOpener(String runId) {
        this.runId = runId;
    }

    @Override
    public void open(
            TableDestination table,
            ChangeStreamPartitionSplit split,
            @Nullable Instant boundedTimestamp,
            ResponseObserver<ChangeStreamRecord> observer) {
        ScheduledExecutorService executor = callbacks();
        int reader = readerNumber();
        event(runId, reader, split.splitId(), "open");
        int active =
                ACTIVE.computeIfAbsent(runId, unused -> new ConcurrentHashMap<>())
                        .computeIfAbsent(reader, unused -> new AtomicInteger())
                        .incrementAndGet();
        PEAK.computeIfAbsent(runId, unused -> new ConcurrentHashMap<>())
                .computeIfAbsent(reader, unused -> new AtomicInteger())
                .accumulateAndGet(active, Math::max);
        observer.onStart(new ScriptedController(runId, reader, split, observer, executor));
    }

    public static List<Integer> peaks(String runId) {
        List<Integer> peaks = new ArrayList<>();
        PEAK.getOrDefault(runId, java.util.Collections.emptyMap())
                .values()
                .forEach(peak -> peaks.add(peak.get()));
        return peaks;
    }

    public static List<String> events(String runId) {
        List<String> events = EVENTS.get(runId);
        if (events == null) {
            return java.util.Collections.emptyList();
        }
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    public static void forget(String runId) {
        ACTIVE.remove(runId);
        PEAK.remove(runId);
        EVENTS.remove(runId);
    }

    private ScheduledExecutorService callbacks() {
        if (callbacks == null) {
            callbacks =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "bigtable-change-stream-script-"
                                                        + THREAD_NUMBER.incrementAndGet());
                                thread.setDaemon(true);
                                return thread;
                            });
        }
        return callbacks;
    }

    private int readerNumber() {
        if (readerNumber == 0) {
            readerNumber = READER_NUMBER.incrementAndGet();
        }
        return readerNumber;
    }

    /** Answers from a script rather than a client, so there is nothing to authenticate. */
    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {}

    @Override
    public void close() {
        if (callbacks != null) {
            callbacks.shutdownNow();
        }
    }

    private static final class ScriptedController implements StreamController {

        private final String runId;
        private final int readerNumber;
        private final ChangeStreamPartitionSplit split;
        private final ResponseObserver<ChangeStreamRecord> observer;
        private final ScheduledExecutorService callbacks;
        private final Instant position;
        private int requestedResponses;
        private boolean autoFlowDisabled;
        private boolean closed;
        private boolean terminalReported;

        private ScriptedController(
                String runId,
                int readerNumber,
                ChangeStreamPartitionSplit split,
                ResponseObserver<ChangeStreamRecord> observer,
                ScheduledExecutorService callbacks) {
            this.runId = runId;
            this.readerNumber = readerNumber;
            this.split = split;
            this.observer = observer;
            this.callbacks = callbacks;
            position = split.getLowWatermark().plusMillis(1);
        }

        @Override
        public synchronized void cancel() {
            event(runId, readerNumber, split.splitId(), "cancel");
            if (!closeOnce()) {
                return;
            }
            callbacks.execute(
                    () -> {
                        reportTerminal();
                        observer.onError(new CancellationException("scripted cancellation"));
                    });
        }

        @Override
        public synchronized void disableAutoInboundFlowControl() {
            autoFlowDisabled = true;
        }

        @Override
        public synchronized void request(int count) {
            if (closed) {
                return;
            }
            if (!autoFlowDisabled || count != 1) {
                throw new IllegalStateException("The scripted stream requires request(1).");
            }
            int response = requestedResponses++;
            event(runId, readerNumber, split.splitId(), "request-" + response);
            if (response == 0) {
                callbacks.schedule(
                        () -> {
                            event(runId, readerNumber, split.splitId(), "mutation");
                            observer.onResponse(
                                    TestChangeStreamRecords.mutation(
                                            position,
                                            position,
                                            split.splitId() + "|" + position.toEpochMilli()));
                        },
                        10,
                        TimeUnit.MILLISECONDS);
            } else {
                callbacks.schedule(
                        () -> {
                            event(runId, readerNumber, split.splitId(), "heartbeat-" + response);
                            observer.onResponse(
                                    TestChangeStreamRecords.heartbeat(
                                            position,
                                            "heartbeat-" + position.toEpochMilli(),
                                            split.getPartition()));
                        },
                        50,
                        TimeUnit.MILLISECONDS);
            }
        }

        private synchronized boolean closeOnce() {
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        }

        private synchronized void reportTerminal() {
            if (terminalReported) {
                return;
            }
            terminalReported = true;
            Map<Integer, AtomicInteger> readers = ACTIVE.get(runId);
            AtomicInteger active = readers == null ? null : readers.get(readerNumber);
            if (active != null) {
                active.decrementAndGet();
            }
        }
    }

    private static void event(String runId, int readerNumber, String splitId, String action) {
        EVENTS.computeIfAbsent(
                        runId, unused -> java.util.Collections.synchronizedList(new ArrayList<>()))
                .add(readerNumber + ":" + splitId + ":" + action);
    }
}
