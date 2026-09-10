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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Local diagnostic admission credits; retains only unacknowledged sequence ownership. */
final class Stage2Admission implements AutoCloseable {
    final int limit;
    private final Map<Long, Reader> pending = new HashMap<>();
    private final Map<Integer, Reader> readers = new HashMap<>();
    private boolean ended;
    private boolean closed;
    private int peakPending;
    private int peakPerReader;
    private long waits;

    Stage2Admission(int limit) {
        if (limit < 1 || limit > 1_000_000) {
            throw new IllegalArgumentException("Invalid diagnostic admission limit");
        }
        this.limit = limit;
    }

    synchronized Reader reader(int subtask) {
        if (readers.containsKey(subtask)) {
            throw new IllegalStateException("Diagnostic admission does not support task restart");
        }
        Reader reader = new Reader();
        readers.put(subtask, reader);
        return reader;
    }

    synchronized void acknowledge(long sequence) {
        Reader reader = pending.remove(sequence);
        if (reader == null) {
            throw new IllegalStateException("Acknowledgement without a diagnostic input credit");
        }
        reader.count--;
        reader.signal();
    }

    synchronized void endWindow() {
        ended = true;
        readers.values().forEach(Reader::signal);
    }

    synchronized String sample() {
        return "{\"maxPendingInputsPerSubtask\":"
                + limit
                + ",\"pendingInputs\":"
                + pending.size()
                + ",\"peakPendingInputs\":"
                + peakPending
                + ",\"peakPendingPerSubtask\":"
                + peakPerReader
                + ",\"availabilityWaits\":"
                + waits
                + "}";
    }

    @Override
    public synchronized void close() {
        closed = true;
        endWindow();
        pending.clear();
        readers.clear();
    }

    final class Reader {
        private int count;
        private boolean stopped;
        private CompletableFuture<Void> available = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> sourceAvailable = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> registeredSource;

        boolean hasRoom() {
            synchronized (Stage2Admission.this) {
                return count < limit;
            }
        }

        void emitted(long sequence) {
            synchronized (Stage2Admission.this) {
                if (closed || stopped || count == limit || pending.containsKey(sequence)) {
                    throw new IllegalStateException("Invalid diagnostic source admission");
                }
                pending.put(sequence, this);
                count++;
                peakPending = Math.max(peakPending, pending.size());
                peakPerReader = Math.max(peakPerReader, count);
            }
        }

        CompletableFuture<Void> available(CompletableFuture<Void> source) {
            synchronized (Stage2Admission.this) {
                sourceAvailable = source;
                if (ended || stopped || (count < limit && source.isDone())) {
                    return CompletableFuture.completedFuture(null);
                }
                if (available.isDone()) {
                    available = new CompletableFuture<>();
                    waits++;
                }
                if (!source.isDone() && registeredSource != source) {
                    registeredSource = source;
                    // A source future may outlive several credit waits.
                    source.whenComplete((ignored, failure) -> signal());
                }
                return available;
            }
        }

        void stop() {
            synchronized (Stage2Admission.this) {
                stopped = true;
                signal();
            }
        }

        private void signal() {
            synchronized (Stage2Admission.this) {
                if (ended || stopped || (count < limit && sourceAvailable.isDone())) {
                    available.complete(null);
                }
            }
        }
    }
}
