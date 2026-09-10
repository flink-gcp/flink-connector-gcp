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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Fixed-slot, disk-backed input inventory. Payloads and completed futures are never retained. */
final class Stage2Ledger implements AutoCloseable {
    static final int SLOT_BYTES = 64;
    static final int WARMUP = 1;
    static final int MEASURED = 2;
    static final int TAIL = 3;
    final int capacity;
    private final RandomAccessFile file;
    private int admitted;
    private int acknowledged;
    private int measured;
    private int warmup;
    private int tail;
    private long firstMeasuredAdmission;
    private long lastMeasuredAdmission;
    private long measuredStart;
    private long measuredEnd;

    Stage2Ledger(Path path, int capacity, long maxBytes) throws IOException {
        if (capacity < 1 || capacity > 1_000_000 || (long) capacity * SLOT_BYTES > maxBytes) {
            throw new IllegalArgumentException("Input inventory exceeds its fixed storage budget");
        }
        Files.createFile(path);
        this.capacity = capacity;
        file =
                sizeInventory(
                        new RandomAccessFile(path.toFile(), "rw"), (long) capacity * SLOT_BYTES);
    }

    static RandomAccessFile sizeInventory(RandomAccessFile opened, long length) throws IOException {
        try {
            opened.setLength(length);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                opened.close();
            } catch (IOException closing) {
                failure.addSuppressed(closing);
            }
            throw failure;
        }
        return opened;
    }

    synchronized void window(long start, long end) {
        if (end <= start) {
            throw new IllegalArgumentException("Measurement window must be positive");
        }
        measuredStart = start;
        measuredEnd = end;
    }

    synchronized void admit(long sequence, long now) throws IOException {
        Entry old = entry(sequence);
        if (old.status != 0) {
            return;
        }
        file.seek(offset(sequence));
        file.writeInt(1);
        file.writeInt(now < measuredStart ? WARMUP : now < measuredEnd ? MEASURED : TAIL);
        file.writeLong(now);
        admitted++;
        if (now >= measuredStart && now < measuredEnd) {
            if (measured == 0) {
                firstMeasuredAdmission = now;
            }
            measured++;
            lastMeasuredAdmission = now;
        } else if (now < measuredStart) {
            warmup++;
        } else {
            tail++;
        }
    }

    synchronized void marker(long sequence, String marker) throws IOException {
        if (!marker.matches("[0-9a-f]{32}")) {
            throw new IOException("Invalid persisted marker identity");
        }
        Entry old = entry(sequence);
        if (old.status == 0) {
            throw new IOException("Marker without admitted input");
        }
        if (!old.marker.isEmpty() && !old.marker.equals(marker)) {
            throw new IOException("Input was re-staged with a different envelope identity");
        }
        file.seek(offset(sequence) + 24);
        file.writeBytes(marker);
    }

    synchronized boolean acknowledge(long sequence, long now) throws IOException {
        Entry old = entry(sequence);
        if (old.status == 0 || now < old.admittedAt) {
            throw new IOException("Acknowledgement without preceding admission");
        }
        if (old.status == 2) {
            return false;
        }
        file.seek(offset(sequence));
        file.writeInt(2);
        file.seek(offset(sequence) + 16);
        file.writeLong(now);
        acknowledged++;
        return true;
    }

    synchronized Entry entry(long sequence) throws IOException {
        file.seek(offset(sequence));
        int status = file.readInt();
        int phase = file.readInt();
        long admission = file.readLong();
        long ack = file.readLong();
        byte[] marker = new byte[32];
        file.readFully(marker);
        return new Entry(
                status,
                phase,
                admission,
                ack,
                marker[0] == 0
                        ? ""
                        : new String(marker, java.nio.charset.StandardCharsets.US_ASCII));
    }

    synchronized Summary summary() throws IOException {
        if (admitted != acknowledged) {
            throw new IOException(
                    "Undrained inventory: admitted=" + admitted + ", acknowledged=" + acknowledged);
        }
        long[] latency = new long[capacity];
        int count = 0;
        long finalAck = measuredStart;
        for (int i = 0; i < capacity; i++) {
            Entry entry = entry(i);
            if (entry.status == 2 && entry.phase == MEASURED) {
                latency[count++] = entry.acknowledgedAt - entry.admittedAt;
                finalAck = Math.max(finalAck, entry.acknowledgedAt);
            }
        }
        if (count == 0 || finalAck <= measuredStart) {
            throw new IOException("No measured acknowledgements");
        }
        Arrays.sort(latency, 0, count);
        return new Summary(
                count,
                count * 1_000_000_000.0 / (finalAck - measuredStart),
                percentile(latency, count, .50),
                percentile(latency, count, .95),
                percentile(latency, count, .99),
                Math.max(0, finalAck - measuredEnd));
    }

    static long percentile(long[] values, int count, double fraction) {
        return values[(int) Math.ceil(count * fraction) - 1];
    }

    synchronized String sample() {
        return "{\"admittedInputs\":"
                + admitted
                + ",\"acknowledgedInputs\":"
                + acknowledged
                + ",\"pendingInputs\":"
                + (admitted - acknowledged)
                + ",\"warmupInputs\":"
                + warmup
                + ",\"measuredInputs\":"
                + measured
                + ",\"tailInputs\":"
                + tail
                + ",\"firstMeasuredAdmissionOffsetNanos\":"
                + (measured == 0 ? -1 : firstMeasuredAdmission - measuredStart)
                + ",\"lastMeasuredAdmissionOffsetNanos\":"
                + (measured == 0 ? -1 : lastMeasuredAdmission - measuredStart)
                + "}";
    }

    synchronized int admittedCount() {
        return admitted;
    }

    synchronized int measuredCount() {
        return measured;
    }

    synchronized int acknowledgedCount() {
        return acknowledged;
    }

    synchronized void sync() throws IOException {
        file.getFD().sync();
    }

    private long offset(long sequence) {
        if (sequence < 0 || sequence >= capacity) {
            throw new IllegalArgumentException("Input inventory capacity exhausted: " + capacity);
        }
        return sequence * SLOT_BYTES;
    }

    @Override
    public synchronized void close() throws IOException {
        file.close();
    }

    static final class Entry {
        final int status;
        final int phase;
        final long admittedAt;
        final long acknowledgedAt;
        final String marker;

        Entry(int status, int phase, long admittedAt, long acknowledgedAt, String marker) {
            this.status = status;
            this.phase = phase;
            this.admittedAt = admittedAt;
            this.acknowledgedAt = acknowledgedAt;
            this.marker = marker;
        }
    }

    static final class Summary {
        final int count;
        final double throughput;
        final long p50;
        final long p95;
        final long p99;
        final long drainNanos;

        Summary(int count, double throughput, long p50, long p95, long p99, long drainNanos) {
            this.count = count;
            this.throughput = throughput;
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.drainNanos = drainNanos;
        }
    }
}
