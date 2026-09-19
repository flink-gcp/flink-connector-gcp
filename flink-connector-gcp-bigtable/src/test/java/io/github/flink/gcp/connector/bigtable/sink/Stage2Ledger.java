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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Fixed-slot, disk-backed input inventory. Payloads and completed futures are never retained. */
final class Stage2Ledger implements AutoCloseable {
    static final int SLOT_BYTES = 64;

    /** Slots read per block by {@link #scan(Visitor)}; one block is 1 MiB. */
    static final int SCAN_SLOTS = 16_384;

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
        if (capacity < 1 || (long) capacity * SLOT_BYTES > maxBytes) {
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
        byte[] slot = new byte[SLOT_BYTES];
        file.seek(offset(sequence));
        file.readFully(slot);
        return decode(ByteBuffer.wrap(slot));
    }

    /** Decodes one slot from the buffer's position and leaves the position after the slot. */
    private static Entry decode(ByteBuffer slot) {
        int start = slot.position();
        int status = slot.getInt();
        int phase = slot.getInt();
        long admission = slot.getLong();
        long ack = slot.getLong();
        byte[] marker = new byte[32];
        slot.get(marker);
        // The slot is padded to SLOT_BYTES; skip the unused tail.
        slot.position(start + SLOT_BYTES);
        return new Entry(
                status,
                phase,
                admission,
                ack,
                marker[0] == 0
                        ? ""
                        : new String(marker, java.nio.charset.StandardCharsets.US_ASCII));
    }

    /** Receives each inventory slot in sequence order until it returns or throws. */
    @FunctionalInterface
    interface Visitor {
        void visit(long sequence, Entry entry) throws IOException;
    }

    /**
     * Visits every slot with one buffered sequential pass over the inventory file. A per-slot
     * {@link #entry(long)} costs several system calls, which at millions of slots takes minutes
     * after every observation; this pass reads the same slots in large blocks. The ledger monitor
     * is held for the whole pass, so a visitor must not take a lock that a ledger writer can hold
     * and must not call {@code admit}, {@code marker} or {@code acknowledge}.
     */
    synchronized void scan(Visitor visitor) throws IOException {
        FileChannel channel = file.getChannel();
        ByteBuffer block = ByteBuffer.allocate(SLOT_BYTES * SCAN_SLOTS);
        long position = 0;
        for (long sequence = 0; sequence < capacity; ) {
            block.clear();
            int remaining =
                    (int) Math.min((long) block.capacity(), (capacity - sequence) * SLOT_BYTES);
            block.limit(remaining);
            while (block.hasRemaining()) {
                int read = channel.read(block, position + block.position());
                if (read < 0) {
                    throw new IOException("Input inventory is shorter than its capacity");
                }
                if (read == 0) {
                    throw new IOException("Input inventory read made no progress");
                }
            }
            block.flip();
            while (block.remaining() >= SLOT_BYTES) {
                visitor.visit(sequence++, decode(block));
            }
            position += remaining;
        }
    }

    synchronized Summary summary() throws IOException {
        if (admitted != acknowledged) {
            throw new IOException(
                    "Undrained inventory: admitted=" + admitted + ", acknowledged=" + acknowledged);
        }
        long[] latency = new long[capacity];
        int[] counted = {0};
        long[] latestAck = {measuredStart};
        scan(
                (sequence, entry) -> {
                    if (entry.status == 2 && entry.phase == MEASURED) {
                        latency[counted[0]++] = entry.acknowledgedAt - entry.admittedAt;
                        latestAck[0] = Math.max(latestAck[0], entry.acknowledgedAt);
                    }
                });
        int count = counted[0];
        long finalAck = latestAck[0];
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
