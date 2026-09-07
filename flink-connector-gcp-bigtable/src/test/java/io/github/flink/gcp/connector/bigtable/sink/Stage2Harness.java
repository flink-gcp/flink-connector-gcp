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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.MutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.Stage2BudgetedBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Sustained instrument sharing the local harness's writer/committer and production bulk seam. */
final class Stage2Harness extends LocalStagedHarness {
    final Stage2Ledger ledger;
    final Stage2Lease lease;
    final AtomicBoolean censored = new AtomicBoolean();
    final AtomicLong sourcePollNanos = new AtomicLong();
    final AtomicLong committerWaitNanos = new AtomicLong();
    final AtomicLong sourceStarvations = new AtomicLong();
    final AtomicLong serializationAllocatedBytes = new AtomicLong();
    final AtomicLong restoreAllocatedBytes = new AtomicLong();
    final AtomicLong clientCompletions = new AtomicLong();
    private final long[] clientBins = new long[4096];
    final AtomicLong attemptsCount = new AtomicLong();
    final long attemptLimit;
    final long wireLimit;
    final AtomicLong wireBytes = new AtomicLong();
    final boolean timed;
    private volatile long admissionEnd = Long.MAX_VALUE;
    private volatile long measureStart;
    private volatile boolean preflightDone;
    MetadataValidator metadataValidator = Stage2Preflight::read;

    interface MetadataValidator {
        void validate(TableDestination destination, String profile) throws IOException;
    }

    private final Map<Object, long[]> writers = new HashMap<>();

    @Override
    synchronized void writerStaged(Object writer, int entries, long bytes) {
        if (entries == 0) {
            writers.remove(writer);
        } else {
            writers.put(writer, new long[] {entries, bytes});
        }
    }

    synchronized long stagedTotal(int index) {
        return writers.values().stream().mapToLong(value -> value[index]).sum();
    }

    Stage2Harness(
            TableDestination table,
            String endpoint,
            Path ledgerPath,
            int capacity,
            int bytes,
            boolean hot,
            boolean aggregate,
            int inFlight,
            boolean timed,
            Stage2Lease lease)
            throws IOException {
        super(table, endpoint, bytes, hot, aggregate, inFlight);
        this.lease = lease;
        this.timed = timed;
        this.wireLimit = (long) capacity * (bytes + 1024) * 4;
        try {
            this.attemptLimit =
                    lease == null ? (long) capacity * 4 : lease.reserve(capacity * 4L, wireLimit);
            ledger = new Stage2Ledger(ledgerPath, capacity, 64L * 1024 * 1024);
        } catch (IOException | RuntimeException failure) {
            receiver.shutdownNow();
            RUNS.remove(id, this);
            throw failure;
        }
        allowInputs.set(false);
    }

    void startWindow(long now, long warmupNanos, long measurementNanos) {
        if (warmupNanos < 0 || measurementNanos <= 0) {
            throw new IllegalArgumentException("Invalid Stage 2 durations");
        }
        measureStart = Math.addExact(now, warmupNanos);
        admissionEnd = Math.addExact(measureStart, measurementNanos);
        ledger.window(measureStart, admissionEnd);
        allowInputs();
    }

    boolean windowEnded(long now) {
        return timed && now >= admissionEnd;
    }

    @Override
    Source<Long, ?, ?> source(long records, boolean hold) {
        return timed ? new Stage2Source(id, records) : super.source(records, hold);
    }

    @Override
    long checkpointTimeoutMillis() {
        return 60_000;
    }

    @Override
    synchronized void admitted(long sequence) {
        try {
            ledger.admit(sequence, System.nanoTime());
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @Override
    synchronized void acknowledged(long sequence, long now) {
        try {
            ledger.acknowledge(sequence, now);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @Override
    int acknowledgedCount() {
        return ledger.acknowledgedCount();
    }

    @Override
    StagedMutationTestSink.Input input(long sequence) {
        StagedMutationTestSink.Input original = super.input(sequence);
        try {
            int phase = ledger.entry(sequence).phase;
            long hash = hash(sequence);
            String row =
                    hot && sequence % 10 != 0
                            ? "hot"
                            : String.format(java.util.Locale.ROOT, "%016x-%d", hash, sequence);
            return new StagedMutationTestSink.Input(
                    original.table(),
                    ByteString.copyFromUtf8(phase + "/" + row),
                    original.mutations());
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    static long hash(long sequence) {
        long value = sequence + 0x9e3779b97f4a7c15L;
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    static void checkDistribution() {
        int[] buckets = new int[16];
        int hot = 0;
        for (int i = 0; i < 10_000; i++) {
            buckets[(int) (hash(i) >>> 60)]++;
            if (i % 10 != 0) {
                hot++;
            }
        }
        int min = java.util.Arrays.stream(buckets).min().orElseThrow();
        int max = java.util.Arrays.stream(buckets).max().orElseThrow();
        if (hot != 9000 || max > min * 1.3) {
            throw new IllegalStateException("Stage 2 distribution calibration failed");
        }
    }

    @Override
    void prepared(Collection<CheckAndMutateRowRequest> requests) throws IOException {
        for (CheckAndMutateRowRequest request : requests) {
            Stage2Preflight.envelope(request);
            ledger.marker(
                    sequence(request.getFalseMutationsList()),
                    request.getFalseMutations(request.getFalseMutationsCount() - 1)
                            .getSetCell()
                            .getColumnQualifier()
                            .toStringUtf8());
        }
        ledger.sync();
    }

    @Override
    void beforeSend(CheckAndMutateRowRequest wire) throws IOException {
        Stage2Preflight.envelope(wire);
        preflight();
        attempt(wire.getSerializedSize());
    }

    @Override
    void beforeBulk(MutateRowsRequest.Entry entry) {
        try {
            if (lease == null) {
                attempt(entry.getSerializedSize());
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private void attempt(long bytes) throws IOException {
        attempt(1, bytes);
    }

    private void attempt(long entries, long bytes) throws IOException {
        if (lease != null) {
            lease.requireLive();
        }
        if (attemptsCount.addAndGet(entries) > attemptLimit
                || wireBytes.addAndGet(bytes) > wireLimit) {
            censored.set(true);
            throw new IOException("Stage 2 attempt budget exhausted");
        }
    }

    synchronized void preflight() throws IOException {
        preflight(false);
    }

    private synchronized void preflight(boolean freshClient) throws IOException {
        if (lease != null) {
            lease.requireTarget(table);
            if (freshClient || !preflightDone) {
                preflightDone = false;
                metadataValidator.validate(table, PROFILE);
                preflightDone = true;
            }
        }
    }

    @Override
    DefaultSingleRowClientFactory singleRowFactory() throws IOException {
        preflight(true);
        return lease == null
                ? super.singleRowFactory()
                : new DefaultSingleRowClientFactory(
                        PROFILE, BigtableRequestOptions.builder().build(), null, null);
    }

    @Override
    MutationBatcherFactory batcherFactory() throws IOException {
        preflight(true);
        return lease == null
                ? super.batcherFactory()
                : new Stage2BudgetedBatcherFactory(
                        PROFILE,
                        inFlight,
                        request -> {
                            try {
                                attempt(request.getEntriesCount(), request.getSerializedSize());
                            } catch (IOException failure) {
                                throw io.grpc.Status.RESOURCE_EXHAUSTED
                                        .withDescription(failure.getMessage())
                                        .withCause(failure)
                                        .asRuntimeException();
                            }
                        });
    }

    @Override
    void trace(CheckAndMutateRowRequest wire, ApiFuture<Boolean> future) {}

    @Override
    void committerWaited(long nanos) {
        committerWaitNanos.addAndGet(nanos);
    }

    @Override
    synchronized void clientCompleted(long sequence, long nanos, long completedAt) {
        try {
            if (ledger.entry(sequence).phase != Stage2Ledger.MEASURED) {
                return;
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        if (nanos <= 0) {
            nanos = 1;
        }
        int exponent = 63 - Long.numberOfLeadingZeros(nanos);
        long base = 1L << exponent;
        int fraction = (int) (((double) nanos / base - 1) * 64);
        clientBins[Math.min(4095, exponent * 64 + fraction)]++;
        clientCompletions.incrementAndGet();
    }

    synchronized long clientQuantileUpperBound(double fraction) {
        long target = (long) Math.ceil(clientCompletions.get() * fraction);
        long count = 0;
        for (int i = 0; i < clientBins.length; i++) {
            count += clientBins[i];
            if (count >= target && count > 0) {
                return (long) Math.ceil(Math.scalb(1.0 + (i % 64 + 1) / 64.0, i / 64));
            }
        }
        return 0;
    }

    @Override
    ApiFuture<Boolean> fake(CheckAndMutateRowRequest request) {
        if (!timed) {
            return super.fake(request);
        }
        SettableApiFuture<Boolean> answer = SettableApiFuture.create();
        receiver.schedule(() -> answer.set(false), delayMillis, TimeUnit.MILLISECONDS);
        return answer;
    }

    @Override
    SimpleVersionedSerializer<CheckAndMutateRowRequest> serializer() {
        SimpleVersionedSerializer<CheckAndMutateRowRequest> delegate = super.serializer();
        return new SimpleVersionedSerializer<>() {

            @Override
            public int getVersion() {
                return delegate.getVersion();
            }

            @Override
            public byte[] serialize(CheckAndMutateRowRequest request) throws IOException {
                long before = allocatedBytes();
                try {
                    return delegate.serialize(request);
                } finally {
                    allocationDelta(serializationAllocatedBytes, before);
                }
            }

            @Override
            public CheckAndMutateRowRequest deserialize(int version, byte[] bytes)
                    throws IOException {
                long before = allocatedBytes();
                try {
                    CheckAndMutateRowRequest request = delegate.deserialize(version, bytes);
                    Stage2Preflight.envelope(request);
                    return request;
                } finally {
                    allocationDelta(restoreAllocatedBytes, before);
                }
            }
        };
    }

    private static long allocatedBytes() {
        java.lang.management.ThreadMXBean bean =
                java.lang.management.ManagementFactory.getThreadMXBean();
        if (bean instanceof com.sun.management.ThreadMXBean) {
            com.sun.management.ThreadMXBean allocated = (com.sun.management.ThreadMXBean) bean;
            if (allocated.isThreadAllocatedMemorySupported()
                    && allocated.isThreadAllocatedMemoryEnabled()) {
                return allocated.getThreadAllocatedBytes(Thread.currentThread().getId());
            }
        }
        return -1;
    }

    private static void allocationDelta(AtomicLong total, long before) {
        long after = allocatedBytes();
        recordAllocation(total, before, after);
    }

    static void recordAllocation(AtomicLong total, long before, long after) {
        if (before >= 0 && after >= before) {
            total.updateAndGet(value -> value < 0 ? -1 : value + after - before);
        } else {
            total.set(-1);
        }
    }

    void verifyFakeSums() throws IOException {
        Map<String, Long> expected = new HashMap<>();
        for (int i = 0; i < ledger.capacity; i++) {
            if (ledger.entry(i).status == 2) {
                expected.merge(input(i).row().toStringUtf8(), 1L, Long::sum);
            }
        }
        for (Map.Entry<String, Long> row : expected.entrySet()) {
            Map<String, ByteString> actual = store.cells.get(tableName(table) + "/" + row.getKey());
            if (actual == null
                    || !actual.containsKey("agg:count")
                    || store.sum(tableName(table), row.getKey()) != row.getValue()) {
                throw new IOException("SUM readback differs from distinct-envelope inventory");
            }
        }
    }

    void readback(boolean staged, boolean emulator) throws IOException {
        if (!emulator && lease == null) {
            throw new IOException("Service readback requires an owned Stage 2 lease");
        }
        Map<String, Long> expectedMarkers = new HashMap<>();
        Set<String> expectedRows = new HashSet<>();
        Map<String, Long> expectedSums = new HashMap<>();
        for (int i = 0; i < ledger.capacity; i++) {
            Stage2Ledger.Entry entry = ledger.entry(i);
            if (entry.status == 0) {
                continue;
            }
            if (entry.status != 2) {
                throw new IOException("Readback attempted before drain");
            }
            String row = input(i).row().toStringUtf8();
            expectedRows.add(row);
            expectedSums.merge(row, 1L, Long::sum);
            if (staged) {
                if (entry.marker.isEmpty() || expectedMarkers.put(entry.marker, (long) i) != null) {
                    throw new IOException("Missing or duplicate expected marker identity");
                }
            }
        }
        BigtableDataSettings.Builder settings =
                emulator
                        ? BigtableDataSettings.newBuilderForEmulator(
                                "127.0.0.1", Integer.parseInt(endpoint.split(":")[1]))
                        : BigtableDataSettings.newBuilder();
        settings.setProjectId(table.getProject())
                .setInstanceId(table.getInstance())
                .setAppProfileId(PROFILE);
        long readBytes = 0;
        long markerCells = 0;
        long markerLogicalBytes = 0;
        long maxRowMarkers = 0;
        if (lease != null) {
            lease.reserveRead((long) ledger.capacity * (payloadBytes + 1024));
        }
        try (BigtableDataClient client = BigtableDataClient.create(settings.build())) {
            for (Row row : client.readRows(Query.create(table.getTable()))) {
                String key = row.getKey().toStringUtf8();
                if (!expectedRows.remove(key)) {
                    throw new IOException("Unexpected or repeated readback row");
                }
                long rowMarkers = 0;
                int dataCells = 0;
                int sumCells = 0;
                for (RowCell cell : row.getCells()) {
                    readBytes +=
                            row.getKey().size()
                                    + cell.getQualifier().size()
                                    + cell.getValue().size()
                                    + 32L;
                    if (readBytes > (long) ledger.capacity * (payloadBytes + 1024)) {
                        throw new IOException("Readback byte cap exhausted");
                    }
                    if (cell.getFamily().equals(StagedMutationTestSink.MARKER_FAMILY)) {
                        rowMarkers++;
                        markerCells++;
                        markerLogicalBytes +=
                                row.getKey().size()
                                        + cell.getFamily().length()
                                        + cell.getQualifier().size()
                                        + cell.getValue().size()
                                        + Long.BYTES;
                        Long sequence = expectedMarkers.remove(cell.getQualifier().toStringUtf8());
                        if (sequence == null
                                || !input(sequence).row().equals(row.getKey())
                                || cell.getTimestamp() != 0
                                || !cell.getValue().toStringUtf8().equals("1")) {
                            throw new IOException(
                                    "Marker readback differs from acknowledged inventory");
                        }
                    } else if (cell.getFamily().equals("cf")) {
                        dataCells++;
                        if (cell.getValue().size() != payloadBytes) {
                            throw new IOException(
                                    "Payload readback size differs from the workload");
                        }
                        long sequence = cell.getValue().asReadOnlyByteBuffer().getLong();
                        if (sequence < 0 || sequence >= ledger.capacity) {
                            throw new IOException(
                                    "Payload sequence is outside the input inventory");
                        }
                        if (ledger.entry(sequence).status != 2
                                || !input(sequence).row().equals(row.getKey())
                                || !input(sequence)
                                        .mutations()
                                        .get(0)
                                        .getSetCell()
                                        .getValue()
                                        .equals(cell.getValue())
                                || cell.getTimestamp() != 1000
                                || !cell.getQualifier().toStringUtf8().equals("q")) {
                            throw new IOException("Payload readback differs from generated inputs");
                        }
                    } else if (cell.getFamily().equals("agg") && aggregate) {
                        sumCells++;
                        if (!cell.getQualifier().toStringUtf8().equals("count")
                                || cell.getTimestamp() != 1000
                                || cell.getValue().asReadOnlyByteBuffer().getLong()
                                        != expectedSums.get(key)) {
                            throw new IOException(
                                    "SUM readback differs from distinct-envelope inventory");
                        }
                    } else {
                        throw new IOException("Unexpected readback family");
                    }
                }
                maxRowMarkers = Math.max(maxRowMarkers, rowMarkers);
                if (dataCells != 1 || sumCells != (aggregate ? 1 : 0)) {
                    throw new IOException("Readback cell cardinality mismatch");
                }
            }
        }
        if (!expectedRows.isEmpty() || !expectedMarkers.isEmpty()) {
            throw new IOException("Missing rows or retained marker identities");
        }
        System.out.println(
                "STAGE2_READBACK inputs="
                        + ledger.admittedCount()
                        + " bytes="
                        + readBytes
                        + " markerCells="
                        + markerCells
                        + " markerLogicalBytes="
                        + markerLogicalBytes
                        + " maxRowMarkers="
                        + maxRowMarkers
                        + " PASS");
    }

    @Override
    public void close() throws InterruptedException {
        try {
            super.close();
        } finally {
            try {
                ledger.close();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }
}
