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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkEvent;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkItem;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkMeta;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures how much of the {@code FILE_LOADS} writer's thread the staging upload takes, with and
 * without the background staging uploader (#2021), against real Cloud Storage.
 *
 * <p>Not a test, and run by hand on a VM in the staging bucket's region. One JVM runs a campaign:
 * every cell of {@code ops} (the uploader's {@code maxInFlightUploadOperations}; 0 is the probe's
 * own name for the cell with the uploader off, not an option value, since the option rejects it),
 * {@code chunksMiB}, {@code dests} and {@code finalizations}, {@code reps} times, in a fresh random
 * order each round so that drift lands on every cell alike. A cell writes {@code inputMiB} of
 * serialized rows from the calling thread, which stands in for the task thread, with a {@code
 * prepareCommit} every {@code checkpointMiB} and once at the end, and then deletes the objects it
 * staged. Rows are #2019's nested event (about 9.5 KB, 120 repeated sub-messages) with three
 * additional fields, each routed to the destination its id hashes to, so the destinations receive
 * similar but not equal volumes.
 *
 * <p>Each run prints one JSON line: the cell, the writer thread's wall and CPU time, records and
 * input bytes per second, staged bytes and files, the peak heap seen, and stack samples of the
 * writer thread every 5 ms, classed as waiting for an uploader permit, inside the Cloud Storage
 * client or its sockets, otherwise runnable, or otherwise waiting. Samples are taken through {@link
 * Thread#getStackTrace()}, so they share a thread dump's safepoint bias.
 *
 * <p>Arguments are {@code key=value}: {@code storage} ({@code gcs} or {@code memory}, the latter
 * discarding bytes for a dry run), {@code stagingPath} ({@code gs://bucket/prefix}, required for
 * {@code gcs}), {@code ops}, {@code chunksMiB}, {@code dests} (comma-separated), {@code inputMiB},
 * {@code finalizations} (comma-separated {@code maxConcurrentCheckpointFinalizations}), {@code
 * checkpointMiB}, {@code reps}, {@code seed} and {@code out}, a file the JSON lines are also
 * appended to. Credentials are application-default.
 */
final class FileLoadsWriterStagingUploadProbe {

    private static final int DISTINCT_INPUTS = 256;
    private static final int ITEMS_PER_EVENT = 120;
    private static final long MIB = 1024L * 1024;
    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private FileLoadsWriterStagingUploadProbe() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        for (String argument : args) {
            int split = argument.indexOf('=');
            arguments.put(argument.substring(0, split), argument.substring(split + 1));
        }
        String storageKind = arguments.getOrDefault("storage", "memory");
        String stagingPath = arguments.getOrDefault("stagingPath", "gs://probe/staging");
        List<Integer> ops = ints(arguments.getOrDefault("ops", "0,1,2,4"));
        List<Integer> chunks = ints(arguments.getOrDefault("chunksMiB", "4,8,16"));
        List<Integer> dests = ints(arguments.getOrDefault("dests", "1,2,16"));
        List<Integer> finalizations = ints(arguments.getOrDefault("finalizations", "1"));
        long inputBytes = Long.parseLong(arguments.getOrDefault("inputMiB", "512")) * MIB;
        long checkpointBytes = Long.parseLong(arguments.getOrDefault("checkpointMiB", "128")) * MIB;
        int reps = Integer.parseInt(arguments.getOrDefault("reps", "5"));
        long seed = Long.parseLong(arguments.getOrDefault("seed", "2021"));
        Path out = arguments.containsKey("out") ? Path.of(arguments.get("out")) : null;
        if (!"gcs".equals(storageKind) && !"memory".equals(storageKind)) {
            throw new IllegalArgumentException("storage must be gcs or memory: " + storageKind);
        }

        Random random = new Random(seed);
        List<BenchmarkEvent> events = events(random);
        List<Cell> cells = new ArrayList<>();
        for (int op : ops) {
            for (int chunk : chunks) {
                for (int dest : dests) {
                    for (int finalization : finalizations) {
                        cells.add(new Cell(op, chunk, dest, finalization));
                    }
                }
            }
        }
        PrintStream stdout = System.out;
        stdout.printf(
                "{\"start\":\"%s\",\"storage\":\"%s\",\"stagingPath\":\"%s\",\"cells\":%d,"
                        + "\"reps\":%d,\"inputMiB\":%d,\"checkpointMiB\":%d,\"eventBytes\":%d,"
                        + "\"maxHeap\":%d,\"cpus\":%d}%n",
                Instant.now(),
                storageKind,
                stagingPath,
                cells.size(),
                reps,
                inputBytes / MIB,
                checkpointBytes / MIB,
                events.get(0).getSerializedSize(),
                Runtime.getRuntime().maxMemory(),
                Runtime.getRuntime().availableProcessors());

        // Discarded warm-up, so the first measured cells do not pay class loading and JIT.
        for (Cell warmUp : List.of(new Cell(0, 4, 1, 1), new Cell(2, 4, 1, 1))) {
            run(warmUp, -1, storageKind, stagingPath, events, inputBytes / 4, checkpointBytes);
        }
        List<Cell> order = new ArrayList<>(cells);
        for (int rep = 0; rep < reps; rep++) {
            Collections.shuffle(order, random);
            for (Cell cell : order) {
                String line =
                        run(
                                cell,
                                rep,
                                storageKind,
                                stagingPath,
                                events,
                                inputBytes,
                                checkpointBytes);
                stdout.println(line);
                if (out != null) {
                    Files.writeString(
                            out,
                            line + "\n",
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
                }
            }
        }
        stdout.printf("{\"end\":\"%s\"}%n", Instant.now());
    }

    private static String run(
            Cell cell,
            int rep,
            String storageKind,
            String stagingPath,
            List<BenchmarkEvent> events,
            long inputBytes,
            long checkpointBytes)
            throws Exception {
        String runPath = stagingPath + "/" + UUID.randomUUID();
        StagingStorage storage =
                "gcs".equals(storageKind) ? new GcsStagingStorage() : new DiscardingStorage();
        FileLoadsOptions.Builder builder =
                FileLoadsOptions.builder()
                        .stagingPath(runPath)
                        .stagingUploadChunkBytes(cell.chunkMiB * MIB)
                        .maxConcurrentCheckpointFinalizations(cell.finalizations);
        if (cell.ops > 0) {
            builder.backgroundStagingUploadEnabled(true).maxInFlightUploadOperations(cell.ops);
        }
        FileLoadsOptions options = builder.build();
        FileLoadsWriter<BenchmarkEvent> writer =
                new FileLoadsWriter<>(
                        config(cell.dests, runPath),
                        options,
                        storage,
                        TestSinkWriterMetricGroup.create(),
                        "0123456789abcdef0123456789abcdef",
                        0,
                        0,
                        new ManualProcessingTimeService());
        System.gc();

        Thread self = Thread.currentThread();
        Sampler sampler = new Sampler(self);
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        List<String> staged = new ArrayList<>();
        long stagedBytes = 0;
        long records = 0;
        long written = 0;
        long sinceCheckpoint = 0;
        int next = 0;

        sampler.start();
        long cpuStart = threads.getCurrentThreadCpuTime();
        long wallStart = System.nanoTime();
        try {
            while (written < inputBytes) {
                BenchmarkEvent event = events.get(next);
                next = (next + 1) % events.size();
                writer.write(event, CONTEXT);
                records++;
                written += event.getSerializedSize();
                sinceCheckpoint += event.getSerializedSize();
                if (sinceCheckpoint >= checkpointBytes) {
                    sinceCheckpoint = 0;
                    for (FileLoadsCommittable committable : writer.prepareCommit()) {
                        staged.add(committable.getUri());
                        stagedBytes += committable.getByteCount();
                    }
                }
            }
            for (FileLoadsCommittable committable : writer.prepareCommit()) {
                staged.add(committable.getUri());
                stagedBytes += committable.getByteCount();
            }
        } finally {
            long wall = System.nanoTime() - wallStart;
            long cpu = threads.getCurrentThreadCpuTime() - cpuStart;
            sampler.stop();
            writer.close();
            storage.deleteObjects(staged);
            storage.close();
            sampler.wallNanos = wall;
            sampler.cpuNanos = cpu;
        }
        double seconds = sampler.wallNanos / 1e9;
        return String.format(
                Locale.ROOT,
                "{\"rep\":%d,\"ops\":%d,\"chunkMiB\":%d,\"dests\":%d,\"finalizations\":%d,\"records\":%d,"
                        + "\"inputBytes\":%d,\"wallS\":%.3f,\"cpuS\":%.3f,\"cpuShare\":%.3f,"
                        + "\"recordsPerS\":%.1f,\"inputMiBPerS\":%.2f,\"stagedBytes\":%d,"
                        + "\"files\":%d,\"peakHeapMiB\":%.1f,\"samples\":%d,\"permitWait\":%.3f,"
                        + "\"gcs\":%.3f,\"runnableOther\":%.3f,\"waitingOther\":%.3f}",
                rep,
                cell.ops,
                cell.chunkMiB,
                cell.dests,
                cell.finalizations,
                records,
                written,
                seconds,
                sampler.cpuNanos / 1e9,
                (double) sampler.cpuNanos / sampler.wallNanos,
                records / seconds,
                written / MIB / seconds,
                stagedBytes,
                staged.size(),
                sampler.peakHeap.get() / (double) MIB,
                sampler.total(),
                sampler.share(sampler.permitWait),
                sampler.share(sampler.gcs),
                sampler.share(sampler.runnableOther),
                sampler.share(sampler.waitingOther));
    }

    /** Builds the sink configuration the way an application would, through the public builder. */
    private static BigQuerySinkConfig<BenchmarkEvent> config(int dests, String stagingPath) {
        BigQueryFileLoadsSink<BenchmarkEvent> sink =
                (BigQueryFileLoadsSink<BenchmarkEvent>)
                        BigQuerySink.<BenchmarkEvent>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        "p",
                                                        "d",
                                                        "t"
                                                                + Math.floorMod(
                                                                        element.getId().hashCode(),
                                                                        dests)))
                                .serializer(
                                        ProtoMessageSerializationSchema.of(BenchmarkEvent.class))
                                .failureHandler(FailureHandler.failJob())
                                .additionalFields(additionalFields())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder().stagingPath(stagingPath).build())
                                .build();
        return sink.getConfig();
    }

    private static AdditionalFields<BenchmarkEvent> additionalFields() {
        return AdditionalFields.<BenchmarkEvent>builder()
                .field(
                        AdditionalField.of(
                                "ingest_id",
                                AdditionalFieldType.STRING,
                                AdditionalFieldNullPolicy.REQUIRED,
                                element -> "ingest-0001"))
                .field(
                        AdditionalField.of(
                                "ingest_time",
                                AdditionalFieldType.TIMESTAMP,
                                AdditionalFieldNullPolicy.REQUIRED,
                                element -> Instant.ofEpochSecond(1_790_000_000L)))
                .field(
                        AdditionalField.of(
                                "ingest_source",
                                AdditionalFieldType.STRING,
                                AdditionalFieldNullPolicy.NULLABLE,
                                element -> "probe"))
                .build();
    }

    private static List<BenchmarkEvent> events(Random random) {
        List<BenchmarkEvent> events = new ArrayList<>(DISTINCT_INPUTS);
        for (int i = 0; i < DISTINCT_INPUTS; i++) {
            BenchmarkEvent.Builder event =
                    BenchmarkEvent.newBuilder()
                            .setId(new UUID(random.nextLong(), random.nextLong()).toString())
                            .setEventTime(timestamp(random))
                            .setKind("kind-" + random.nextInt(16))
                            .setMeta(
                                    BenchmarkMeta.newBuilder()
                                            .setRegion("asia-northeast1")
                                            .setDevice("device-" + random.nextInt(10_000))
                                            .setSequence(random.nextLong()));
            for (int j = 0; j < ITEMS_PER_EVENT; j++) {
                event.addItems(
                        BenchmarkItem.newBuilder()
                                .setSku("sku-" + random.nextInt(1_000_000))
                                .setQuantity(random.nextInt(100))
                                .setPrice(random.nextDouble() * 1_000)
                                .setLabel("label-" + Long.toHexString(random.nextLong()))
                                .addTags("tag-" + random.nextInt(50))
                                .addTags("tag-" + random.nextInt(50))
                                .setObservedAt(timestamp(random)));
            }
            events.add(event.build());
        }
        return events;
    }

    private static Timestamp timestamp(Random random) {
        return Timestamp.newBuilder()
                .setSeconds(1_700_000_000L + random.nextInt(100_000_000))
                .setNanos(random.nextInt(1_000_000) * 1_000)
                .build();
    }

    private static List<Integer> ints(String csv) {
        List<Integer> values = new ArrayList<>();
        for (String value : csv.split(",")) {
            values.add(Integer.parseInt(value.trim()));
        }
        return values;
    }

    private static final class Cell {
        private final int ops;
        private final int chunkMiB;
        private final int dests;
        private final int finalizations;

        private Cell(int ops, int chunkMiB, int dests, int finalizations) {
            this.ops = ops;
            this.chunkMiB = chunkMiB;
            this.dests = dests;
            this.finalizations = finalizations;
        }
    }

    /** Samples the writer thread's stack and the used heap every 5 ms. */
    private static final class Sampler {
        private final Thread target;
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicLong peakHeap = new AtomicLong();
        private long permitWait;
        private long gcs;
        private long runnableOther;
        private long waitingOther;
        private long wallNanos;
        private long cpuNanos;
        private Thread thread;

        private Sampler(Thread target) {
            this.target = target;
        }

        private void start() {
            running.set(true);
            thread =
                    new Thread(
                            () -> {
                                Runtime runtime = Runtime.getRuntime();
                                while (running.get()) {
                                    classify(target.getState(), target.getStackTrace());
                                    peakHeap.accumulateAndGet(
                                            runtime.totalMemory() - runtime.freeMemory(),
                                            Math::max);
                                    try {
                                        Thread.sleep(5);
                                    } catch (InterruptedException e) {
                                        return;
                                    }
                                }
                            },
                            "probe sampler");
            thread.setDaemon(true);
            thread.start();
        }

        private void stop() throws InterruptedException {
            running.set(false);
            thread.join();
        }

        private synchronized void classify(Thread.State state, StackTraceElement[] stack) {
            for (StackTraceElement frame : stack) {
                if (frame.getClassName().startsWith("java.util.concurrent.Semaphore")) {
                    permitWait++;
                    return;
                }
            }
            for (StackTraceElement frame : stack) {
                String name = frame.getClassName();
                if (name.startsWith("com.google.cloud.storage.")
                        || name.startsWith("com.google.api.client.")
                        || name.startsWith("sun.nio.ch.")
                        || name.startsWith("sun.security.ssl.")
                        || name.startsWith("java.net.")) {
                    gcs++;
                    return;
                }
            }
            if (state == Thread.State.RUNNABLE) {
                runnableOther++;
            } else {
                waitingOther++;
            }
        }

        private synchronized long total() {
            return permitWait + gcs + runnableOther + waitingOther;
        }

        private synchronized double share(long count) {
            long total = permitWait + gcs + runnableOther + waitingOther;
            return total == 0 ? 0 : (double) count / total;
        }
    }

    /** Counts and discards staged bytes, for a dry run without Cloud Storage. */
    private static final class DiscardingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        @Override
        public WritableByteChannel createObject(String gcsUri, int chunkBytes) {
            return new WritableByteChannel() {
                private boolean open = true;

                @Override
                public int write(ByteBuffer source) {
                    int length = source.remaining();
                    source.position(source.limit());
                    return length;
                }

                @Override
                public boolean isOpen() {
                    return open;
                }

                @Override
                public void close() {
                    open = false;
                }
            };
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}
    }
}
