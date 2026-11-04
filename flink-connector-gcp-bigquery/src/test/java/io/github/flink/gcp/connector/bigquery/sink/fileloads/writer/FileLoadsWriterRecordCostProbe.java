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

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkEvent;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkItem;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkMeta;
import io.github.flink.gcp.connector.bigquery.testproto.benchmark.BenchmarkSingle;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;

import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Measures the {@code FILE_LOADS} writer's task-thread cost per record (#2019): serialization,
 * additional fields, Avro conversion and encoding, with staging bytes discarded.
 *
 * <p>Not a test, and run by hand: build the test classpath and run {@code main}, optionally with a
 * seed. Four arms, a nested event (printed as the generated message's serialized size, about 9.5
 * KB, with 120 repeated sub-messages) and a one-field record, each with and without three
 * additional fields, run in a random order every round so that drift lands on every arm alike. It
 * prints the median and the interquartile range of nanoseconds per record for each arm.
 *
 * <p>A timed batch covers {@code write} only. It includes re-creating the destination's conversion
 * state and opening its file, because the untimed {@code prepareCommit} after each batch finishes
 * them; it excludes compressing the last Avro block and finishing the file, which for the one-field
 * arms is all of their compression.
 *
 * <p>It uses only API that predates #2019, so this file and {@code benchmark.proto}, copied onto
 * the earlier code, measure it too. Compare the two in alternating JVM runs with the same heap
 * settings, since the change also cuts allocation.
 */
final class FileLoadsWriterRecordCostProbe {

    private static final int WARMUP_ROUNDS = 15;
    private static final int MEASURED_ROUNDS = 40;
    private static final int RECORDS_PER_BATCH = 2_000;
    private static final int DISTINCT_INPUTS = 256;
    private static final int ITEMS_PER_EVENT = 120;
    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    private FileLoadsWriterRecordCostProbe() {}

    public static void main(String[] args) throws Exception {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : 2019L;
        Random random = new Random(seed);
        List<Arm<?>> arms = new ArrayList<>();
        arms.add(arm("nested+3", BenchmarkEvent.class, events(random), true));
        arms.add(arm("nested", BenchmarkEvent.class, events(random), false));
        arms.add(arm("single+3", BenchmarkSingle.class, singles(random), true));
        arms.add(arm("single", BenchmarkSingle.class, singles(random), false));
        System.out.printf(
                "nested event: %d serialized bytes%n",
                ((BenchmarkEvent) arms.get(0).inputs.get(0)).getSerializedSize());

        List<Arm<?>> order = new ArrayList<>(arms);
        for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
            Collections.shuffle(order, random);
            for (Arm<?> arm : order) {
                long nanos = arm.runBatch();
                if (round >= WARMUP_ROUNDS) {
                    arm.samples.add((double) nanos / RECORDS_PER_BATCH);
                }
            }
        }
        for (Arm<?> arm : arms) {
            arm.writer.close();
            double[] sorted = arm.samples.stream().mapToDouble(Double::doubleValue).toArray();
            Arrays.sort(sorted);
            System.out.printf(
                    "%-9s median %8.0f ns/record  IQR %8.0f-%8.0f  staged %6.0f B/record%n",
                    arm.name,
                    quantile(sorted, 0.5),
                    quantile(sorted, 0.25),
                    quantile(sorted, 0.75),
                    (double) arm.storage.bytes
                            / ((long) (WARMUP_ROUNDS + MEASURED_ROUNDS) * RECORDS_PER_BATCH));
        }
    }

    private static <T extends Message> Arm<T> arm(
            String name, Class<T> type, List<T> inputs, boolean additionalFields) {
        var builder =
                BigQuerySink.<T>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .destinationResolver(
                                (element, context) -> TableDestination.of("p", "d", name))
                        .serializer(ProtoMessageSerializationSchema.of(type))
                        .failureHandler(FailureHandler.failJob())
                        .fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket/probe")
                                        .build());
        if (additionalFields) {
            builder.additionalFields(
                    AdditionalFields.<T>builder()
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
                            .build());
        }
        BigQueryFileLoadsSink<T> sink = (BigQueryFileLoadsSink<T>) builder.build();
        DiscardingStorage storage = new DiscardingStorage();
        FileLoadsWriter<T> writer =
                new FileLoadsWriter<>(
                        sink.getConfig(),
                        FileLoadsOptions.builder().stagingPath("gs://bucket/probe").build(),
                        storage,
                        TestSinkWriterMetricGroup.create(),
                        "0123456789abcdef0123456789abcdef",
                        0,
                        0,
                        new ManualProcessingTimeService());
        return new Arm<>(name, inputs, writer, storage);
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

    private static List<BenchmarkSingle> singles(Random random) {
        List<BenchmarkSingle> singles = new ArrayList<>(DISTINCT_INPUTS);
        for (int i = 0; i < DISTINCT_INPUTS; i++) {
            singles.add(BenchmarkSingle.newBuilder().setId("id-" + random.nextInt()).build());
        }
        return singles;
    }

    private static Timestamp timestamp(Random random) {
        return Timestamp.newBuilder()
                .setSeconds(1_700_000_000L + random.nextInt(100_000_000))
                .setNanos(random.nextInt(1_000_000) * 1_000)
                .build();
    }

    private static double quantile(double[] sorted, double q) {
        double position = q * (sorted.length - 1);
        int lower = (int) Math.floor(position);
        int upper = (int) Math.ceil(position);
        return sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower);
    }

    private static final class Arm<T> {
        private final String name;
        private final List<T> inputs;
        private final FileLoadsWriter<T> writer;
        private final DiscardingStorage storage;
        private final List<Double> samples = new ArrayList<>();
        private int next;

        private Arm(
                String name, List<T> inputs, FileLoadsWriter<T> writer, DiscardingStorage storage) {
            this.name = name;
            this.inputs = inputs;
            this.writer = writer;
            this.storage = storage;
        }

        /** Writes one batch, timed, then finishes its files untimed so memory stays flat. */
        private long runBatch() throws Exception {
            long start = System.nanoTime();
            for (int i = 0; i < RECORDS_PER_BATCH; i++) {
                writer.write(inputs.get(next), CONTEXT);
                next = (next + 1) % inputs.size();
            }
            long elapsed = System.nanoTime() - start;
            writer.prepareCommit();
            return elapsed;
        }
    }

    /** Counts and discards staged bytes. */
    private static final class DiscardingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        private long bytes;

        @Override
        public OutputStream createObject(String gcsUri) {
            return new OutputStream() {
                @Override
                public void write(int value) {
                    bytes++;
                }

                @Override
                public void write(byte[] buffer, int offset, int length) {
                    bytes += length;
                }
            };
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}
    }
}
