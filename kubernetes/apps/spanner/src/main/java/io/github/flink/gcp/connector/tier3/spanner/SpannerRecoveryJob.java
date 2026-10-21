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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.spanner.KeySet;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerSinkBuilder;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSource;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;

/**
 * Internal finite workload for one separately admitted Spanner recovery trial.
 *
 * <p>One input reaches the production sink, the production Change Streams source relays what the
 * sink wrote, and the production batch source copies a seeded table, so one trial recovers all
 * three.
 */
@Internal
public final class SpannerRecoveryJob {
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(30);
    private static final int FLINK_MAX_PARALLELISM = 128;
    // The input sink writes only at a checkpoint. Each two-column row costs two cells, so the
    // batch overflows at 40,000 rows, minutes of input at the highest rate, while a checkpoint's
    // input is a few thousand.
    private static final int INPUT_BATCH_LIMIT = 80_000;
    private static final long INPUT_BATCH_BYTES = 64L * 1024 * 1024;

    private SpannerRecoveryJob() {}

    /** Runs one trial against the instance the external lifecycle runner created. */
    public static void main(String[] args) throws Exception {
        var options = RecoveryOptions.parse(args);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options, Components.production(options), CHECKPOINT_INTERVAL);
        env.execute("Spanner recovery " + options.runId);
    }

    /** The connectors of one graph; only a test points them at an emulator. */
    record Components(
            Sink<Long> input,
            Source<Tuple2<String, Long>, ?, ?> changeStream,
            Sink<Tuple3<String, String, Long>> relay,
            Source<Tuple2<String, Long>, ?, ?> seed,
            Sink<Tuple3<String, String, Long>> batchCopy) {

        /** The production builders on the workload's ADC. */
        static Components production(RecoveryOptions options) {
            return connectors(options, null);
        }

        static Components connectors(RecoveryOptions options, @Nullable String emulatorEndpoint) {
            SpannerSinkBuilder<Long> input =
                    SpannerSink.<Long>builder()
                            .database(options.database())
                            .serializer((sequence, context) -> options.input(sequence))
                            .writerOptions(
                                    SpannerWriterOptions.builder()
                                            .maxBatchMutations(INPUT_BATCH_LIMIT)
                                            .maxBatchCells(INPUT_BATCH_LIMIT)
                                            .maxBatchBytes(INPUT_BATCH_BYTES)
                                            .build());
            SpannerChangeStreamSourceBuilder<Tuple2<String, Long>> changeStream =
                    SpannerChangeStreamSource.<Tuple2<String, Long>>builder()
                            .database(options.database())
                            .changeStreamName(RecoveryOptions.CHANGE_STREAM)
                            .deserializer(new ChangeRelay.Deserializer())
                            // A fresh start reads from when the coordinator initializes. No
                            // checkpoint completes before that, and the input sink writes only
                            // at a checkpoint, so no input commit precedes it.
                            .startPosition(StartPosition.latest())
                            .boundedTimestamp(options.changeStreamEnd);
            SpannerSinkBuilder<Tuple3<String, String, Long>> relay =
                    SpannerSink.<Tuple3<String, String, Long>>builder()
                            .database(options.database())
                            .serializer((delivery, context) -> ChangeRelay.mutation(delivery));
            SpannerSourceBuilder<Tuple2<String, Long>> seed =
                    SpannerSource.<Tuple2<String, Long>>builder()
                            .database(options.database())
                            .readOperation(
                                    SpannerReadOperation.read(
                                            RecoveryOptions.SEED_TABLE,
                                            KeySet.all(),
                                            List.of(
                                                    RecoveryOptions.KEY_COLUMN,
                                                    RecoveryOptions.SEQUENCE_COLUMN)))
                            .deserializer(new SeedCopy.Deserializer());
            SpannerSinkBuilder<Tuple3<String, String, Long>> batchCopy =
                    SpannerSink.<Tuple3<String, String, Long>>builder()
                            .database(options.database())
                            .serializer((delivery, context) -> SeedCopy.mutation(delivery));
            if (emulatorEndpoint != null) {
                input.emulatorEndpoint(emulatorEndpoint);
                changeStream.emulatorEndpoint(emulatorEndpoint);
                relay.emulatorEndpoint(emulatorEndpoint);
                seed.emulatorEndpoint(emulatorEndpoint);
                batchCopy.emulatorEndpoint(emulatorEndpoint);
            }
            return new Components(
                    input.build(),
                    changeStream.build(),
                    relay.build(),
                    seed.build(),
                    batchCopy.build());
        }
    }

    static void configure(
            StreamExecutionEnvironment env,
            RecoveryOptions options,
            Components components,
            Duration checkpointInterval) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointInterval.toMillis());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        input(env, options)
                .partitionCustom(
                        (Long sequence, int partitions) -> (int) (sequence % partitions),
                        sequence -> sequence)
                .sinkTo(components.input())
                .name("Spanner input")
                .uid("sp1360-input-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
        env.fromSource(
                        components.changeStream(),
                        WatermarkStrategy.noWatermarks(),
                        "Spanner change stream")
                .uid("sp1360-change-stream-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new Delivery(options.phase))
                .name("Spanner relay delivery")
                .uid("sp1360-relay-delivery-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .sinkTo(components.relay())
                .name("Spanner relay")
                .uid("sp1360-relay-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
        env.fromSource(components.seed(), WatermarkStrategy.noWatermarks(), "Spanner seed")
                .uid("sp1360-seed-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new SeedCopy.Throttle(options.seedRowsPerSecond))
                .name("Spanner seed pace")
                .uid("sp1360-seed-pace-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new Delivery(options.phase))
                .name("Spanner seed delivery")
                .uid("sp1360-seed-delivery-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .sinkTo(components.batchCopy())
                .name("Spanner batch copy")
                .uid("sp1360-batch-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
    }

    private static DataStream<Long> input(StreamExecutionEnvironment env, RecoveryOptions options) {
        var source =
                new DataGeneratorSource<Long>(
                        value -> value,
                        options.records,
                        RateLimiterStrategy.perSecond(options.recordsPerSecond),
                        Types.LONG);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Spanner input")
                .uid("sp1360-source-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new RecoveryInput(options))
                .name("Spanner input identity")
                .uid("sp1360-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }
}
