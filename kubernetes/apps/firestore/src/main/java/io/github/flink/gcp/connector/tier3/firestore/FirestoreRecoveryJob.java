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

package io.github.flink.gcp.connector.tier3.firestore;

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

import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSinkBuilder;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkBuilder;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * Internal finite workload for one separately admitted Firestore recovery trial.
 *
 * <p>One input reaches the production Native-mode and Datastore-mode sinks, and the production
 * batch source of each mode copies a seeded collection or kind, so one trial recovers all four.
 */
@Internal
public final class FirestoreRecoveryJob {
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(30);
    private static final int FLINK_MAX_PARALLELISM = 128;
    // The number of splits each batch read asks for, above the largest trial parallelism, so a
    // rescale has splits to move and a subtask that finishes early takes another. Both are upper
    // bounds that the service answers below for a small seed.
    private static final int SPLITS = 4;

    private FirestoreRecoveryJob() {}

    /** Runs one trial against the databases the external lifecycle runner created. */
    public static void main(String[] args) throws Exception {
        var options = RecoveryOptions.parse(args);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options, Components.production(options), CHECKPOINT_INTERVAL);
        env.execute("Firestore recovery " + options.runId);
    }

    /** The connectors of one graph; only a test points them at the emulators. */
    record Components(
            Sink<Long> nativeInput,
            Sink<Long> datastoreInput,
            Source<Tuple2<String, Long>, ?, ?> nativeSeed,
            Source<Tuple2<String, Long>, ?, ?> datastoreSeed,
            Sink<Tuple3<String, String, Long>> nativeCopy,
            Sink<Tuple3<String, String, Long>> datastoreCopy) {

        /** The production builders on the workload's ADC. */
        static Components production(RecoveryOptions options) {
            return connectors(options, null, null);
        }

        static Components connectors(
                RecoveryOptions options,
                @Nullable String nativeEmulator,
                @Nullable String datastoreEmulator) {
            FirestoreSinkBuilder<Long> nativeInput =
                    FirestoreSink.<Long>builder()
                            .database(options.nativeDestination())
                            .serializer((sequence, context) -> options.nativeInput(sequence));
            DatastoreSinkBuilder<Long> datastoreInput =
                    DatastoreSink.<Long>builder()
                            .database(options.datastoreDestination())
                            .serializer((sequence, context) -> options.datastoreInput(sequence));
            FirestoreSourceBuilder<Tuple2<String, Long>> nativeSeed =
                    FirestoreSource.<Tuple2<String, Long>>builder()
                            .database(options.nativeDestination())
                            .collectionGroup(RecoveryOptions.SEED)
                            // The emulator does not implement PartitionQuery; a count of one is
                            // the client library's single partition without a call.
                            .partitionCount(nativeEmulator == null ? SPLITS : 1)
                            .deserializer(new SeedCopy.NativeDeserializer());
            DatastoreSourceBuilder<Tuple2<String, Long>> datastoreSeed =
                    DatastoreSource.<Tuple2<String, Long>>builder()
                            .database(options.datastoreDestination())
                            .kind(RecoveryOptions.SEED)
                            .splitCount(SPLITS)
                            .deserializer(new SeedCopy.DatastoreDeserializer());
            FirestoreSinkBuilder<Tuple3<String, String, Long>> nativeCopy =
                    FirestoreSink.<Tuple3<String, String, Long>>builder()
                            .database(options.nativeDestination())
                            .serializer((delivery, context) -> SeedCopy.nativeCopy(delivery));
            DatastoreSinkBuilder<Tuple3<String, String, Long>> datastoreCopy =
                    DatastoreSink.<Tuple3<String, String, Long>>builder()
                            .database(options.datastoreDestination())
                            .serializer(
                                    (delivery, context) ->
                                            SeedCopy.datastoreCopy(options, delivery));
            if (nativeEmulator != null) {
                nativeInput.emulatorEndpoint(nativeEmulator);
                nativeSeed.emulatorEndpoint(nativeEmulator);
                nativeCopy.emulatorEndpoint(nativeEmulator);
            }
            if (datastoreEmulator != null) {
                datastoreInput.emulatorEndpoint(datastoreEmulator);
                datastoreSeed.emulatorEndpoint(datastoreEmulator);
                datastoreCopy.emulatorEndpoint(datastoreEmulator);
            }
            return new Components(
                    nativeInput.build(),
                    datastoreInput.build(),
                    nativeSeed.build(),
                    datastoreSeed.build(),
                    nativeCopy.build(),
                    datastoreCopy.build());
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
        DataStream<Long> input = input(env, options);
        sinkInput(input, components.nativeInput(), "Firestore native input", "native", options);
        sinkInput(
                input,
                components.datastoreInput(),
                "Firestore datastore input",
                "datastore",
                options);
        copy(
                env,
                components.nativeSeed(),
                components.nativeCopy(),
                "Firestore native",
                "native",
                options);
        copy(
                env,
                components.datastoreSeed(),
                components.datastoreCopy(),
                "Firestore datastore",
                "datastore",
                options);
    }

    private static void sinkInput(
            DataStream<Long> input,
            Sink<Long> sink,
            String name,
            String mode,
            RecoveryOptions options) {
        input.partitionCustom(
                        (Long sequence, int partitions) -> (int) (sequence % partitions),
                        sequence -> sequence)
                .sinkTo(sink)
                .name(name)
                .uid("fs1879-" + mode + "-input-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
    }

    private static void copy(
            StreamExecutionEnvironment env,
            Source<Tuple2<String, Long>, ?, ?> seed,
            Sink<Tuple3<String, String, Long>> copy,
            String name,
            String mode,
            RecoveryOptions options) {
        env.fromSource(seed, WatermarkStrategy.noWatermarks(), name + " seed")
                .uid("fs1879-" + mode + "-seed-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new SeedCopy.Throttle(options.seedRecordsPerSecond))
                .name(name + " seed pace")
                .uid("fs1879-" + mode + "-seed-pace-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new Delivery(options.phase))
                .name(name + " seed delivery")
                .uid("fs1879-" + mode + "-seed-delivery-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .sinkTo(copy)
                .name(name + " batch copy")
                .uid("fs1879-" + mode + "-batch-sink-v1")
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
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Firestore input")
                .uid("fs1879-source-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new RecoveryInput(options))
                .name("Firestore input identity")
                .uid("fs1879-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }
}
