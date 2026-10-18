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

package io.github.flink.gcp.connector.tier3.bigtable;

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

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.sink.BigtableDeliveryGuarantee;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;

import java.time.Duration;

/**
 * Internal finite workload for one separately admitted Bigtable recovery trial.
 *
 * <p>One input reaches the production sink in both delivery modes, and the production change-
 * stream source relays what the {@code EXACTLY_ONCE} sink applied, so one trial recovers all three.
 */
@Internal
public final class BigtableRecoveryJob {
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(30);
    private static final int FLINK_MAX_PARALLELISM = 128;

    private BigtableRecoveryJob() {}

    /** Runs one trial against the instance the external lifecycle runner created. */
    public static void main(String[] args) throws Exception {
        var options = RecoveryOptions.parse(args);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options, Components.production(options), CHECKPOINT_INTERVAL);
        env.execute("Bigtable recovery " + options.runId);
    }

    /** The connectors of one graph; only a test replaces the production ones. */
    record Components(
            Sink<Long> exactlyOnce,
            Sink<Long> atLeastOnce,
            Source<Tuple2<String, Long>, ?, ?> changeStream,
            Sink<Tuple3<String, String, Long>> relay) {

        /** The production builders on the native transport and the workload's ADC. */
        static Components production(RecoveryOptions options) {
            return new Components(
                    BigtableSink.<Long>builder()
                            .table(options.table(RecoveryOptions.EXACTLY_ONCE_TABLE))
                            .appProfileId(RecoveryOptions.APP_PROFILE)
                            .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                            .stagedOptions(
                                    BigtableStagedOptions.builder()
                                            .markerFamily(RecoveryOptions.MARKER_FAMILY)
                                            .build())
                            .serializer((sequence, context) -> options.contribution(sequence))
                            .build(),
                    BigtableSink.<Long>builder()
                            .table(options.table(RecoveryOptions.AT_LEAST_ONCE_TABLE))
                            .appProfileId(RecoveryOptions.APP_PROFILE)
                            .deliveryGuarantee(BigtableDeliveryGuarantee.AT_LEAST_ONCE)
                            .serializer((sequence, context) -> options.contribution(sequence))
                            .build(),
                    BigtableChangeStreamSource.<Tuple2<String, Long>>builder()
                            .table(options.table(RecoveryOptions.EXACTLY_ONCE_TABLE))
                            .appProfileId(RecoveryOptions.APP_PROFILE)
                            .deserializer(new ChangeRelay.Deserializer())
                            .startPosition(StartPosition.at(options.changeStreamStart))
                            .boundedTimestamp(options.changeStreamEnd)
                            .build(),
                    BigtableSink.<Tuple3<String, String, Long>>builder()
                            .table(options.table(RecoveryOptions.RELAY_TABLE))
                            .appProfileId(RecoveryOptions.APP_PROFILE)
                            .deliveryGuarantee(BigtableDeliveryGuarantee.AT_LEAST_ONCE)
                            .serializer((delivery, context) -> ChangeRelay.mutation(delivery))
                            .build());
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
        DataStream<Long> input =
                input(env, options)
                        .partitionCustom(
                                (Long sequence, int partitions) -> (int) (sequence % partitions),
                                sequence -> sequence);
        input.sinkTo(components.exactlyOnce())
                .name("Bigtable EXACTLY_ONCE")
                .uid("bt1316-eo-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
        input.sinkTo(components.atLeastOnce())
                .name("Bigtable AT_LEAST_ONCE")
                .uid("bt1316-alo-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM);
        env.fromSource(
                        components.changeStream(),
                        WatermarkStrategy.noWatermarks(),
                        "Bigtable change stream")
                .uid("bt1316-change-stream-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .map(new ChangeRelay.Delivery(options.phase))
                .name("Bigtable relay delivery")
                .uid("bt1316-delivery-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(FLINK_MAX_PARALLELISM)
                .sinkTo(components.relay())
                .name("Bigtable relay")
                .uid("bt1316-relay-sink-v1")
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
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Bigtable input")
                .uid("bt1316-source-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new RecoveryInput(options))
                .name("Bigtable input identity")
                .uid("bt1316-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }
}
