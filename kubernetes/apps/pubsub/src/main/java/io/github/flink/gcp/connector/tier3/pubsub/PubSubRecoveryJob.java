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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

/** Internal DataStream relay for independently supervised Pub/Sub recovery trials. */
@Internal
public final class PubSubRecoveryJob {
    private PubSubRecoveryJob() {}

    /** Runs the relay against pre-created resources using workload ADC. */
    public static void main(String[] args) throws Exception {
        RecoveryOptions options = RecoveryOptions.parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(options.parallelism);
        env.setMaxParallelism(128);
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        attach(env, options, null);
        env.execute("Pub/Sub Tier-3 recovery " + options.runId);
    }

    static void attach(StreamExecutionEnvironment env, RecoveryOptions options, String emulator) {
        var source =
                PubSubSource.<String>builder()
                        .subscriptions(
                                SubscriptionDestination.of(
                                        RecoveryOptions.PROJECT, options.input(0)),
                                SubscriptionDestination.of(
                                        RecoveryOptions.PROJECT, options.input(1)))
                        .deserializer(new RecoveryDeserializer(options));
        var sink =
                PubSubSink.<String>builder()
                        .topic(TopicDestination.of(RecoveryOptions.PROJECT, options.output()))
                        .serializer(PubSubSerializationSchema.payload(new SimpleStringSchema()))
                        .createDisposition(CreateDisposition.CREATE_NEVER);
        if (emulator != null) {
            source.emulatorEndpoint(emulator);
            sink.emulatorEndpoint(emulator);
        }
        env.fromSource(source.build(), WatermarkStrategy.noWatermarks(), "Pub/Sub input")
                .uid("pubsub-input-v1")
                .setParallelism(options.parallelism)
                .map(new RecoveryObserver(options))
                .uid("pubsub-observer-v1")
                .setParallelism(options.parallelism)
                .sinkTo(sink.build())
                .uid("pubsub-output-v1")
                .setParallelism(options.parallelism);
    }
}
