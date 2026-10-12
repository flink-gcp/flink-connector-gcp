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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;

import javax.annotation.Nullable;

import java.time.Duration;

/** Internal finite workload for one separately admitted Cloud Tasks recovery trial. */
@Internal
public final class CloudTasksRecoveryJob {
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(30);

    private CloudTasksRecoveryJob() {}

    /** Runs one trial against the paused queue the external lifecycle runner created. */
    public static void main(String[] args) throws Exception {
        var options = RecoveryOptions.parse(args);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options, sink(options, null), CHECKPOINT_INTERVAL);
        env.execute("Cloud Tasks recovery " + options.runId);
    }

    static void configure(
            StreamExecutionEnvironment env,
            RecoveryOptions options,
            Sink<Long> sink,
            Duration checkpointInterval) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointInterval.toMillis());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        input(env, options)
                .partitionCustom(
                        (Long sequence, int partitions) -> (int) (sequence % partitions),
                        sequence -> sequence)
                .sinkTo(sink)
                .name("Cloud Tasks EXACTLY_ONCE")
                .uid("ct1362-sink-v1")
                .setParallelism(2)
                .setMaxParallelism(128);
    }

    private static SingleOutputStreamOperator<Long> input(
            StreamExecutionEnvironment env, RecoveryOptions options) {
        var source =
                new DataGeneratorSource<Long>(
                        value -> value,
                        options.records,
                        RateLimiterStrategy.perSecond(options.recordsPerSecond),
                        Types.LONG);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Cloud Tasks input")
                .uid("ct1362-source-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new RecoveryInput(options))
                .name("Cloud Tasks input identity")
                .uid("ct1362-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    /**
     * The production staged sink with its default staging options, so the committer reads the
     * queue's retention back before it creates a task. Only a test passes an endpoint.
     */
    static Sink<Long> sink(RecoveryOptions options, @Nullable String emulatorEndpoint) {
        var builder =
                CloudTasksSink.<Long>builder()
                        .queue(
                                QueueDestination.of(
                                        RecoveryOptions.PROJECT,
                                        RecoveryOptions.LOCATION,
                                        options.queue))
                        .serializer(options::task)
                        .taskIdExtractor(options::taskKey)
                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        return builder.build();
    }
}
