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
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;

import java.time.Duration;

/** Internal finite STREAMING workload for one explicitly admitted Cloud Tasks measurement cell. */
@Internal
public final class CloudTasksMeasurementJob {
    private CloudTasksMeasurementJob() {}

    /**
     * Runs one cell; queue administration and lifecycle admission belong to the external runner.
     */
    public static void main(String[] args) throws Exception {
        MeasurementOptions options = MeasurementOptions.parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options);
        env.execute("Cloud Tasks measurement " + options.runId + "/" + options.cellId);
    }

    static void configure(StreamExecutionEnvironment env, MeasurementOptions options) {
        configure(env, options, null);
    }

    static void configure(
            StreamExecutionEnvironment env, MeasurementOptions options, String emulatorEndpoint) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(options.parallelism);
        env.enableCheckpointing(options.checkpointSeconds * 1000L);
        String[] path = options.queue.split("/");
        var writerOptions =
                CloudTasksWriterOptions.builder()
                        .maxInFlightTasks(options.concurrency)
                        .recoveryMaxAttempts(3)
                        .notFoundRecoveryMaxAttempts(1);
        if (emulatorEndpoint == null) {
            writerOptions.channelPoolSize(options.channelPoolSize);
        }
        var builder =
                CloudTasksSink.<Long>builder()
                        .queue(QueueDestination.of(path[1], path[3], path[5]))
                        .serializer(
                                sequence -> {
                                    var body =
                                            MeasurementPayload.create(options.bodyBytes, sequence);
                                    return Task.newBuilder()
                                            .setHttpRequest(
                                                    HttpRequest.newBuilder()
                                                            .setHttpMethod(HttpMethod.POST)
                                                            .setUrl(options.target)
                                                            .setBody(body))
                                            .build();
                                })
                        .writerOptions(writerOptions.build());
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (options.arm == MeasurementOptions.Arm.NAMED_HASH
                || options.arm == MeasurementOptions.Arm.NAMED_RANDOM_CONTROL
                || options.arm == MeasurementOptions.Arm.STAGED_HASH) {
            builder.taskIdExtractor(
                    sequence -> options.runId + "/" + options.cellId + "/" + sequence);
        }
        if (options.arm == MeasurementOptions.Arm.STAGED_HASH
                || options.arm == MeasurementOptions.Arm.STAGED_RANDOM) {
            builder.deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                    .stagedOptions(
                            CloudTasksStagedOptions.builder()
                                    .nameRetention(Duration.ofHours(1))
                                    .clockSkewAllowance(Duration.ofMinutes(1))
                                    .requestTimeout(Duration.ofSeconds(20))
                                    .build());
        }
        var source =
                new DataGeneratorSource<Long>(
                        index -> index,
                        options.records,
                        RateLimiterStrategy.perSecond(options.offeredRate),
                        Types.LONG);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Measurement input")
                .uid("ct1246-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .partitionCustom(
                        (Long key, int count) -> options.partition(key, count), value -> value)
                .sinkTo(ObservedSinks.observe(builder.build(), options))
                .name("Measured Cloud Tasks " + options.arm)
                .uid("ct1246-sink-v1")
                .setParallelism(options.parallelism)
                .setMaxParallelism(128);
    }
}
