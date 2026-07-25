/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.cloudtasks.sink.createtask.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests against the Cloud Tasks emulator, driving the sink exclusively
 * through the public {@code CloudTasksSink.builder()...emulatorEndpoint(...)} path — no test seams.
 * These are the only tests that build the sink through {@code CloudTasksCreateTaskSink}, so they
 * are what covers the serializer's {@code open(...)} and the writer's construction by the runtime.
 *
 * <p>Both jobs run on the MiniCluster with a rate-limited source, so the streaming one checkpoints
 * several times while records are still arriving and the batch one has nothing but the end-of-input
 * flush. Delivery is asserted at the target the emulator dispatches to; a lost flush shows up as a
 * missing record.
 */
class CloudTasksSinkJobITCase extends AbstractCloudTasksEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    @Test
    void streamingJobDispatchesEveryRecord() throws Exception {
        runJob(RuntimeExecutionMode.STREAMING, "streaming", "/streaming");
    }

    @Test
    void batchJobDispatchesEveryRecord() throws Exception {
        // Batch has no checkpoints, so everything rides the end-of-input flush.
        runJob(RuntimeExecutionMode.BATCH, "batch", "/batch");
    }

    private static void runJob(RuntimeExecutionMode mode, String queueId, String path)
            throws Exception {
        QueueDestination queue = createQueue(queueId);

        // Both travel into the job graph as plain values (the container handle is not
        // serializable).
        String endpoint = emulatorEndpoint();
        String url = targetUrl(path);

        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a permanently
        // failing creation would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(mode);
        env.setParallelism(2);
        if (mode == RuntimeExecutionMode.STREAMING) {
            // 40 records at 10/s outlive several of these, so the checkpoint flush runs while the
            // source is still producing rather than only at the end.
            env.enableCheckpointing(1_000);
        }

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) index -> "record-" + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        CloudTasksSink.<String>builder()
                                .queue(queue)
                                .serializer(
                                        CloudTasksSerializationSchema.httpTarget(url)
                                                .withBody(new SimpleStringSchema()))
                                .emulatorEndpoint(endpoint)
                                .build());

        env.execute("cloudtasks-sink-" + queueId + "-it");

        // Distinct-body equality dedupes at-least-once duplicates while proving every record was
        // dispatched and nothing foreign was.
        Set<String> expected = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            expected.add("record-" + index);
        }
        assertThat(awaitDistinctBodies(path, (int) RECORD_COUNT))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
