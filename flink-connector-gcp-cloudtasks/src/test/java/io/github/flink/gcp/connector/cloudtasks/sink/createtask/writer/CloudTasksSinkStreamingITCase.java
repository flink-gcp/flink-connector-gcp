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

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end streaming integration test against the Cloud Tasks emulator, driving the sink
 * exclusively through the public {@code CloudTasksSink.builder()...emulatorEndpoint(...)} path — no
 * test seams.
 *
 * <p>Runs a MiniCluster DataStream job in streaming mode with a rate-limited source that spans
 * several 1-second checkpoints, so the checkpoint flush — the sink's whole delivery guarantee —
 * fires mid-stream rather than only at end of input, and asserts the tasks were dispatched to the
 * target.
 */
class CloudTasksSinkStreamingITCase extends AbstractCloudTasksEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    private static final String PATH = "/streaming";

    @Test
    void streamingJobDispatchesEveryRecordAcrossCheckpoints() throws Exception {
        QueueDestination queue = createQueue("streaming");

        // Both travel into the job graph as plain values (the container handle is not
        // serializable).
        String endpoint = emulatorEndpoint();
        String url = targetUrl(PATH);

        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a permanently
        // failing creation would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(1_000);
        env.setParallelism(2);

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

        env.execute("cloudtasks-sink-streaming-it");

        // Distinct-body equality dedupes at-least-once duplicates while proving every record was
        // dispatched and nothing foreign was.
        Set<String> expected = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            expected.add("record-" + index);
        }
        List<RecordedRequest> requests =
                awaitRequests(PATH, (int) RECORD_COUNT, Duration.ofSeconds(60));
        assertThat(requests.stream().map(request -> request.body).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
