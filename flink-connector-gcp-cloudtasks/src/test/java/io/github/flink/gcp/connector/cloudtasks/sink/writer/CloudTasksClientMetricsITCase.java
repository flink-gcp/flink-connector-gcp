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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

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

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The client RPC metrics of {@code exportClientMetricsToFlink(true)}, observed the way a user
 * observes them: through a configured Flink metric reporter, while a job creates tasks in the
 * emulator.
 *
 * <p>The real client records them: gax's tracer factory measures each call whatever the endpoint,
 * so the job runs the production client construction against the emulator. The at-least-once writer
 * registers the series; under {@code EXACTLY_ONCE} the committer does, since it owns the client
 * there.
 */
class CloudTasksClientMetricsITCase extends AbstractCloudTasksEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    @BeforeEach
    void resetReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @AfterEach
    void clearReporter() {
        RpcMethodSeriesReporter.reset();
    }

    @ParameterizedTest
    @EnumSource(CloudTasksDeliveryGuarantee.class)
    void aReporterSeesTheClientsCreateTaskSeries(CloudTasksDeliveryGuarantee guarantee)
            throws Exception {
        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            runJob("client-metrics-" + guarantee.name().toLowerCase().replace('_', '-'), guarantee);

            // Registered by one component only, at parallelism 1: the writer, or the committer
            // under EXACTLY_ONCE, which owns the client there.
            assertThat(RpcMethodSeriesReporter.registrations())
                    .containsOnly(
                            entry("CreateTask.operations", 1L),
                            entry("CreateTask.operationTimeMillis", 1L));
            Map<String, Long> reported = RpcMethodSeriesReporter.reported();
            assertThat(reported)
                    .containsOnlyKeys("CreateTask.operations", "CreateTask.operationTimeMillis");
            // A report read the values while the job ran, which is what a reporter ever sees: the
            // owning component's group is unregistered as the task closes.
            assertThat(reported.get("CreateTask.operations")).isPositive();

            // Read from the values the client metrics kept when the client closed. Every record
            // is one CreateTask call at least; a sink-owned retry would be one more. A call
            // recorded twice would reach twice the record count, which retries against the
            // emulator do not approach.
            Map<String, Long> last = RpcMethodSeriesReporter.current();
            assertThat(last.get("CreateTask.operations"))
                    .isBetween(RECORD_COUNT, 2 * RECORD_COUNT - 1);
            assertThat(last.get("CreateTask.operationTimeMillis")).isPositive();
            // Every RPC the client reported was one the component registered a series for.
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void nothingIsRegisteredWhenTheExportIsOff() throws Exception {
        runJob("client-metrics-off", CloudTasksDeliveryGuarantee.AT_LEAST_ONCE, false);

        // The reporter ran and saw the job's other metrics, so the empty answer is not its absence.
        assertThat(RpcMethodSeriesReporter.sawAnyMetric()).isTrue();
        assertThat(RpcMethodSeriesReporter.reported()).isEmpty();
    }

    private static void runJob(String queueId, CloudTasksDeliveryGuarantee guarantee)
            throws Exception {
        runJob(queueId, guarantee, true);
    }

    private static void runJob(
            String queueId, CloudTasksDeliveryGuarantee guarantee, boolean exportToFlink)
            throws Exception {
        QueueDestination queue = createQueue(queueId);
        String endpoint = emulatorEndpoint();
        String url = targetUrl("/" + queueId);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        configuration.setString(
                "metrics.reporter.client.factory.class", RpcMethodSeriesReporter.class.getName());
        configuration.setString("metrics.reporter.client.interval", "100 ms");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(1_000);

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
                                .deliveryGuarantee(guarantee)
                                .serializer(
                                        CloudTasksSerializationSchema.httpTarget(url)
                                                .withBody(new SimpleStringSchema()))
                                .emulatorEndpoint(endpoint)
                                .exportClientMetricsToFlink(exportToFlink)
                                .build());

        env.execute("cloudtasks-client-metrics-it");

        assertThat(awaitDistinctBodies("/" + queueId, (int) RECORD_COUNT))
                .hasSize((int) RECORD_COUNT);
    }
}
