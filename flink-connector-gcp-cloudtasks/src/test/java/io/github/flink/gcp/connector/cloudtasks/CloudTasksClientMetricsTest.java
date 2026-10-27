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

package io.github.flink.gcp.connector.cloudtasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CloudTasksClientMetrics}: each leaf reads the part of gax's histogram the
 * documentation says, replayed here under gax's own instrument name, unit and attribute (gax
 * 2.87.0, {@code GoldenSignalsMetricsRecorder}). The emulator ITCase shows the real client
 * recording it; this pins which leaf reads which, and the conversion from seconds.
 */
class CloudTasksClientMetricsTest {

    private static final Attributes CREATE_TASK =
            Attributes.of(
                    AttributeKey.stringKey("rpc.method"),
                    "google.cloud.tasks.v2.CloudTasks/CreateTask");

    private final MetricListener listener = new MetricListener();
    private final ClientMetrics metrics =
            CloudTasksClientMetrics.register(listener.getMetricGroup());
    private final DoubleHistogram requestDuration =
            metrics.openTelemetry()
                    .getMeterProvider()
                    .get("com.google.cloud:google-cloud-tasks")
                    .histogramBuilder("gcp.client.request.duration")
                    .setUnit("s")
                    .build();

    @Test
    void operationsCountTheCallsAndTheirTimeIsTheSumInMilliseconds() {
        requestDuration.record(0.25, CREATE_TASK);
        requestDuration.record(0.0105, CREATE_TASK);

        assertThat(leaf("operations")).isEqualTo(2);
        // 250 ms + 10.5 ms, rounded once on the read rather than per call.
        assertThat(leaf("operationTimeMillis")).isEqualTo(261);
    }

    @Test
    void theStatusAndEveryOtherAttributeAreAggregatedOut() {
        requestDuration.record(
                0.1,
                CREATE_TASK.toBuilder()
                        .put("rpc.response.status_code", "OK")
                        .put("server.address", "cloudtasks.googleapis.com")
                        .build());
        requestDuration.record(
                0.2,
                CREATE_TASK.toBuilder().put("rpc.response.status_code", "UNAVAILABLE").build());

        assertThat(leaf("operations")).isEqualTo(2);
        assertThat(leaf("operationTimeMillis")).isEqualTo(300);
    }

    @Test
    void registersBothLeavesBeforeAnythingIsRecorded() {
        assertThat(leaf("operations")).isZero();
        assertThat(leaf("operationTimeMillis")).isZero();
    }

    private long leaf(String leaf) {
        Optional<Counter> counter = listener.getCounter("rpcMethod", "CreateTask", leaf);
        assertThat(counter).as(leaf).isPresent();
        return counter.get().getCount();
    }
}
