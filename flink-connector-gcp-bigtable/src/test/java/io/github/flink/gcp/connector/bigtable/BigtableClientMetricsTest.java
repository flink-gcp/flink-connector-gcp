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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BigtableClientMetrics}: each leaf reads the instrument and the part of it the
 * documentation says, replayed here under the client's own names and units ({@code
 * google-cloud-bigtable} 2.85.0, {@code internal.csm.metrics}). The emulator ITCase shows the real
 * client recording them; this pins which leaf reads which.
 */
class BigtableClientMetricsTest {

    private static final String PREFIX = "bigtable.googleapis.com/internal/client/";
    private static final Attributes MUTATE_ROWS =
            Attributes.of(AttributeKey.stringKey("method"), "Bigtable.MutateRows");

    private final MetricListener listener = new MetricListener();
    private final ClientMetrics metrics =
            BigtableClientMetrics.register(listener.getMetricGroup(), "MutateRows");
    private final Meter meter = metrics.openTelemetry().getMeterProvider().get("bigtable");

    @Test
    void eachHistogramLeafReadsItsInstrumentsCountOrMillisecondSum() {
        // Every count and every sum distinct from all the others, so a leaf reading another
        // instrument, or the other half of its own, cannot pass by coincidence.
        histogram("operation_latencies", 10, 20);
        histogram("attempt_latencies", 10, 20, 30);
        histogram("server_latencies", 40);
        histogram("first_response_latencies", 10, 20, 20, 20);
        histogram("application_latencies", 20, 20, 20, 20, 20);

        assertThat(leaf("operations")).isEqualTo(2);
        assertThat(leaf("operationTimeMillis")).isEqualTo(30);
        assertThat(leaf("attempts")).isEqualTo(3);
        assertThat(leaf("attemptTimeMillis")).isEqualTo(60);
        assertThat(leaf("attemptsWithServerTiming")).isEqualTo(1);
        assertThat(leaf("serverTimeMillis")).isEqualTo(40);
        assertThat(leaf("firstResponseTimeMillis")).isEqualTo(70);
        assertThat(leaf("applicationTimeMillis")).isEqualTo(100);
    }

    @Test
    void throttlingLatencyIsDroppedWithoutAWarning() {
        // 2.85.0 records it in the wrong unit and cumulatively across attempts.
        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            histogram("throttling_latencies", 500);

            assertThat(capture.getMessages()).isEmpty();
        }
        assertThat(listener.getCounter("rpcMethod", "MutateRows", "throttlingTimeMillis"))
                .isEmpty();
    }

    @Test
    void eachSumLeafReadsItsCounter() {
        meter.counterBuilder(PREFIX + "retry_count").setUnit("1").build().add(7, MUTATE_ROWS);
        meter.counterBuilder(PREFIX + "connectivity_error_count")
                .setUnit("1")
                .build()
                .add(9, MUTATE_ROWS);

        assertThat(leaf("retries")).isEqualTo(7);
        assertThat(leaf("connectivityErrors")).isEqualTo(9);
    }

    @Test
    void registersEveryLeafForEveryMethodBeforeAnythingIsRecorded() {
        MetricListener readRows = new MetricListener();
        BigtableClientMetrics.register(readRows.getMetricGroup(), "ReadRows", "SampleRowKeys");

        for (String method : new String[] {"ReadRows", "SampleRowKeys"}) {
            for (String leaf :
                    new String[] {
                        "operations",
                        "operationTimeMillis",
                        "attempts",
                        "attemptTimeMillis",
                        "retries",
                        "connectivityErrors",
                        "attemptsWithServerTiming",
                        "serverTimeMillis",
                        "firstResponseTimeMillis",
                        "applicationTimeMillis"
                    }) {
                assertThat(readRows.getCounter("rpcMethod", method, leaf))
                        .as("%s.%s", method, leaf)
                        .isPresent();
            }
        }
    }

    private void histogram(String instrument, double... values) {
        DoubleHistogram histogram =
                meter.histogramBuilder(PREFIX + instrument).setUnit("ms").build();
        for (double value : values) {
            histogram.record(value, MUTATE_ROWS);
        }
    }

    private long leaf(String leaf) {
        Optional<Counter> counter = listener.getCounter("rpcMethod", "MutateRows", leaf);
        assertThat(counter).as(leaf).isPresent();
        return counter.get().getCount();
    }
}
