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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SpannerClientMetrics}: each leaf reads the instrument and the part of it the
 * documentation says, replayed here under the client's own names, units and meters ({@code
 * google-cloud-spanner} 6.124.0's {@code BuiltInMetricsRecorder} and gax's {@code
 * OpenTelemetryMetricsRecorder}). The emulator ITCase shows the real client recording them; this
 * pins which leaf reads which.
 */
class SpannerClientMetricsTest {

    private static final String PREFIX = "spanner/client/";
    private static final Attributes BATCH_WRITE =
            Attributes.builder()
                    .put(AttributeKey.stringKey("method"), "Spanner.BatchWrite")
                    // Labels the client records with and the series ignore.
                    .put(AttributeKey.stringKey("status"), "OK")
                    .put(AttributeKey.stringKey("client_uid"), "uid")
                    .build();

    private static final String[] LEAVES = {
        "operations",
        "operationTimeMillis",
        "attempts",
        "attemptTimeMillis",
        "attemptsWithGfeTiming",
        "gfeTimeMillis",
        "gfeConnectivityErrors",
        "attemptsWithAfeTiming",
        "afeTimeMillis",
        "afeConnectivityErrors"
    };

    private final MetricListener listener = new MetricListener();
    private final ClientMetrics metrics =
            SpannerClientMetrics.register(
                    listener.getMetricGroup(), SpannerClientMetrics.BATCH_WRITE);
    private final Meter gax = metrics.openTelemetry().meterBuilder("gax-java").build();
    private final Meter spanner = metrics.openTelemetry().meterBuilder("spanner-java").build();

    @Test
    void eachLeafReadsItsInstrumentAndTheRightPartOfIt() {
        // Every value distinct from all the others, so a leaf reading another instrument, or the
        // other half of its own, cannot pass by coincidence.
        counter(gax, "operation_count", 2);
        histogram(gax, "operation_latency", 10, 20);
        counter(gax, "attempt_count", 3);
        histogram(gax, "attempt_latency", 15, 25, 30);
        histogram(spanner, "gfe_latencies", 40, 50, 60, 70);
        counter(spanner, "gfe_connectivity_error_count", 5);
        histogram(spanner, "afe_latencies", 1, 2, 3, 4, 7, 8);
        counter(spanner, "afe_connectivity_error_count", 9);

        assertThat(leaf("operations")).isEqualTo(2);
        assertThat(leaf("operationTimeMillis")).isEqualTo(30);
        assertThat(leaf("attempts")).isEqualTo(3);
        assertThat(leaf("attemptTimeMillis")).isEqualTo(70);
        assertThat(leaf("attemptsWithGfeTiming")).isEqualTo(4);
        assertThat(leaf("gfeTimeMillis")).isEqualTo(220);
        assertThat(leaf("gfeConnectivityErrors")).isEqualTo(5);
        assertThat(leaf("attemptsWithAfeTiming")).isEqualTo(6);
        assertThat(leaf("afeTimeMillis")).isEqualTo(25);
        assertThat(leaf("afeConnectivityErrors")).isEqualTo(9);
    }

    @Test
    void theLatencyHistogramsCountsAreKeptSilentlyBesideTheCounters() {
        // The client records each latency where it counts the operation or attempt, so the
        // histogram's own count is not reported; recording into it must not warn either.
        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            histogram(gax, "operation_latency", 12);
            histogram(gax, "attempt_latency", 12);

            assertThat(capture.getMessages()).isEmpty();
        }
        assertThat(leaf("operations")).isZero();
        assertThat(leaf("attempts")).isZero();
    }

    @Test
    void registersEveryLeafForEveryMethodBeforeAnythingIsRecorded() {
        MetricListener reader = new MetricListener();
        SpannerClientMetrics.register(
                reader.getMetricGroup(),
                SpannerClientMetrics.EXECUTE_STREAMING_SQL,
                SpannerClientMetrics.STREAMING_READ);

        for (String method : new String[] {"ExecuteStreamingSql", "StreamingRead"}) {
            for (String leaf : LEAVES) {
                Optional<Counter> counter = reader.getCounter("rpcMethod", method, leaf);
                assertThat(counter).as("%s.%s", method, leaf).isPresent();
                assertThat(counter.get().getCount()).as("%s.%s", method, leaf).isZero();
            }
        }
    }

    private static void counter(Meter meter, String instrument, long value) {
        LongCounter counter = meter.counterBuilder(PREFIX + instrument).setUnit("1").build();
        counter.add(value, BATCH_WRITE);
    }

    private static void histogram(Meter meter, String instrument, double... values) {
        DoubleHistogram histogram =
                meter.histogramBuilder(PREFIX + instrument).setUnit("ms").build();
        for (double value : values) {
            histogram.record(value, BATCH_WRITE);
        }
    }

    private long leaf(String leaf) {
        Optional<Counter> counter = listener.getCounter("rpcMethod", "BatchWrite", leaf);
        assertThat(counter).as(leaf).isPresent();
        return counter.get().getCount();
    }

    @Test
    void enumeratorsOnOneGroupShareOneSetAndEachMethodIsRegisteredOnce() {
        // A HybridSource's sub-sources share one enumerator group and call different methods.
        MetricListener listener = new MetricListener();

        ClientMetrics first =
                SpannerClientMetrics.registerForEnumerators(
                        new InternalSplitEnumeratorMetricGroup(listener.getMetricGroup()),
                        "PartitionQuery");
        ClientMetrics second =
                SpannerClientMetrics.registerForEnumerators(
                        new InternalSplitEnumeratorMetricGroup(listener.getMetricGroup()),
                        "PartitionQuery",
                        "ExecuteStreamingSql");

        assertThat(second).isSameAs(first);
        assertThat(listener.getCounter("enumerator", "rpcMethod", "PartitionQuery", "operations"))
                .isPresent();
        assertThat(
                        listener.getCounter(
                                "enumerator", "rpcMethod", "ExecuteStreamingSql", "operations"))
                .isPresent();
        assertThat(SpannerClientMetrics.registerForEnumerators(null, "PartitionQuery")).isNull();
    }
}
