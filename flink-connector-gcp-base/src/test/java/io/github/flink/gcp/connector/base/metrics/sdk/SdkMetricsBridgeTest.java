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

package io.github.flink.gcp.connector.base.metrics.sdk;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.testutils.LogCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SdkMetricsBridge}, driving a meter of the bridge's own {@link
 * SdkMetricsBridge#openTelemetry()} the way a client library records into it.
 */
class SdkMetricsBridgeTest {

    private static final AttributeKey<String> METHOD = AttributeKey.stringKey("method");
    private static final AttributeKey<String> STATUS = AttributeKey.stringKey("status");
    private static final String RETRIES = "client/retry_count";
    private static final String LATENCIES = "client/operation_latencies";

    private final AtomicLong clock = new AtomicLong();
    private final AtomicBoolean failCollects = new AtomicBoolean();
    private final SdkMetricsBridge bridge =
            SdkMetricsBridge.create(
                    "method",
                    clock::get,
                    collect ->
                            () -> {
                                if (failCollects.get()) {
                                    throw new IllegalStateException("collect failed");
                                }
                                return collect.get();
                            });
    private final Meter meter = bridge.openTelemetry().getMeterProvider().get("client");
    private final LongCounter retries = meter.counterBuilder(RETRIES).build();
    private final DoubleHistogram latencies =
            meter.histogramBuilder(LATENCIES).setUnit("ms").build();

    @Test
    void aSumCounterReadsTheCumulativeValueOfItsMethod() {
        Counter mutateRows = bridge.sum(RETRIES, "MutateRows");
        Counter readRows = bridge.sum(RETRIES, "ReadRows");

        retries.add(2, method("MutateRows"));
        retries.add(3, method("MutateRows"));
        retries.add(7, method("ReadRows"));

        assertThat(mutateRows.getCount()).isEqualTo(5);
        assertThat(readRows.getCount()).isEqualTo(7);
    }

    @Test
    void aHistogramReadsAsItsCountAndItsSumInMilliseconds() {
        Counter operations = bridge.histogramCount(LATENCIES, "MutateRows");
        Counter operationMillis = bridge.histogramSumMillis(LATENCIES, "MutateRows");

        latencies.record(10.25, method("MutateRows"));
        latencies.record(30.5, method("MutateRows"));

        assertThat(operations.getCount()).isEqualTo(2);
        // The sum, never a per-event value: 40.75 rounds to 41.
        assertThat(operationMillis.getCount()).isEqualTo(41);
    }

    @Test
    void aHistogramInSecondsIsConvertedToMilliseconds() {
        DoubleHistogram seconds = meter.histogramBuilder("request.duration").setUnit("s").build();
        Counter requestMillis = bridge.histogramSumMillis("request.duration", "CreateTask");

        seconds.record(0.25, method("CreateTask"));
        seconds.record(1.5, method("CreateTask"));

        assertThat(requestMillis.getCount()).isEqualTo(1750);
    }

    @Test
    void aHistogramInAnotherUnitReportsItsCountButNotItsSum() {
        DoubleHistogram bytes = meter.histogramBuilder("payload.size").setUnit("By").build();
        Counter payloads = bridge.histogramCount("payload.size", "MutateRows");
        Counter payloadMillis = bridge.histogramSumMillis("payload.size", "MutateRows");

        try (LogCapture capture = LogCapture.of(SdkMetricsBridge.class)) {
            bytes.record(512, method("MutateRows"));
            bytes.record(512, method("MutateRows"));

            assertThat(payloads.getCount()).isEqualTo(2);
            assertThat(payloadMillis.getCount()).isZero();
            assertThat(capture.getMessages())
                    .singleElement(InstanceOfAssertFactories.STRING)
                    .contains("payload.size", "'By'");
        }
    }

    @Test
    void everyAttributeButTheMethodIsAggregatedAway() {
        Counter mutateRows = bridge.sum(RETRIES, "MutateRows");

        retries.add(1, Attributes.of(METHOD, "MutateRows", STATUS, "OK"));
        retries.add(4, Attributes.of(METHOD, "MutateRows", STATUS, "UNAVAILABLE"));

        // One series for the method: the status split never reaches the snapshot.
        assertThat(mutateRows.getCount()).isEqualTo(5);
    }

    @Test
    void collectsAtMostOncePerIntervalHoweverOftenTheReportersRead() {
        Counter mutateRows = bridge.sum(RETRIES, "MutateRows");
        retries.add(1, method("MutateRows"));
        assertThat(mutateRows.getCount()).isEqualTo(1);

        retries.add(1, method("MutateRows"));
        clock.addAndGet(SdkMetricsBridge.COLLECT_INTERVAL_NANOS - 1);
        assertThat(mutateRows.getCount()).isEqualTo(1);

        clock.addAndGet(1);
        assertThat(mutateRows.getCount()).isEqualTo(2);
    }

    @Test
    void theSecondCounterOfASweepReadsTheFirstOnesCollect() {
        Counter operations = bridge.histogramCount(LATENCIES, "MutateRows");
        Counter operationMillis = bridge.histogramSumMillis(LATENCIES, "MutateRows");
        latencies.record(10, method("MutateRows"));
        assertThat(operations.getCount()).isEqualTo(1);

        // Recorded between two reads of one sweep: the sum must agree with the count it is
        // divided by, so it reads the same snapshot rather than collecting again.
        latencies.record(10, method("MutateRows"));
        assertThat(operationMillis.getCount()).isEqualTo(10);
    }

    @Test
    void aReadNeverRegistersAMetric() {
        MetricListener listener = new MetricListener();
        MetricGroup group = listener.getMetricGroup();
        Counter registered = bridge.sum(RETRIES, "MutateRows");
        group.counter("retries", registered);

        retries.add(1, method("MutateRows"));
        retries.add(1, method("CheckAndMutateRow"));
        try (LogCapture capture = LogCapture.of(SdkMetricsBridge.class)) {
            registered.getCount();
            registered.getCount();

            // A reporter's read is where a registration would corrupt the reporter's own maps,
            // so the unregistered method is logged, once, and nothing else happens.
            assertThat(capture.getMessages())
                    .singleElement(InstanceOfAssertFactories.STRING)
                    .contains("CheckAndMutateRow");
        }
        assertThat(listener.getCounter("CheckAndMutateRow", "retries")).isEmpty();
    }

    @Test
    void aFailingCollectKeepsThePreviousValuesAndIsLoggedOnce() {
        Counter mutateRows = bridge.sum(RETRIES, "MutateRows");
        retries.add(3, method("MutateRows"));
        assertThat(mutateRows.getCount()).isEqualTo(3);

        failCollects.set(true);
        retries.add(1, method("MutateRows"));
        try (LogCapture capture = LogCapture.of(SdkMetricsBridge.class)) {
            clock.addAndGet(SdkMetricsBridge.COLLECT_INTERVAL_NANOS);
            assertThat(mutateRows.getCount()).isEqualTo(3);
            clock.addAndGet(SdkMetricsBridge.COLLECT_INTERVAL_NANOS);
            assertThat(mutateRows.getCount()).isEqualTo(3);

            assertThat(capture.getEvents()).hasSize(1);
        }

        failCollects.set(false);
        clock.addAndGet(SdkMetricsBridge.COLLECT_INTERVAL_NANOS);
        assertThat(mutateRows.getCount()).isEqualTo(4);
    }

    @Test
    void closeCollectsOnceMoreAndThenFreezesTheValues() {
        Counter mutateRows = bridge.sum(RETRIES, "MutateRows");
        retries.add(2, method("MutateRows"));
        assertThat(mutateRows.getCount()).isEqualTo(2);

        retries.add(3, method("MutateRows"));
        bridge.close();
        assertThat(mutateRows.getCount()).isEqualTo(5);

        // Nothing recorded after the close reaches the counter, and it never falls back to zero.
        retries.add(100, method("MutateRows"));
        clock.addAndGet(SdkMetricsBridge.COLLECT_INTERVAL_NANOS);
        assertThat(mutateRows.getCount()).isEqualTo(5);

        bridge.close();
        assertThat(mutateRows.getCount()).isEqualTo(5);
    }

    @Test
    void aCounterWithNothingRecordedReadsZero() {
        assertThat(bridge.sum(RETRIES, "ReadRows").getCount()).isZero();
        assertThat(bridge.histogramCount(LATENCIES, "ReadRows").getCount()).isZero();
    }

    @Test
    void aBridgedCounterIsReadOnly() {
        Counter counter = bridge.sum(RETRIES, "MutateRows");

        assertThatThrownBy(counter::inc).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.inc(2)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(counter::dec).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.dec(2)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static Attributes method(String method) {
        return Attributes.of(METHOD, method);
    }
}
