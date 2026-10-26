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

package io.github.flink.gcp.connector.base.metrics;

import org.apache.flink.metrics.Counter;

import io.github.flink.gcp.connector.testutils.LogCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.context.Context;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ClientMetrics}, recording through its {@link ClientMetrics#openTelemetry()} the
 * way a client library does.
 */
class ClientMetricsTest {

    private static final AttributeKey<String> METHOD = AttributeKey.stringKey("method");
    private static final AttributeKey<String> STATUS = AttributeKey.stringKey("status");
    private static final String RETRIES = "client/retry_count";
    private static final String LATENCIES = "client/operation_latencies";

    private final ClientMetrics metrics = ClientMetrics.create("method");
    private final Meter meter = metrics.openTelemetry().getMeterProvider().get("client");

    @Test
    void aLongCounterTotalsWhatWasAddedForItsMethod() {
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");
        Counter readRows = metrics.counterSum(RETRIES, "ReadRows");
        LongCounter retries = meter.counterBuilder(RETRIES).build();

        retries.add(2, method("MutateRows"));
        retries.add(3, method("MutateRows"));
        retries.add(7, method("ReadRows"));

        assertThat(mutateRows.getCount()).isEqualTo(5);
        assertThat(readRows.getCount()).isEqualTo(7);
    }

    @Test
    void aHistogramReadsAsItsCountAndItsSumInMilliseconds() {
        Counter operations = metrics.histogramCount(LATENCIES, "MutateRows");
        Counter operationMillis = metrics.histogramSumMillis(LATENCIES, "MutateRows");
        DoubleHistogram latencies = meter.histogramBuilder(LATENCIES).setUnit("ms").build();

        latencies.record(10.25, method("MutateRows"));
        latencies.record(30.5, method("MutateRows"), Context.root());

        assertThat(operations.getCount()).isEqualTo(2);
        assertThat(operationMillis.getCount()).isEqualTo(41);
    }

    @Test
    void theSumIsRoundedOnceWhenReadNotPerValue() {
        Counter operationMillis = metrics.histogramSumMillis(LATENCIES, "MutateRows");
        DoubleHistogram latencies = meter.histogramBuilder(LATENCIES).setUnit("ms").build();

        latencies.record(0.4, method("MutateRows"));
        latencies.record(0.4, method("MutateRows"));

        // 0.8 rounds to 1; rounding each value first would read 0.
        assertThat(operationMillis.getCount()).isEqualTo(1);
    }

    @Test
    void zeroIsAValueLikeAnyOther() {
        // A server timing of zero milliseconds, or a connectivity count of zero for an attempt
        // that reached the server, is a real recording: counted, and never warned about.
        Counter attempts = metrics.histogramCount(LATENCIES, "MutateRows");
        Counter errors = metrics.counterSum(RETRIES, "MutateRows");
        DoubleHistogram latencies = meter.histogramBuilder(LATENCIES).setUnit("ms").build();
        LongCounter counter = meter.counterBuilder(RETRIES).build();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            latencies.record(0, method("MutateRows"));
            counter.add(0, method("MutateRows"));

            assertThat(attempts.getCount()).isEqualTo(1);
            assertThat(errors.getCount()).isZero();
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void aHistogramInSecondsIsConvertedToMilliseconds() {
        Counter requestMillis = metrics.histogramSumMillis("request.duration", "CreateTask");
        DoubleHistogram seconds = meter.histogramBuilder("request.duration").setUnit("s").build();

        seconds.record(0.25, method("CreateTask"));
        seconds.record(1.5, method("CreateTask"));

        assertThat(requestMillis.getCount()).isEqualTo(1750);
    }

    @Test
    void aLongHistogramRecordsLikeADoubleOne() {
        Counter requests = metrics.histogramCount(LATENCIES, "MutateRows");
        Counter requestMillis = metrics.histogramSumMillis(LATENCIES, "MutateRows");
        // The unit set after ofLongs(), as the clients also write it.
        LongHistogram latencies = meter.histogramBuilder(LATENCIES).ofLongs().setUnit("ms").build();

        latencies.record(4, method("MutateRows"));
        latencies.record(6, method("MutateRows"), Context.root());

        assertThat(requests.getCount()).isEqualTo(2);
        assertThat(requestMillis.getCount()).isEqualTo(10);
    }

    @Test
    void aLongHistogramKeepsAUnitSetBeforeOfLongs() {
        Counter requestMillis = metrics.histogramSumMillis("request.duration", "CreateTask");
        LongHistogram seconds =
                meter.histogramBuilder("request.duration").setUnit("s").ofLongs().build();

        seconds.record(2, method("CreateTask"));

        assertThat(requestMillis.getCount()).isEqualTo(2000);
    }

    @Test
    void aHistogramInAnotherUnitReportsItsCountButNotItsSum() {
        Counter payloads = metrics.histogramCount("payload.size", "MutateRows");
        Counter payloadMillis = metrics.histogramSumMillis("payload.size", "MutateRows");

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            DoubleHistogram bytes = meter.histogramBuilder("payload.size").setUnit("By").build();
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
    void everyAttributeButTheMethodIsIgnored() {
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");
        LongCounter retries = meter.counterBuilder(RETRIES).build();

        retries.add(1, Attributes.of(METHOD, "MutateRows", STATUS, "OK"));
        retries.add(4, Attributes.of(METHOD, "MutateRows", STATUS, "UNAVAILABLE"));

        assertThat(mutateRows.getCount()).isEqualTo(5);
    }

    @Test
    void aReadSeesEveryRecordingBeforeIt() {
        // No collection step: a value recorded between two reads is in the second one.
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");
        LongCounter retries = meter.counterBuilder(RETRIES).build();

        retries.add(1, method("MutateRows"));
        assertThat(mutateRows.getCount()).isEqualTo(1);
        retries.add(1, method("MutateRows"), Context.root());
        assertThat(mutateRows.getCount()).isEqualTo(2);
    }

    @Test
    void anInstrumentBuiltBeforeItsCounterIsRegisteredStillCounts() {
        LongCounter retries = meter.counterBuilder(RETRIES).build();
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");

        retries.add(3, method("MutateRows"));

        assertThat(mutateRows.getCount()).isEqualTo(3);
    }

    @Test
    void aNegativeOrNonFiniteValueIsDroppedSoEveryCounterOnlyGrows() {
        Counter retries = metrics.counterSum(RETRIES, "MutateRows");
        Counter operations = metrics.histogramCount(LATENCIES, "MutateRows");
        Counter operationMillis = metrics.histogramSumMillis(LATENCIES, "MutateRows");
        LongCounter counter = meter.counterBuilder(RETRIES).build();
        DoubleHistogram histogram = meter.histogramBuilder(LATENCIES).setUnit("ms").build();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            counter.add(5, method("MutateRows"));
            counter.add(-2, method("MutateRows"));
            counter.add(-1, method("MutateRows"));
            histogram.record(7, method("MutateRows"));
            histogram.record(-3, method("MutateRows"));
            histogram.record(Double.NaN, method("MutateRows"));
            histogram.record(Double.POSITIVE_INFINITY, method("MutateRows"));

            assertThat(retries.getCount()).isEqualTo(5);
            assertThat(operations.getCount()).isEqualTo(1);
            assertThat(operationMillis.getCount()).isEqualTo(7);
            // Once per instrument, however many values it dropped.
            assertThat(capture.getMessages()).hasSize(2);
        }
    }

    @Test
    void aMethodWithoutACounterIsDroppedAndLoggedOnce() {
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");
        LongCounter retries = meter.counterBuilder(RETRIES).build();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            retries.add(1, method("MutateRows"));
            retries.add(1, method("CheckAndMutateRow"));
            retries.add(1, method("CheckAndMutateRow"));
            retries.add(1, Attributes.empty());

            assertThat(mutateRows.getCount()).isEqualTo(1);
            assertThat(capture.getMessages())
                    .hasSize(2)
                    .anySatisfy(message -> assertThat(message).contains("CheckAndMutateRow"))
                    .anySatisfy(message -> assertThat(message).contains("method null"));
        }
    }

    @Test
    void anInstrumentWithoutACounterIsDroppedAndLoggedOnce() {
        metrics.counterSum(RETRIES, "MutateRows");
        LongCounter unmapped = meter.counterBuilder("client/unmapped").build();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            unmapped.add(1, method("MutateRows"));
            unmapped.add(1, method("MutateRows"));

            assertThat(capture.getMessages())
                    .singleElement(InstanceOfAssertFactories.STRING)
                    .contains("client/unmapped");
        }
    }

    @Test
    void anIgnoredInstrumentIsDroppedWithoutAWarning() {
        metrics.counterSum(RETRIES, "MutateRows");
        metrics.ignore("client/throttling_latencies");
        DoubleHistogram ignored =
                meter.histogramBuilder("client/throttling_latencies").setUnit("ms").build();

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            ignored.record(5, method("MutateRows"));

            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void anUnsupportedInstrumentIsANoOpLoggedOncePerName() {
        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            meter.upDownCounterBuilder("client/in_flight").build().add(-1, method("MutateRows"));
            meter.upDownCounterBuilder("client/in_flight").build().add(1, method("MutateRows"));
            meter.gaugeBuilder("client/queued").ofLongs().build().set(4, method("MutateRows"));
            meter.counterBuilder("client/bytes").ofDoubles().build().add(1.5);
            meter.counterBuilder("client/sessions").buildWithCallback(measurement -> {});
            meter.counterBuilder("client/channels").buildObserver();
            meter.batchCallback(() -> {}, meter.counterBuilder("client/observed").buildObserver());

            assertThat(capture.getMessages())
                    .hasSize(7)
                    .anySatisfy(message -> assertThat(message).contains("client/channels"))
                    .anySatisfy(message -> assertThat(message).contains("client/observed"))
                    .anySatisfy(message -> assertThat(message).contains("batch callback"))
                    .anySatisfy(message -> assertThat(message).contains("client/in_flight"))
                    .anySatisfy(message -> assertThat(message).contains("client/queued"))
                    .anySatisfy(message -> assertThat(message).contains("client/bytes"))
                    .anySatisfy(message -> assertThat(message).contains("client/sessions"));
        }
    }

    @Test
    void everyMeterOfTheProviderRecordsIntoTheSameCounters() {
        // Spanner and gax build their meters through meterBuilder(), with a version.
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");

        metrics.openTelemetry()
                .getMeterProvider()
                .meterBuilder("gax")
                .setInstrumentationVersion("2.87.0")
                .setSchemaUrl("https://opentelemetry.io/schemas/1.24.0")
                .build()
                .counterBuilder(RETRIES)
                .build()
                .add(2, method("MutateRows"));
        metrics.openTelemetry()
                .getMeter("spanner")
                .counterBuilder(RETRIES)
                .build()
                .add(3, method("MutateRows"));

        assertThat(mutateRows.getCount()).isEqualTo(5);
    }

    @Test
    void recordingsFromManyThreadsAreAllCounted() throws Exception {
        Counter operations = metrics.histogramCount(LATENCIES, "MutateRows");
        Counter operationMillis = metrics.histogramSumMillis(LATENCIES, "MutateRows");
        DoubleHistogram latencies = meter.histogramBuilder(LATENCIES).setUnit("ms").build();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    for (int i = 0; i < 10_000; i++) {
                                        latencies.record(1, method("MutateRows"));
                                    }
                                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(operations.getCount()).isEqualTo(40_000);
        assertThat(operationMillis.getCount()).isEqualTo(40_000);
    }

    @Test
    void closeKeepsEveryValueAndAcceptsNoMoreRecordings() {
        Counter mutateRows = metrics.counterSum(RETRIES, "MutateRows");
        LongCounter retries = meter.counterBuilder(RETRIES).build();
        retries.add(5, method("MutateRows"));

        metrics.close();
        retries.add(100, method("MutateRows"));

        // Frozen, never back to zero, so a rate-computing backend sees no reset.
        assertThat(mutateRows.getCount()).isEqualTo(5);
        metrics.close();
        assertThat(mutateRows.getCount()).isEqualTo(5);
    }

    @Test
    void aCounterWithNothingRecordedReadsZero() {
        assertThat(metrics.counterSum(RETRIES, "ReadRows").getCount()).isZero();
        assertThat(metrics.histogramCount(LATENCIES, "ReadRows").getCount()).isZero();
        assertThat(metrics.histogramSumMillis(LATENCIES, "ReadRows").getCount()).isZero();
    }

    @Test
    void aClientMetricsCounterIsReadOnly() {
        Counter counter = metrics.counterSum(RETRIES, "MutateRows");

        assertThatThrownBy(counter::inc).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.inc(2)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(counter::dec).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.dec(2)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static Attributes method(String method) {
        return Attributes.of(METHOD, method);
    }
}
