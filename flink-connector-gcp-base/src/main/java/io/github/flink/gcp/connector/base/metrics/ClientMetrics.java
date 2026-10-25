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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Preconditions;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterBuilder;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableMeasurement;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Turns a Google Cloud client library's own OpenTelemetry metrics into Flink metrics, so that every
 * configured Flink metric reporter sees them.
 *
 * <p>A connector hands {@link #openTelemetry()} to the client it builds, through the client's
 * metrics-provider hook, and registers one Flink {@link Counter} per series it wants with {@link
 * #counterSum(String, String)}, {@link #histogramCount(String, String)} or {@link
 * #histogramSumMillis(String, String)}. The instance implements the part of the OpenTelemetry
 * Metrics API those clients record through, and nothing more: no OpenTelemetry SDK is involved, and
 * nothing touches {@code GlobalOpenTelemetry}, so a shaded copy stays self-contained.
 *
 * <h2>Recording</h2>
 *
 * <p>A client's {@code add} or {@code record} adds straight into the accumulator of the series its
 * instrument and its method attribute name, on the client's own thread. A Flink counter reads that
 * accumulator, so a reporter always reads the current total, and this class owns no thread, buffer
 * or collection step. Every attribute other than the method is ignored, so a component reports
 * instruments times methods series whatever the client labels its data with.
 *
 * <p>What is kept, per instrument:
 *
 * <ul>
 *   <li>a long counter: the total of what was added;
 *   <li>a histogram, double or long: how many values were recorded and, for a duration in {@code
 *       ms} or {@code s}, their sum in milliseconds. Under any other unit only the count is kept.
 *       Percentiles, buckets and extremes are not kept: a backend computes the mean over any window
 *       as {@code rate(sum) / rate(count)}.
 * </ul>
 *
 * <p>Every counter only grows. A negative or non-finite value is dropped, as the OpenTelemetry SDK
 * drops it, because a reporter's backend computes rates from these counters. A count and its sum
 * are two accumulators, so a reporter reading both while the client records may see one more value
 * in one than in the other; over any rate window that difference vanishes.
 *
 * <h2>What is not reported</h2>
 *
 * <p>Up-down counters, gauges, double counters and every observable (callback) instrument are
 * handed to OpenTelemetry's no-op meter, with one {@code WARN} per instrument name. So is a
 * recording for a method no counter was registered for, with one {@code WARN} per method, and a
 * recording into an instrument nothing was registered for, with one per instrument, unless the
 * connector declared it with {@link #ignore(String)}.
 *
 * <h2>Registration happens at open, not on first sight</h2>
 *
 * <p>The connector registers every counter when its component opens, for the RPC methods that
 * component's client calls. A registration made later, from a client's thread, would run while a
 * reporter iterates its own metric map: {@code Slf4jReporter} iterates without a lock and loses
 * that report to the {@code ConcurrentModificationException}.
 *
 * <p>Close the client before this object. {@link #close()} stops it accepting recordings, so the
 * counters keep their last values rather than reading zero.
 */
@Internal
public final class ClientMetrics implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMetrics.class);

    private final AttributeKey<String> methodAttribute;
    private final OpenTelemetry openTelemetry = new MetricsOnlyOpenTelemetry(new ClientMeter());
    private final Meter noop = MeterProvider.noop().get(ClientMetrics.class.getName());

    /** Instrument name, then method attribute value, to the series registered for it. */
    private final ConcurrentMap<String, ConcurrentMap<String, Accumulator>> registered =
            new ConcurrentHashMap<>();

    // One set per warning, keyed by the bare name, so a recording that warns allocates nothing.
    private final Set<String> warnedInstruments = ConcurrentHashMap.newKeySet();
    private final Set<String> warnedMethods = ConcurrentHashMap.newKeySet();
    private final Set<String> warnedValues = ConcurrentHashMap.newKeySet();
    private final Set<String> warnedKinds = ConcurrentHashMap.newKeySet();
    private final Set<String> warnedUnits = ConcurrentHashMap.newKeySet();

    /** Instruments the connector chose not to report; recordings into them are dropped silently. */
    private final Set<String> ignored = ConcurrentHashMap.newKeySet();

    private volatile boolean closed;

    private ClientMetrics(String methodAttribute) {
        this.methodAttribute =
                AttributeKey.stringKey(
                        Preconditions.checkNotNull(methodAttribute, "methodAttribute"));
    }

    /**
     * Creates the client metrics of one component, whose series are keyed by the given method
     * attribute.
     *
     * @param methodAttribute the attribute the client labels each RPC method with, the one
     *     attribute kept
     * @return the client metrics, owned by the caller
     */
    public static ClientMetrics create(String methodAttribute) {
        return new ClientMetrics(methodAttribute);
    }

    /**
     * Returns the instance to hand to the client's metrics-provider hook. It carries this object's
     * meter provider and no-op tracing and propagation.
     */
    public OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    /**
     * Returns a counter reading the total of a long counter instrument for one method.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter counterSum(String instrument, String method) {
        Accumulator accumulator = accumulator(instrument, method);
        return new ReadOnlyCounter(accumulator.total::sum);
    }

    /**
     * Returns a counter reading how many values a histogram instrument recorded for one method.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter histogramCount(String instrument, String method) {
        Accumulator accumulator = accumulator(instrument, method);
        return new ReadOnlyCounter(accumulator.count::sum);
    }

    /**
     * Returns a counter reading the sum of a duration histogram for one method, in milliseconds,
     * rounded to the nearest millisecond. The instrument's unit must be {@code ms} or {@code s};
     * under any other unit the counter reads zero and it logs the unit once, because a number in an
     * unknown unit under a {@code Millis} name would be false.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter histogramSumMillis(String instrument, String method) {
        Accumulator accumulator = accumulator(instrument, method);
        return new ReadOnlyCounter(() -> Math.round(accumulator.millis.sum()));
    }

    /**
     * Drops an instrument the connector deliberately does not report, without the warning an
     * unregistered instrument gets. Use it for an instrument known to record something wrong, and
     * say why where it is called.
     *
     * @param instrument the instrument name, as the client creates it
     */
    public void ignore(String instrument) {
        ignored.add(Preconditions.checkNotNull(instrument, "instrument"));
    }

    private Accumulator accumulator(String instrument, String method) {
        Preconditions.checkNotNull(instrument, "instrument");
        Preconditions.checkNotNull(method, "method");
        return byMethod(instrument).computeIfAbsent(method, ignored -> new Accumulator());
    }

    private ConcurrentMap<String, Accumulator> byMethod(String instrument) {
        return registered.computeIfAbsent(instrument, ignored -> new ConcurrentHashMap<>());
    }

    /** Stops accepting recordings; every counter keeps the value it has. */
    @Override
    public void close() {
        closed = true;
    }

    /** The series a recording goes to, or {@code null} after a warning when there is none. */
    @Nullable
    private Accumulator target(
            String instrument, ConcurrentMap<String, Accumulator> byMethod, Attributes attributes) {
        if (closed || ignored.contains(instrument)) {
            return null;
        }
        String method = attributes.get(methodAttribute);
        Accumulator accumulator = method == null ? null : byMethod.get(method);
        if (accumulator == null) {
            if (byMethod.isEmpty()) {
                warnOnce(
                        warnedInstruments,
                        instrument,
                        "The client library recorded into its instrument {}, for which no Flink"
                                + " metric is registered; it is not reported.",
                        instrument);
            } else {
                warnOnce(
                        warnedMethods,
                        String.valueOf(method),
                        "The client library reported metrics for method {}, for which no Flink"
                                + " metric is registered; they are not reported.",
                        method);
            }
        }
        return accumulator;
    }

    private boolean accepted(String instrument, double value) {
        if (value >= 0 && !Double.isInfinite(value)) {
            return true;
        }
        warnOnce(
                warnedValues,
                instrument,
                "The client library recorded {} into its instrument {}; a negative or non-finite"
                        + " value is dropped, so the Flink counter keeps growing monotonically.",
                value,
                instrument);
        return false;
    }

    private static void warnOnce(
            Set<String> warned, String key, String message, Object... arguments) {
        if (!warned.contains(key) && warned.add(key)) {
            LOG.warn(message, arguments);
        }
    }

    private <T> T unsupported(String instrument, String kind, T noopInstrument) {
        warnOnce(
                warnedKinds,
                instrument,
                "The client library created its instrument {} as a {}, which ClientMetrics"
                        + " does not map; it is not reported.",
                instrument,
                kind);
        return noopInstrument;
    }

    private static double millisPerUnit(@Nullable String unit) {
        if ("ms".equals(unit)) {
            return 1.0;
        }
        if ("s".equals(unit)) {
            return 1000.0;
        }
        return Double.NaN;
    }

    /** One series' totals; which of them a counter reads depends on the instrument's kind. */
    private static final class Accumulator {
        private final LongAdder total = new LongAdder();
        private final LongAdder count = new LongAdder();
        private final DoubleAdder millis = new DoubleAdder();
    }

    /** A Flink counter whose value is an accumulator's; nothing increments it. */
    private static final class ReadOnlyCounter implements Counter {

        private final LongSupplier value;

        private ReadOnlyCounter(LongSupplier value) {
            this.value = value;
        }

        @Override
        public void inc() {
            throw new UnsupportedOperationException("A client-metrics counter is read-only.");
        }

        @Override
        public void inc(long n) {
            throw new UnsupportedOperationException("A client-metrics counter is read-only.");
        }

        @Override
        public void dec() {
            throw new UnsupportedOperationException("A client-metrics counter is read-only.");
        }

        @Override
        public void dec(long n) {
            throw new UnsupportedOperationException("A client-metrics counter is read-only.");
        }

        @Override
        public long getCount() {
            return value.getAsLong();
        }
    }

    /**
     * The meter every scope gets: one set of client metrics, whatever the client names its meter.
     */
    private final class ClientMeter implements MeterProvider, MeterBuilder, Meter {

        @Override
        public MeterBuilder meterBuilder(String instrumentationScopeName) {
            return this;
        }

        @Override
        public MeterBuilder setSchemaUrl(String schemaUrl) {
            return this;
        }

        @Override
        public MeterBuilder setInstrumentationVersion(String instrumentationScopeVersion) {
            return this;
        }

        @Override
        public Meter build() {
            return this;
        }

        @Override
        public LongCounterBuilder counterBuilder(String name) {
            return new ClientLongCounterBuilder(name);
        }

        @Override
        public DoubleHistogramBuilder histogramBuilder(String name) {
            return new ClientDoubleHistogramBuilder(name);
        }

        @Override
        public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
            return unsupported(name, "up-down counter", noop.upDownCounterBuilder(name));
        }

        @Override
        public DoubleGaugeBuilder gaugeBuilder(String name) {
            return unsupported(name, "gauge", noop.gaugeBuilder(name));
        }

        @Override
        public BatchCallback batchCallback(
                Runnable callback,
                ObservableMeasurement observableMeasurement,
                ObservableMeasurement... additionalMeasurements) {
            return unsupported(
                    "batch callback",
                    "batch callback",
                    noop.batchCallback(callback, observableMeasurement, additionalMeasurements));
        }
    }

    private final class ClientLongCounterBuilder implements LongCounterBuilder {

        private final String name;

        private ClientLongCounterBuilder(String name) {
            this.name = name;
        }

        @Override
        public LongCounterBuilder setDescription(String description) {
            return this;
        }

        @Override
        public LongCounterBuilder setUnit(String unit) {
            return this;
        }

        @Override
        public DoubleCounterBuilder ofDoubles() {
            return unsupported(name, "double counter", noop.counterBuilder(name).ofDoubles());
        }

        @Override
        public LongCounter build() {
            return new ClientLongCounter(name, byMethod(name));
        }

        @Override
        public ObservableLongCounter buildWithCallback(
                Consumer<ObservableLongMeasurement> callback) {
            return unsupported(
                    name,
                    "observable counter",
                    noop.counterBuilder(name).buildWithCallback(callback));
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            return unsupported(
                    name, "observable counter", noop.counterBuilder(name).buildObserver());
        }
    }

    private final class ClientLongCounter implements LongCounter {

        private final String name;
        private final ConcurrentMap<String, Accumulator> byMethod;

        private ClientLongCounter(String name, ConcurrentMap<String, Accumulator> byMethod) {
            this.name = name;
            this.byMethod = byMethod;
        }

        @Override
        public void add(long value) {
            add(value, Attributes.empty());
        }

        @Override
        public void add(long value, Attributes attributes) {
            if (!accepted(name, value)) {
                return;
            }
            Accumulator accumulator = target(name, byMethod, attributes);
            if (accumulator != null) {
                accumulator.total.add(value);
            }
        }

        @Override
        public void add(long value, Attributes attributes, Context context) {
            add(value, attributes);
        }
    }

    private final class ClientDoubleHistogramBuilder implements DoubleHistogramBuilder {

        private final String name;
        @Nullable private String unit;

        private ClientDoubleHistogramBuilder(String name) {
            this.name = name;
        }

        @Override
        public DoubleHistogramBuilder setDescription(String description) {
            return this;
        }

        @Override
        public DoubleHistogramBuilder setUnit(String unit) {
            this.unit = unit;
            return this;
        }

        @Override
        public LongHistogramBuilder ofLongs() {
            return new ClientLongHistogramBuilder(name, unit);
        }

        @Override
        public DoubleHistogram build() {
            return new ClientHistogram(name, unit);
        }
    }

    private final class ClientLongHistogramBuilder implements LongHistogramBuilder {

        private final String name;
        @Nullable private String unit;

        private ClientLongHistogramBuilder(String name, @Nullable String unit) {
            this.name = name;
            this.unit = unit;
        }

        @Override
        public LongHistogramBuilder setDescription(String description) {
            return this;
        }

        @Override
        public LongHistogramBuilder setUnit(String unit) {
            this.unit = unit;
            return this;
        }

        @Override
        public LongHistogram build() {
            return new ClientLongHistogram(new ClientHistogram(name, unit));
        }
    }

    private final class ClientHistogram implements DoubleHistogram {

        private final String name;
        private final ConcurrentMap<String, Accumulator> byMethod;
        private final double toMillis;

        private ClientHistogram(String name, @Nullable String unit) {
            this.name = name;
            this.byMethod = byMethod(name);
            this.toMillis = millisPerUnit(unit);
            if (Double.isNaN(toMillis)) {
                warnOnce(
                        warnedUnits,
                        name,
                        "The client library's histogram {} is in unit '{}', which is not a"
                                + " duration this class converts; its sum is not reported.",
                        name,
                        unit);
            }
        }

        @Override
        public void record(double value) {
            record(value, Attributes.empty());
        }

        @Override
        public void record(double value, Attributes attributes) {
            if (!accepted(name, value)) {
                return;
            }
            Accumulator accumulator = target(name, byMethod, attributes);
            if (accumulator == null) {
                return;
            }
            accumulator.count.increment();
            if (!Double.isNaN(toMillis)) {
                accumulator.millis.add(value * toMillis);
            }
        }

        @Override
        public void record(double value, Attributes attributes, Context context) {
            record(value, attributes);
        }
    }

    /**
     * A long histogram, recording each value as the double it converts to. A separate class rather
     * than a second interface on {@code ClientHistogram}: the two interfaces declare the same
     * default methods, which one class cannot inherit from both.
     */
    private static final class ClientLongHistogram implements LongHistogram {

        private final ClientHistogram histogram;

        private ClientLongHistogram(ClientHistogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void record(long value) {
            histogram.record((double) value);
        }

        @Override
        public void record(long value, Attributes attributes) {
            histogram.record((double) value, attributes);
        }

        @Override
        public void record(long value, Attributes attributes, Context context) {
            histogram.record((double) value, attributes);
        }
    }

    /** The metrics half of OpenTelemetry; tracing and propagation are no-ops. */
    private static final class MetricsOnlyOpenTelemetry implements OpenTelemetry {

        private final MeterProvider meterProvider;

        private MetricsOnlyOpenTelemetry(MeterProvider meterProvider) {
            this.meterProvider = meterProvider;
        }

        @Override
        public TracerProvider getTracerProvider() {
            return TracerProvider.noop();
        }

        @Override
        public MeterProvider getMeterProvider() {
            return meterProvider;
        }

        @Override
        public ContextPropagators getPropagators() {
            return ContextPropagators.noop();
        }
    }
}
