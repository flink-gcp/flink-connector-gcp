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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Preconditions;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.ExemplarFilter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Reads a Google Cloud client library's own OpenTelemetry metrics into Flink metrics, so that every
 * configured Flink metric reporter sees them.
 *
 * <p>A connector hands {@link #openTelemetry()} to the client it builds, through the client's
 * metrics-provider hook, and registers one Flink {@link Counter} per series it wants with {@link
 * #sum}, {@link #histogramCount} or {@link #histogramSumMillis}. The bridge owns the {@link
 * SdkMeterProvider} behind that instance, which nothing else in the JVM shares: it never touches
 * {@code GlobalOpenTelemetry}, so a shaded copy of this class and of OpenTelemetry stays
 * self-contained.
 *
 * <h2>Collection</h2>
 *
 * <p>Pull-based, and driven by the reporters. Reading any bridged counter collects the meter
 * provider's cumulative data at most once per second and swaps in an immutable snapshot, so the
 * first read of a reporter sweep collects and the rest read the snapshot. The bridge therefore owns
 * no thread and costs nothing when no reporter reads it. A failing collect logs once and keeps the
 * previous snapshot: a reporter sweep is never broken by it. A series missing from a later collect
 * keeps its previous value, so a counter never moves backwards.
 *
 * <h2>Registration happens before any read, never during one</h2>
 *
 * <p>The connector registers every counter when its component opens, for the RPC methods that
 * component's client calls. Registering a series the first time a collect sees it would be simpler,
 * but that collect runs inside a reporter's read, and Flink's {@code OpenTelemetryMetricReporter}
 * reads its counters by iterating a {@code HashMap} that a same-thread registration modifies: the
 * reporter fails that report with a {@code ConcurrentModificationException} ({@code Slf4jReporter}
 * swallows the exception and loses the report). A method the client calls that no counter was
 * registered for is logged once, at WARN, and otherwise ignored.
 *
 * <h2>What is kept</h2>
 *
 * <p>OpenTelemetry views drop every attribute except the RPC method before anything is aggregated,
 * so the number of series is the number of instruments times the number of methods, whatever the
 * client labels its data with. Histograms aggregate to their count and sum only: a cumulative
 * histogram carries no per-event samples, so no quantile, bucket or extreme could be reported
 * truthfully, and a backend computes the mean over any window as {@code rate(sum) / rate(count)}.
 *
 * <p>Close the client before the bridge. {@link #close()} collects once more, shuts the meter
 * provider down and freezes the snapshot: the counters keep their last values rather than reading
 * zero.
 */
@Internal
public final class SdkMetricsBridge implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SdkMetricsBridge.class);

    /** The shortest interval between two collects, however often the reporters read. */
    @VisibleForTesting static final long COLLECT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final AttributeKey<String> methodAttribute;
    private final LongSupplier nanoClock;
    private final PullReader reader = new PullReader();
    private final Supplier<Collection<MetricData>> collector;
    private final SdkMeterProvider meterProvider;
    private final OpenTelemetry openTelemetry;

    /** The methods some counter was registered for; written on registration, read by collects. */
    private final Set<String> registeredMethods = ConcurrentHashMap.newKeySet();

    /** Methods, instruments and units already warned about; touched under {@code this} only. */
    private final Set<String> warned = new HashSet<>();

    private volatile Map<Series, Double> snapshot = Collections.emptyMap();

    // Guarded by this.
    private boolean collected;
    private long lastCollectNanos;
    private boolean closed;
    private boolean collectFailureLogged;

    private SdkMetricsBridge(
            String methodAttribute,
            LongSupplier nanoClock,
            UnaryOperator<Supplier<Collection<MetricData>>> collectorDecorator) {
        this.methodAttribute =
                AttributeKey.stringKey(
                        Preconditions.checkNotNull(methodAttribute, "methodAttribute"));
        this.nanoClock = Preconditions.checkNotNull(nanoClock, "nanoClock");
        SdkMeterProviderBuilder builder =
                SdkMeterProvider.builder()
                        .setResource(Resource.empty())
                        .setExemplarFilter(ExemplarFilter.alwaysOff())
                        .registerMetricReader(reader);
        // One view per instrument type, because a view's selector needs a criterion and only the
        // histogram view changes the aggregation. Two views matching one instrument would export it
        // twice under one name.
        for (InstrumentType type : InstrumentType.values()) {
            ViewBuilder view =
                    View.builder().setAttributeFilter(Collections.singleton(methodAttribute));
            if (type == InstrumentType.HISTOGRAM) {
                // No bucket boundaries: one bucket, so only the count and sum are accumulated.
                view.setAggregation(Aggregation.explicitBucketHistogram(Collections.emptyList()));
            }
            builder.registerView(InstrumentSelector.builder().setType(type).build(), view.build());
        }
        this.meterProvider = builder.build();
        this.openTelemetry = new MetricsOnlyOpenTelemetry(meterProvider);
        this.collector = collectorDecorator.apply(reader::collect);
    }

    /**
     * Creates a bridge whose series are keyed by the given method attribute.
     *
     * @param methodAttribute the attribute the client labels each RPC method with, the one
     *     attribute kept
     * @return the bridge, owned by the caller
     */
    public static SdkMetricsBridge create(String methodAttribute) {
        return new SdkMetricsBridge(methodAttribute, System::nanoTime, UnaryOperator.identity());
    }

    /**
     * Creates a bridge on a test clock, whose collects go through {@code collectorDecorator} — the
     * one way a test can make a collect fail, since the SDK absorbs an instrument callback's own
     * exceptions.
     */
    @VisibleForTesting
    static SdkMetricsBridge create(
            String methodAttribute,
            LongSupplier nanoClock,
            UnaryOperator<Supplier<Collection<MetricData>>> collectorDecorator) {
        return new SdkMetricsBridge(methodAttribute, nanoClock, collectorDecorator);
    }

    /**
     * Returns the instance to hand to the client's metrics-provider hook. It carries this bridge's
     * meter provider and no-op tracing and propagation.
     */
    public OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    /**
     * Returns a counter reading a cumulative monotonic sum instrument for one method.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter sum(String instrument, String method) {
        return counter(instrument, method, Component.SUM);
    }

    /**
     * Returns a counter reading how many values a histogram instrument recorded for one method.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter histogramCount(String instrument, String method) {
        return counter(instrument, method, Component.COUNT);
    }

    /**
     * Returns a counter reading the sum of a duration histogram for one method, in milliseconds.
     * The instrument's unit must be {@code ms} or {@code s}; under any other unit the counter reads
     * zero and the bridge logs the unit once, because a number in an unknown unit under a {@code
     * Millis} name would be false.
     *
     * @param instrument the instrument name, as the client creates it
     * @param method the method attribute's value
     * @return a read-only counter to register on a metric group
     */
    public Counter histogramSumMillis(String instrument, String method) {
        return counter(instrument, method, Component.SUM_MILLIS);
    }

    private Counter counter(String instrument, String method, Component component) {
        Series series =
                new Series(
                        Preconditions.checkNotNull(instrument, "instrument"),
                        Preconditions.checkNotNull(method, "method"),
                        component);
        registeredMethods.add(method);
        return new BridgedCounter(this, series);
    }

    /** Collects once more, shuts the meter provider down, and freezes every counter's value. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            collect();
        } finally {
            closed = true;
            meterProvider.close();
        }
    }

    private long read(Series series) {
        refreshIfDue();
        Double value = snapshot.get(series);
        return value == null ? 0L : Math.round(value);
    }

    private synchronized void refreshIfDue() {
        if (closed) {
            return;
        }
        long now = nanoClock.getAsLong();
        if (collected && now - lastCollectNanos < COLLECT_INTERVAL_NANOS) {
            return;
        }
        lastCollectNanos = now;
        collected = true;
        collect();
    }

    /** Called under {@code this}. */
    private void collect() {
        Collection<MetricData> data;
        try {
            data = collector.get();
        } catch (RuntimeException e) {
            if (!collectFailureLogged) {
                collectFailureLogged = true;
                LOG.warn(
                        "Collecting the client library's metrics failed; its Flink metrics keep"
                                + " their previous values. Logged once per component.",
                        e);
            }
            return;
        }
        Map<Series, Double> next = new HashMap<>(snapshot);
        for (MetricData metric : data) {
            switch (metric.getType()) {
                case LONG_SUM:
                    putSum(next, metric, metric.getLongSumData());
                    break;
                case DOUBLE_SUM:
                    putSum(next, metric, metric.getDoubleSumData());
                    break;
                case HISTOGRAM:
                    putHistogram(next, metric);
                    break;
                default:
                    warnOnce(
                            "type:" + metric.getName(),
                            "The client library's instrument {} produces {} data, which this"
                                    + " bridge does not map; it is not reported.",
                            metric.getName(),
                            metric.getType());
            }
        }
        snapshot = Collections.unmodifiableMap(next);
    }

    private void putSum(
            Map<Series, Double> next, MetricData metric, SumData<? extends PointData> sum) {
        if (!sum.isMonotonic()) {
            warnOnce(
                    "type:" + metric.getName(),
                    "The client library's instrument {} is a non-monotonic sum, which this bridge"
                            + " does not map; it is not reported.",
                    metric.getName());
            return;
        }
        for (PointData point : sum.getPoints()) {
            double value =
                    point instanceof LongPointData
                            ? ((LongPointData) point).getValue()
                            : ((DoublePointData) point).getValue();
            String method = method(point.getAttributes());
            if (method != null) {
                next.put(new Series(metric.getName(), method, Component.SUM), value);
            }
        }
    }

    private void putHistogram(Map<Series, Double> next, MetricData metric) {
        double toMillis = millisPerUnit(metric.getUnit());
        if (Double.isNaN(toMillis)) {
            warnOnce(
                    "unit:" + metric.getName(),
                    "The client library's histogram {} is in unit '{}', which is not a duration"
                            + " this bridge converts; its sum is not reported.",
                    metric.getName(),
                    metric.getUnit());
        }
        for (HistogramPointData point : metric.getHistogramData().getPoints()) {
            String method = method(point.getAttributes());
            if (method == null) {
                continue;
            }
            next.put(
                    new Series(metric.getName(), method, Component.COUNT),
                    (double) point.getCount());
            if (!Double.isNaN(toMillis)) {
                next.put(
                        new Series(metric.getName(), method, Component.SUM_MILLIS),
                        point.getSum() * toMillis);
            }
        }
    }

    @Nullable
    private String method(Attributes attributes) {
        String method = attributes.get(methodAttribute);
        if (method == null) {
            return null;
        }
        if (!registeredMethods.contains(method)) {
            warnOnce(
                    "method:" + method,
                    "The client library reported metrics for method {}, for which no Flink metric"
                            + " is registered; they are not reported.",
                    method);
        }
        return method;
    }

    private static double millisPerUnit(String unit) {
        switch (unit) {
            case "ms":
                return 1.0;
            case "s":
                return 1000.0;
            default:
                return Double.NaN;
        }
    }

    private void warnOnce(String key, String message, Object... arguments) {
        if (warned.add(key)) {
            LOG.warn(message, arguments);
        }
    }

    private enum Component {
        SUM,
        COUNT,
        SUM_MILLIS
    }

    private static final class Series {

        private final String instrument;
        private final String method;
        private final Component component;

        private Series(String instrument, String method, Component component) {
            this.instrument = instrument;
            this.method = method;
            this.component = component;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Series)) {
                return false;
            }
            Series that = (Series) o;
            return instrument.equals(that.instrument)
                    && method.equals(that.method)
                    && component == that.component;
        }

        @Override
        public int hashCode() {
            return Objects.hash(instrument, method, component);
        }
    }

    /** A Flink counter whose value is the bridge's; nothing increments it. */
    private static final class BridgedCounter implements Counter {

        private final SdkMetricsBridge bridge;
        private final Series series;

        private BridgedCounter(SdkMetricsBridge bridge, Series series) {
            this.bridge = bridge;
            this.series = series;
        }

        @Override
        public void inc() {
            throw new UnsupportedOperationException("A bridged counter is read-only.");
        }

        @Override
        public void inc(long n) {
            throw new UnsupportedOperationException("A bridged counter is read-only.");
        }

        @Override
        public void dec() {
            throw new UnsupportedOperationException("A bridged counter is read-only.");
        }

        @Override
        public void dec(long n) {
            throw new UnsupportedOperationException("A bridged counter is read-only.");
        }

        @Override
        public long getCount() {
            return bridge.read(series);
        }
    }

    /**
     * Collects only when asked. The OpenTelemetry SDK's own pull readers serve an HTTP endpoint or
     * a periodic exporter; this one serves the Flink reporters' reads.
     */
    private static final class PullReader implements MetricReader {

        private volatile CollectionRegistration registration = CollectionRegistration.noop();

        @Override
        public void register(CollectionRegistration registration) {
            this.registration = registration;
        }

        Collection<MetricData> collect() {
            return registration.collectAllMetrics();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.CUMULATIVE;
        }

        @Override
        public CompletableResultCode forceFlush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            registration = CollectionRegistration.noop();
            return CompletableResultCode.ofSuccess();
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
