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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import javax.annotation.Nullable;

/**
 * A {@link SinkWriterMetricGroup} whose metrics can be read back by the names they registered
 * under, so a sink writer's metrics are asserted the way a reporter would see them.
 *
 * <p>Everything a writer registers — including the FLIP-33 standard counters, which are registered
 * here under their documented names rather than merely held — goes through one {@link
 * MetricListener}, so {@link #counter} and {@link #gauge} reach all of them and a renamed or
 * unregistered metric fails its test. That is what the alternatives cannot do: {@code
 * UnregisteredMetricsGroup.createSinkWriterMetricGroup()} hands out a fresh {@code SimpleCounter}
 * on every call, so the counter the writer captured is unreachable afterwards, and {@code
 * InternalSinkWriterMetricGroup} has no {@code mock(...)} factory in either supported Flink line
 * (1.20 and 2.x offer a package-private constructor and {@code wrap(OperatorMetricGroup)}, which a
 * listener group cannot satisfy).
 *
 * <p>{@link ProxyMetricGroup} supplies the delegation to the listener's group, which is why the
 * registration methods are not overridden here.
 */
@Internal
public final class TestSinkWriterMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SinkWriterMetricGroup {

    /** FLIP-33 name of the records counter, as a reporter sees it. */
    public static final String NUM_RECORDS_SEND = "numRecordsSend";

    /** FLIP-33 name of the bytes counter. */
    public static final String NUM_BYTES_SEND = "numBytesSend";

    /** FLIP-33 name of the send-error counter. */
    public static final String NUM_RECORDS_SEND_ERRORS = "numRecordsSendErrors";

    private final MetricListener listener;
    private final Counter numRecordsSend;
    private final Counter numBytesSend;
    private final Counter numRecordsSendErrors;

    @Nullable private Gauge<Long> currentSendTimeGauge;

    private TestSinkWriterMetricGroup(MetricListener listener) {
        super(listener.getMetricGroup());
        this.listener = listener;
        this.numRecordsSend = counter(NUM_RECORDS_SEND);
        this.numBytesSend = counter(NUM_BYTES_SEND);
        this.numRecordsSendErrors = counter(NUM_RECORDS_SEND_ERRORS);
    }

    /** Creates a group over a fresh listener. */
    public static TestSinkWriterMetricGroup create() {
        return new TestSinkWriterMetricGroup(new MetricListener());
    }

    /**
     * Returns the counter registered under {@code identifier}, relative to the group.
     *
     * @param identifier the name path, one element per group level (for example {@code
     *     "errorClass", "UNAVAILABLE", "errors"})
     * @return the counter's value
     * @throws AssertionError if nothing was registered under that name
     */
    public long counterValue(String... identifier) {
        return listener.getCounter(identifier)
                .orElseThrow(() -> new AssertionError(noMetric(identifier)))
                .getCount();
    }

    /**
     * Whether any metric is registered under {@code identifier} — the assertion an opt-in metric
     * needs, since "switched off" means nothing was registered rather than a counter left at zero.
     */
    public boolean hasMetric(String... identifier) {
        return listener.getMetric(Metric.class, identifier).isPresent();
    }

    /**
     * Returns the value of the gauge registered under {@code identifier}.
     *
     * @param identifier the name path, one element per group level
     * @param <T> the gauge's value type
     * @return the gauge's current value
     * @throws AssertionError if nothing was registered under that name
     */
    public <T> T gaugeValue(String... identifier) {
        Gauge<T> gauge =
                listener.<T>getGauge(identifier)
                        .orElseThrow(() -> new AssertionError(noMetric(identifier)));
        return gauge.getValue();
    }

    /** The gauge a writer passed to {@link #setCurrentSendTimeGauge}, or {@code null}. */
    @Nullable
    public Gauge<Long> getCurrentSendTimeGauge() {
        return currentSendTimeGauge;
    }

    private static String noMetric(String... identifier) {
        return "No metric registered under " + String.join(".", identifier) + ".";
    }

    @Override
    public Counter getNumRecordsSendCounter() {
        return numRecordsSend;
    }

    @Override
    public Counter getNumBytesSendCounter() {
        return numBytesSend;
    }

    @Override
    public Counter getNumRecordsSendErrorsCounter() {
        return numRecordsSendErrors;
    }

    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        // One counter behind both accessors, as Flink's own sink writer group has: the deprecated
        // name and the current one are the same metric there, so a test asserting either sees
        // every routed record.
        return numRecordsSendErrors;
    }

    @Override
    public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
        // Captured rather than registered: the connectors deliberately leave this unset (#37), and
        // capturing is what lets a test say so.
        this.currentSendTimeGauge = currentSendTimeGauge;
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return UnregisteredMetricsGroup.createOperatorIOMetricGroup();
    }
}
