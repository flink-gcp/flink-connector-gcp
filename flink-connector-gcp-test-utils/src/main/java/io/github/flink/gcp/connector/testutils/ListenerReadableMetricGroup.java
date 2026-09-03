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
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

/**
 * The listener plumbing shared by {@link TestSinkWriterMetricGroup} and {@link
 * TestSinkCommitterMetricGroup}: everything the writer or committer under test registers goes
 * through one {@link MetricListener}, and these accessors read it back by the names it registered
 * under — the way a reporter would see it.
 *
 * <p>Package-private on purpose: the two harnesses are the public surface, and a test asserts
 * through them; this class holds only the listener and its read-back accessors, which the two files
 * previously duplicated. What stays in the harnesses is theirs: the interface obligations
 * ({@code @Override}-checked against both supported Flink lines there) and the captured gauges.
 */
@Internal
abstract class ListenerReadableMetricGroup extends ProxyMetricGroup<MetricGroup> {

    private final MetricListener listener;

    ListenerReadableMetricGroup() {
        this(new MetricListener());
    }

    private ListenerReadableMetricGroup(MetricListener listener) {
        super(listener.getMetricGroup());
        this.listener = listener;
    }

    /**
     * Returns the counter registered under {@code identifier}, relative to the group.
     *
     * @param identifier the name path, one element per group level (for example {@code
     *     "errorClass", "UNAVAILABLE", "errors"})
     * @return the counter's value
     * @throws AssertionError if nothing was registered under that name
     */
    public final long counterValue(String... identifier) {
        return registeredCounter(identifier).getCount();
    }

    /**
     * Returns the counter registered under {@code identifier} itself — for the assertion that cares
     * which implementation a runtime registered, such as a thread-safe one on a surface whose
     * counts arrive from client threads.
     *
     * @param identifier the name path, one element per group level
     * @return the registered counter
     * @throws AssertionError if nothing was registered under that name
     */
    public final Counter registeredCounter(String... identifier) {
        return listener.getCounter(identifier)
                .orElseThrow(() -> new AssertionError(noMetric(identifier)));
    }

    /**
     * Whether any metric is registered under {@code identifier} — the assertion a metric that may
     * legitimately be absent needs, since "not registered" is not "registered at zero".
     */
    public final boolean hasMetric(String... identifier) {
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
    public final <T> T gaugeValue(String... identifier) {
        Gauge<T> gauge =
                listener.<T>getGauge(identifier)
                        .orElseThrow(() -> new AssertionError(noMetric(identifier)));
        return gauge.getValue();
    }

    private static String noMetric(String... identifier) {
        return "No metric registered under " + String.join(".", identifier) + ".";
    }
}
