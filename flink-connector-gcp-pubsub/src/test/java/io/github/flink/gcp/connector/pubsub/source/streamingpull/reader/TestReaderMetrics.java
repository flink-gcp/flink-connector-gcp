/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

/**
 * A {@link PubSubSourceReaderMetrics} whose registered metrics can be read back, so tests can
 * assert on counts instead of only on behavior.
 */
final class TestReaderMetrics {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup;
    private final PubSubSourceReaderMetrics metrics;

    TestReaderMetrics() {
        // One group instance for the whole holder: PubSubSourceReaderMetrics retains it to
        // register the gauges later, so a fresh mock per call would split them across groups.
        this.metricGroup = InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
        this.metrics = new PubSubSourceReaderMetrics(metricGroup);
    }

    SourceReaderMetricGroup metricGroup() {
        return metricGroup;
    }

    PubSubSourceReaderMetrics metrics() {
        return metrics;
    }

    long numRecordsInErrors() {
        return metricGroup.getNumRecordsInErrorsCounter().getCount();
    }

    long counter(String name) {
        return listener.getCounter(name)
                .orElseThrow(() -> new AssertionError("No counter named " + name + " registered."))
                .getCount();
    }

    /**
     * Reads a gauge by its registered name.
     *
     * <p>Typed {@code long} rather than to the gauge's own value type: the reader registers both
     * {@code Gauge<Integer>} (counts of splits) and {@code Gauge<Long>} (buffer sizes, which do not
     * fit an int in bytes), and one accessor reading both as a number keeps a test from having to
     * know which.
     */
    long gauge(String name) {
        return listener.<Number>getGauge(name)
                .orElseThrow(() -> new AssertionError("No gauge named " + name + " registered."))
                .getValue()
                .longValue();
    }
}
