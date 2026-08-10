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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

/**
 * A {@link SpannerSourceReaderMetrics} whose registered counters can be read back, so tests can
 * assert on counts instead of only on behaviour.
 */
final class TestReaderMetrics {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup;
    private final SpannerSourceReaderMetrics metrics;

    TestReaderMetrics() {
        this.metricGroup = InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
        this.metrics = new SpannerSourceReaderMetrics(metricGroup);
    }

    /** The group the metrics registered on, for a reader context that has to hand one back. */
    SourceReaderMetricGroup metricGroup() {
        return metricGroup;
    }

    SpannerSourceReaderMetrics metrics() {
        return metrics;
    }

    long counter(String name) {
        return listener.getCounter(name)
                .orElseThrow(() -> new AssertionError("No counter named " + name + " registered."))
                .getCount();
    }
}
