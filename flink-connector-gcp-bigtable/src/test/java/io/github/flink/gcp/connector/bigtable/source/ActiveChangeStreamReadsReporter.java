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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.metrics.reporter.Scheduled;

import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Captures live per-subtask Change Streams read peaks for the gated acceptance test. */
public final class ActiveChangeStreamReadsReporter
        implements MetricReporter, MetricReporterFactory, Scheduled {

    private static final ConcurrentMap<Gauge<?>, Integer> SUBTASK_BY_GAUGE =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, AtomicInteger> PEAK_BY_SUBTASK =
            new ConcurrentHashMap<>();

    public ActiveChangeStreamReadsReporter() {}

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new ActiveChangeStreamReadsReporter();
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        if (!BigtableMetricNames.ACTIVE_CHANGE_STREAM_READS.equals(metricName)
                || !(metric instanceof Gauge)) {
            return;
        }
        String subtask = group.getAllVariables().get("<subtask_index>");
        if (subtask != null) {
            SUBTASK_BY_GAUGE.put((Gauge<?>) metric, Integer.parseInt(subtask));
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        SUBTASK_BY_GAUGE.remove(metric);
    }

    @Override
    public void report() {
        SUBTASK_BY_GAUGE.forEach(
                (gauge, subtask) -> {
                    Object value = gauge.getValue();
                    if (value instanceof Number) {
                        PEAK_BY_SUBTASK
                                .computeIfAbsent(subtask, ignored -> new AtomicInteger())
                                .accumulateAndGet(((Number) value).intValue(), Math::max);
                    }
                });
    }

    static void reset() {
        SUBTASK_BY_GAUGE.clear();
        PEAK_BY_SUBTASK.clear();
    }

    static Map<Integer, Integer> peaks() {
        Map<Integer, Integer> peaks = new TreeMap<>();
        PEAK_BY_SUBTASK.forEach((subtask, peak) -> peaks.put(subtask, peak.get()));
        return peaks;
    }
}
