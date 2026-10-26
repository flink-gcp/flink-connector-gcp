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

package io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records the largest value a scheduled report read from each {@code rpcMethod.METHOD.leaf}
 * counter, keyed {@code METHOD.leaf}: what a configured Flink reporter observes of the client
 * metrics, read through the same {@code report()} call a real reporter makes.
 *
 * <p>It also keeps every counter it was given, past its removal, so a test can read the values the
 * client metrics kept when it closed. Read after the job, they no longer move, so they agree with
 * each other, which the per-series maxima of separate reports need not: a report reads one series
 * and then another while the client is still recording.
 */
public final class RpcMethodSeriesReporter
        implements MetricReporter, MetricReporterFactory, Scheduled {

    private static final ConcurrentMap<Counter, String> SERIES_BY_COUNTER =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicLong> REPORTED = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Counter> REGISTERED = new ConcurrentHashMap<>();

    public RpcMethodSeriesReporter() {}

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new RpcMethodSeriesReporter();
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String method = group.getAllVariables().get("<rpcMethod>");
        if (method != null && metric instanceof Counter) {
            // In this order: report() runs on another thread and looks the series up.
            REPORTED.putIfAbsent(method + "." + metricName, new AtomicLong());
            REGISTERED.put(method + "." + metricName, (Counter) metric);
            SERIES_BY_COUNTER.put((Counter) metric, method + "." + metricName);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        SERIES_BY_COUNTER.remove(metric);
    }

    @Override
    public void report() {
        SERIES_BY_COUNTER.forEach(
                (counter, series) ->
                        REPORTED.get(series).accumulateAndGet(counter.getCount(), Math::max));
    }

    static void reset() {
        SERIES_BY_COUNTER.clear();
        REPORTED.clear();
        REGISTERED.clear();
    }

    /**
     * Every series ever registered, read now: after the job, the values the client metrics kept.
     */
    static Map<String, Long> current() {
        Map<String, Long> current = new TreeMap<>();
        REGISTERED.forEach((series, counter) -> current.put(series, counter.getCount()));
        return current;
    }

    /** Every series ever registered, with the largest value a report read from it. */
    static Map<String, Long> reported() {
        Map<String, Long> reported = new TreeMap<>();
        REPORTED.forEach((series, value) -> reported.put(series, value.get()));
        return reported;
    }
}
