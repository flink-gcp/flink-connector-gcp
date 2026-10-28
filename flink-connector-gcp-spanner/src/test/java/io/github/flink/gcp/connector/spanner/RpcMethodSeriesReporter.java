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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records what a configured Flink reporter observes of the client-metrics series, keyed {@code
 * COMPONENT/METHOD.leaf}, where {@code COMPONENT} is {@code coordinator} for a split enumerator's
 * group and {@code task} for a group inside a task (a writer, a reader, a lookup function). Several
 * groups under one key, such as the readers of several subtasks, are summed, so neither component
 * nor subtask masks another.
 *
 * <p>{@link #reported()} keeps the largest sum a scheduled report read, through the same {@code
 * report()} call a real reporter makes. {@link #current()} reads every counter ever registered,
 * past its removal: after the job, the values the client metrics kept when they closed, which agree
 * with each other where the per-key maxima of separate reports need not.
 */
public final class RpcMethodSeriesReporter
        implements MetricReporter, MetricReporterFactory, Scheduled {

    private static final ConcurrentMap<String, List<Counter>> REGISTERED =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, List<Counter>> LIVE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicLong> REPORTED = new ConcurrentHashMap<>();

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
        String key = key(metricName, group);
        if (key != null && metric instanceof Counter) {
            // In this order: report() runs on another thread and looks the key up.
            REPORTED.putIfAbsent(key, new AtomicLong());
            REGISTERED
                    .computeIfAbsent(key, ignored -> new CopyOnWriteArrayList<>())
                    .add((Counter) metric);
            LIVE.computeIfAbsent(key, ignored -> new CopyOnWriteArrayList<>())
                    .add((Counter) metric);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        String key = key(metricName, group);
        if (key != null) {
            List<Counter> live = LIVE.get(key);
            if (live != null) {
                live.remove(metric);
            }
        }
    }

    @Override
    public void report() {
        LIVE.forEach(
                (key, counters) -> {
                    if (!counters.isEmpty()) {
                        REPORTED.get(key).accumulateAndGet(sum(counters), Math::max);
                    }
                });
    }

    public static void reset() {
        REGISTERED.clear();
        LIVE.clear();
        REPORTED.clear();
    }

    /** Every key ever registered, read now: after the job, the values the client metrics kept. */
    public static Map<String, Long> current() {
        Map<String, Long> current = new TreeMap<>();
        REGISTERED.forEach((key, counters) -> current.put(key, sum(counters)));
        return current;
    }

    /** Every key ever registered, with the largest sum a report read from it. */
    public static Map<String, Long> reported() {
        Map<String, Long> reported = new TreeMap<>();
        REPORTED.forEach((key, value) -> reported.put(key, value.get()));
        return reported;
    }

    private static String key(String metricName, MetricGroup group) {
        Map<String, String> variables = group.getAllVariables();
        String method = variables.get("<rpcMethod>");
        if (method == null) {
            return null;
        }
        String component = variables.containsKey("<subtask_index>") ? "task" : "coordinator";
        return component + "/" + method + "." + metricName;
    }

    private static long sum(List<Counter> counters) {
        long sum = 0;
        for (Counter counter : counters) {
            sum += counter.getCount();
        }
        return sum;
    }
}
