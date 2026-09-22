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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the {@link Gauge} objects a sink registers, so a rig in the same JVM can read them.
 *
 * <p>A cluster asks Flink's REST API which metrics a vertex has and samples what it names. Run
 * {@code cal1246c-221-1} got an empty listing on all ten of its cells, and the staged gauges the
 * assessment exists to read were never sampled. A listing is a question about a metric store; a
 * gauge object is the thing itself, and one that was never registered is a recorded failure here
 * rather than an absence nobody notices.
 *
 * <p>Flink hands every registration to its reporters, which is why this needs nothing of the sink
 * under measurement — {@code ObservedSinks} wraps the connector for its own reasons and knows
 * nothing about this class. It follows {@code ActiveChangeStreamReadsReporter} in the Bigtable
 * module, including the static state: Flink instantiates a reporter reflectively, so an instance
 * field would not be reachable from the rig that wants to read it. {@link #reset()} before a run
 * and one run at a time in a JVM are what that costs.
 *
 * <p>A removed metric is dropped. The probe restarts a failed job up to three times, and a writer
 * from a dead attempt goes on answering {@code getValue()} forever — it would otherwise add its
 * stale count to every later reading of the same name.
 *
 * <p>Registration is not observation. Two of the staged gauges are live only inside a call: the
 * committer sets its replay snapshot when a commit starts and clears it in that commit's {@code
 * finally}, so a reader on a one-second period sees only the {@code -1} it reports when idle almost
 * every time. {@link #registered()} says the sink offered the gauge; {@link #observed()} says a
 * reading caught a real value.
 */
public final class SinkGaugeReporter implements MetricReporter, MetricReporterFactory {
    /**
     * What an age or a budget gauge reports when it has no snapshot to describe. Both {@code
     * CloudTasksReplaySnapshot.ageMillis} and {@code remainingMillis} use it, and no gauge this rig
     * reads is legitimately negative, so it is filtered per registration rather than per name.
     */
    private static final long IDLE = -1L;

    private static final Map<String, Set<Gauge<?>>> GAUGES = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED = ConcurrentHashMap.newKeySet();
    private static final Set<String> OBSERVED = ConcurrentHashMap.newKeySet();
    private static final Set<String> DEAF = ConcurrentHashMap.newKeySet();
    private static final Set<String> WATCHED = ConcurrentHashMap.newKeySet();

    public SinkGaugeReporter() {}

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new SinkGaugeReporter();
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        // Flink offers every metric in the process, its own included, and some of those gauges are
        // not numbers at all — `lastCheckpointExternalPath` reports the string "n/a". Watching an
        // explicit set is what makes a non-numeric reading under a watched name an error worth
        // raising rather than noise to skip.
        if (!(metric instanceof Gauge)
                || !WATCHED.contains(metricName)
                || DEAF.contains(metricName)) {
            return;
        }
        GAUGES.computeIfAbsent(metricName, ignored -> ConcurrentHashMap.newKeySet())
                .add((Gauge<?>) metric);
        REGISTERED.add(metricName);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        Set<Gauge<?>> registrations = GAUGES.get(metricName);
        if (registrations != null) {
            registrations.remove(metric);
        }
    }

    /** Forgets everything, so one run in a JVM cannot read another's registrations. */
    static void reset() {
        GAUGES.clear();
        REGISTERED.clear();
        OBSERVED.clear();
        DEAF.clear();
        WATCHED.clear();
    }

    /**
     * The gauge names to keep. Nothing is kept until this is called, which is deliberate: the
     * reporter is offered every metric in the process and a rig that silently collected all of them
     * would be reading Flink's own instead of the sink's.
     */
    static void watch(Collection<String> names) {
        WATCHED.clear();
        WATCHED.addAll(names);
    }

    /**
     * Makes the reporter ignore the named registrations, so a test can produce the absence this
     * class exists to report. The sink still registers them and the job behaves identically; what
     * differs is what reaches the reader, which is the side the incident was on.
     */
    static void deafTo(Set<String> names) {
        DEAF.clear();
        DEAF.addAll(names);
    }

    /** Every gauge name the sink has registered, whether or not a reading ever caught a value. */
    static Set<String> registered() {
        return new TreeSet<>(REGISTERED);
    }

    /** The names whose value a reading has caught, rather than only the idle sentinel. */
    static Set<String> observed() {
        return new TreeSet<>(OBSERVED);
    }

    /**
     * One reading of every gauge that currently has a value.
     *
     * <p>A name whose every live registration reports {@link #IDLE} yields no reading at all, which
     * is what keeps {@link #observed()} and the summary's extremes in step: a gauge is either
     * observed and has an extreme, or is neither. Reducing first and filtering afterwards does not
     * hold that — at parallelism 4, one committer between commits drags the minimum to {@code -1}
     * while another's real budget sets the maximum, and the name ends up in neither place.
     *
     * <p>The gauges are read from the reader's thread while the task's thread moves the state they
     * describe, which is what Flink's own reporter does too. A reading may be stale by whatever the
     * task did since. It may also be worse than stale: {@code CloudTasksStagedWriter.stagedBytes}
     * is a non-volatile {@code long}, which the JLS permits a reader to see as a mixture of two
     * writes, and {@code staged} is a reassigned reference whose size can be read across a swap.
     * Neither is introduced here — Flink's own reporter reads the same gauges off-thread — but a
     * gauge added later that walks a collection would need its own answer.
     */
    static Map<String, Reading> read() {
        Map<String, Reading> readings = new LinkedHashMap<>();
        GAUGES.forEach(
                (name, registrations) -> {
                    List<Long> values = new ArrayList<>(registrations.size());
                    for (Gauge<?> gauge : registrations) {
                        Object value = gauge.getValue();
                        if (!(value instanceof Number number)) {
                            throw new IllegalStateException(
                                    "Gauge " + name + " is not a number: " + value);
                        }
                        if (number.longValue() > IDLE) {
                            values.add(number.longValue());
                        }
                    }
                    if (values.isEmpty()) {
                        return;
                    }
                    OBSERVED.add(name);
                    readings.put(name, Reading.of(values));
                });
        return new TreeMap<>(readings);
    }

    /**
     * One gauge's value across the subtasks reporting it, as the {@code sum}, {@code min} and
     * {@code max} Flink's own subtask-metrics endpoint offers, so a figure taken here and a figure
     * taken from a cluster mean the same thing.
     */
    record Reading(long sum, long min, long max) {
        static Reading of(List<Long> values) {
            long sum = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (long value : values) {
                sum += value;
                min = Math.min(min, value);
                max = Math.max(max, value);
            }
            return new Reading(sum, min, max);
        }
    }
}
