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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One enumerator's view of the metrics registered on its source's enumerator metric group, which
 * outlives the enumerator: every enumerator built on the same group shares one registration of each
 * name.
 *
 * <p>Flink keeps one enumerator metric group per source operator for as long as the JobMaster leads
 * the job. A coordinator reset builds the next enumerator on that same group — after a global
 * failover under the default scheduler, and at every restart under the adaptive scheduler, which
 * also deserializes a new source object. The group drops a second registration of a name with a
 * "Name collision" warning and keeps reporting the first, so an enumerator that registered its own
 * metrics would count into series no reporter sees, and its gauges would keep reading the first
 * enumerator (Flink 1.20.4 to 2.3.0: {@code SourceCoordinatorContext#metricGroup}, {@code
 * AbstractMetricGroup#addGroup} and {@code #addMetric}; issue #1908, {@code docs/adr/0188}).
 *
 * <p>So the first enumerator registers and every later one takes the same registrations:
 *
 * <ul>
 *   <li>A {@linkplain #counter(String) counter} is the same object for every enumerator on the
 *       group, so it keeps counting across resets for as long as this JobMaster leads the job.
 *   <li>A {@linkplain #gauge(String, Gauge) gauge} reads the enumerator that bound it last. {@link
 *       #close()} freezes it at the closing enumerator's last value, unless a later enumerator has
 *       bound it already, so the registration holds no closed enumerator.
 *   <li>A {@linkplain #shared(SplitEnumeratorMetricGroup, String, Function) shared} value, such as
 *       a client's metrics, is registered once and handed to every later enumerator.
 * </ul>
 *
 * <p>A group is one per source operator, so more than one source shares it only inside Flink's
 * {@code HybridSource}, whose sub-sources' enumerators all run on the hybrid's group. They share
 * its counters and gauges by name as consecutive enumerators do, and a shared value is keyed by
 * what it registers rather than by who asked for it. That holds for sub-sources that load the same
 * copy of this class; each SQL connector jar relocates its own, and two copies on one group collide
 * as enumerators did before this class existed.
 *
 * <p>The registrations are found again through the group's identity, which neither the source
 * object nor the context carries across a reset. {@code SplitEnumeratorContext#metricGroup()}
 * answers a new wrapper each call, but {@code addGroup} answers the existing open subgroup of a
 * name, so the subgroup {@code sharedRegistrations} is the same object for as long as the
 * enumerator group is open, and a new one once Flink closes it, which it does when the JobMaster
 * stops: at the job's end, or when the JobMaster loses leadership. It never holds a metric, so no
 * reporter sees it. The registrations are held in a process-wide map, weakly keyed by that
 * subgroup, so a finished job's entry goes once its metric groups are collected and the map is next
 * used.
 *
 * <p>A context that offers no metric group gets {@link #unregistered()}: counters nothing reports,
 * no gauges and no shared values; its enumerator runs unchanged.
 *
 * <p>An enumerator registers on the coordinator thread and may close on another. The counters are
 * thread-safe, as the reporter thread reads them while an enumerator's callbacks increment them.
 */
@Internal
public final class EnumeratorMetrics implements AutoCloseable {

    /** The subgroup whose identity names an enumerator group's registrations. */
    @VisibleForTesting static final String IDENTITY_GROUP = "sharedRegistrations";

    /** The name Flink registers {@code setUnassignedSplitsGauge} under. */
    private static final String UNASSIGNED_SPLITS = "unassignedSplits";

    /** Guarded by itself. Weakly keyed, so an entry lasts as long as its job's metric groups. */
    private static final Map<MetricGroup, Registrations> REGISTRATIONS = new WeakHashMap<>();

    @Nullable private final SplitEnumeratorMetricGroup group;
    @Nullable private final Registrations registrations;

    /**
     * Releases each gauge this enumerator bound, for {@link #close()}. Guarded by itself: an
     * enumerator may close on another thread than the coordinator's.
     */
    private final List<Runnable> releases = new ArrayList<>();

    private EnumeratorMetrics(
            @Nullable SplitEnumeratorMetricGroup group, @Nullable Registrations registrations) {
        this.group = group;
        this.registrations = registrations;
    }

    /**
     * Returns an enumerator's view of the registrations on its metric group.
     *
     * @param group the group {@code SplitEnumeratorContext#metricGroup()} answered, or {@code null}
     *     where the context offers none
     * @return the view; one per enumerator, closed with it
     */
    public static EnumeratorMetrics of(@Nullable SplitEnumeratorMetricGroup group) {
        return group == null
                ? unregistered()
                : new EnumeratorMetrics(group, registrationsOf(group));
    }

    /**
     * Returns a view that registers nothing, for an enumerator that has no group or has not
     * started.
     *
     * @return a view whose counters nothing reports
     */
    public static EnumeratorMetrics unregistered() {
        return new EnumeratorMetrics(null, null);
    }

    /**
     * Returns the value registered under a key on an enumerator group, registering it with the
     * given function the first time.
     *
     * @param group the group {@code SplitEnumeratorContext#metricGroup()} answered, or {@code null}
     *     where the context offers none
     * @param key names what the value registers, so that two callers registering different series
     *     never share one value
     * @param registration registers the value's metrics on the group, once
     * @param <T> the value's type, the same for every caller of the key
     * @return the value every enumerator on this group shares, or {@code null} where there is no
     *     group
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T shared(
            @Nullable SplitEnumeratorMetricGroup group,
            String key,
            Function<? super MetricGroup, ? extends T> registration) {
        Preconditions.checkNotNull(key, "key must not be null");
        Preconditions.checkNotNull(registration, "registration must not be null");
        if (group == null) {
            return null;
        }
        Registrations registrations = registrationsOf(group);
        synchronized (registrations) {
            return (T)
                    registrations.shared.computeIfAbsent(key, ignored -> registration.apply(group));
        }
    }

    /**
     * Returns the counter registered under a name, registering it the first time.
     *
     * @param name the metric name, from the connector's {@code *MetricNames} inventory
     * @return the counter every enumerator on this group shares, or an unreported one where there
     *     is no group
     */
    public Counter counter(String name) {
        Preconditions.checkNotNull(name, "name must not be null");
        if (registrations == null) {
            return new ThreadSafeSimpleCounter();
        }
        synchronized (registrations) {
            return registrations.counters.computeIfAbsent(
                    name, key -> group.counter(key, new ThreadSafeSimpleCounter()));
        }
    }

    /**
     * Points the gauge registered under a name at this enumerator, registering it the first time.
     *
     * @param name the metric name, from the connector's {@code *MetricNames} inventory
     * @param gauge what this enumerator reports under the name
     * @param <T> the gauge's value type, the same for every enumerator on the group
     */
    public <T> void gauge(String name, Gauge<T> gauge) {
        Preconditions.checkNotNull(name, "name must not be null");
        bind(name, gauge, registered -> group.gauge(name, registered));
    }

    /**
     * Points Flink's standard {@code unassignedSplits} gauge at this enumerator, registering it
     * through {@link SplitEnumeratorMetricGroup#setUnassignedSplitsGauge(Gauge)} the first time.
     *
     * @param gauge what this enumerator reports as its unassigned splits
     */
    public void unassignedSplitsGauge(Gauge<Long> gauge) {
        bind(UNASSIGNED_SPLITS, gauge, registered -> group.setUnassignedSplitsGauge(registered));
    }

    /**
     * Freezes every gauge this enumerator still feeds at its current value.
     *
     * <p>A gauge that throws as it is read for the last time is frozen at {@code null} instead, so
     * closing never fails over a metric and never leaves a gauge holding the closed enumerator.
     */
    @Override
    public void close() {
        synchronized (releases) {
            for (Runnable release : releases) {
                release.run();
            }
            releases.clear();
        }
    }

    private static Registrations registrationsOf(SplitEnumeratorMetricGroup group) {
        MetricGroup identity = group.addGroup(IDENTITY_GROUP);
        synchronized (REGISTRATIONS) {
            return REGISTRATIONS.computeIfAbsent(identity, ignored -> new Registrations());
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void bind(String name, Gauge<T> gauge, Consumer<RebindableGauge<T>> registerFirst) {
        Preconditions.checkNotNull(gauge, "gauge must not be null");
        if (registrations == null) {
            return;
        }
        synchronized (registrations) {
            RebindableGauge<T> registered = (RebindableGauge<T>) registrations.gauges.get(name);
            if (registered == null) {
                // Registered already bound, so no reporter reads it before anything feeds it.
                registered = new RebindableGauge<>(gauge);
                registerFirst.accept(registered);
                registrations.gauges.put(name, registered);
            } else {
                registered.bind(gauge);
            }
            RebindableGauge<T> release = registered;
            synchronized (releases) {
                releases.add(() -> release.release(gauge));
            }
        }
    }

    /** What one enumerator group has registered. Guarded by itself. */
    private static final class Registrations {
        private final Map<String, Counter> counters = new HashMap<>();
        private final Map<String, RebindableGauge<?>> gauges = new HashMap<>();
        private final Map<String, Object> shared = new HashMap<>();
    }

    /**
     * The gauge a group registers once, reading whichever enumerator bound it last.
     *
     * @param <T> the value type
     */
    private static final class RebindableGauge<T> implements Gauge<T> {

        private volatile Gauge<T> target;

        private RebindableGauge(Gauge<T> target) {
            this.target = target;
        }

        private synchronized void bind(Gauge<T> gauge) {
            target = gauge;
        }

        /** Replaces the gauge with its current value, if it is still the one being read. */
        private synchronized void release(Gauge<T> gauge) {
            if (target != gauge) {
                return;
            }
            T last;
            try {
                last = gauge.getValue();
            } catch (RuntimeException e) {
                // The enumerator's state can be mid-change on the coordinator thread as it closes.
                last = null;
            }
            T frozen = last;
            target = () -> frozen;
        }

        @Override
        public T getValue() {
            return target.getValue();
        }
    }
}
