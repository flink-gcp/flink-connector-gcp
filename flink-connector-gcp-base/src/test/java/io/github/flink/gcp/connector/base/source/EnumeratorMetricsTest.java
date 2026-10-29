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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class EnumeratorMetricsTest {

    private final MetricListener listener = new MetricListener();

    /**
     * A new wrapper over the same parent, as {@code SourceCoordinatorContext#metricGroup()} answers
     * on every call: the enumerator group under it is the same open group each time.
     */
    private SplitEnumeratorMetricGroup enumeratorGroup() {
        return new InternalSplitEnumeratorMetricGroup(listener.getMetricGroup());
    }

    @Test
    void aCounterKeepsCountingAcrossEnumeratorsOnOneGroup() {
        EnumeratorMetrics first = EnumeratorMetrics.of(enumeratorGroup());
        first.counter("splitsAssigned").inc(2);
        first.close();

        EnumeratorMetrics second = EnumeratorMetrics.of(enumeratorGroup());
        second.counter("splitsAssigned").inc();

        assertThat(counter("splitsAssigned")).isEqualTo(3);
    }

    @Test
    void aGaugeReadsTheEnumeratorThatBoundItLast() {
        AtomicLong firstValue = new AtomicLong(1);
        AtomicLong secondValue = new AtomicLong(2);
        EnumeratorMetrics first = EnumeratorMetrics.of(enumeratorGroup());
        first.gauge("lagMillis", (Gauge<Long>) firstValue::get);

        EnumeratorMetrics second = EnumeratorMetrics.of(enumeratorGroup());
        second.gauge("lagMillis", (Gauge<Long>) secondValue::get);
        // The earlier enumerator may close after the later one bound; it must not take it back.
        first.close();
        secondValue.set(5);

        assertThat(this.<Long>gauge("lagMillis")).isEqualTo(5L);
    }

    @Test
    void closingFreezesAGaugeNoLaterEnumeratorBound() {
        AtomicLong value = new AtomicLong(7);
        EnumeratorMetrics metrics = EnumeratorMetrics.of(enumeratorGroup());
        metrics.gauge("lagMillis", (Gauge<Long>) value::get);

        metrics.close();
        value.set(9);

        assertThat(this.<Long>gauge("lagMillis")).isEqualTo(7L);
    }

    @Test
    void theUnassignedSplitsGaugeFollowsTheEnumeratorThatBoundItLast() {
        EnumeratorMetrics first = EnumeratorMetrics.of(enumeratorGroup());
        first.unassignedSplitsGauge(() -> 4L);
        first.close();

        EnumeratorMetrics second = EnumeratorMetrics.of(enumeratorGroup());
        second.unassignedSplitsGauge(() -> 1L);

        assertThat(this.<Long>gauge("unassignedSplits")).isEqualTo(1L);
    }

    @Test
    void closingFreezesAGaugeThatThrowsAtNullAndStillReleasesTheRest() {
        AtomicLong value = new AtomicLong(3);
        EnumeratorMetrics metrics = EnumeratorMetrics.of(enumeratorGroup());
        metrics.gauge(
                "assignedSplits",
                (Gauge<Long>)
                        () -> {
                            throw new java.util.ConcurrentModificationException();
                        });
        metrics.gauge("lagMillis", (Gauge<Long>) value::get);

        metrics.close();
        value.set(9);

        assertThat(this.<Long>gauge("assignedSplits")).isNull();
        assertThat(this.<Long>gauge("lagMillis")).isEqualTo(3L);
    }

    @Test
    void aSharedValueIsRegisteredOnceForEveryEnumeratorOnTheGroup() {
        AtomicInteger registrations = new AtomicInteger();

        Object first =
                EnumeratorMetrics.shared(
                        enumeratorGroup(), "client", group -> registrations.incrementAndGet());
        Object second =
                EnumeratorMetrics.shared(
                        enumeratorGroup(), "client", group -> registrations.incrementAndGet());
        Object other =
                EnumeratorMetrics.shared(
                        enumeratorGroup(), "other", group -> registrations.incrementAndGet());

        assertThat(second).isSameAs(first);
        assertThat(other).isNotEqualTo(first);
        assertThat(registrations).hasValue(2);
    }

    @Test
    void anotherGroupGetsItsOwnRegistrations() {
        MetricListener other = new MetricListener();
        EnumeratorMetrics.of(enumeratorGroup()).counter("splitsAssigned").inc();

        EnumeratorMetrics.of(new InternalSplitEnumeratorMetricGroup(other.getMetricGroup()))
                .counter("splitsAssigned");

        assertThat(other.getCounter("enumerator", "splitsAssigned"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isZero());
    }

    @Test
    void anEnumeratorGroupFlinkClosedIsReplacedWithFreshRegistrations() {
        // The job's end closes the enumerator group; one opened afterwards under the same parent,
        // as a job resubmitted into the same JobManager gets, must not take the old registrations.
        Counter before = EnumeratorMetrics.of(enumeratorGroup()).counter("splitsAssigned");
        before.inc();
        ((AbstractMetricGroup<?>) listener.getMetricGroup().addGroup("enumerator")).close();

        Counter after = EnumeratorMetrics.of(enumeratorGroup()).counter("splitsAssigned");

        assertThat(after).isNotSameAs(before);
        assertThat(counter("splitsAssigned")).isZero();
    }

    @Test
    void theIdentityGroupHoldsNoMetric() {
        EnumeratorMetrics.of(enumeratorGroup()).counter("splitsAssigned");

        assertThat(
                        listener.getCounter(
                                "enumerator", EnumeratorMetrics.IDENTITY_GROUP, "splitsAssigned"))
                .isEmpty();
        assertThat(listener.getCounter("enumerator", "splitsAssigned")).isPresent();
    }

    @Test
    void withoutAGroupItHandsOutCountersAndRegistersNothing() {
        EnumeratorMetrics metrics = EnumeratorMetrics.of(null);
        AtomicInteger registrations = new AtomicInteger();

        Counter counter = metrics.counter("splitsAssigned");
        counter.inc();
        metrics.gauge("lagMillis", (Gauge<Long>) () -> 1L);
        metrics.unassignedSplitsGauge(() -> 1L);
        Object shared =
                EnumeratorMetrics.shared(null, "client", group -> registrations.incrementAndGet());
        metrics.close();

        assertThat(counter.getCount()).isEqualTo(1);
        assertThat(metrics.counter("splitsAssigned")).isNotSameAs(counter);
        assertThat(shared).isNull();
        assertThat(registrations).hasValue(0);
    }

    private long counter(String name) {
        return listener.getCounter("enumerator", name).orElseThrow(AssertionError::new).getCount();
    }

    private <T> T gauge(String name) {
        return listener.<T>getGauge("enumerator", name).orElseThrow(AssertionError::new).getValue();
    }
}
