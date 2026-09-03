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

package io.github.flink.gcp.connector.base.metrics;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DestinationMetrics}, asserting through registered names. */
class DestinationMetricsTest {

    private static final String TOPIC_A = "projects/p/topics/a";
    private static final String TOPIC_B = "projects/p/topics/b";

    private final MetricListener listener = new MetricListener();

    @Test
    void countsSendsAndErrorsPerDestinationWhenEnabled() {
        DestinationMetrics metrics = DestinationMetrics.of(listener.getMetricGroup(), true);

        metrics.forDestination(TOPIC_A).recordSent();
        metrics.forDestination(TOPIC_A).recordSent();
        metrics.forDestination(TOPIC_A).sendFailed();
        metrics.forDestination(TOPIC_B).recordSent();

        assertThat(counter(TOPIC_A, "recordsSend")).isEqualTo(2);
        assertThat(counter(TOPIC_A, "sendErrors")).isEqualTo(1);
        assertThat(counter(TOPIC_B, "recordsSend")).isEqualTo(1);
        assertThat(counter(TOPIC_B, "sendErrors")).isZero();
    }

    @Test
    void registersNothingAtAllWhenDisabled() {
        DestinationMetrics metrics = DestinationMetrics.of(listener.getMetricGroup(), false);

        metrics.forDestination(TOPIC_A).recordSent();
        metrics.forDestination(TOPIC_A).sendFailed();

        // Off means off: not a registered counter left at zero, but no registration at all — which
        // is the whole point, since Flink cannot unregister one afterwards.
        assertThat(listener.getCounter("destination", TOPIC_A, "recordsSend")).isEmpty();
        assertThat(listener.getCounter("destination", TOPIC_A, "sendErrors")).isEmpty();
    }

    @Test
    void handsOutTheSameCountersForADestinationItAlreadyKnows() {
        // The eviction case: a writer that dropped its per-destination state and rebuilt it later
        // must keep counting where it left off, because a re-registration would be refused.
        DestinationMetrics metrics = DestinationMetrics.of(listener.getMetricGroup(), true);

        DestinationMetrics.Counters first = metrics.forDestination(TOPIC_A);
        first.recordSent();
        DestinationMetrics.Counters afterRebuild = metrics.forDestination(TOPIC_A);
        afterRebuild.recordSent();

        assertThat(afterRebuild).isSameAs(first);
        assertThat(counter(TOPIC_A, "recordsSend")).isEqualTo(2);
    }

    @Test
    void registersTheSuppliedCounterTypeUnderTheSameNames() {
        DestinationMetrics metrics =
                DestinationMetrics.of(
                        listener.getMetricGroup(), true, ThreadSafeSimpleCounter::new);

        metrics.forDestination(TOPIC_A).recordSent();
        metrics.forDestination(TOPIC_A).sendFailed();

        assertThat(listener.getCounter("destination", TOPIC_A, "recordsSend"))
                .get()
                .isInstanceOf(ThreadSafeSimpleCounter.class);
        assertThat(listener.getCounter("destination", TOPIC_A, "sendErrors"))
                .get()
                .isInstanceOf(ThreadSafeSimpleCounter.class);
        assertThat(counter(TOPIC_A, "recordsSend")).isEqualTo(1);
        assertThat(counter(TOPIC_A, "sendErrors")).isEqualTo(1);
    }

    @Test
    void theDefaultCounterIsThePlainOne() {
        DestinationMetrics metrics = DestinationMetrics.of(listener.getMetricGroup(), true);

        metrics.forDestination(TOPIC_A).recordSent();

        assertThat(listener.getCounter("destination", TOPIC_A, "recordsSend"))
                .get()
                .isExactlyInstanceOf(SimpleCounter.class);
    }

    private long counter(String destination, String name) {
        return listener.getCounter("destination", destination, name)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No "
                                                + name
                                                + " counter registered for "
                                                + destination
                                                + "."))
                .getCount();
    }
}
