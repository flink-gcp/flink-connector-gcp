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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** The reader that keeps the sink's gauge objects, exercised without a cluster. */
class SinkGaugeReporterTest {
    private static final String NAME = "stagedBytes";

    // The reporter takes a group but reads nothing from it; Flink's own unregistered
    // group is enough, and keeps this module free of a test-utils dependency.
    private final MetricGroup group = UnregisteredMetricsGroup.createSinkWriterMetricGroup();
    private final SinkGaugeReporter reporter = new SinkGaugeReporter();

    @BeforeEach
    @AfterEach
    void forget() {
        SinkGaugeReporter.reset();
        SinkGaugeReporter.watch(Set.of(NAME, "stagedTasks", "requests"));
    }

    private Gauge<Long> register(String name, AtomicLong value) {
        Gauge<Long> gauge = value::get;
        reporter.notifyOfAddedMetric(gauge, name, group);
        return gauge;
    }

    @Test
    void everySubtaskIsKeptSoACellIsNotOneSubtask() {
        register(NAME, new AtomicLong(30));
        register(NAME, new AtomicLong(70));

        assertThat(SinkGaugeReporter.read().get(NAME))
                .isEqualTo(new SinkGaugeReporter.Reading(100, 30, 70));
    }

    @Test
    void aRegisteredGaugeNoReadingCaughtIsRegisteredButNotObserved() {
        // The committer's replay gauges report -1 outside a commit, and a one-second reader sees
        // that almost every time. It is registered; it was never observed; neither is a defect.
        register(NAME, new AtomicLong(-1));

        assertThat(SinkGaugeReporter.read()).doesNotContainKey(NAME);
        assertThat(SinkGaugeReporter.registered()).containsExactly(NAME);
        assertThat(SinkGaugeReporter.observed()).isEmpty();
    }

    @Test
    void oneIdleSubtaskDoesNotDiscardAnotherSubtasksValue() {
        // The defect this shape exists to prevent: reducing first and filtering the sentinel
        // afterwards drags the minimum to -1, so a four-way cell loses the budget a committer
        // inside a commit was actually reporting, and the name lands in neither summary field.
        register(NAME, new AtomicLong(-1));
        register(NAME, new AtomicLong(500));
        register(NAME, new AtomicLong(-1));

        assertThat(SinkGaugeReporter.read().get(NAME))
                .isEqualTo(new SinkGaugeReporter.Reading(500, 500, 500));
        assertThat(SinkGaugeReporter.observed()).containsExactly(NAME);
    }

    @Test
    void observationSurvivesTheGaugeGoingIdleAgain() {
        AtomicLong value = new AtomicLong(42);
        register(NAME, value);
        SinkGaugeReporter.read();
        value.set(-1);

        assertThat(SinkGaugeReporter.read()).doesNotContainKey(NAME);
        assertThat(SinkGaugeReporter.observed()).containsExactly(NAME);
    }

    @Test
    void aRemovedRegistrationStopsContributing() {
        // A restarted task's metrics are unregistered. Its writer answers getValue() forever, so
        // keeping it would add a dead attempt's count to every later reading of the same name.
        Gauge<Long> dead = register(NAME, new AtomicLong(70));
        register(NAME, new AtomicLong(30));
        reporter.notifyOfRemovedMetric(dead, NAME, group);

        assertThat(SinkGaugeReporter.read().get(NAME))
                .isEqualTo(new SinkGaugeReporter.Reading(30, 30, 30));
        // The name was registered, and a restart does not unsay that.
        assertThat(SinkGaugeReporter.registered()).containsExactly(NAME);
    }

    @Test
    void aDeafReaderNeverSeesTheRegistration() {
        SinkGaugeReporter.deafTo(Set.of(NAME));
        register(NAME, new AtomicLong(30));
        register("stagedTasks", new AtomicLong(1));

        assertThat(SinkGaugeReporter.registered()).containsExactly("stagedTasks");
        assertThat(SinkGaugeReporter.read()).doesNotContainKey(NAME);
    }

    @Test
    void aGaugeOutsideTheWatchedSetIsIgnored() {
        // Flink offers the reporter every metric in the process, including its own; collecting
        // them all would mean reading Flink's numbers as though they were the sink's.
        register("numRecordsOut", new AtomicLong(99));

        assertThat(SinkGaugeReporter.registered()).isEmpty();
        assertThat(SinkGaugeReporter.read()).isEmpty();
    }

    @Test
    void aMetricThatIsNotAGaugeIsIgnored() {
        reporter.notifyOfAddedMetric(new SimpleCounter(), "requests", group);

        assertThat(SinkGaugeReporter.registered()).isEmpty();
    }

    @Test
    void aGaugeThatIsNotANumberIsAnErrorRatherThanASilentSkip() {
        Gauge<String> text = () -> "green";
        reporter.notifyOfAddedMetric(text, NAME, group);

        assertThat(SinkGaugeReporter.registered()).containsExactly(NAME);
        org.assertj.core.api.Assertions.assertThatThrownBy(SinkGaugeReporter::read)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NAME);
    }

    @Test
    void resetForgetsEverythingSoOneRunCannotReadAnothers() {
        register(NAME, new AtomicLong(30));
        SinkGaugeReporter.read();
        SinkGaugeReporter.reset();

        assertThat(SinkGaugeReporter.registered()).isEmpty();
        assertThat(SinkGaugeReporter.observed()).isEmpty();
        assertThat(SinkGaugeReporter.read()).isEmpty();
    }
}
