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

import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the two subscriber-teardown counters the reader registers without incrementing (#358).
 *
 * <p>Everything else these metrics do is asserted where it is driven — the counters and gauges
 * through {@code PubSubSplitReader} and {@code PubSubSourceReader}, the teardown increments through
 * {@code PubSubNotifyingPullSubscriberTest} — and what is left over is the wiring between them:
 * that each name reads the adder the teardowns write, and reads it live rather than as a value
 * snapshotted when the reader was built. Read through the registered metric a reporter would call,
 * never through the adder, since the adder is the half already covered.
 */
class PubSubSourceReaderMetricsTest {

    /**
     * Before and after: the residues are static, so this class both needs a clean start to assert
     * absolute counts and owes the fork's later classes one.
     */
    @BeforeEach
    void clearTheResidues() {
        PubSubShutdownResidue.resetForTests();
    }

    @AfterEach
    void clearTheResiduesAgain() {
        PubSubShutdownResidue.resetForTests();
    }

    @Test
    void eachTeardownCounterReadsItsOwnResidueLive() {
        TestReaderMetrics metrics = new TestReaderMetrics();

        // Registered by a reader that has seen nothing, which is the case the whole mechanism is
        // for: the residue of an earlier attempt is reported by whichever attempt runs next.
        assertThat(metrics.counter("subscriberShutdownsAbandoned")).isZero();
        assertThat(metrics.counter("subscriberFailuresUnreported")).isZero();

        PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.increment();

        // Incremented after the registration, so a counter holding a snapshot would still say zero.
        // The second assertion is what a swapped pair of adders would fail.
        assertThat(metrics.counter("subscriberShutdownsAbandoned")).isEqualTo(1);
        assertThat(metrics.counter("subscriberFailuresUnreported")).isZero();

        PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.increment();
        PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.increment();

        assertThat(metrics.counter("subscriberFailuresUnreported")).isEqualTo(2);
        assertThat(metrics.counter("subscriberShutdownsAbandoned")).isEqualTo(1);
    }

    @Test
    void aPublishersAbandonedTeardownIsNotTheReadersToReport() {
        TestReaderMetrics metrics = new TestReaderMetrics();

        // A job with a Pub/Sub source and a Pub/Sub sink runs both in one class loader, so all four
        // residues are live at once, and the sink's are reported on a group of their own. Reading a
        // publisher's give-up here would tell an operator that subscribers are failing to shut
        // down.
        PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.increment();
        PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.increment();

        assertThat(metrics.counter("subscriberShutdownsAbandoned")).isZero();
        assertThat(metrics.counter("subscriberFailuresUnreported")).isZero();
    }
}
