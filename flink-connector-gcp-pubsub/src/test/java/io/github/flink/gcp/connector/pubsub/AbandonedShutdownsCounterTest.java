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

package io.github.flink.gcp.connector.pubsub;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the read-only counter view the two residues register through. Its own class since #329,
 * when the dead-letter queue became the second registrar — before that it was a private class
 * inside {@code PubSubSinkWriterMetrics} and only its registered behaviour was covered.
 */
class AbandonedShutdownsCounterTest {

    @Test
    void readsTheAdderItWasGivenLiveRatherThanASnapshot() {
        LongAdder residue = new LongAdder();
        AbandonedShutdownsCounter counter = new AbandonedShutdownsCounter(residue);

        assertThat(counter.getCount()).isZero();

        residue.increment();
        residue.increment();

        // Read after construction, so a view holding a snapshot would report zero here — which is
        // the whole point: the count outlives the task and grows between attempts.
        assertThat(counter.getCount()).isEqualTo(2);
    }

    @Test
    void everyMutatorThrowsRatherThanSilentlyDoingNothing() {
        AbandonedShutdownsCounter counter = new AbandonedShutdownsCounter(new LongAdder());

        // A no-op would hide a caller that believed it was counting something: the teardowns count
        // through the adder, never through the metric.
        assertThatThrownBy(counter::inc).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.inc(3)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(counter::dec).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.dec(3)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void theTwoResiduesAreSeparateAdders() {
        // What keeps `deadLetterPublisherShutdownsAbandoned` from being a second name for one
        // number — and what lets the queue register on a host sink group that already carries the
        // sink's own name.
        assertThat(PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED)
                .isNotSameAs(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED);
    }

    @Test
    void resetForTestsClearsBothResidues() {
        PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.increment();
        PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.increment();

        PubSubShutdownResidue.resetForTests();

        // A reset that forgot the second adder would leave one test's give-up leaking into the
        // absolute assertions of every later class in the fork.
        assertThat(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.sum()).isZero();
        assertThat(PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.sum()).isZero();
    }
}
