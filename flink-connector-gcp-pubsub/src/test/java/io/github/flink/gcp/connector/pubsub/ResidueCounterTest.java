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

package io.github.flink.gcp.connector.pubsub;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the read-only counter view the two residues register through. Its own class since #329,
 * when the dead-letter queue became the second registrar — before that it was a private class
 * inside {@code PubSubWriterMetrics} and only its registered behaviour was covered.
 */
class ResidueCounterTest {

    @Test
    void readsTheAdderItWasGivenLiveRatherThanASnapshot() {
        LongAdder residue = new LongAdder();
        ResidueCounter counter = new ResidueCounter(residue);

        assertThat(counter.getCount()).isZero();

        residue.increment();
        residue.increment();

        // Read after construction, so a view holding a snapshot would report zero here — which is
        // the whole point: the count outlives the task and grows between attempts.
        assertThat(counter.getCount()).isEqualTo(2);
    }

    @Test
    void everyMutatorThrowsRatherThanSilentlyDoingNothing() {
        ResidueCounter counter = new ResidueCounter(new LongAdder());

        // A no-op would hide a caller that believed it was counting something: the teardowns count
        // through the adder, never through the metric.
        assertThatThrownBy(counter::inc).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.inc(3)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(counter::dec).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> counter.dec(3)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void everyResidueIsAnAdderOfItsOwn() {
        // What keeps `deadLetterPublisherShutdownsAbandoned` from being a second name for one
        // number — and what lets the queue register on a host sink group that already carries the
        // sink's own name. The source's two are separate for a different reason (#358): they are
        // registered on the reader's group, where neither could collide with a publisher name, but
        // an expired wait and a failure nothing else reports are different things to act on.
        // By identity rather than through doesNotHaveDuplicates(), which would rest on LongAdder
        // not overriding equals — true today and not the property being asserted.
        Set<LongAdder> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        distinct.addAll(
                List.of(
                        PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED,
                        PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED,
                        PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED,
                        PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED));

        assertThat(distinct).hasSize(4);
    }

    @Test
    void resetForTestsClearsEveryResidue() {
        PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.increment();
        PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.increment();
        PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.increment();
        PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.increment();

        PubSubShutdownResidue.resetForTests();

        // A reset that forgot one adder would leave that test's give-up leaking into the absolute
        // assertions of every later class in the fork.
        assertThat(PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED.sum()).isZero();
        assertThat(PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.sum()).isZero();
        assertThat(PubSubShutdownResidue.SUBSCRIBER_SHUTDOWNS_ABANDONED.sum()).isZero();
        assertThat(PubSubShutdownResidue.SUBSCRIBER_FAILURES_UNREPORTED.sum()).isZero();
    }
}
