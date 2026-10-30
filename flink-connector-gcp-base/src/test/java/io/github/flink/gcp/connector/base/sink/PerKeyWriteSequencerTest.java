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

package io.github.flink.gcp.connector.base.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PerKeyWriteSequencerTest {

    private final PerKeyWriteSequencer<String, String> sequencer = new PerKeyWriteSequencer<>();

    @Test
    void admitsTheFirstWriteOfAKeyAndHandsBackHeldWritesInArrivalOrder() {
        assertThat(sequencer.tryAdmit("a")).isTrue();
        assertThat(sequencer.tryAdmit("b")).isTrue();
        assertThat(sequencer.tryAdmit("a")).isFalse();
        sequencer.hold("a", "a2");
        sequencer.hold("a", "a3");

        assertThat(sequencer.release("a")).isEqualTo("a2");
        // a2 holds the key in its turn, so a later write is held behind a3.
        assertThat(sequencer.tryAdmit("a")).isFalse();
        sequencer.hold("a", "a4");
        assertThat(sequencer.release("a")).isEqualTo("a3");
        assertThat(sequencer.release("a")).isEqualTo("a4");
        assertThat(sequencer.release("a")).isNull();
        // b was never held behind a.
        assertThat(sequencer.release("b")).isNull();
    }

    @Test
    void aReleasedKeyAdmitsAgain() {
        sequencer.tryAdmit("a");
        assertThat(sequencer.release("a")).isNull();

        assertThat(sequencer.tryAdmit("a")).isTrue();
    }

    @Test
    void releasingOrHoldingForAKeyWithNoAdmittedWriteIsRefused() {
        assertThatThrownBy(() -> sequencer.release("a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no admitted write");
        assertThatThrownBy(() -> sequencer.hold("a", "a1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no admitted write");
        sequencer.tryAdmit("a");
        sequencer.release("a");
        assertThatThrownBy(() -> sequencer.release("a")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void clearForgetsEveryKeyAndHeldWrite() {
        sequencer.tryAdmit("a");
        sequencer.hold("a", "a2");
        sequencer.clear();

        assertThat(sequencer.tryAdmit("a")).isTrue();
        assertThat(sequencer.release("a")).isNull();
    }
}
