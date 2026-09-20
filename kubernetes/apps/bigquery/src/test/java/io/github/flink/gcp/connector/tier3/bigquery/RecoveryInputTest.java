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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryInputTest {
    @Test
    void resumesAtTheSnapshottedPositionAndRejectsReplayOrGaps() throws Exception {
        OperatorSubtaskState saved;
        try (var harness = harness()) {
            harness.open();
            for (long sequence = 0; sequence < 10; sequence++) {
                harness.processElement(sequence, 0);
            }
            saved = harness.snapshot(1, 0);
        }
        try (var harness = harness("--phase", "upgrade", "--require-restored", "true")) {
            harness.initializeState(saved);
            harness.open();
            assertThatThrownBy(() -> harness.processElement(9L, 0))
                    .hasMessageContaining("expected=10 actual=9");
            harness.processElement(10L, 0);
            assertThatThrownBy(() -> harness.processElement(12L, 0))
                    .hasMessageContaining("expected=11 actual=12");
            assertThat(harness.extractOutputValues()).containsExactly(10L);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "run-id,other",
        "mode,ALO",
        "destinations,50",
        "records,100",
        "bytes-per-second,1024"
    })
    void refusesDifferentTrialInputs(String name, String value) throws Exception {
        OperatorSubtaskState saved;
        try (var harness = harness()) {
            harness.open();
            harness.processElement(0L, 0);
            saved = harness.snapshot(1, 0);
        }
        try (var harness = harness("--" + name, value)) {
            assertThatThrownBy(() -> harness.initializeState(saved))
                    .hasMessageContaining("another trial or configuration");
        }
    }

    @Test
    void requiredStateCannotBeReplacedByAFreshStart() throws Exception {
        try (var harness = harness("--require-restored", "true")) {
            assertThatThrownBy(harness::open).hasMessageContaining("requires restored state");
        }
        try (var harness = harness()) {
            assertThatThrownBy(
                            () -> harness.initializeState(OperatorSubtaskState.builder().build()))
                    .hasMessageContaining("state is missing");
        }
    }

    private static OneInputStreamOperatorTestHarness<Long, Long> harness(String... overrides)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new StreamMap<>(
                        new RecoveryInput(
                                RecoveryOptions.parse(RecoveryOptionsTest.arguments(overrides)))));
    }
}
