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

package io.github.flink.gcp.connector.tier3.smoke;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SmokeVerifierTest {
    @Test
    void restoresLineageAndEveryKeySequence() throws Exception {
        OperatorSubtaskState state;
        String first;
        try (var harness = harness("initial", false)) {
            harness.open();
            for (long value = 0; value < 8; value++) {
                harness.processElement(value, 0);
            }
            first = harness.extractOutputValues().get(0);
            state = harness.snapshot(1, 0);
        }
        try (var harness = harness("upgrade", true)) {
            harness.initializeState(state);
            harness.open();
            for (long value = 8; value < 12; value++) {
                harness.processElement(value, 0);
            }
            assertThat(harness.extractOutputValues())
                    .singleElement()
                    .asString()
                    .contains(
                            "phase=upgrade",
                            "restored=true",
                            "processed=9",
                            "sequence=8",
                            "lineage=" + field(first, "lineage"));
        }
    }

    @Test
    void missingKeyedStateCannotMasqueradeAsARestore() throws Exception {
        OperatorSubtaskState state;
        try (var harness = harness("initial", false)) {
            harness.open();
            harness.processElement(0L, 0);
            state = harness.snapshot(1, 0);
        }
        try (var harness = harness("upgrade", true)) {
            harness.initializeState(
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(state.getManagedOperatorState())
                            .build());
            harness.open();
            assertThatThrownBy(() -> harness.processElement(4L, 0))
                    .hasMessageContaining("expected=0 actual=4");
        }
    }

    @Test
    void rejectsDuplicateAndMissingRecords() throws Exception {
        try (var harness = harness("initial", false)) {
            harness.open();
            harness.processElement(0L, 0);
            assertThatThrownBy(() -> harness.processElement(0L, 0))
                    .hasMessageContaining("key=0 expected=4 actual=0");
            assertThatThrownBy(() -> harness.processElement(8L, 0))
                    .hasMessageContaining("key=0 expected=4 actual=8");
        }
    }

    @Test
    void freshUpgradeRequiresState() throws Exception {
        try (var harness = harness("upgrade", true)) {
            assertThatThrownBy(harness::open).hasStackTraceContaining("--require-restored=true");
        }
    }

    @Test
    void rejectsStateFromAnotherRun() throws Exception {
        OperatorSubtaskState state;
        try (var harness = harness("initial", false)) {
            harness.open();
            harness.processElement(0L, 0);
            state = harness.snapshot(1, 0);
        }
        var options = SmokeOptions.parse("--run-id", "other-run", "--require-restored", "true");
        try (var harness = harness(options)) {
            assertThatThrownBy(() -> harness.initializeState(state))
                    .hasStackTraceContaining("this --run-id");
        }
    }

    static String field(String line, String name) {
        for (String field : line.split(" ")) {
            if (field.startsWith(name + "=")) {
                return field.substring(name.length() + 1);
            }
        }
        throw new AssertionError("Missing field " + name + ": " + line);
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, Long, String> harness(
            String phase, boolean restored) throws Exception {
        return harness(
                SmokeOptions.parse(
                        "--run-id",
                        "smoke-test",
                        "--phase",
                        phase,
                        "--require-restored",
                        Boolean.toString(restored)));
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, Long, String> harness(
            SmokeOptions options) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new SmokeVerifier(options)),
                value -> (int) (value % 4),
                Types.INT,
                1,
                1,
                0);
    }
}
