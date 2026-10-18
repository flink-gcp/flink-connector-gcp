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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryInputTest {
    // The line the shared recovery exercise in tools/tier3 parses (flink_tier3/exercise.py).
    private static final Pattern PROGRESS =
            Pattern.compile(
                    "event=bigtable-progress run_id=(\\S+) phase=(\\S+) lineage=(\\S+) "
                            + "restored=(true|false) processed=([0-9]+) sequence=([0-9]+)");

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
        "instance,bt1316-other",
        "change-stream-start,2026-10-18T00:00:01Z",
        "change-stream-end,2026-10-18T02:00:01Z",
        "records,101",
        "records-per-second,99"
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

    @Test
    void progressLinesCarryTheLineageAcrossTheUpgradeInTheExerciseShape() throws Exception {
        // One record per second reports the first record, every thirtieth and the last.
        String[] slow = {"--records", "65", "--records-per-second", "1"};
        OperatorSubtaskState saved;
        List<String> initial;
        try (var capture = LogCapture.of(RecoveryInput.class, LogCapture.Level.INFO);
                var harness = harness(slow)) {
            harness.open();
            for (long sequence = 0; sequence < 40; sequence++) {
                harness.processElement(sequence, 0);
            }
            saved = harness.snapshot(1, 0);
            initial = progress(capture.getMessages());
        }
        List<String> upgrade;
        try (var capture = LogCapture.of(RecoveryInput.class, LogCapture.Level.INFO);
                var harness =
                        harness(
                                "--records",
                                "65",
                                "--records-per-second",
                                "1",
                                "--phase",
                                "upgrade",
                                "--require-restored",
                                "true")) {
            harness.initializeState(saved);
            harness.open();
            for (long sequence = 40; sequence < 65; sequence++) {
                harness.processElement(sequence, 0);
            }
            upgrade = progress(capture.getMessages());
        }
        assertThat(initial).hasSize(2);
        assertThat(upgrade).hasSize(3);
        Matcher first = match(initial.get(0));
        String lineage = first.group(3);
        assertThat(lineage).matches("[a-f0-9-]{36}");
        assertThat(fields(first)).containsExactly("bigtable-it", "initial", "false", "1", "0");
        assertThat(fields(match(initial.get(1))))
                .containsExactly("bigtable-it", "initial", "false", "30", "29");
        assertThat(upgrade.stream().map(RecoveryInputTest::match).map(RecoveryInputTest::fields))
                .containsExactly(
                        List.of("bigtable-it", "upgrade", "true", "41", "40"),
                        List.of("bigtable-it", "upgrade", "true", "60", "59"),
                        List.of("bigtable-it", "upgrade", "true", "65", "64"));
        assertThat(upgrade).allSatisfy(line -> assertThat(match(line).group(3)).isEqualTo(lineage));
    }

    private static List<String> progress(List<String> messages) {
        return messages.stream().filter(line -> PROGRESS.matcher(line).find()).toList();
    }

    private static Matcher match(String line) {
        Matcher matcher = PROGRESS.matcher(line);
        assertThat(matcher.find()).isTrue();
        return matcher;
    }

    // Run, phase, restored, processed and sequence: everything but the random lineage.
    private static List<String> fields(Matcher matcher) {
        return List.of(
                matcher.group(1),
                matcher.group(2),
                matcher.group(4),
                matcher.group(5),
                matcher.group(6));
    }

    private static OneInputStreamOperatorTestHarness<Long, Long> harness(String... overrides)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new StreamMap<>(
                        new RecoveryInput(
                                RecoveryOptions.parse(RecoveryOptionsTest.arguments(overrides)))));
    }
}
