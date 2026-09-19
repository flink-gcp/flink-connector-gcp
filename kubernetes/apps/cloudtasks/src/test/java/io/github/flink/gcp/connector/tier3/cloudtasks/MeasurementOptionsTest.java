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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeasurementOptionsTest {
    static String[] arguments(String... changes) {
        List<String> args =
                new ArrayList<>(
                        List.of(
                                "--run-id",
                                "ct1246-test",
                                "--cell-id",
                                "hash-1",
                                "--queue",
                                "projects/flink-gcp/locations/us-central1/queues/ct1246-test",
                                "--target",
                                "https://example.invalid/measurement",
                                "--arm",
                                "STAGED_HASH",
                                "--body-bytes",
                                "1024",
                                "--parallelism",
                                "4",
                                "--concurrency",
                                "4",
                                "--checkpoint-seconds",
                                "1",
                                "--records",
                                "100",
                                "--warmup-records",
                                "10",
                                "--distribution",
                                "even",
                                "--offered-rate",
                                "100"));
        for (int i = 0; i < changes.length; i += 2) {
            int position = args.indexOf(changes[i]);
            if (position < 0) {
                args.add(changes[i]);
                args.add(changes[i + 1]);
            } else {
                args.set(position + 1, changes[i + 1]);
            }
        }
        return args.toArray(String[]::new);
    }

    static String[] windowArguments(String... changes) {
        List<String> args = new ArrayList<>(List.of(arguments(changes)));
        for (String name : List.of("--records", "--warmup-records")) {
            int position = args.indexOf(name);
            args.remove(position + 1);
            args.remove(position);
        }
        for (String[] entry :
                List.of(
                        new String[] {"--warmup-seconds", "60"},
                        new String[] {"--observation-seconds", "180"},
                        new String[] {"--record-limit", "10000000"},
                        new String[] {"--attempt-limit", "60000"})) {
            if (!args.contains(entry[0])) {
                args.addAll(List.of(entry));
            }
        }
        return args.toArray(String[]::new);
    }

    @Test
    void budgetsTheWholeWindowAndTwoPeriodicCheckpointsBeforeEndOfInput() {
        var options =
                MeasurementOptions.parse(
                        windowArguments(
                                "--offered-rate",
                                "1000",
                                "--checkpoint-seconds",
                                "60",
                                "--warmup-seconds",
                                "120",
                                "--observation-seconds",
                                "360"));
        assertThat(options.records).isEqualTo(601000);
        assertThat(options.warmup).isZero();
        assertThat(options.attemptLimit).isEqualTo(60000);
        assertThat(options.receiptPrefix())
                .isEqualTo(
                        "gs://flink-gcp-cloudtasks-benchmark/runs/ct1246-test/cells/hash-1/receipts/");
        assertThat(options.rowsPrefix())
                .isEqualTo(
                        "gs://flink-gcp-cloudtasks-benchmark/runs/ct1246-test/cells/hash-1/rows/");
        assertThatThrownBy(
                        () ->
                                MeasurementOptions.parse(
                                        windowArguments(
                                                "--offered-rate",
                                                "1000",
                                                "--record-limit",
                                                "100000")))
                .hasMessageContaining("record-limit");
    }

    @Test
    void rejectsAmbiguousWindowInputsAndUnapprovedLimits() {
        for (String[] change :
                List.of(
                        new String[] {"--warmup-seconds", "0"},
                        new String[] {"--observation-seconds", "601"},
                        new String[] {"--record-limit", "10000001"},
                        new String[] {"--record-limit", "0"},
                        new String[] {"--attempt-limit", "0"},
                        new String[] {"--attempt-limit", "100000000"})) {
            assertThatThrownBy(() -> MeasurementOptions.parse(windowArguments(change)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(
                        () -> MeasurementOptions.parse(arguments("--observation-seconds", "180")))
                .hasMessageContaining("record-count mode");
        assertThatThrownBy(() -> MeasurementOptions.parse(arguments("--record-limit", "100")))
                .hasMessageContaining("require observation-seconds");
    }

    @Test
    void buildsEveryExistingProductionArmWithoutAccessingCredentials() {
        for (var arm : MeasurementOptions.Arm.values()) {
            var options = MeasurementOptions.parse(arguments("--arm", arm.name()));
            var environment = StreamExecutionEnvironment.getExecutionEnvironment();
            CloudTasksMeasurementJob.configure(environment, options);
            var graph = environment.getStreamGraph();
            assertThat(graph.getStreamNodes()).isNotEmpty();
            boolean staged =
                    arm == MeasurementOptions.Arm.STAGED_HASH
                            || arm == MeasurementOptions.Arm.STAGED_RANDOM;
            assertThat(
                            graph.getStreamNodes().stream()
                                    .filter(
                                            node ->
                                                    node.getOperatorFactory()
                                                            instanceof CommitterOperatorFactory)
                                    .count())
                    .as("committer topology for %s", arm)
                    .isEqualTo(staged ? 1L : 0L);
            assertThat(environment.getCheckpointConfig().getCheckpointInterval()).isEqualTo(1000);
        }
    }

    @Test
    void confinesCalibrationControlsToExplicitWindowInputs() {
        var delay =
                MeasurementOptions.parse(
                        windowArguments(
                                "--arm",
                                "UNNAMED",
                                "--parallelism",
                                "1",
                                "--concurrency",
                                "1",
                                "--control-delay-millis",
                                "100"));
        assertThat(delay.controlDelayMillis).isEqualTo(100);
        assertThat(delay.emitAttempts).isTrue();
        assertThat(
                        MeasurementOptions.parse(windowArguments("--emit-attempts", "false"))
                                .emitAttempts)
                .isFalse();
        assertThatThrownBy(() -> MeasurementOptions.parse(arguments("--emit-attempts", "false")))
                .hasMessageContaining("require window mode");
        assertThatThrownBy(
                        () ->
                                MeasurementOptions.parse(
                                        windowArguments("--control-delay-millis", "100")))
                .hasMessageContaining("Delay control requires");
        assertThatThrownBy(
                        () ->
                                MeasurementOptions.parse(
                                        windowArguments("--control-delay-millis", "50")))
                .hasMessageContaining("Unsupported");
        assertThatThrownBy(
                        () -> MeasurementOptions.parse(windowArguments("--emit-attempts", "TRUE")))
                .hasMessageContaining("true or false");
    }

    @Test
    void sendsNinetyPercentToOneSubtaskWithoutRepeatingAnInputIdentity() {
        var skew = MeasurementOptions.parse(arguments("--distribution", "skew"));
        int[] counts = new int[4];
        for (long sequence = 0; sequence < 300; sequence++) {
            counts[skew.partition(sequence, 4)]++;
        }
        assertThat(counts).containsExactly(270, 10, 10, 10);
        var even = MeasurementOptions.parse(arguments());
        Arrays.fill(counts, 0);
        for (long sequence = 0; sequence < 300; sequence++) {
            counts[even.partition(sequence, 4)]++;
        }
        assertThat(counts).containsExactly(75, 75, 75, 75);
        assertThat(skew.partition(29, 1)).isZero();
    }

    @Test
    void rejectsMissingMalformedAndDuplicateArguments() {
        assertThatThrownBy(() -> MeasurementOptions.parse()).hasMessage("Missing --run-id");
        assertThatThrownBy(() -> MeasurementOptions.parse("--run-id"))
                .hasMessage("Expected --name value pairs");
        assertThatThrownBy(() -> MeasurementOptions.parse("run-id", "value"))
                .hasMessage("Expected an option starting with --");
        assertThatThrownBy(
                        () -> MeasurementOptions.parse("--run-id", "first", "--run-id", "second"))
                .hasMessage("Duplicate argument: --run-id");
    }

    @Test
    void rejectsInvalidCostAndStatisticalInputsBeforeCreatingAJob() {
        for (String[] change :
                List.of(
                        new String[] {"--records", "100001"},
                        new String[] {"--records", "10"},
                        new String[] {"--warmup-records", "0"},
                        new String[] {"--offered-rate", "NaN"},
                        new String[] {"--offered-rate", "Infinity"},
                        new String[] {"--offered-rate", "0"},
                        new String[] {"--offered-rate", "10001"},
                        new String[] {"--checkpoint-seconds", "0"},
                        new String[] {"--parallelism", "2"},
                        new String[] {
                            "--queue", "projects/other/locations/us-central1/queues/ct1246-test"
                        },
                        new String[] {"--target", "https://user:secret@example.invalid/"},
                        new String[] {"--extra", "1"})) {
            assertThatThrownBy(() -> MeasurementOptions.parse(arguments(change)))
                    .as(Arrays.toString(change))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @Tag("slow")
    void knownDelayIsIncludedInTheMeasuredSerializationOrigin() {
        var options =
                MeasurementOptions.parse(
                        windowArguments(
                                "--arm",
                                "UNNAMED",
                                "--parallelism",
                                "1",
                                "--concurrency",
                                "1",
                                "--control-delay-millis",
                                "100"));
        var task = CloudTasksMeasurementJob.serialize(options, 0);
        var origin = MeasurementPayload.read(task.getHttpRequest().getBody());
        assertThat(origin.elapsedNanos(MeasurementPayload.process(), System.nanoTime()))
                .isGreaterThanOrEqualTo(100000000L);
    }

    @Test
    void interruptedControlCannotEmitATaskAndPreservesInterruption() {
        var options =
                MeasurementOptions.parse(
                        windowArguments(
                                "--arm",
                                "UNNAMED",
                                "--parallelism",
                                "1",
                                "--concurrency",
                                "1",
                                "--control-delay-millis",
                                "100"));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> CloudTasksMeasurementJob.serialize(options, 0))
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
