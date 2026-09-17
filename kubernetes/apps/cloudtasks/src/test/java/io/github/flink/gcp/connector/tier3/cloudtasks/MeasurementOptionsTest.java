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
}
