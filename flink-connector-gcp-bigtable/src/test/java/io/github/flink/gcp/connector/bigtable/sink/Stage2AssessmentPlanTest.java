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

package io.github.flink.gcp.connector.bigtable.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2AssessmentPlanTest {
    @TempDir Path directory;

    @Test
    void everyFactorialCellHasThreeFreshPairedRepetitionsInDeclaredOrder() {
        var runs = Stage2AssessmentPlan.runs();
        assertThat(runs).hasSize(648).doesNotHaveDuplicates();
        assertThat(runs).extracting(Stage2AssessmentPlan.Run::table).doesNotHaveDuplicates();
        var cells = runs.stream().collect(Collectors.groupingBy(run -> run.cell));
        assertThat(cells).hasSize(108);
        assertThat(cells.keySet())
                .extracting(Stage2AssessmentPlan.Cell::id)
                .doesNotHaveDuplicates();
        cells.forEach(
                (cell, repetitions) -> {
                    assertThat(repetitions)
                            .extracting(run -> run.repetition)
                            .containsExactly(1, 1, 2, 2, 3, 3);
                    assertThat(repetitions)
                            .extracting(run -> run.staged)
                            .containsExactly(false, true, true, false, false, true);
                });
        assertThat(cells.keySet()).extracting(cell -> cell.payloadBytes).containsOnly(1024, 65536);
        assertThat(cells.keySet()).extracting(cell -> cell.hot).containsOnly(false, true);
        assertThat(cells.keySet()).extracting(cell -> cell.parallelism).containsOnly(1, 4, 16);
        assertThat(cells.keySet()).extracting(cell -> cell.inFlight).containsOnly(1, 4, 16);
        assertThat(cells.keySet())
                .extracting(cell -> cell.checkpointSeconds)
                .containsOnly(1, 10, 60);
    }

    @Test
    void sixtySecondCheckpointsKeepTheirFullWarmupAndObservation() {
        var longest = new Stage2AssessmentPlan.Cell(65536, true, 16, 16, 60);
        assertThat(longest.warmupSeconds()).isEqualTo(60);
        assertThat(longest.measurementSeconds()).isEqualTo(180);
        long seconds =
                Stage2AssessmentPlan.runs().stream()
                        .mapToLong(run -> run.cell.warmupSeconds() + run.cell.measurementSeconds())
                        .sum();
        assertThat(seconds).isEqualTo(69_120);
    }

    @Test
    void generatingThePlanDoesNotCreateALeaseAndCannotOverwriteEvidence() throws Exception {
        Path output = directory.resolve("matrix.csv");
        BigtableStage2Probe.main(new String[] {"plan-formal", output.toString()});
        List<String> lines = Files.readAllLines(output);
        assertThat(lines).hasSize(649);
        assertThat(lines.get(1))
                .isEqualTo(
                        "1,b1024-even-p1-i1-c1,b1024-even-p1-i1-c1-r1-bulk,bulk,1,1024,even,1,1,1,10,30");
        assertThat(lines.get(648))
                .isEqualTo(
                        "648,b65536-hot-p16-i16-c60,b65536-hot-p16-i16-c60-r3-staged,staged,3,65536,hot,16,16,60,60,180");
        assertThatThrownBy(() -> Stage2AssessmentPlan.write(output))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readAllLines(output)).isEqualTo(lines);
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).containsExactly(output);
        }
    }

    @Test
    void alteredCellsAndAdditionalRepetitionsAreRejected() {
        assertThatThrownBy(() -> new Stage2AssessmentPlan.Cell(1024, false, 2, 4, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Stage2AssessmentPlan.Cell(1024, false, 1, 4, 5))
                .isInstanceOf(IllegalArgumentException.class);
        var cell = new Stage2AssessmentPlan.Cell(1024, false, 1, 4, 1);
        assertThatThrownBy(() -> new Stage2AssessmentPlan.Run(cell, 4, true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
