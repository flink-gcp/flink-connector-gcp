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

import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2RunLimitsTest {
    @TempDir Path directory;

    @Test
    void explicitSizingCanExceedTheOldProbeWithoutChangingObservationTiming() throws Exception {
        Path file = directory.resolve("limits.properties");
        Files.writeString(
                file,
                "inventoryEntries=2000000\ninventoryBytes=128000000\n"
                        + "stagedEntries=200000\nstagedBytes=268435456\nworkBytes=4294967296\n"
                        + "checkpointTimeoutMillis=300000\ndrainMillis=240000\n");
        Stage2RunLimits limits = Stage2RunLimits.read(file);
        assertThat(limits.inventoryEntries).isEqualTo(2_000_000);
        assertThat(limits.stagedBytes).isEqualTo(256L << 20);
        assertThat(limits.workBytes).isEqualTo(4L << 30);
        assertThat(limits.checkpointTimeoutMillis).isEqualTo(300_000);
        Path inventory = directory.resolve("inventory.bin");
        try (Stage2Harness harness =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        inventory,
                        limits.inventoryEntries,
                        1024,
                        false,
                        false,
                        1,
                        true,
                        null,
                        limits)) {
            assertThat(Files.size(inventory)).isEqualTo(limits.inventoryBytes);
            harness.ledger.admit(1_999_999, 110);
            assertThat(harness.ledger.entry(1_999_999).admittedAt).isEqualTo(110);
            assertThatThrownBy(() -> harness.ledger.entry(2_000_000))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("capacity exhausted");
        }
        assertThatThrownBy(() -> Stage2RunLimits.historical(2_000_000))
                .hasMessageContaining("Historical calibration inventory exceeds its limit");
        var run = Stage2AssessmentPlan.run("b65536-hot-p16-i16-c60-r3-staged");
        assertThat(run.cell.warmupSeconds()).isEqualTo(60);
        assertThat(run.cell.measurementSeconds()).isEqualTo(180);
        Files.writeString(file, "measurementSeconds=1\n", java.nio.file.StandardOpenOption.APPEND);
        assertThatThrownBy(() -> Stage2RunLimits.read(file)).hasMessageContaining("exactly");
    }

    @Test
    void impossibleInventoryOrOverflowingDeadlineIsRejectedBeforeFileAllocation() {
        assertThatThrownBy(() -> new Stage2RunLimits(10, 639, 10, 1000, 10000, 1000, 1000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> new Stage2RunLimits(10, 640, 10, 1000, 10000, 1000, Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> new Stage2RunLimits(10, 640, 0, 1000, 10000, 1000, 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownCellAndMissingLimitsCannotStartCalibration() throws Exception {
        Path work = directory.resolve("work");
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.main(
                                        new String[] {
                                            "local-formal",
                                            work.toString(),
                                            "b1024-even-p2-i1-c1-r1-bulk",
                                            "missing"
                                        }))
                .hasMessageContaining("Not a formal Stage 2 table");
        Path limits = directory.resolve("empty.properties");
        Files.writeString(limits, "");
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.main(
                                        new String[] {
                                            "local-formal",
                                            work.toString(),
                                            "b1024-even-p1-i1-c1-r1-bulk",
                                            limits.toString()
                                        }))
                .hasMessageContaining("exactly");
        assertThat(work).doesNotExist();
    }
}
