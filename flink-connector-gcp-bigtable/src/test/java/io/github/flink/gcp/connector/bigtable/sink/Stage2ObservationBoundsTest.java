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

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2ObservationBoundsTest {
    @TempDir Path directory;

    @Test
    void ordinaryObservationRetainsItsFiveMinuteBound() {
        Path work = directory.resolve("ordinary");
        var limits = Stage2RunLimits.historical(1000);
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.timedRun(
                                        null,
                                        "ordinary",
                                        work,
                                        true,
                                        false,
                                        "",
                                        1024,
                                        1,
                                        1,
                                        1000,
                                        10000,
                                        300001,
                                        1000,
                                        0,
                                        true,
                                        limits))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(work).doesNotExist();
    }

    @Test
    void inFlightAboveTheBulkDefaultIsRejectedBeforeCreatingWork() {
        Path work = directory.resolve("in-flight");
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.timedRun(
                                        null,
                                        "in-flight",
                                        work,
                                        false,
                                        false,
                                        "",
                                        1024,
                                        1,
                                        BigtableStage2Probe.MAX_IN_FLIGHT + 1,
                                        1000,
                                        10000,
                                        30000,
                                        1000,
                                        0,
                                        false,
                                        Stage2RunLimits.historical(1000)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(work).doesNotExist();
    }

    @Test
    void sustainedObservationIsBoundedBeforeCreatingWork() {
        Path work = directory.resolve("sustained");
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.sustainedHotRun(
                                        null,
                                        "sustained",
                                        work,
                                        1024,
                                        1,
                                        4,
                                        1000,
                                        10000,
                                        3600001,
                                        Stage2RunLimits.historical(1000)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(work).doesNotExist();
    }

    @Test
    void sustainedDrainCannotOverflowItsNanosecondWindow() {
        Path work = directory.resolve("overflow");
        var limits =
                new Stage2RunLimits(
                        1000,
                        64000,
                        1000,
                        1048576,
                        1048576,
                        60000,
                        Long.MAX_VALUE / 1_000_000L - 360000);
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.sustainedHotRun(
                                        null,
                                        "overflow",
                                        work,
                                        1024,
                                        1,
                                        4,
                                        1000,
                                        10000,
                                        3600000,
                                        limits))
                .isInstanceOf(ArithmeticException.class);
        assertThat(work).doesNotExist();
    }

    @Test
    void largerSampleReservationIsExplicitAndCannotHideExhaustion() throws Exception {
        Path work = Files.createDirectory(directory.resolve("sustained"));
        Path samples = work.resolve("samples.jsonl");
        try (var file = new java.io.RandomAccessFile(samples.toFile(), "rw")) {
            file.setLength(8L * 1024 * 1024 + 1);
        }
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.preserveSamples(
                                        null, work, false, 8L * 1024 * 1024))
                .hasMessageContaining("storage cap");
        assertThat(directory.resolve("sustained-samples.jsonl")).doesNotExist();
        BigtableStage2Probe.preserveSamples(null, work, false, 64L * 1024 * 1024);
        assertThat(Files.size(directory.resolve("sustained-samples.jsonl")))
                .isEqualTo(Files.size(samples));
    }
}
