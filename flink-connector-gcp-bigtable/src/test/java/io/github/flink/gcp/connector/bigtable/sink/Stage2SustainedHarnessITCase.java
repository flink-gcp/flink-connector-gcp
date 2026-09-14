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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(180)
class Stage2SustainedHarnessITCase {
    @TempDir Path directory;

    @Test
    void sustainedEntryPointRetainsProductionSamplesAndRemovesLocalWork() throws Exception {
        Path work = directory.resolve("hot-row");
        boolean observed =
                BigtableStage2Probe.sustainedHotRun(
                        null,
                        "hot-row",
                        work,
                        1024,
                        1,
                        1,
                        100,
                        0,
                        500,
                        Stage2RunLimits.historical(100000));
        assertThat(observed).isTrue();
        assertThat(work).doesNotExist();
        String samples = Files.readString(directory.resolve("hot-row-samples.jsonl"));
        assertThat(samples).contains("notificationProgress", "checkpoint", "stagedEntries");
    }
}
