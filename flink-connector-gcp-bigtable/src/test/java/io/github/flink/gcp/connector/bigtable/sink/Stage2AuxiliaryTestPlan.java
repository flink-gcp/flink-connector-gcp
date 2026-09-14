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

import java.nio.file.Files;
import java.nio.file.Path;

/** Synthetic reservations for auxiliary state-machine tests; not calibration inputs. */
final class Stage2AuxiliaryTestPlan {
    private Stage2AuxiliaryTestPlan() {}

    static Path write(Path directory) throws Exception {
        return write(directory, "-Xmx2g");
    }

    static Path write(Path directory, String flags) throws Exception {
        Path campaign = Stage2CampaignTestPlan.write(directory, flags, 10000);
        StringBuilder values =
                new StringBuilder(
                        "runOverheadSeconds=300\nstorageAndCleanupSeconds=900\n"
                                + "otherPreparationSeconds=600\nserializedSeconds=60\nsustainedSeconds=1800\n");
        for (String name : Stage2AuxiliaryPlan.NAMES) {
            for (String field :
                    new String[] {
                        "inventoryEntries=1000",
                        "inventoryBytes=64000",
                        "stagedEntries=1000",
                        "stagedBytes=1048576",
                        "workBytes=134217728",
                        "checkpointTimeoutMillis=60000",
                        "drainMillis=120000",
                        "writeAttempts=4000",
                        "writeBytes=8192000",
                        "readBytes=2048000"
                    }) {
                values.append(name).append('.').append(field).append('\n');
            }
        }
        Files.writeString(campaign.resolve("auxiliary.properties"), values);
        return campaign;
    }

    static Stage2CampaignJournal active(Path directory) throws Exception {
        Stage2CampaignJournal journal = new Stage2CampaignJournal(write(directory));
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal,
                "a".repeat(32),
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        // Auxiliary unit tests start from a synthetic completed-matrix state.
        var state = journal.read();
        state.setProperty("nextRun", "648");
        state.setProperty("cell", "107");
        state.setProperty("phase", "AUXILIARY_READY");
        try (var output = Files.newOutputStream(journal.directory.resolve("state.properties"))) {
            state.store(output, "Synthetic matrix completion; not an observation record");
        }
        return journal;
    }
}
