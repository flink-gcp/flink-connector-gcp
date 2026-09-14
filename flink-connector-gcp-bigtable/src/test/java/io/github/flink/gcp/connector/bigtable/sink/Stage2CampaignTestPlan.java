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

/** Synthetic prices and capacity inputs shared by campaign lifecycle tests. */
final class Stage2CampaignTestPlan {
    private Stage2CampaignTestPlan() {}

    static Path write(Path directory) throws Exception {
        return write(directory, "-Xmx2g");
    }

    static Path write(Path directory, String jvmFlags) throws Exception {
        return write(directory, jvmFlags, 600);
    }

    static Path write(Path directory, String jvmFlags, long campaignOverheadSeconds)
            throws Exception {
        Path input =
                Files.writeString(
                        directory.resolve("inputs.properties"),
                        "project=example-project\ninstance=example-trial\ngceZone=us-central1-b\n"
                                + "gceMachineType=example-machine\njvmFlags="
                                + escapeValue(jvmFlags)
                                + "\nsourceSha="
                                + "a".repeat(40)
                                + "\nruntimeSha256="
                                + "b".repeat(64)
                                + "\nleaseLimitSeconds=3600\nrunOverheadSeconds=300\nleaseOverheadSeconds=60\n"
                                + "campaignOverheadSeconds="
                                + campaignOverheadSeconds
                                + "\nhostMicrousdPerHour=3600\notherCostMicrousd=1000000\n"
                                + "costCeilingMicrousd=20000000\nmaxWriteAttemptsPerRun=1000000\n"
                                + "maxWriteBytesPerRun=1073741824\nmaxReadBytesPerRun=1073741824\n"
                                + "maxPhysicalStorageBytes=1073741824\n");
        Path limits =
                Files.writeString(
                        directory.resolve("limits.properties"),
                        "inventoryEntries=1000\ninventoryBytes=67108864\nstagedEntries=100000\n"
                                + "stagedBytes=67108864\nworkBytes=2147483648\n"
                                + "checkpointTimeoutMillis=60000\ndrainMillis=120000\n");
        Path campaign = directory.resolve("campaign");
        Stage2CampaignPlan.write(input, limits, campaign);
        return campaign;
    }

    private static String escapeValue(String value) {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

    /**
     * Builds an in-JVM state fixture for fake supervisor callbacks. Real process activation and
     * termination are covered separately by the child-JVM tests.
     */
    static void startWithSimulatedSupervisor(
            Stage2CampaignJournal journal,
            String owner,
            long supervisorPid,
            String supervisorStart,
            long hostStartedAt)
            throws java.io.IOException {
        journal.start(owner, Long.MAX_VALUE, "synthetic-supervisor", hostStartedAt);
        var state = journal.read();
        state.setProperty("supervisorPid", Long.toString(supervisorPid));
        state.setProperty("supervisorStart", supervisorStart);
        try (var output = Files.newOutputStream(journal.directory.resolve("state.properties"))) {
            state.store(output, "In-JVM supervisor fixture; not an execution identity");
        }
    }
}
