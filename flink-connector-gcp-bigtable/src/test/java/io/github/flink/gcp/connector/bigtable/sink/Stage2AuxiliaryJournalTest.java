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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2AuxiliaryJournalTest {
    @TempDir Path directory;

    @Test
    void lastMatrixCellRetainsEvidenceBeforeAuxiliaryAdmission() throws Exception {
        Path campaign = Stage2AuxiliaryTestPlan.write(directory);
        var journal = new Stage2CampaignJournal(campaign);
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal,
                "a".repeat(32),
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        assertThatThrownBy(() -> journal.prepareAuxiliary("serialized"))
                .hasMessageContaining("out of order");
        // Simulate the preceding 107 cells; exercise the final six unchanged matrix claims.
        var state = journal.read();
        state.setProperty("nextRun", "642");
        state.setProperty("cell", "106");
        try (var output = Files.newOutputStream(campaign.resolve("state.properties"))) {
            state.store(output, "Synthetic preceding matrix cells");
        }
        journal.prepareCell(107);
        journal.tablesReady();
        for (int order = 642; order < 648; order++) {
            String table = Stage2AssessmentPlan.runs().get(order).table();
            var lease = journal.claim(table, 999999000L + order, "worker");
            Files.createDirectories(lease.workDirectory());
            journal.finish(table, 999999000L + order, "worker", true);
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("RETAINING");
        assertThatThrownBy(() -> journal.prepareAuxiliary("serialized"))
                .hasMessageContaining("out of order");
        journal.cellCleaned("f".repeat(64));
        assertThat(campaign.resolve("cell-107.properties")).exists();
        assertThat(journal.read().getProperty("phase")).isEqualTo("AUXILIARY_READY");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("648");
        journal.prepareAuxiliary("serialized");
    }

    @Test
    void separateOrderedEvidenceFollowsTheUnchangedMatrix() throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        assertThatThrownBy(() -> journal.prepareCell(0))
                .hasMessageContaining("next unclaimed cell");
        assertThatThrownBy(() -> journal.prepareAuxiliary("sustained"))
                .hasMessageContaining("out of order");
        int count = 0;
        for (String name : Stage2AuxiliaryPlan.NAMES) {
            journal.prepareAuxiliary(name);
            journal.tablesReady();
            long pid = 999999990L + count++;
            var lease = journal.claimAuxiliary(name, pid, "worker");
            Files.createDirectories(lease.workDirectory());
            Files.writeString(lease.workDirectory().resolve("checkpoint"), "retained state");
            lease.reserve(1, 10);
            lease.reserveRead(10);
            assertThatThrownBy(
                            () ->
                                    journal.finish(
                                            lease.workDirectory().getFileName().toString(),
                                            pid,
                                            "worker",
                                            true))
                    .hasMessageContaining("separate outcome");
            journal.finishAuxiliary(name, pid, "worker", true);
            assertThat(journal.read().getProperty("nextRun")).isEqualTo("648");
            assertThatThrownBy(() -> journal.cellCleaned("f".repeat(64)))
                    .hasMessageContaining("separate completion");
            assertThat(journal.directory.resolve("auxiliary-" + name + "-evidence.properties"))
                    .doesNotExist();
            journal.auxiliaryCleaned(name, "f".repeat(64));
            assertThat(lease.workDirectory()).doesNotExist();
            assertThat(journal.directory.resolve("auxiliary-" + name + "-run.properties")).exists();
            assertThat(journal.directory.resolve("auxiliary-" + name + "-evidence.properties"))
                    .exists();
            assertThatThrownBy(() -> journal.prepareAuxiliary(name))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("CAMPAIGN_COMPLETE");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("648");
        assertThatThrownBy(() -> journal.prepareCell(0)).hasMessageContaining("stopped");
    }

    @Test
    void auxiliaryBudgetsAreAtomicAndRemainSpentAfterReopening() throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        journal.prepareAuxiliary("serialized");
        journal.tablesReady();
        var lease = journal.claimAuxiliary("serialized", 999999990L, "worker");
        lease.reserve(10, 100);
        assertThatThrownBy(() -> lease.reserve(1, 8192000))
                .hasMessageContaining("budget exhausted");
        assertThat(
                        new Stage2CampaignJournal(journal.directory)
                                .read()
                                .getProperty("remainingAttempts"))
                .isEqualTo("3990");
        assertThatThrownBy(
                        () ->
                                new Stage2CampaignJournal(journal.directory)
                                        .claimAuxiliary("serialized", 999999991L, "retry"))
                .hasMessageContaining("repeated");
        assertThatThrownBy(() -> lease.requireWindow(1800000))
                .hasMessageContaining("no longer fit");
        journal.stop();
        assertThatThrownBy(() -> lease.reserve(1, 1)).hasMessageContaining("stopped");
    }

    @Test
    void controllerCannotClaimItsOwnAuxiliaryWorker() throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        // Use the live test launcher as a distinct supervisor identity. No signal is sent.
        ProcessHandle supervisor = ProcessHandle.current().parent().orElseThrow();
        assertThat(supervisor.pid()).isNotEqualTo(ProcessHandle.current().pid());
        var state = journal.read();
        state.setProperty("supervisorPid", Long.toString(supervisor.pid()));
        state.setProperty(
                "supervisorStart", supervisor.info().startInstant().orElseThrow().toString());
        try (var output = Files.newOutputStream(journal.directory.resolve("state.properties"))) {
            state.store(output, "Distinct live supervisor for the controller-identity oracle");
        }
        journal.prepareAuxiliary("serialized");
        journal.tablesReady();
        assertThatThrownBy(
                        () ->
                                journal.claimAuxiliary(
                                        "serialized", ProcessHandle.current().pid(), "worker"))
                .hasMessageContaining("process identity");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedOrCensoredAuxiliaryWorkerIsRecordedAndTheNextPhaseRemainsAdmissible(
            boolean throwsFailure) throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        journal.prepareAuxiliary("serialized");
        journal.tablesReady();
        assertThatThrownBy(
                        () ->
                                Stage2CampaignWorker.executeAuxiliary(
                                        journal,
                                        "serialized",
                                        999999990L,
                                        "worker",
                                        "-Xmx2g",
                                        (lease, phase) -> {
                                            lease.reserve(1, 1);
                                            if (throwsFailure) {
                                                throw new java.io.IOException("observation failed");
                                            }
                                            return false;
                                        }))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining(throwsFailure ? "observation failed" : "empty or censored");
        assertThat(journal.read().getProperty("phase")).isEqualTo("RETAINING");
        assertThat(journal.read().getProperty("runStatus")).isEqualTo("FAILED");
        assertThat(journal.read().getProperty("failedRuns")).isEqualTo("1");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("648");
        assertThat(
                        java.nio.file.Files.readString(
                                journal.directory.resolve("auxiliary-serialized-run.properties")))
                .contains("runStatus=FAILED")
                .contains("workerTable=stage2-serialized");
        assertThat(journal.directory.resolve("auxiliary-serialized-evidence.properties"))
                .doesNotExist();
        // The failed phase is retained like a successful one; the next phase stays admissible
        // and the failed one is never repeated.
        journal.auxiliaryCleaned("serialized", "a".repeat(64));
        assertThat(journal.read().getProperty("phase")).isEqualTo("AUXILIARY_READY");
        assertThatThrownBy(() -> journal.prepareAuxiliary("serialized"))
                .hasMessageContaining("repeated or out of order");
        journal.prepareAuxiliary("sustained");
        assertThat(journal.read().getProperty("phase")).isEqualTo("PREPARING");
    }

    @Test
    void frozenAuxiliaryPlanCannotChangeBetweenProcesses() throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        Path plan = journal.directory.resolve("auxiliary.properties");
        Files.writeString(
                plan,
                Files.readString(plan).replace("serializedSeconds=60", "serializedSeconds=90"));
        assertThatThrownBy(() -> new Stage2CampaignJournal(journal.directory).read())
                .hasMessageContaining("Auxiliary plan changed");
        assertThatThrownBy(
                        () ->
                                journal.heartbeat(
                                        ProcessHandle.current().pid(),
                                        ProcessHandle.current()
                                                .info()
                                                .startInstant()
                                                .orElseThrow()
                                                .toString()))
                .hasMessageContaining("Auxiliary plan changed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void auxiliaryCleanupCannotPublishEvidenceAfterStopOrDeletionFailure(boolean stopDuringDeletion)
            throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        journal.prepareAuxiliary("serialized");
        journal.tablesReady();
        var lease = journal.claimAuxiliary("serialized", 999999990L, "worker");
        Files.createDirectories(lease.workDirectory());
        journal.finishAuxiliary("serialized", 999999990L, "worker", true);
        assertThatThrownBy(
                        () ->
                                journal.auxiliaryCleaned(
                                        "serialized",
                                        "f".repeat(64),
                                        work -> {
                                            // Heartbeat acquisition here proves deletion does not
                                            // retain the publication lock.
                                            journal.heartbeat(
                                                    ProcessHandle.current().pid(),
                                                    ProcessHandle.current()
                                                            .info()
                                                            .startInstant()
                                                            .orElseThrow()
                                                            .toString());
                                            if (stopDuringDeletion) {
                                                Stage2Lease.removeWork(work);
                                                journal.stop();
                                            } else {
                                                throw new java.io.IOException("deletion failed");
                                            }
                                        }))
                .isInstanceOfAny(java.io.IOException.class, IllegalStateException.class);
        assertThat(journal.directory.resolve("auxiliary-serialized-evidence.properties"))
                .doesNotExist();
        assertThat(journal.read().getProperty("nextAuxiliary")).isEqualTo("0");
        assertThat(journal.read().getProperty("phase"))
                .isEqualTo(stopDuringDeletion ? "STOPPED" : "RETAINING");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void heartbeatRejectsOversizedOrNonregularAuxiliaryReplacement(boolean nonregular)
            throws Exception {
        var journal = Stage2AuxiliaryTestPlan.active(directory);
        Path plan = journal.directory.resolve("auxiliary.properties");
        Files.delete(plan);
        if (nonregular) {
            Files.createDirectory(plan);
        } else {
            Files.writeString(plan, "x".repeat(16385));
        }
        assertThatThrownBy(
                        () ->
                                journal.heartbeat(
                                        ProcessHandle.current().pid(),
                                        ProcessHandle.current()
                                                .info()
                                                .startInstant()
                                                .orElseThrow()
                                                .toString()))
                .hasMessageContaining("bounded regular file");
        assertThatThrownBy(() -> new Stage2CampaignJournal(journal.directory))
                .hasMessageContaining("bounded regular file");
    }

    @ParameterizedTest
    @ValueSource(strings = {"serialized", "sustained"})
    void offlinePlanCannotInvokeAnAuxiliaryServiceWorker(String name) throws Exception {
        Path campaign =
                Stage2AuxiliaryTestPlan.write(
                        directory,
                        String.join(
                                " ",
                                java.lang.management.ManagementFactory.getRuntimeMXBean()
                                        .getInputArguments()));
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.main(
                                        new String[] {
                                            "service-auxiliary", campaign.toString(), name
                                        }))
                .isInstanceOf(java.nio.file.NoSuchFileException.class)
                .hasMessageContaining("state.properties");
        assertThat(campaign.resolve("work")).doesNotExist();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"serialized.inventoryEntries=3000000000", "serialized.inventoryBytes=1"})
    void invalidAuxiliaryCapacityNamesThePlan(String replacement) throws Exception {
        Path campaign = Stage2AuxiliaryTestPlan.write(directory);
        Path plan = campaign.resolve("auxiliary.properties");
        String key = replacement.substring(0, replacement.indexOf('='));
        Files.writeString(
                plan,
                Files.readString(plan)
                        .replaceAll(
                                "(?m)^" + java.util.regex.Pattern.quote(key) + "=.*$",
                                replacement));
        assertThatThrownBy(() -> new Stage2CampaignJournal(campaign))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Invalid auxiliary plan");
        assertThat(campaign.resolve("activation.started")).doesNotExist();
    }

    @Test
    void auxiliaryReservationCannotConsumeUnbudgetedHostTime() throws Exception {
        Path campaign = Stage2AuxiliaryTestPlan.write(directory);
        Path plan = campaign.resolve("auxiliary.properties");
        Files.writeString(
                plan,
                Files.readString(plan)
                        .replace("otherPreparationSeconds=600", "otherPreparationSeconds=10000"));
        assertThatThrownBy(() -> new Stage2CampaignJournal(campaign))
                .hasMessageContaining("overhead reservation");
        assertThat(campaign.resolve("activation.started")).doesNotExist();
    }
}
