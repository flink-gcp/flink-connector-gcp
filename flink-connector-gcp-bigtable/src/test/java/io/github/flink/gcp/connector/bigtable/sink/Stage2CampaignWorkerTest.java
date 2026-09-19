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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2CampaignWorkerTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void admittedWorkerConstructsServiceHarnessAndPublishesOnlyObservedRuns(boolean observed)
            throws Exception {
        Stage2CampaignJournal journal = active();
        String table = Stage2AssessmentPlan.runs().get(0).table();
        Stage2CampaignWorker.Observation observation =
                (lease, run, limits) -> {
                    Files.createDirectories(lease.workDirectory());
                    // Construction must work with the service endpoint selection without opening
                    // ADC.
                    try (Stage2Harness harness =
                            new Stage2Harness(
                                    lease.table(run.table()),
                                    "",
                                    lease.workDirectory().resolve("inventory.bin"),
                                    limits.inventoryEntries,
                                    run.cell.payloadBytes,
                                    run.cell.hot,
                                    false,
                                    run.cell.inFlight,
                                    true,
                                    lease,
                                    limits)) {
                        assertThat(harness.lease).isSameAs(lease);
                        harness.beforeProductionSend(1024);
                    }
                    return observed;
                };
        if (observed) {
            Stage2CampaignWorker.execute(
                    journal, table, 999_999_999L, "worker", "-Xmx2g", observation);
            assertThat(journal.read().getProperty("nextRun")).isEqualTo("1");
            assertThat(journal.read().getProperty("runStatus")).isEqualTo("OBSERVED");
        } else {
            assertThatThrownBy(
                            () ->
                                    Stage2CampaignWorker.execute(
                                            journal,
                                            table,
                                            999_999_999L,
                                            "worker",
                                            "-Xmx2g",
                                            observation))
                    .hasMessageContaining("empty or censored");
            assertThat(journal.read().getProperty("nextRun")).isEqualTo("1");
            assertThat(journal.read().getProperty("runStatus")).isEqualTo("FAILED");
            assertThat(journal.read().getProperty("phase")).isEqualTo("RUNNING");
            assertThat(journal.directory.resolve("run-0.properties")).exists();
        }
    }

    @Test
    void flagsMismatchCannotClaimAndObservationFailureIsRecordedAndAdvances() throws Exception {
        Stage2CampaignJournal journal = active();
        String table = Stage2AssessmentPlan.runs().get(0).table();
        Stage2CampaignWorker.Observation failure =
                (lease, run, limits) -> {
                    throw new IOException("Measurement failed");
                };
        assertThatThrownBy(
                        () ->
                                Stage2CampaignWorker.execute(
                                        journal, table, 999_999_999L, "worker", "-Xmx1g", failure))
                .hasMessageContaining("JVM flags differ");
        assertThat(journal.read()).doesNotContainKey("workerPid");
        assertThatThrownBy(
                        () ->
                                Stage2CampaignWorker.execute(
                                        journal, table, 999_999_999L, "worker", "-Xmx2g", failure))
                .hasMessageContaining("Measurement failed");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("1");
        assertThat(journal.read().getProperty("runStatus")).isEqualTo("FAILED");
        assertThat(journal.read().getProperty("phase")).isEqualTo("RUNNING");
        // The failed run is never repeated; the next preregistered run is the only admission.
        assertThatThrownBy(() -> journal.claim(table, 999_999_998L, "worker-2"))
                .hasMessageContaining("out of order");
        journal.claim(Stage2AssessmentPlan.runs().get(1).table(), 999_999_998L, "worker-2");
    }

    @Test
    void interruptedObservationStopsTheCampaignWithoutRecordingARun() throws Exception {
        Stage2CampaignJournal journal = active();
        String table = Stage2AssessmentPlan.runs().get(0).table();
        Stage2CampaignWorker.Observation interrupted =
                (lease, run, limits) -> {
                    throw new InterruptedException("worker interrupted");
                };
        try {
            assertThatThrownBy(
                            () ->
                                    Stage2CampaignWorker.execute(
                                            journal,
                                            table,
                                            999_999_999L,
                                            "worker",
                                            "-Xmx2g",
                                            interrupted))
                    .isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            assertThat(Thread.interrupted()).isTrue();
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("0");
        assertThat(journal.directory.resolve("run-0.properties")).doesNotExist();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void unrecordableOutcomeStopsTheCampaign(boolean throwsFailure) throws Exception {
        Stage2CampaignJournal journal = active();
        String table = Stage2AssessmentPlan.runs().get(0).table();
        // An existing outcome file makes the journal's CREATE_NEW record fail.
        Files.writeString(journal.directory.resolve("run-0.properties"), "stale");
        Stage2CampaignWorker.Observation observation =
                (lease, run, limits) -> {
                    if (throwsFailure) {
                        throw new IOException("Measurement failed");
                    }
                    return false;
                };
        var thrown =
                assertThatThrownBy(
                        () ->
                                Stage2CampaignWorker.execute(
                                        journal,
                                        table,
                                        999_999_999L,
                                        "worker",
                                        "-Xmx2g",
                                        observation));
        if (throwsFailure) {
            thrown.hasMessageContaining("Measurement failed")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .anySatisfy(
                                                    suppressed ->
                                                            assertThat(suppressed)
                                                                    .isInstanceOf(
                                                                            java.nio.file
                                                                                    .FileAlreadyExistsException
                                                                                    .class)));
        } else {
            thrown.isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("0");
        assertThat(journal.read().getProperty("runStatus")).isEqualTo("STARTED");
    }

    private Stage2CampaignJournal active() throws Exception {
        Stage2CampaignJournal journal =
                new Stage2CampaignJournal(Stage2CampaignTestPlan.write(directory));
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal,
                "a".repeat(32),
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        journal.prepareCell(0);
        journal.tablesReady();
        return journal;
    }
}
