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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2CampaignSupervisorTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void missingWorkerStopsAdmissionBeforeDeletionAndRecordsVerifiedAbsence(
            boolean interruptedPublication) throws Exception {
        Stage2CampaignJournal journal = active();
        journal.prepareCell(0);
        journal.tablesReady();
        String table = Stage2AssessmentPlan.runs().get(0).table();
        var worker = journal.claim(table, 999_999_999L, "gone-worker");
        if (interruptedPublication) {
            Files.writeString(journal.directory.resolve("state.next"), "interrupted publication");
        }
        List<String> events = new ArrayList<>();
        var resources =
                new Stage2CampaignSupervisor.Resources() {
                    @Override
                    public void verifyOwned() {
                        events.add("verify");
                    }

                    @Override
                    public void deleteOwnedAndVerifyAbsent() throws Exception {
                        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
                        assertThat(Files.exists(journal.directory.resolve("stop"))).isTrue();
                        events.add("delete-and-verify");
                    }
                };
        Stage2CampaignSupervisor supervisor =
                new Stage2CampaignSupervisor(
                        journal,
                        resources,
                        (pid, start) -> {
                            assertThat(pid).isEqualTo(999_999_999L);
                            events.add("stop-worker");
                        });
        assertThatThrownBy(supervisor::tick).hasMessageContaining("disappeared");
        assertThat(events).containsExactly("verify", "stop-worker", "delete-and-verify");
        assertThat(journal.read().getProperty("phase")).isEqualTo("ABSENT");
        assertThatThrownBy(worker::requireLive).hasMessageContaining("stopped");
        assertThat(supervisor.tick()).isFalse();
        assertThat(events).hasSize(3);
        try (var files = Files.list(journal.directory)) {
            var interrupted =
                    files.filter(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .startsWith("state.interrupted-"))
                            .collect(java.util.stream.Collectors.toList());
            assertThat(interrupted).hasSize(interruptedPublication ? 1 : 0);
            if (interruptedPublication) {
                assertThat(Files.readString(interrupted.get(0)))
                        .isEqualTo("interrupted publication");
            }
        }
    }

    @Test
    void foreignResourceIsPreservedAndFailedCleanupCannotRecordAbsence() throws Exception {
        Stage2CampaignJournal journal = active();
        Path retained = Files.createDirectories(journal.directory.resolve("work"));
        var resources =
                new Stage2CampaignSupervisor.Resources() {
                    @Override
                    public void verifyOwned() throws IOException {
                        throw new IOException("Foreign instance owner");
                    }

                    @Override
                    public void deleteOwnedAndVerifyAbsent() throws IOException {
                        throw new IOException("Refusing deletion of foreign instance");
                    }
                };
        Stage2CampaignSupervisor supervisor =
                new Stage2CampaignSupervisor(
                        journal,
                        resources,
                        (pid, start) -> {
                            throw new AssertionError("No worker has been claimed");
                        });
        assertThatThrownBy(supervisor::tick)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Foreign instance")
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
        assertThat(retained).exists();
    }

    @Test
    void workerTerminationFailurePreventsCloudDeletion() throws Exception {
        Stage2CampaignJournal journal = active();
        journal.prepareCell(0);
        journal.tablesReady();
        journal.claim(Stage2AssessmentPlan.runs().get(0).table(), 999_999_999L, "worker");
        var resources =
                new Stage2CampaignSupervisor.Resources() {
                    @Override
                    public void verifyOwned() {}

                    @Override
                    public void deleteOwnedAndVerifyAbsent() {
                        throw new AssertionError("Worker termination has not been verified");
                    }
                };
        Stage2CampaignSupervisor supervisor =
                new Stage2CampaignSupervisor(
                        journal,
                        resources,
                        (pid, start) -> {
                            throw new IOException("Worker did not exit");
                        });
        assertThatThrownBy(supervisor::cleanup).hasMessageContaining("did not exit");
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
    }

    @Test
    void processCleanupPreservesReusedPidAndRefusesTheSupervisorItself() throws Exception {
        long pid = ProcessHandle.current().pid();
        Stage2CampaignSupervisor.stopProcess(pid, "other-start-instant");
        assertThatThrownBy(
                        () ->
                                Stage2CampaignSupervisor.stopProcess(
                                        pid,
                                        ProcessHandle.current()
                                                .info()
                                                .startInstant()
                                                .orElseThrow()
                                                .toString()))
                .hasMessageContaining("refuses to terminate itself");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptedInitialPublicationCanOnlyCleanUp(boolean partialSnapshot) throws Exception {
        Stage2CampaignJournal journal =
                new Stage2CampaignJournal(Stage2CampaignTestPlan.write(directory));
        Files.writeString(
                journal.directory.resolve("activation.started"), "interrupted activation");
        if (partialSnapshot) {
            Files.writeString(journal.directory.resolve("state.initial"), "phase=REA");
        }
        assertThatThrownBy(
                        () ->
                                journal.start(
                                        "a".repeat(32),
                                        ProcessHandle.current().pid(),
                                        ProcessHandle.current()
                                                .info()
                                                .startInstant()
                                                .orElseThrow()
                                                .toString(),
                                        System.currentTimeMillis()))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        List<String> events = new ArrayList<>();
        Stage2CampaignSupervisor supervisor =
                new Stage2CampaignSupervisor(
                        journal,
                        new Stage2CampaignSupervisor.Resources() {
                            @Override
                            public void verifyOwned() {
                                throw new AssertionError("An incomplete activation cannot resume");
                            }

                            @Override
                            public void deleteOwnedAndVerifyAbsent() throws Exception {
                                assertThat(journal.read().getProperty("phase"))
                                        .isEqualTo("STOPPED");
                                events.add("verified deletion");
                            }
                        },
                        (pid, started) -> {
                            throw new AssertionError("No worker could be admitted");
                        });
        assertThatThrownBy(supervisor::tick).isInstanceOf(java.nio.file.NoSuchFileException.class);
        assertThat(events).containsExactly("verified deletion");
        assertThat(journal.read().getProperty("phase")).isEqualTo("ABSENT");
        assertThat(journal.directory.resolve("state.initial")).doesNotExist();
        if (partialSnapshot) {
            try (var files = Files.list(journal.directory)) {
                Path archived =
                        files.filter(
                                        path ->
                                                path.getFileName()
                                                        .toString()
                                                        .startsWith("state.interrupted-"))
                                .findFirst()
                                .orElseThrow();
                assertThat(Files.readString(archived)).isEqualTo("phase=REA");
            }
        }
    }

    private Stage2CampaignJournal active() throws Exception {
        Stage2CampaignJournal journal =
                new Stage2CampaignJournal(Stage2CampaignTestPlan.write(directory));
        journal.start(
                "a".repeat(32),
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        return journal;
    }
}
