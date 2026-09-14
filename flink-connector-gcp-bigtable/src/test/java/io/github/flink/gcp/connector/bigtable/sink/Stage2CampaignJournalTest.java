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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2CampaignJournalTest {
    @TempDir Path directory;
    private final MutableClock clock = new MutableClock();
    private final long supervisor = ProcessHandle.current().pid();
    private final String supervisorStart =
            ProcessHandle.current().info().startInstant().orElseThrow().toString();

    @Test
    void activationRejectsItsOwnSupervisorBeforePublishingAnything() throws Exception {
        Stage2CampaignJournal journal = prepare();
        assertThatThrownBy(
                        () ->
                                journal.start(
                                        "a".repeat(32),
                                        supervisor,
                                        supervisorStart,
                                        clock.millis()))
                .hasMessageContaining("supervisor must be a different process");
        assertThat(journal.directory.resolve("activation.started")).doesNotExist();
        assertThat(journal.directory.resolve("state.properties")).doesNotExist();
    }

    @Test
    void modifiedSnapshotAndModifiedCostSummaryCannotStart() throws Exception {
        Stage2CampaignJournal journal = prepare();
        Path input = journal.directory.resolve("inputs.properties");
        String original = Files.readString(input);
        Files.writeString(input, original.replace("example-trial", "other-trial"));
        assertThatThrownBy(() -> new Stage2CampaignJournal(journal.directory, clock))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("digest mismatch");
        Files.writeString(input, original);
        Path summary = journal.directory.resolve("campaign.properties");
        Files.writeString(
                summary,
                Files.readString(summary)
                        .replace("hostBoundSeconds=270600", "hostBoundSeconds=270601"));
        Stage2CampaignJournal changed = new Stage2CampaignJournal(journal.directory, clock);
        assertThatThrownBy(() -> start(changed))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("complete validated plan");
        assertThat(journal.directory.resolve("state.properties")).doesNotExist();
    }

    @Test
    void reservationsAreAtomicAndRemainSpentAfterReopening() throws Exception {
        Stage2CampaignJournal journal = active();
        Stage2RunLease worker = journal.claim(table(0), 999_999_991L, "worker-0");
        worker.reserve(400, 1024);
        var reopened = new Stage2CampaignJournal(journal.directory, clock);
        assertThat(reopened.read().getProperty("remainingAttempts")).isEqualTo("999600");
        assertThat(reopened.read().getProperty("remainingWriteBytes")).isEqualTo("1073740800");
        assertThatThrownBy(() -> worker.reserve(10, 1073741824))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("budget exhausted");
        assertThat(reopened.read().getProperty("remainingAttempts")).isEqualTo("999600");
        worker.reserveRead(1073741824);
        assertThatThrownBy(() -> worker.reserveRead(1)).hasMessageContaining("budget exhausted");
        assertThatThrownBy(() -> reopened.claim(table(0), 999_999_992L, "replacement"))
                .hasMessageContaining("repeated");
    }

    @Test
    void expiredHeartbeatCannotBeRevivedAndStopSurvivesReopening() throws Exception {
        Stage2CampaignJournal journal = active();
        Stage2RunLease worker = journal.claim(table(0), 999_999_991L, "worker-0");
        clock.advance(19_999);
        worker.requireLive();
        clock.advance(1);
        assertThatThrownBy(worker::requireLive)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("lost supervision");
        assertThatThrownBy(() -> journal.heartbeat(supervisor, supervisorStart))
                .hasMessageContaining("lost supervision");
        journal.stop();
        assertThat(new Stage2CampaignJournal(journal.directory, clock).read().getProperty("phase"))
                .isEqualTo("STOPPED");
        assertThatThrownBy(() -> journal.claim(table(1), 999_999_992L, "worker-1"))
                .hasMessageContaining("stopped");
    }

    @Test
    void exactTargetAndCompleteWindowAreCheckedBeforeMetadataOrAdmission() throws Exception {
        Stage2CampaignJournal journal = active();
        Stage2RunLease worker = journal.claim(table(0), 999_999_991L, "worker-0");
        worker.requireTarget(TableDestination.of("example-project", "example-trial", table(0)));
        assertThatThrownBy(() -> worker.table(table(1))).hasMessageContaining("not owned");
        assertThatThrownBy(
                        () ->
                                worker.requireTarget(
                                        TableDestination.of("other", "example-trial", table(0))))
                .hasMessageContaining("not owned");
        worker.requireWindow(40_000);
        assertThatThrownBy(() -> worker.requireWindow(221_000))
                .hasMessageContaining("observation and drain");
        assertThat(worker.workDirectory())
                .isEqualTo(journal.directory.resolve("work").resolve(table(0)));
    }

    @Test
    void onlyTheActiveWorkerCanFinishOnceAndFailureNeverAdvances() throws Exception {
        Stage2CampaignJournal journal = active();
        Stage2RunLease worker = journal.claim(table(0), 999_999_991L, "worker-0");
        assertThatThrownBy(() -> journal.finish(table(1), 999_999_991L, "worker-0", true))
                .hasMessageContaining("not active");
        journal.finish(table(0), 999_999_991L, "worker-0", true);
        assertThatThrownBy(worker::requireLive).hasMessageContaining("not active");
        assertThatThrownBy(() -> journal.finish(table(0), 999_999_991L, "worker-0", true))
                .hasMessageContaining("not active");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("1");
        journal.claim(table(1), 999_999_992L, "worker-1");
        journal.finish(table(1), 999_999_992L, "worker-1", false);
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
        assertThat(journal.read().getProperty("nextRun")).isEqualTo("1");
        assertThat(journal.read().getProperty("runStatus")).isEqualTo("FAILED");
    }

    @Test
    void allSixOrderedRunsMustDrainAndRetainEvidenceBeforeTheNextCell() throws Exception {
        Stage2CampaignJournal journal = active();
        assertThatThrownBy(() -> journal.claim(table(1), 999_999_992L, "worker-1"))
                .hasMessageContaining("out of order");
        for (int index = 0; index < 6; index++) {
            var worker = journal.claim(table(index), 999_999_990L + index, "worker-" + index);
            Files.createDirectories(worker.workDirectory());
            Files.writeString(
                    worker.workDirectory().resolve("checkpoint"), "retained until evidence");
            journal.finish(table(index), 999_999_990L + index, "worker-" + index, true);
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("RETAINING");
        assertThatThrownBy(() -> journal.prepareCell(1)).hasMessageContaining("next unclaimed");
        assertThatThrownBy(() -> journal.claim(table(6), 999_999_999L, "worker-6"))
                .hasMessageContaining("outside the cell");
        assertThatThrownBy(() -> journal.cellCleaned("missing")).hasMessageContaining("incomplete");
        journal.cellCleaned("f".repeat(64));
        for (int index = 0; index < 6; index++) {
            assertThat(journal.directory.resolve("work").resolve(table(index))).doesNotExist();
            assertThat(journal.directory.resolve("run-" + index + ".properties")).exists();
        }
        journal.prepareCell(1);
        assertThat(journal.read().getProperty("phase")).isEqualTo("PREPARING");
        assertThat(Files.readString(journal.directory.resolve("cell-0.properties")))
                .contains("evidenceSha256=" + "f".repeat(64));
        assertThat(journal.read().stringPropertyNames())
                .noneMatch(key -> key.startsWith("cell.") || key.startsWith("run."));
        assertThat(Files.readString(journal.directory.resolve("run-0.properties")))
                .contains("runStatus=OBSERVED", "nextRun=0");
        assertThatThrownBy(() -> journal.prepareCell(1)).hasMessageContaining("next unclaimed");
    }

    @Test
    void cellWorkCleanupAllowsHeartbeatsBeforePublishingEvidence() throws Exception {
        Stage2CampaignJournal journal = retaining();
        Stage2CampaignJournal supervisorJournal =
                new Stage2CampaignJournal(journal.directory, clock);
        java.util.List<Path> removed = new java.util.ArrayList<>();
        journal.cellCleaned(
                "f".repeat(64),
                work -> {
                    assertThat(journal.directory.resolve("cell-0.properties")).doesNotExist();
                    assertThat(journal.read().getProperty("phase")).isEqualTo("RETAINING");
                    clock.advance(10_000);
                    supervisorJournal.heartbeat(supervisor, supervisorStart);
                    removed.add(work);
                    Stage2Lease.removeWork(work);
                });
        assertThat(removed)
                .containsExactlyElementsOf(
                        Stage2AssessmentPlan.runs().subList(0, 6).stream()
                                .map(run -> journal.directory.resolve("work").resolve(run.table()))
                                .collect(java.util.stream.Collectors.toList()));
        assertThat(journal.directory.resolve("cell-0.properties")).exists();
        assertThat(journal.read().getProperty("phase")).isEqualTo("READY");
    }

    @Test
    void failedCellWorkCleanupDoesNotPublishEvidenceOrAdvance() throws Exception {
        Stage2CampaignJournal journal = retaining();
        assertThatThrownBy(
                        () ->
                                journal.cellCleaned(
                                        "f".repeat(64),
                                        work -> {
                                            throw new IOException("checkpoint deletion failed");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checkpoint deletion failed");
        assertThat(journal.directory.resolve("cell-0.properties")).doesNotExist();
        assertThat(journal.read().getProperty("phase")).isEqualTo("RETAINING");
        assertThatThrownBy(() -> journal.prepareCell(1)).hasMessageContaining("next unclaimed");
        journal.stop();
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
    }

    @Test
    void stopDuringCellWorkCleanupCannotBeOverwrittenByCompletion() throws Exception {
        Stage2CampaignJournal journal = retaining();
        assertThatThrownBy(() -> journal.cellCleaned("f".repeat(64), work -> journal.stop()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stopped");
        assertThat(journal.directory.resolve("cell-0.properties")).doesNotExist();
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
    }

    private Stage2CampaignJournal retaining() throws Exception {
        Stage2CampaignJournal journal = active();
        for (int index = 0; index < 6; index++) {
            journal.claim(table(index), 999_999_990L + index, "worker-" + index);
            journal.finish(table(index), 999_999_990L + index, "worker-" + index, true);
        }
        return journal;
    }

    @Test
    void livePreviousWorkerAndReusedSupervisorIdentityAreRefused() throws Exception {
        Stage2CampaignJournal journal = active();
        ProcessHandle parent = ProcessHandle.current().parent().orElseThrow();
        String parentStart = parent.info().startInstant().orElseThrow().toString();
        journal.claim(table(0), parent.pid(), parentStart);
        journal.finish(table(0), parent.pid(), parentStart, true);
        assertThatThrownBy(() -> journal.claim(table(1), 999_999_992L, "worker-1"))
                .hasMessageContaining("still alive");
        assertThatThrownBy(() -> journal.heartbeat(supervisor, "other-start"))
                .hasMessageContaining("Different campaign supervisor");
    }

    @Test
    void interruptedTableCreationAndCampaignStartCannotBeRetried() throws Exception {
        Stage2CampaignJournal journal = prepare();
        start(journal);
        assertThatThrownBy(() -> start(journal))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        journal.prepareCell(0);
        Stage2CampaignJournal reopened = new Stage2CampaignJournal(journal.directory, clock);
        assertThatThrownBy(() -> reopened.prepareCell(0)).hasMessageContaining("next unclaimed");
        assertThatThrownBy(() -> reopened.claim(table(0), 999_999_991L, "worker-0"))
                .hasMessageContaining("outside the cell");
    }

    @Test
    void timeSpentBeforeActivationIsNotGrantedAgain() throws Exception {
        Stage2CampaignJournal journal = prepare();
        long started = clock.millis() - journal.hostBoundSeconds() * 1000 + 50_000;
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal, "a".repeat(32), supervisor, supervisorStart, started);
        assertThat(journal.read().getProperty("deadline"))
                .isEqualTo(Long.toString(clock.millis() + 50_000));
        assertThatThrownBy(() -> journal.prepareCell(0))
                .hasMessageContaining("Full cell does not fit");
    }

    @Test
    void heartbeatCannotExtendAnActiveWorkersDeadline() throws Exception {
        Stage2CampaignJournal journal = active();
        assertThatThrownBy(() -> journal.claim(table(0), supervisor, supervisorStart))
                .hasMessageContaining("outside the cell");
        ProcessHandle parent = ProcessHandle.current().parent().orElseThrow();
        Stage2RunLease worker =
                journal.claim(
                        table(0),
                        parent.pid(),
                        parent.info().startInstant().orElseThrow().toString());
        for (int index = 0; index < 17; index++) {
            clock.advance(19_000);
            journal.heartbeat(supervisor, supervisorStart);
        }
        clock.advance(17_000);
        assertThatThrownBy(worker::requireLive).hasMessageContaining("deadline expired");
        assertThatThrownBy(() -> journal.heartbeat(supervisor, supervisorStart))
                .hasMessageContaining("expired");
    }

    @Test
    void incompleteOfflinePlanCannotInvokeServiceWorker() throws Exception {
        Stage2CampaignJournal journal =
                new Stage2CampaignJournal(
                        Stage2CampaignTestPlan.write(
                                directory,
                                String.join(
                                        " ",
                                        java.lang.management.ManagementFactory.getRuntimeMXBean()
                                                .getInputArguments())));
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.main(
                                        new String[] {
                                            "service-formal", journal.directory.toString(), table(0)
                                        }))
                .isInstanceOf(java.nio.file.NoSuchFileException.class)
                .hasMessageContaining("state.properties");
        assertThat(journal.directory.resolve("work")).doesNotExist();
    }

    @Test
    void supervisorDetectsManifestChangesWithoutPreventingTerminalCleanup() throws Exception {
        Stage2CampaignJournal journal = active();
        Files.writeString(journal.directory.resolve("campaign.properties"), "changed");
        assertThatThrownBy(() -> journal.heartbeat(supervisor, supervisorStart))
                .hasMessageContaining("plan changed");
        journal.stop();
        assertThat(journal.read().getProperty("phase")).isEqualTo("STOPPED");
    }

    @Test
    void completingTheFullMatrixDoesNotGrowTheMeasuredGuardState() throws Exception {
        Stage2CampaignJournal journal = active();
        long firstSize = 0;
        for (int cell = 0; cell < 108; cell++) {
            if (cell > 0) {
                journal.prepareCell(cell);
                journal.tablesReady();
            }
            for (int offset = 0; offset < 6; offset++) {
                int order = cell * 6 + offset;
                journal.claim(table(order), 999_999_999L, "gone-worker");
                if (order == 0) {
                    firstSize = Files.size(journal.directory.resolve("state.properties"));
                }
                assertThat(Files.size(journal.directory.resolve("state.properties")))
                        .isLessThanOrEqualTo(firstSize + 100);
                journal.finish(table(order), 999_999_999L, "gone-worker", true);
            }
            journal.cellCleaned("f".repeat(64));
        }
        assertThat(journal.read().getProperty("phase")).isEqualTo("MATRIX_COMPLETE");
        assertThat(Files.readString(journal.directory.resolve("run-647.properties")))
                .contains("runStatus=OBSERVED", "nextRun=647");
    }

    @ParameterizedTest
    @ValueSource(strings = {"maxWriteAttemptsPerRun", "maxWriteBytesPerRun", "maxReadBytesPerRun"})
    void insufficientMatrixReservationsAreRejectedBeforeAdoption(String field) throws Exception {
        Stage2CampaignTestPlan.write(directory);
        Path input = directory.resolve("inputs.properties");
        Files.writeString(
                input, Files.readString(input).replaceAll(field + "=[0-9]+", field + "=1"));
        Path candidate = directory.resolve("insufficient");
        Stage2CampaignPlan.write(input, directory.resolve("limits.properties"), candidate);
        assertThatThrownBy(() -> new Stage2CampaignJournal(candidate))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("full matrix inventory");
        assertThat(candidate.resolve("activation.started")).doesNotExist();
        assertThat(candidate.resolve("adoption.started")).doesNotExist();
    }

    @Test
    void sixInventoriesMustFitAndPriorRunFilesCountTowardTheWorkCap() throws Exception {
        Stage2CampaignJournal journal = active();
        var worker = journal.claim(table(0), 999_999_999L, "worker");
        Files.createDirectories(worker.workDirectory());
        Files.write(worker.workDirectory().resolve("current"), new byte[40]);
        Path previous = Files.createDirectories(worker.workRoot().resolve("previous-run"));
        Files.write(previous.resolve("checkpoint"), new byte[40]);
        BigtableStage2Probe.requireWorkWithinLimit(null, worker.workDirectory(), 64);
        assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.requireWorkWithinLimit(
                                        worker, worker.workDirectory(), 64))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("storage cap exhausted");
        Path limits = directory.resolve("limits.properties");
        Files.writeString(
                limits,
                Files.readString(limits).replace("workBytes=2147483648", "workBytes=64000"));
        Path candidate = directory.resolve("insufficient-work");
        Stage2CampaignPlan.write(directory.resolve("inputs.properties"), limits, candidate);
        assertThatThrownBy(() -> new Stage2CampaignJournal(candidate))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("six cell inventories");
    }

    @Test
    void frozenJvmFlagsPreserveBackslashes() throws Exception {
        String flags = "-Dexample=C:" + "\\" + "runtime -Xmx2g";
        Path campaign = Stage2CampaignTestPlan.write(directory, flags);
        assertThat(new Stage2CampaignJournal(campaign).inputs.getProperty("jvmFlags"))
                .isEqualTo(flags);
    }

    private Stage2CampaignJournal active() throws Exception {
        Stage2CampaignJournal journal = prepare();
        start(journal);
        journal.prepareCell(0);
        journal.tablesReady();
        return journal;
    }

    private void start(Stage2CampaignJournal journal) throws IOException {
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal, "a".repeat(32), supervisor, supervisorStart, clock.millis());
    }

    private Stage2CampaignJournal prepare() throws Exception {
        return new Stage2CampaignJournal(Stage2CampaignTestPlan.write(directory), clock);
    }

    private static String table(int index) {
        return Stage2AssessmentPlan.runs().get(index).table();
    }

    private static final class MutableClock extends Clock {
        private long millis = 1_800_000_000_000L;

        void advance(long amount) {
            millis += amount;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }
}
