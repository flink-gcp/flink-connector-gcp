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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class Stage2JobITCase {
    @TempDir Path directory;

    @Test
    void emptyCheckpointNotificationsAreObservedForBothSourceSubtasks() throws Exception {
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("notifications"),
                        4,
                        1024,
                        false,
                        false,
                        1,
                        true,
                        null)) {
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run,
                            directory.resolve("job"),
                            true,
                            false,
                            2,
                            4,
                            3_600_000,
                            true,
                            null,
                            false)) {
                await(
                        "both source readers started",
                        Duration.ofSeconds(20),
                        () -> run.readersStarted.get() == 2);
                job.checkpoint();
                await(
                        "both completion notifications returned",
                        Duration.ofSeconds(10),
                        () -> {
                            String sample = run.notifications.sample(System.nanoTime());
                            return sample.contains("\"finishedNotifications\":2,")
                                    && sample.contains("\"activeNotifications\":[]");
                        });
                var sample =
                        new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                                        .ObjectMapper()
                                .readTree(run.notifications.sample(System.nanoTime()));
                assertThat(sample.path("maxNotificationEntries").asLong()).isZero();
                assertThat(sample.path("outsideNotificationInvocations").asLong()).isZero();
                assertThat(run.ledger.admittedCount()).isZero();
            }
            assertThat(run.active.get()).isZero();
        }
    }

    @Test
    void heldCommitExpiresTheNextCheckpointAndReleasesItsProgressOnCancellation() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        try (LocalStagedHarness run =
                new LocalStagedHarness(1024, false, false, 4) {
                    @Override
                    long checkpointTimeoutMillis() {
                        return 5_000;
                    }

                    @Override
                    void commitStarted(Object committer, int entries) {
                        progress.started(committer, entries, System.nanoTime());
                    }

                    @Override
                    void commitFinished(Object committer, boolean successful) {
                        progress.finished(committer, successful, System.nanoTime());
                    }
                }) {
            run.hang = true;
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 4, 3_600_000, true, null, false)) {
                job.awaitAdmissions(4);
                job.checkpoint();
                await("commit requests held", Duration.ofSeconds(10), () -> run.active.get() == 4);
                var waiting =
                        new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                                        .ObjectMapper()
                                .readTree(progress.sample(System.nanoTime()));
                assertThat(waiting.path("activeBatches").asInt()).isEqualTo(1);
                assertThat(waiting.path("activeBatchEntries").asInt()).isEqualTo(4);
                org.assertj.core.api.Assertions.assertThatThrownBy(job::checkpoint)
                        .hasStackTraceContaining("Checkpoint expired before completing");
                org.assertj.core.api.Assertions.assertThatThrownBy(
                                () -> job.result.get(20, TimeUnit.SECONDS))
                        .hasStackTraceContaining("Exceeded checkpoint tolerable failure threshold");
            }
            assertThat(run.originalFutures)
                    .hasSize(4)
                    .allSatisfy(future -> assertThat(future.isCancelled()).isTrue());
            assertThat(run.active.get()).isZero();
            var stopped =
                    new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                                    .ObjectMapper()
                            .readTree(progress.sample(System.nanoTime()));
            assertThat(stopped.path("activeBatches").asInt()).isZero();
            assertThat(stopped.path("failedBatches").asInt()).isEqualTo(1);
        }
    }

    @Test
    void failedRunPreservesSamplesBeforeOwnedWorkRemoval() throws Exception {
        Path work = directory.resolve("overflow");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () ->
                                BigtableStage2Probe.timed(
                                        null,
                                        "local-table",
                                        work,
                                        true,
                                        false,
                                        "127.0.0.1:1",
                                        65536,
                                        1,
                                        4,
                                        60000,
                                        0,
                                        30000,
                                        2000,
                                        0,
                                        false))
                .hasStackTraceContaining("Staging capacity exceeded");
        assertThat(java.nio.file.Files.exists(work)).isFalse();
        Path evidence = directory.resolve("overflow-failed-samples.jsonl");
        assertThat(evidence).isRegularFile();
        assertThat(java.nio.file.Files.size(evidence)).isBetween(1L, 8L * 1024 * 1024);
        assertThat(java.nio.file.Files.readString(evidence))
                .contains("\"phase\":\"before-admission\"", "checkpoints");
    }

    @Test
    void completedCheckpointRecoveryPreservesDiskInventoryAndSum() throws Exception {
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("inventory"),
                        64,
                        1024,
                        true,
                        true,
                        2,
                        false,
                        null)) {
            run.startWindow(System.nanoTime(), 0, TimeUnit.HOURS.toNanos(1));
            run.loseAnswerAt = 3;
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, true, false, 2, 24, 3_600_000, true, null, true)) {
                await(
                        "ledger admissions",
                        Duration.ofSeconds(30),
                        () -> {
                            if (job.result.isDone()) {
                                job.result.join();
                            }
                            return run.ledger.admittedCount() == 24;
                        },
                        () -> "admitted=" + run.ledger.admittedCount());
                job.checkpoint();
                await(
                        "ledger acknowledgements",
                        Duration.ofSeconds(30),
                        () -> {
                            if (job.result.isDone()) {
                                job.result.join();
                            }
                            return run.ledger.acknowledgedCount() == 24;
                        },
                        () -> "ack=" + run.ledger.acknowledgedCount());
                job.finish();
                assertThat(run.ledger.summary().count).isEqualTo(24);
                assertThat(run.store.applied).isEqualTo(24);
                run.verifyFakeSums();
                assertThat(run.loseAnswerAt).isEqualTo(-1);
                assertThat(run.deduplicated.get()).isPositive();
                assertThat(run.staged.get()).isEqualTo(24);
                assertThat(run.active.get()).isZero();
                assertThat(run.attempts).isEmpty();
                assertThat(run.originalFutures).isEmpty();
                assertThat(run.notifications.sample(System.nanoTime()))
                        .contains("\"startedNotifications\":0");
            }
        }
    }

    @Test
    void serviceRecoverySequenceReplaysAtBothRescaledParallelismsLocally() throws Exception {
        try (Stage2Harness run =
                new Stage2Harness(
                        TableDestination.of("local-project", "local-instance", "local-table"),
                        "127.0.0.1:1",
                        directory.resolve("inventory"),
                        1024,
                        1024,
                        true,
                        true,
                        4,
                        false,
                        null)) {
            BigtableStage2Probe.recovery(run, directory.resolve("recovery"), false);
            assertThat(run.ledger.acknowledgedCount()).isEqualTo(128);
            assertThat(run.store.applied).isEqualTo(128);
            assertThat(run.staged.get()).isEqualTo(128);
            assertThat(run.loseAnswerAt).isEqualTo(-1);
            assertThat(run.active.get()).isZero();
        }
    }

    @Test
    void warmupCapacityExhaustionStillDrainsAndCleans() throws Exception {
        BigtableStage2Probe.timed(
                null,
                "local-table",
                directory.resolve("censored"),
                false,
                false,
                "127.0.0.1:1",
                1024,
                1,
                4,
                100,
                10_000,
                1000,
                10,
                0,
                false);
        assertThat(directory.resolve("censored")).doesNotExist();
        assertThat(directory.resolve("censored-samples.jsonl")).exists();
    }

    @Test
    void sustainedSourceFinishesAndDrainsWithoutFiniteTraceRetention() throws Exception {
        BigtableStage2Probe.timed(
                null,
                "local-table",
                directory.resolve("run"),
                true,
                false,
                "127.0.0.1:1",
                1024,
                1,
                4,
                100,
                500,
                1000,
                10_000,
                1,
                true);
        assertThat(directory.resolve("run")).doesNotExist();
    }
}
