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

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Coordinator-driven checkpoint/recovery acceptance for the experimental local protocol. */
@Timeout(180)
class BigtableLocalStagedJobITCase {
    @TempDir Path directory;

    @Test
    void ordinarySavepointWaitsForANotifiedSnapshotBeforeCommitting() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 2);
                LocalStagedJob job =
                        new LocalStagedJob(
                                run, directory, true, false, 2, 20, 3_600_000, true, null, false)) {
            job.awaitAdmissions(20);
            assertThat(run.attempts).isEmpty();
            job.savepoint(directory, false);
            assertThat(run.attempts).isEmpty();
            job.savepoint(directory, true);
            assertThat(run.acknowledgements).hasSize(20);
            assertThat(run.store.applied).isEqualTo(20);
            assertThat(totalSum(run)).isEqualTo(20);
        }
    }

    @Test
    void responseLossAfterPartialCommitRestartsFromCompletedCheckpoint() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 1)) {
            run.loseAnswerAt = 3;
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 20, 3_600_000, true, null, true)) {
                job.awaitAdmissions(20);
                job.checkpoint();
                job.awaitAcks(20);
                job.finish();
                assertThat(run.acknowledgements).hasSize(20);
                assertThat(run.store.applied).isEqualTo(20);
                assertThat(totalSum(run)).isEqualTo(20);
                assertThat(run.store.deduplicated).isGreaterThanOrEqualTo(4);
                Map<Long, CheckAndMutateRowRequest> original = new HashMap<>();
                for (CheckAndMutateRowRequest attempt : run.attempts) {
                    CheckAndMutateRowRequest first =
                            original.putIfAbsent(
                                    LocalStagedHarness.sequence(attempt.getFalseMutationsList()),
                                    attempt);
                    if (first != null) {
                        assertThat(attempt).isEqualTo(first);
                    }
                }
                assertThat(original).hasSize(20);
                assertThat(run.committersClosed.get()).isGreaterThanOrEqualTo(2);
                assertThat(run.active.get()).isZero();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void successfulStopAndResumeRedistributePersistedEnvelopes(int restoredParallelism)
            throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 2)) {
            String path;
            try (LocalStagedJob initial =
                    new LocalStagedJob(
                            run, directory, true, false, 2, 24, 3_600_000, true, null, false)) {
                initial.awaitAdmissions(24);
                path = initial.savepoint(directory, true);
                assertThat(initial.result.isCompletedExceptionally()).isFalse();
                assertThat(run.store.applied).isEqualTo(24);
            }
            int stagedBeforeRestore = run.staged.get();
            try (LocalStagedJob restored =
                    new LocalStagedJob(
                            run,
                            directory,
                            true,
                            false,
                            restoredParallelism,
                            24,
                            100,
                            true,
                            path,
                            false)) {
                restored.awaitAcks(24);
                restored.finish();
                assertThat(run.staged.get()).isEqualTo(stagedBeforeRestore);
                assertThat(run.store.applied).isEqualTo(24);
                assertThat(totalSum(run)).isEqualTo(24);
                assertThat(run.store.deduplicated).isEqualTo(24);
            }
        }
    }

    @Test
    void cancelBeforeAnyCompletedCheckpointLeavesNoEffects() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 1)) {
            try (LocalStagedJob initial =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 12, 3_600_000, true, null, false)) {
                initial.awaitAdmissions(12);
            }
            assertThat(run.store.applied).isZero();
            try (LocalStagedJob replay =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 12, 100, false, null, false)) {
                replay.finish();
            }
            assertThat(run.store.applied).isEqualTo(12);
            assertThat(totalSum(run)).isEqualTo(12);
        }
    }

    @Test
    void failedStopRequiresItsSavepointInsteadOfTheEarlierCheckpoint() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, true, true, 1)) {
            run.allowInputs.set(false);
            String previousCheckpoint;
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 12, 3_600_000, true, null, false)) {
                io.github.flink.gcp.connector.testutils.Awaits.await(
                        "source ready",
                        java.time.Duration.ofSeconds(20),
                        () -> run.readersStarted.get() == 1);
                previousCheckpoint = job.checkpoint();
                run.allowInputs();
                job.awaitAdmissions(12);
                run.failWriterClose.set(true);
                assertThatThrownBy(() -> job.savepoint(directory, true))
                        .hasStackTraceContaining("failed during stopping");
                assertThat(totalSum(run)).isEqualTo(12);
            }
            String completedSavepoint;
            try (var paths = java.nio.file.Files.list(directory)) {
                var savepoints =
                        paths.filter(path -> path.getFileName().toString().startsWith("savepoint-"))
                                .collect(java.util.stream.Collectors.toList());
                assertThat(savepoints).hasSize(1);
                completedSavepoint = savepoints.get(0).toUri().toString();
            }
            try (LocalStagedJob correct =
                    new LocalStagedJob(
                            run,
                            directory,
                            true,
                            false,
                            1,
                            12,
                            100,
                            true,
                            completedSavepoint,
                            false)) {
                correct.finish();
                assertThat(run.staged.get()).isEqualTo(12);
                assertThat(totalSum(run)).isEqualTo(12);
                assertThat(run.deduplicated.get()).isEqualTo(12);
            }
            try (LocalStagedJob older =
                    new LocalStagedJob(
                            run,
                            directory,
                            true,
                            false,
                            1,
                            12,
                            100,
                            false,
                            previousCheckpoint,
                            false)) {
                older.finish();
                assertThat(run.staged.get()).isEqualTo(24);
                assertThat(totalSum(run)).isEqualTo(24);
            }
        }
    }

    @Test
    void capacityFailureTerminatesRatherThanBlockingTheNextBarrier() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(65536, false, false, 1)) {
            run.maxBytes = 1024;
            try (LocalStagedJob job =
                    new LocalStagedJob(
                            run, directory, true, false, 1, 1, 100, false, null, false)) {
                assertThatThrownBy(job::finish)
                        .hasStackTraceContaining("Staging capacity exceeded");
                assertThat(run.attempts).isEmpty();
            }
        }
    }

    @Test
    void bulkWriterReportsEachAcknowledgementThroughItsExistingFactorySeam() throws Exception {
        try (LocalStagedHarness run = new LocalStagedHarness(1024, false, false, 4);
                LocalStagedJob job =
                        new LocalStagedJob(
                                run, directory, false, false, 2, 40, 100, false, null, false)) {
            job.finish();
            assertThat(run.admissions).hasSize(40);
            assertThat(run.acknowledgements.keySet()).isEqualTo(run.admissions.keySet());
            assertThat(run.throughput()).isPositive();
            assertThat(run.percentileNanos(0.95)).isPositive();
        }
    }

    static long totalSum(LocalStagedHarness run) {
        synchronized (run.store) {
            return run.store.cells.values().stream()
                    .mapToLong(row -> Long.parseLong(row.get("agg:count").toStringUtf8()))
                    .sum();
        }
    }
}
