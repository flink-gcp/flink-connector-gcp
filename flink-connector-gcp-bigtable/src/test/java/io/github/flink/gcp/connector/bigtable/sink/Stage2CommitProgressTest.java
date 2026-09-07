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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(30)
class Stage2CommitProgressTest {
    @TempDir Path directory;

    @Test
    void reportsConcurrentInvocationsAndRetainsOnlyTotalsAfterCompletion() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object first = new Object();
        Object second = new Object();
        progress.started(first, 80, 100);
        progress.started(second, 30, 200);
        JsonNode active = json(progress.sample(500));
        assertThat(active.path("activeBatches").asInt()).isEqualTo(2);
        assertThat(active.path("activeBatchEntries").asInt()).isEqualTo(110);
        assertThat(active.path("oldestBatchNanos").asLong()).isEqualTo(400);
        assertThat(active.path("largestBatchEntries").asInt()).isEqualTo(80);
        progress.finished(first, true, 600);
        assertThat(json(progress.sample(700)).path("oldestBatchNanos").asLong()).isEqualTo(500);
        progress.finished(second, false, 800);
        JsonNode finished = json(progress.sample(1000));
        assertThat(finished.path("activeBatches").asInt()).isZero();
        assertThat(finished.path("activeBatchEntries").asInt()).isZero();
        assertThat(finished.path("oldestBatchNanos").asLong()).isZero();
        assertThat(finished.path("startedBatches").asInt()).isEqualTo(2);
        assertThat(finished.path("finishedBatches").asInt()).isEqualTo(2);
        assertThat(finished.path("failedBatches").asInt()).isEqualTo(1);
        assertThat(finished.path("maxFinishedBatchNanos").asLong()).isEqualTo(600);
        progress.started(first, 1, 1100);
        progress.finished(first, true, 1200);
        assertThat(json(progress.sample(1300)).path("finishedBatches").asInt()).isEqualTo(3);
    }

    @Test
    void interruptedCommitDropsActiveBatchWithoutRetainingRequestHistory() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (Stage2Harness run = harness();
                var committer = new LocalStagedHarness.LocalCommitter(run, false)) {
            run.hang = true;
            run.startWindow(System.nanoTime(), 0, TimeUnit.MINUTES.toNanos(1));
            var requests = LocalStagedHarnessTest.envelopes(run, 3);
            run.prepared(
                    requests.stream()
                            .map(request -> request.wire)
                            .collect(java.util.stream.Collectors.toList()));
            var task =
                    executor.submit(
                            () -> {
                                try {
                                    committer.commit(new ArrayList<>(requests));
                                } catch (Throwable caught) {
                                    failure.set(caught);
                                }
                            });
            await("two requests held", Duration.ofSeconds(5), () -> run.active.get() == 2);
            JsonNode snapshot = json(run.commits.sample(System.nanoTime()));
            assertThat(snapshot.path("activeBatches").asInt()).isEqualTo(1);
            assertThat(snapshot.path("activeBatchEntries").asInt()).isEqualTo(3);
            assertThat(snapshot.path("oldestBatchNanos").asLong()).isPositive();
            task.cancel(true);
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isInstanceOf(InterruptedException.class);
            assertThat(run.originalFutures).isEmpty();
            assertThat(run.active.get()).isZero();
            JsonNode cancelled = json(run.commits.sample(System.nanoTime()));
            assertThat(cancelled.path("activeBatches").asInt()).isZero();
            assertThat(cancelled.path("failedBatches").asInt()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void successfulCommitCompletesTheProgressRecord() throws Exception {
        try (Stage2Harness run = harness();
                var committer = new LocalStagedHarness.LocalCommitter(run, false)) {
            run.startWindow(System.nanoTime(), 0, TimeUnit.MINUTES.toNanos(1));
            var requests = LocalStagedHarnessTest.envelopes(run, 3);
            run.prepared(
                    requests.stream()
                            .map(request -> request.wire)
                            .collect(java.util.stream.Collectors.toList()));
            committer.commit(new ArrayList<>(requests));
            JsonNode snapshot = json(run.commits.sample(System.nanoTime()));
            assertThat(snapshot.path("activeBatches").asInt()).isZero();
            assertThat(snapshot.path("startedBatches").asInt()).isEqualTo(1);
            assertThat(snapshot.path("finishedBatches").asInt()).isEqualTo(1);
            assertThat(snapshot.path("failedBatches").asInt()).isZero();
            assertThat(run.ledger.acknowledgedCount()).isEqualTo(3);
        }
    }

    private Stage2Harness harness() throws Exception {
        return new Stage2Harness(
                TableDestination.of("local-project", "local-instance", "local-table"),
                "127.0.0.1:1",
                directory.resolve("inventory"),
                16,
                1024,
                false,
                false,
                2,
                false,
                null);
    }

    private static JsonNode json(String value) throws Exception {
        return new ObjectMapper().readTree(value);
    }
}
