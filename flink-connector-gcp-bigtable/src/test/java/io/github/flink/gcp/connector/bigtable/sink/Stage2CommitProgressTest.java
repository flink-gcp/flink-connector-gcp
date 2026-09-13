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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.api.core.SettableApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
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
    void oneCompletionNotificationSynchronouslyDrainsAllEarlierCheckpointCollections()
            throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Stage2NotificationProgress notifications = new Stage2NotificationProgress();
        try (LocalStagedHarness run =
                new LocalStagedHarness(1024, false, false, 2) {
                    @Override
                    void commitStarted(Object committer, int entries) {
                        progress.started(committer, entries, System.nanoTime());
                        notifications.invocation(entries);
                    }

                    @Override
                    void commitFinished(Object committer, boolean successful) {
                        progress.finished(committer, successful, System.nanoTime());
                    }
                }) {
            run.hang = true;
            var sink = new LocalStagedHarness.StagedSink(run, false);
            try (var writer =
                            new OneInputStreamOperatorTestHarness<
                                    Long, CommittableMessage<CheckAndMutateRowRequest>>(
                                    new SinkWriterOperatorFactory<>(sink), 16, 1, 0);
                    var committer =
                            new OneInputStreamOperatorTestHarness<
                                    CommittableMessage<CheckAndMutateRowRequest>,
                                    CommittableMessage<CheckAndMutateRowRequest>>(
                                    new CommitterOperatorFactory<>(sink, false, true), 16, 1, 0)) {
                writer.setup(
                        CommittableMessageTypeInfo.of(sink::getCommittableSerializer)
                                .createSerializer(new SerializerConfigImpl()));
                writer.open();
                committer.open();
                long sequence = 0;
                int[] sizes = {2, 3, 1};
                for (int checkpoint = 1; checkpoint <= sizes.length; checkpoint++) {
                    for (int entry = 0; entry < sizes[checkpoint - 1]; entry++) {
                        writer.processElement(sequence++, 0);
                    }
                    writer.getOperator().prepareSnapshotPreBarrier(checkpoint);
                    for (var message : writer.extractOutputValues()) {
                        committer.processElement(new StreamRecord<>(message));
                    }
                    writer.getOutput().clear();
                    committer.snapshot(checkpoint, 0);
                }
                committer.getOperator().notifyCheckpointAborted(1);
                assertThat(run.originalFutures).isEmpty();
                var observed =
                        Stage2NotificationOperatorFactory.observe(
                                committer.getOperator(), notifications);
                var notification =
                        executor.submit(
                                () -> {
                                    observed.notifyCheckpointComplete(3);
                                    return null;
                                });
                try {
                    for (int sent = 1; sent <= 6; sent++) {
                        int expected = sent;
                        await(
                                "next synchronous request",
                                Duration.ofSeconds(5),
                                () -> run.originalFutures.size() >= expected);
                        assertThat(notification).isNotDone();
                        JsonNode active = json(notifications.sample(System.nanoTime()));
                        assertThat(active.path("activeNotifications")).hasSize(1);
                        assertThat(
                                        active.path("activeNotifications")
                                                .get(0)
                                                .path("checkpointId")
                                                .asLong())
                                .isEqualTo(3);
                        assertThat(active.path("finishedNotifications").asInt()).isZero();
                        ((SettableApiFuture<Boolean>) run.originalFutures.get(sent - 1)).set(false);
                    }
                    notification.get(5, TimeUnit.SECONDS);
                    JsonNode completed = json(progress.sample(System.nanoTime()));
                    assertThat(completed.path("finishedBatches").asInt()).isEqualTo(3);
                    assertThat(completed.path("largestBatchEntries").asInt()).isEqualTo(3);
                    assertThat(completed.path("failedBatches").asInt()).isZero();
                    assertThat(run.acknowledgedCount()).isEqualTo(6);
                    assertThat(run.active.get()).isZero();
                    JsonNode notificationResult = json(notifications.sample(System.nanoTime()));
                    assertThat(notificationResult.path("activeNotifications")).isEmpty();
                    assertThat(notificationResult.path("finishedNotifications").asInt())
                            .isEqualTo(1);
                    assertThat(notificationResult.path("maxNotificationInvocations").asInt())
                            .isEqualTo(3);
                    assertThat(notificationResult.path("maxNotificationEntries").asInt())
                            .isEqualTo(6);
                    assertThat(notificationResult.path("maxFinishedNotificationNanos").asLong())
                            .isGreaterThanOrEqualTo(
                                    completed.path("maxFinishedBatchNanos").asLong());
                } finally {
                    notification.cancel(true);
                    executor.shutdownNow();
                    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                }
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

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
