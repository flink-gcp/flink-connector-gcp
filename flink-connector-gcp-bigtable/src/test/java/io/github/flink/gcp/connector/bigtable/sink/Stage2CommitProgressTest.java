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
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.committer.BigtableStagedCommitter;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    void reportsConcurrentInvocationsAndRetainsOneRecordPerFinishedInvocation() throws Exception {
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
        var records = progress.finishedRecords();
        assertThat(records).hasSize(3);
        JsonNode firstRecord = json(records.get(0));
        assertThat(firstRecord.path("committer").asInt()).isZero();
        assertThat(firstRecord.path("entries").asInt()).isEqualTo(80);
        assertThat(firstRecord.path("startedNanos").asLong()).isEqualTo(100);
        assertThat(firstRecord.path("durationNanos").asLong()).isEqualTo(500);
        assertThat(firstRecord.path("successful").asBoolean()).isTrue();
        JsonNode secondRecord = json(records.get(1));
        assertThat(secondRecord.path("committer").asInt()).isEqualTo(1);
        assertThat(secondRecord.path("durationNanos").asLong()).isEqualTo(600);
        assertThat(secondRecord.path("successful").asBoolean()).isFalse();
        assertThat(json(records.get(2)).path("committer").asInt()).isZero();
        assertThat(firstRecord.path("threadCpuNanos").asLong())
                .as("an invocation started without a CPU reading reports none")
                .isEqualTo(-1);
        assertThat(progress.droppedRecords()).isZero();
    }

    @Test
    void recordsTheCommittingThreadsCpuTimeAcrossAnInvocation() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object committer = new Object();
        progress.started(committer, 10, 100, 5_000);
        progress.finished(committer, true, 900, 7_500);
        JsonNode record = json(progress.finishedRecords().get(0));
        assertThat(record.path("durationNanos").asLong()).isEqualTo(800);
        assertThat(record.path("threadCpuNanos").asLong()).isEqualTo(2_500);
    }

    @Test
    void reportsNoCpuTimeWhenEitherReadingIsMissing() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object first = new Object();
        Object second = new Object();
        progress.started(first, 1, 100, 5_000);
        progress.finished(first, true, 200, -1);
        progress.started(second, 1, 300, -1);
        progress.finished(second, true, 400, 7_000);
        var records = progress.finishedRecords();
        assertThat(json(records.get(0)).path("threadCpuNanos").asLong()).isEqualTo(-1);
        assertThat(json(records.get(1)).path("threadCpuNanos").asLong()).isEqualTo(-1);
    }

    @Test
    void finishedRecordsAreBoundedAndCountWhatTheyDrop() {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object committer = new Object();
        int total = Stage2CommitProgress.MAX_FINISHED_RECORDS + 3;
        for (int i = 0; i < total; i++) {
            progress.started(committer, 1, i);
            progress.finished(committer, true, i + 1);
        }
        var records = progress.finishedRecords();
        assertThat(records).hasSize(Stage2CommitProgress.MAX_FINISHED_RECORDS);
        assertThat(records.get(0)).contains("\"startedNanos\":0,");
        assertThat(records.get(records.size() - 1))
                .contains(
                        "\"startedNanos\":"
                                + (Stage2CommitProgress.MAX_FINISHED_RECORDS - 1)
                                + ",");
        assertThat(progress.droppedRecords()).isEqualTo(3);
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

    @Test
    void classifiesTheProductionCommittersWaitsAtTheBoundAndInTheDrain() throws Exception {
        var bean = java.lang.management.ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadContentionMonitoringSupported());
        boolean accounting = bean.isThreadContentionMonitoringEnabled();
        Stage2CommitProgress.enableWaitAccounting();
        Stage2CommitProgress progress = new Stage2CommitProgress();
        AtomicLong totalWait = new AtomicLong();
        List<SettableApiFuture<Boolean>> sent = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (LocalStagedHarness run =
                new LocalStagedHarness(1024, false, false, 2) {
                    @Override
                    Stage2CommitProgress.Invocation commitSent() {
                        return progress.sent();
                    }

                    @Override
                    void committerWaited(long nanos) {
                        totalWait.addAndGet(nanos);
                    }
                }) {
            SingleRowClient client =
                    new SingleRowClient() {
                        @Override
                        public ApiFuture<Boolean> checkAndMutateRow(
                                ConditionalRowMutation mutation) {
                            SettableApiFuture<Boolean> original = SettableApiFuture.create();
                            // Every second request completes at once, behind a slower head.
                            if (sent.size() % 2 == 1) {
                                original.set(false);
                            }
                            sent.add(original);
                            return new Stage2ObservedFuture<>(original, run, (value, now) -> {});
                        }

                        @Override
                        public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
                            throw new AssertionError("Only conditional requests");
                        }
                    };
            var options =
                    BigtableStagedOptions.builder()
                            .markerFamily(StagedMutationTestSink.MARKER_FAMILY)
                            .requestOptions(
                                    BigtableRequestOptions.builder().maxInFlightRequests(2).build())
                            .build();
            Object committerId = new Object();
            try (var committer =
                    new BigtableStagedCommitter(
                            options,
                            (table, profile, marker, expected) -> {},
                            profile -> new FixedClients(client),
                            Map.of(),
                            new UnregisteredMetricsGroup())) {
                List<CommitRequest<BigtableCommittable>> requests = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    requests.add(new Request());
                }
                Thread thread =
                        new Thread(
                                () -> {
                                    progress.started(committerId, 4, System.nanoTime(), -1);
                                    boolean successful = false;
                                    try {
                                        committer.commit(requests);
                                        successful = true;
                                    } catch (Throwable caught) {
                                        failure.set(caught);
                                    } finally {
                                        progress.finished(
                                                committerId, successful, System.nanoTime(), -1);
                                    }
                                });
                thread.start();
                // The committer blocks twice: on the first request at the bound of two, while the
                // second is already complete, and on the third in the final drain. Each time it is
                // held for 20 ms before the request it waits on completes.
                for (int head : new int[] {0, 2}) {
                    await(
                            "committer blocked on request " + head,
                            Duration.ofSeconds(5),
                            () -> sent.size() > head && isBlocked(thread),
                            () -> "sent=" + sent.size() + " state=" + thread.getState());
                    Thread.sleep(20);
                    sent.get(head).set(false);
                }
                thread.join(5000);
                assertThat(thread.isAlive()).isFalse();
            }
        } finally {
            bean.setThreadContentionMonitoringEnabled(accounting);
        }
        assertThat(failure.get()).isNull();
        JsonNode record = json(progress.finishedRecords().get(0));
        long held = TimeUnit.MILLISECONDS.toNanos(15);
        assertThat(record.path("sends").asInt()).isEqualTo(4);
        assertThat(record.path("boundBlockedWaits").asInt()).isEqualTo(1);
        assertThat(record.path("completedBehindAtBound").asInt())
                .as("the second request had completed while the committer waited on the first")
                .isEqualTo(1);
        assertThat(record.path("drainBlockedWaits").asInt()).isEqualTo(1);
        assertThat(record.path("boundWaitNanos").asLong()).isGreaterThanOrEqualTo(held);
        assertThat(record.path("drainWaitNanos").asLong()).isGreaterThanOrEqualTo(held);
        assertThat(record.path("sendPhaseNanos").asLong()).isGreaterThanOrEqualTo(held);
        assertThat(record.path("drainPhaseNanos").asLong()).isGreaterThanOrEqualTo(held);
        assertThat(record.path("waitedMillis").asLong())
                .as("two holds of 20 ms, counted in whole milliseconds")
                .isGreaterThanOrEqualTo(30);
        assertThat(record.path("boundWaitNanos").asLong() + record.path("drainWaitNanos").asLong())
                .as("every wait the harness saw is classified once")
                .isEqualTo(totalWait.get());
    }

    private static boolean isBlocked(Thread thread) {
        Thread.State state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    @Test
    void classifiesEachWaitByWhetherASendFollowsIt() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object committer = new Object();
        progress.started(committer, 4, 0, -1);
        // Three requests go out at a bound of three, and the second completes first.
        Stage2CommitProgress.Invocation invocation = progress.sent();
        progress.sent();
        progress.sent();
        invocation.completed();
        // The committer blocks on the first with the second already done behind it.
        int behind = invocation.completedBehindHead(false);
        assertThat(behind)
                .as("one of three in flight completed behind a pending head")
                .isEqualTo(1);
        invocation.completed();
        invocation.waited(100, true, behind);
        // The second is the head now and is done, so collecting it does not block.
        assertThat(invocation.completedBehindHead(true)).isZero();
        invocation.waited(5, false, 0);
        progress.sent();
        // In the drain the third and fourth are both pending: nothing is done behind the head.
        int drainBehind = invocation.completedBehindHead(false);
        assertThat(drainBehind).isZero();
        invocation.waited(30, true, drainBehind);
        invocation.completed();
        invocation.completed();
        invocation.waited(0, false, 0);
        progress.finished(committer, true, 1_000, -1);
        JsonNode record = json(progress.finishedRecords().get(0));
        assertThat(record.path("sends").asInt()).isEqualTo(4);
        assertThat(record.path("boundWaitNanos").asLong()).isEqualTo(105);
        assertThat(record.path("drainWaitNanos").asLong()).isEqualTo(30);
        assertThat(record.path("boundBlockedWaits").asInt()).isEqualTo(1);
        assertThat(record.path("drainBlockedWaits").asInt()).isEqualTo(1);
        assertThat(record.path("completedBehindAtBound").asInt()).isEqualTo(1);
    }

    @Test
    void anInvocationWithoutSendsHasNoDrainPhase() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        Object committer = new Object();
        progress.started(committer, 0, 100, -1);
        progress.finished(committer, true, 400, -1);
        JsonNode record = json(progress.finishedRecords().get(0));
        assertThat(record.path("sendPhaseNanos").asLong()).isEqualTo(300);
        assertThat(record.path("drainPhaseNanos").asLong()).isZero();
    }

    @Test
    void sendsOutsideAnInvocationAreNotAttributed() throws Exception {
        Stage2CommitProgress progress = new Stage2CommitProgress();
        assertThat(progress.sent()).isNull();
        Object committer = new Object();
        progress.started(committer, 1, 0, -1);
        progress.finished(committer, true, 1, -1);
        assertThat(progress.sent()).as("the finished invocation is no longer current").isNull();
        JsonNode record = json(progress.finishedRecords().get(0));
        assertThat(record.path("sends").asInt()).isZero();
        assertThat(record.path("boundWaitNanos").asLong()).isZero();
        assertThat(record.path("drainWaitNanos").asLong()).isZero();
        assertThat(record.path("boundBlockedWaits").asInt()).isZero();
    }

    private static final class FixedClients implements SingleRowClientFactory {
        private static final long serialVersionUID = 1L;
        private final transient SingleRowClient client;

        FixedClients(SingleRowClient client) {
            this.client = client;
        }

        @Override
        public SingleRowClient create(TableDestination table) {
            return client;
        }

        @Override
        public void release(TableDestination table) {}

        @Override
        public void close() {}
    }

    private static final class Request implements CommitRequest<BigtableCommittable> {
        private final BigtableCommittable value;

        Request() {
            try {
                value =
                        BigtableCommittable.stage(
                                TableDestination.of("p", "i", "t"),
                                LocalStagedHarness.PROFILE,
                                StagedMutationTestSink.MARKER_FAMILY,
                                RowMutationEntry.create("r")
                                        .setCell("cf", "q", 1000, "v")
                                        .toProto(),
                                new SecureRandom());
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }

        @Override
        public BigtableCommittable getCommittable() {
            return value;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalAlreadyCommitted() {
            throw new AssertionError("No request matched its marker");
        }

        @Override
        public void retryLater() {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void updateAndRetryLater(BigtableCommittable value) {
            throw new AssertionError("Must retain identity");
        }

        @Override
        public void signalFailedWithKnownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable failure) {
            throw new AssertionError("Must fail the commit");
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
