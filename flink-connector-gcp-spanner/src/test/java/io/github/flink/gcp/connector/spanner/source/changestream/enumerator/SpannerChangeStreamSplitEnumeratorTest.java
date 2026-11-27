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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.ClientMetricsProbe;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedAckEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorStateSerializer;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamInitializationEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarkEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarks;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Timeout(30)
class SpannerChangeStreamSplitEnumeratorTest {

    @Test
    void anEnumeratorBuiltOnTheSameGroupKeepsReportingThroughTheFirstRegistrations()
            throws Exception {
        // A coordinator reset builds the next enumerator on the group the first one registered
        // on, where Flink drops a second registration of a name (#1908).
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator first = enumerator(context, new ScriptedClient(), null);
        first.start();
        first.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();
        first.close();

        SpannerChangeStreamSplitEnumerator second = enumerator(context, new ScriptedClient(), null);
        second.start();
        second.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(context.counter(SpannerMetricNames.SPLITS_ASSIGNED)).isEqualTo(2);
        second.close();
    }

    @Test
    void freshLatestSeedsOneNullTokenAfterInspectingStreamMetadata() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(client.initializeCalls).isEqualTo(1);
        assertThat(context.sourceEvents(0))
                .singleElement()
                .isInstanceOfSatisfying(
                        SpannerChangeStreamInitializationEvent.class,
                        event -> {
                            assertThat(event.shouldDiscardRestoredSplits()).isFalse();
                            assertThat(event.getSourceWatermark())
                                    .isEqualTo(
                                            SpannerChangeStreamWatermarks.beforeInstant(
                                                    context.assignedSplits(0)
                                                            .get(0)
                                                            .getStartTimestamp()));
                        });
        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.splitId())
                                    .isEqualTo(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
                            assertThat(split.getPartitionToken()).isNull();
                            assertThat(split.getLifecycleState())
                                    .isEqualTo(PartitionLifecycleState.RUNNING);
                        });
        assertThat(context.counter(SpannerMetricNames.SPLITS_ASSIGNED)).isEqualTo(1);
        // Closed as soon as initialization returned, not with the enumerator: on the JobManager
        // Flink closes the enumerator without waiting and in practice after it releases the job's
        // class loader (docs/adr/0208).
        assertThat(client.closeCalls).isEqualTo(1);
        enumerator.close();
        assertThat(client.closeCalls).isEqualTo(1);
    }

    @Test
    void restorePreservesTheLedgerAndIgnoresTheConfiguredFreshStart() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit child =
                child("child", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, child));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        clientMetrics -> client,
                        StartPosition.at(Instant.now().plus(Duration.ofDays(1))),
                        null,
                        null,
                        2_000,
                        true,
                        Duration.ofMinutes(10),
                        restored,
                        null);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(client.initializeCalls).isEqualTo(1);
        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        assigned -> {
                            assertThat(assigned.getPartitionToken()).isEqualTo("child");
                            assertThat(assigned.getCurrentPosition()).isEqualTo(recent);
                        });
        enumerator.close();
    }

    @Test
    void returnedSplitsRestartFromTheReturnedProgressAndBecomeScheduledAgain() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        Instant returnedProgress = running.getCurrentPosition().plusSeconds(5);
        Instant coordinatorProgress = running.getCurrentPosition().plusSeconds(10);
        Instant coordinatorWatermark = running.getWatermark().plusSeconds(20);
        Instant returnedWatermark = running.getWatermark().plusSeconds(25);
        enumerator.handleSourceEvent(
                0, committedProgress(running.splitId(), coordinatorProgress, coordinatorWatermark));

        enumerator.addSplitsBack(
                Collections.singletonList(
                        running.withProgress(returnedProgress, returnedWatermark)),
                0);

        ChangeStreamPartitionSplit returned = enumerator.snapshotState(1).getPartitions().get(0);
        assertThat(returned.getLifecycleState()).isEqualTo(PartitionLifecycleState.SCHEDULED);
        // The subtask's downstream rolled back with it, so reported progress is not kept.
        assertThat(returned.getCurrentPosition()).isEqualTo(returnedProgress);
        assertThat(returned.getWatermark()).isEqualTo(returnedWatermark);
        assertThat(enumerator.snapshotState(2).getSourceWatermark())
                .isEqualTo(returnedWatermark.toEpochMilli() - 1);
        assertThat(context.counter(SpannerMetricNames.SPLITS_RETURNED)).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void scheduledPartitionHoldsBackAnActivePartitionUntilItIsRead() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit scheduled =
                child("scheduled", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ChangeStreamPartitionSplit active =
                child("active", recent.plusSeconds(5), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, scheduled, active));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        active.splitId(), recent.plusSeconds(20), recent.plusSeconds(20)));

        assertThat(watermarkEvents(context, 0)).isEmpty();

        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assigned = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        assigned.splitId(), recent.plusSeconds(30), recent.plusSeconds(30)));

        assertThat(watermarkEvents(context, 0))
                .extracting(SpannerChangeStreamWatermarkEvent::getSourceWatermark)
                .containsExactly(recent.plusSeconds(20).toEpochMilli() - 1);
        enumerator.close();
    }

    @Test
    void readerQueuedRunningPartitionParticipatesInTheCompleteLedgerMinimum() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit queued =
                child("queued", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit active =
                child("active", recent.plusSeconds(5), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, queued, active));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        active.splitId(), recent.plusSeconds(20), recent.plusSeconds(20)));

        assertThat(watermarkEvents(context, 0)).isEmpty();

        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        queued.splitId(), recent.plusSeconds(30), recent.plusSeconds(30)));

        assertThat(watermarkEvents(context, 0))
                .extracting(SpannerChangeStreamWatermarkEvent::getSourceWatermark)
                .containsExactly(recent.plusSeconds(20).toEpochMilli() - 1);
        enumerator.close();
    }

    @Test
    void duplicateMinimumAdvancesOnlyAfterEveryPartitionLeavesIt() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit first =
                child("first", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit second =
                child("second", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, first, second));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);

        enumerator.start();
        context.runAsyncCalls();
        Instant firstProgress = recent.plusSeconds(10);
        enumerator.handleSourceEvent(
                0, committedProgress(first.splitId(), firstProgress, firstProgress));

        assertThat(watermarkEvents(context, 0)).isEmpty();

        Instant secondProgress = recent.plusSeconds(20);
        enumerator.handleSourceEvent(
                0, committedProgress(second.splitId(), secondProgress, secondProgress));

        assertThat(watermarkEvents(context, 0))
                .extracting(SpannerChangeStreamWatermarkEvent::getSourceWatermark)
                .containsExactly(firstProgress.toEpochMilli() - 1);
        enumerator.close();
    }

    @Test
    void childStartingBehindTheEmittedFrontierFailsThePartitionProtocol() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant advanced = initial.getStartTimestamp().plusSeconds(10);
        enumerator.handleSourceEvent(0, committedProgress(initial.splitId(), advanced, advanced));

        assertThatThrownBy(
                        () ->
                                enumerator.handleSourceEvent(
                                        0,
                                        children(
                                                initial,
                                                initial.getStartTimestamp().plusSeconds(5),
                                                "late-child")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("moved backwards");
        enumerator.close();
    }

    @Test
    void broadcastsTheFrontierToSplitlessAndLateRegisteredReaders() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(3);
        context.registerReader(0);
        context.registerReader(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        Instant advanced = running.getStartTimestamp().plusSeconds(5);

        enumerator.handleSourceEvent(0, committedProgress(running.splitId(), advanced, advanced));

        assertThat(watermarkEvents(context, 0))
                .extracting(SpannerChangeStreamWatermarkEvent::getSourceWatermark)
                .containsExactly(advanced.toEpochMilli() - 1);
        assertThat(watermarkEvents(context, 1))
                .extracting(SpannerChangeStreamWatermarkEvent::getSourceWatermark)
                .containsExactly(advanced.toEpochMilli() - 1);
        assertThat(context.assignedSplits(1)).isEmpty();

        context.registerReader(2);
        enumerator.addReader(2);

        assertThat(context.sourceEvents(2))
                .singleElement()
                .isInstanceOfSatisfying(
                        SpannerChangeStreamInitializationEvent.class,
                        event ->
                                assertThat(event.getSourceWatermark())
                                        .isEqualTo(advanced.toEpochMilli() - 1));
        enumerator.close();
    }

    @Test
    void discoveryCounterCountsNewChildTokensButNotDuplicateReports() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);

        ChildPartitionsEvent children =
                children(initial, initial.getCurrentPosition().plusSeconds(1), "a", "b");
        enumerator.handleSourceEvent(0, children);
        enumerator.handleSourceEvent(0, children);

        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(2);
        enumerator.close();
    }

    @Test
    void unassignedLagTracksTheOldestScheduledPositionAcrossAssignment() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit older =
                child("older", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ChangeStreamPartitionSplit sameAge =
                child("same-age", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ChangeStreamPartitionSplit newer =
                child("newer", recent.plusMillis(500), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, older, sameAge, newer));
        AtomicLong now = new AtomicLong(recent.plusSeconds(1).toEpochMilli());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        clientMetrics -> new ScriptedClient(),
                        StartPosition.latest(),
                        null,
                        null,
                        2_000,
                        true,
                        Duration.ofMinutes(10),
                        restored,
                        null,
                        now::get,
                        System::nanoTime);

        enumerator.start();
        context.runAsyncCalls();
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis"))
                .isEqualTo(1_000L);
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isZero();

        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis"))
                .isEqualTo(1_000L);
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis")).isEqualTo(500L);
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis")).isZero();
        now.set(recent.minusSeconds(1).toEpochMilli());
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis")).isZero();
        enumerator.close();
    }

    @Test
    void multipleChildRecordsWaitForParentCompletion() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant childStart = initial.getStartTimestamp().plusSeconds(1);

        enumerator.handleSourceEvent(0, children(initial, childStart, "left"));
        enumerator.handleSourceEvent(0, children(initial, childStart, "right"));

        assertThat(enumerator.snapshotState(1).getPartitions())
                .filteredOn(p -> p.getLifecycleState() == PartitionLifecycleState.CREATED)
                .hasSize(2);
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.assignedSplits(1)).isEmpty();

        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(initial.splitId(), childStart, childStart, false));

        assertThat(context.assignedSplits(1)).hasSize(1);
        SpannerChangeStreamEnumeratorState compact = enumerator.snapshotState(2);
        assertThat(compact.getPartitions())
                .filteredOn(p -> p.getLifecycleState() == PartitionLifecycleState.SCHEDULED)
                .hasSize(1);
        assertThat(compact.getPartitions()).hasSize(2);
        assertThat(compact.getFinishedParentProofs()).isEmpty();
        // The initial query's finish holds the frontier until a checkpoint covering it completes.
        assertThat(compact.getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(initial.getWatermark()));
        enumerator.notifyCheckpointComplete(2);
        assertThat(enumerator.snapshotState(3).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(childStart));
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isEqualTo(2L);
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS))
                .isZero();
        enumerator.close();
    }

    @Test
    void mergedChildIsReleasedOnlyAfterEveryParentFinishes() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant splitAt = initial.getStartTimestamp().plusSeconds(1);
        enumerator.handleSourceEvent(0, children(initial, splitAt, "left", "right"));
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(initial.splitId(), splitAt, splitAt, false));
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(1);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        Instant mergeAt = splitAt.plusSeconds(1);
        ChildPartitionsEvent merge =
                new ChildPartitionsEvent(
                        left.splitId(),
                        mergeAt,
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "merged", Arrays.asList(left.splitId(), right.splitId()))));

        enumerator.handleSourceEvent(0, merge);
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(left.splitId(), mergeAt, mergeAt, false));
        // Release left's hold, so the merged child below waits only for right.
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);

        SpannerChangeStreamEnumeratorState waitingForRight = enumerator.snapshotState(2);
        assertThat(waitingForRight.getPartitions())
                .filteredOn(p -> "merged".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.CREATED);
        assertThat(waitingForRight.getPartitions())
                .noneMatch(partition -> partition.splitId().equals(left.splitId()));
        assertThat(waitingForRight.getFinishedParentProofs()).containsExactly(left.splitId());
        assertThat(waitingForRight.getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(right.getWatermark()));
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isEqualTo(2L);
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS))
                .isEqualTo(1L);
        assertThat(enumerator.pendingParentDependencyCount()).isEqualTo(1);

        enumerator.handleSourceEvent(0, merge);
        ChildPartitionsEvent conflictingDuplicate =
                new ChildPartitionsEvent(
                        left.splitId(),
                        mergeAt.plusSeconds(1),
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "merged", Arrays.asList(left.splitId(), right.splitId()))));
        assertThatThrownBy(() -> enumerator.handleSourceEvent(0, conflictingDuplicate))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicting partition metadata");
        ChildPartitionsEvent newChildFromCompactedParent =
                new ChildPartitionsEvent(
                        left.splitId(),
                        mergeAt,
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "invented",
                                        Arrays.asList(left.splitId(), right.splitId()))));
        // A restored reader repeats the child reports of a finish the ledger applied already, so
        // a child of a compacted parent that the ledger no longer holds is one that finished.
        enumerator.handleSourceEvent(0, newChildFromCompactedParent);
        assertThat(enumerator.snapshotState(2).getPartitions())
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .doesNotContain("invented");
        // Skipping a finished child still checks the siblings reported beside it.
        ChildPartitionsEvent finishedThenConflicting =
                new ChildPartitionsEvent(
                        left.splitId(),
                        mergeAt.plusSeconds(1),
                        Arrays.asList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "invented", Arrays.asList(left.splitId(), right.splitId())),
                                new ChildPartitionsEvent.ChildPartition(
                                        "merged", Arrays.asList(left.splitId(), right.splitId()))));
        assertThatThrownBy(() -> enumerator.handleSourceEvent(0, finishedThenConflicting))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conflicting partition metadata");

        enumerator.handleSourceEvent(
                1,
                new ChildPartitionsEvent(
                        right.splitId(),
                        mergeAt,
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "merged",
                                        Arrays.asList(right.splitId(), left.splitId())))));
        enumerator.handleSourceEvent(
                1, new PartitionFinishedEvent(right.splitId(), mergeAt, mergeAt, false));
        // Parallelism 2: the parents' children wait for a checkpoint taken after both finishes.
        enumerator.snapshotState(3);
        enumerator.notifyCheckpointComplete(3);

        assertThat(enumerator.snapshotState(4).getPartitions())
                .filteredOn(p -> "merged".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.SCHEDULED);
        assertThat(enumerator.snapshotState(5).getPartitions()).hasSize(1);
        SpannerChangeStreamEnumeratorState compactMerge = enumerator.snapshotState(6);
        assertThat(compactMerge.getFinishedParentProofs()).isEmpty();
        assertThat(compactMerge.getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(mergeAt));
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isEqualTo(1L);
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS))
                .isZero();
        assertThat(enumerator.pendingParentDependencyCount()).isZero();
        enumerator.close();
    }

    @Test
    void sharedFinishedParentProofIsRetainedUntilEveryWaitingChildIsReleased() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        String sharedProof = "change-stream-token:shared-finished-parent";
        ChangeStreamPartitionSplit firstLiveParent =
                child("first-live-parent", start, "released-first-parent")
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit secondLiveParent =
                child("second-live-parent", start, "released-second-parent")
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit firstWaitingChild =
                new ChangeStreamPartitionSplit(
                        "first-waiting-child",
                        Arrays.asList(sharedProof, firstLiveParent.splitId()),
                        start.plusSeconds(1),
                        null,
                        2_000,
                        start.plusSeconds(1),
                        PartitionLifecycleState.CREATED,
                        start.plusSeconds(1));
        ChangeStreamPartitionSplit secondWaitingChild =
                new ChangeStreamPartitionSplit(
                        "second-waiting-child",
                        Arrays.asList(sharedProof, secondLiveParent.splitId()),
                        start.plusSeconds(1),
                        null,
                        2_000,
                        start.plusSeconds(1),
                        PartitionLifecycleState.CREATED,
                        start.plusSeconds(1));
        SpannerChangeStreamEnumeratorState restored =
                SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                        Arrays.asList(
                                firstLiveParent,
                                secondLiveParent,
                                firstWaitingChild,
                                secondWaitingChild),
                        Collections.singleton(sharedProof),
                        null,
                        Long.MIN_VALUE);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        firstLiveParent.splitId(),
                        start.plusSeconds(1),
                        start.plusSeconds(1),
                        false));
        // Parallelism 2: the child waits for a checkpoint taken after its parent's finish.
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);

        SpannerChangeStreamEnumeratorState oneChildStillWaiting = enumerator.snapshotState(2);
        assertThat(oneChildStillWaiting.getFinishedParentProofs()).containsExactly(sharedProof);
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();
        SpannerChangeStreamEnumeratorState roundTripped =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(oneChildStillWaiting));
        assertThat(roundTripped).isEqualTo(oneChildStillWaiting);
        assertThat(oneChildStillWaiting.getPartitions())
                .filteredOn(partition -> partition.splitId().equals(firstWaitingChild.splitId()))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.SCHEDULED);
        assertThat(oneChildStillWaiting.getPartitions())
                .filteredOn(partition -> partition.splitId().equals(secondWaitingChild.splitId()))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.CREATED);
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(2);
        SpannerChangeStreamSplitEnumerator restoredEnumerator =
                enumerator(restoredContext, new ScriptedClient(), roundTripped);
        restoredEnumerator.start();
        restoredContext.runAsyncCalls();

        restoredEnumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit returned =
                restoredContext.assignedSplits(0).stream()
                        .filter(
                                partition ->
                                        partition.splitId().equals(firstWaitingChild.splitId()))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        restoredEnumerator.addSplitsBack(Collections.singletonList(returned), 0);

        assertThat(restoredEnumerator.snapshotState(2).getFinishedParentProofs())
                .containsExactly(sharedProof);

        restoredEnumerator.handleSourceEvent(
                1,
                new PartitionFinishedEvent(
                        secondLiveParent.splitId(),
                        start.plusSeconds(1),
                        start.plusSeconds(1),
                        false));
        restoredEnumerator.snapshotState(3);
        restoredEnumerator.notifyCheckpointComplete(3);

        assertThat(restoredEnumerator.snapshotState(4).getFinishedParentProofs()).isEmpty();
        assertThat(restoredEnumerator.snapshotState(5).getPartitions())
                .filteredOn(
                        partition ->
                                partition.getLifecycleState() == PartitionLifecycleState.SCHEDULED)
                .hasSize(2);
        restoredEnumerator.close();
    }

    @Test
    void progressIsMonotonicAndSnapshotsDoNotAliasTheLiveLedger() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        Instant laterPosition = running.getCurrentPosition().plusSeconds(5);
        Instant laterWatermark = running.getWatermark().plusSeconds(7);
        enumerator.handleSourceEvent(
                0, committedProgress(running.splitId(), laterPosition, laterWatermark));
        SpannerChangeStreamEnumeratorState snapshot = enumerator.snapshotState(1);

        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        running.splitId(), running.getCurrentPosition(), running.getWatermark()));
        SpannerChangeStreamEnumeratorState afterStaleProgress = enumerator.snapshotState(2);
        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(), laterPosition.plusSeconds(1), laterWatermark, false));

        assertThat(snapshot.getPartitions().get(0).getCurrentPosition()).isEqualTo(laterPosition);
        assertThat(snapshot.getPartitions().get(0).getWatermark()).isEqualTo(laterWatermark);
        assertThat(snapshot.getPartitions().get(0).getLifecycleState())
                .isEqualTo(PartitionLifecycleState.RUNNING);
        assertThat(afterStaleProgress.getPartitions().get(0).getCurrentPosition())
                .isEqualTo(laterPosition);
        assertThat(afterStaleProgress.getPartitions().get(0).getWatermark())
                .isEqualTo(laterWatermark);
        enumerator.close();
    }

    @Test
    void splitReturnedDuringRestoreIsReplayedWithMonotonicProgress() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit running =
                child("running", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, running));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);
        Instant returnedPosition = recent.plusSeconds(10);
        Instant returnedWatermark = recent.plusSeconds(12);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.addSplitsBack(
                Collections.singletonList(
                        running.withProgress(returnedPosition, returnedWatermark)),
                0);
        context.runAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        assigned -> {
                            assertThat(assigned.getLifecycleState())
                                    .isEqualTo(PartitionLifecycleState.RUNNING);
                            assertThat(assigned.getCurrentPosition()).isEqualTo(returnedPosition);
                            assertThat(assigned.getWatermark()).isEqualTo(returnedWatermark);
                        });
        enumerator.close();
    }

    @Test
    void expiredRestoreFailsWithoutFallback() {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit initial = finishedInitial(expired.minusSeconds(1));
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(
                                initial,
                                child("expired", expired, initial.splitId())
                                        .withLifecycleState(PartitionLifecycleState.SCHEDULED)));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);
        enumerator.start();

        Throwable failure = catchThrowable(context::runAsyncCalls);
        assertThat(failure)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to initialize");
        assertThat(failure)
                .rootCause()
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("change-stream-token:expired")
                .hasMessageContaining("older than the computed earliest position");
        assertThat(failure)
                .hasStackTraceContaining("No restore fallback was configured")
                .hasStackTraceContaining("SpannerChangeStreamSourceBuilder.resumeFallback(...)");
    }

    @Test
    void expiredRestoreWithFallbackReplacesTheWholeLedgerWithOneInitialPartition()
            throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit initial = finishedInitial(expired.minusSeconds(1));
        ChangeStreamPartitionSplit unfinished =
                child("expired", expired, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit finished =
                child("finished", expired, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, unfinished, finished));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        Instant fallback = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        clientMetrics -> new ScriptedClient(),
                        StartPosition.latest(),
                        StartPosition.at(fallback),
                        null,
                        2_000,
                        true,
                        Duration.ofMinutes(10),
                        restored,
                        null);

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(enumerator.snapshotState(1).getPartitions())
                    .singleElement()
                    .satisfies(
                            partition -> {
                                assertThat(partition.splitId())
                                        .isEqualTo(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
                                assertThat(partition.getPartitionToken()).isNull();
                                assertThat(partition.getStartTimestamp()).isEqualTo(fallback);
                                assertThat(partition.getLifecycleState())
                                        .isEqualTo(PartitionLifecycleState.SCHEDULED);
                            });
            assertThat(context.sourceEvents(0))
                    .singleElement()
                    .isInstanceOfSatisfying(
                            SpannerChangeStreamInitializationEvent.class,
                            event -> {
                                assertThat(event.shouldDiscardRestoredSplits()).isTrue();
                                assertThat(event.getSourceWatermark())
                                        .isEqualTo(fallback.toEpochMilli() - 1);
                            });
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            warning -> {
                                assertThat(warning).contains(expired.toString());
                                assertThat(warning).contains(fallback.toString());
                                assertThat(warning).contains("computed earliest");
                                assertThat(warning).contains("unavailable range");
                                assertThat(warning).contains("delivered again");
                            });
        }
        enumerator.close();
    }

    @Test
    void aCloseBeforeInitializationLeavesNoClientToCreate() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        AtomicInteger creations = new AtomicInteger();
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> {
                            creations.incrementAndGet();
                            return new ScriptedClient();
                        },
                        null);

        enumerator.start();
        enumerator.close();
        context.runAsyncCalls();

        assertThat(creations).hasValue(0);
        assertThat(context.assignedSplits(0)).isEmpty();
    }

    @Test
    void aCloseWhileTheClientIsBeingCreatedReturnsOnlyOnceTheLateClientIsClosed() throws Exception {
        // Before #2305 the close found no client and returned at once, and the client created
        // afterwards was closed on the coordinator's worker after the enumerator's close had
        // returned, when Flink may already have released the job's class loader (docs/adr/0208).
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch creationGate = new CountDownLatch(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> {
                            creationEntered.countDown();
                            creationGate.await();
                            return client;
                        },
                        null);
        enumerator.start();
        AtomicReference<Throwable> initializationFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicInteger clientClosesWhenCloseReturned = new AtomicInteger(-1);
        Thread initializing =
                new Thread(
                        () -> initializationFailure.set(catchThrowable(context::runAsyncCalls)),
                        "init");
        Thread closing =
                new Thread(
                        () -> {
                            closeFailure.set(catchThrowable(enumerator::close));
                            clientClosesWhenCloseReturned.set(client.closeCalls);
                        },
                        "close");
        try {
            initializing.start();
            assertThat(creationEntered.await(10, TimeUnit.SECONDS))
                    .as("initialization began creating the client")
                    .isTrue();

            closing.start();
            assertThat(awaitParkedOrEnded(closing))
                    .as("the enumerator's close waits for the client being created")
                    .isEqualTo(Thread.State.WAITING);
        } finally {
            creationGate.countDown();
            initializing.join(10_000);
            closing.join(10_000);
        }
        assertThat(initializing.isAlive()).as("initialization ended").isFalse();
        assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
        assertThat(initializationFailure.get()).isNull();
        assertThat(closeFailure.get()).isNull();
        assertThat(clientClosesWhenCloseReturned)
                .as("client closes when the enumerator's close returned")
                .hasValue(1);
        assertThat(client.initializeCalls).isZero();
        assertThat(context.assignedSplits(0)).isEmpty();
    }

    @Test
    void aCloseWhileTheClientsCreationFailsReturnsOnceTheCreationHasFailed() throws Exception {
        // The close does not cancel the creation, so its failure is the stream's, not the close's:
        // before #2358 the closed enumerator dropped it without a trace.
        java.io.IOException creationFailure = new java.io.IOException("no emulator is listening");
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch creationGate = new CountDownLatch(1);
        AtomicBoolean creationFailed = new AtomicBoolean();
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> {
                            creationEntered.countDown();
                            creationGate.await();
                            creationFailed.set(true);
                            throw creationFailure;
                        },
                        null);
        enumerator.start();
        AtomicReference<Throwable> initializationFailure = new AtomicReference<>();
        Thread initializing =
                new Thread(
                        () -> initializationFailure.set(catchThrowable(context::runAsyncCalls)),
                        "init");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicReference<Boolean> failedWhenCloseReturned = new AtomicReference<>();
        Thread closing =
                new Thread(
                        () -> {
                            closeFailure.set(catchThrowable(enumerator::close));
                            failedWhenCloseReturned.set(creationFailed.get());
                        },
                        "close");
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            try {
                initializing.start();
                assertThat(creationEntered.await(10, TimeUnit.SECONDS)).isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for the client being created")
                        .isEqualTo(Thread.State.WAITING);
            } finally {
                creationGate.countDown();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
            assertThat(capture.getEvents())
                    .as("the creation failure is logged")
                    .singleElement()
                    .satisfies(SpannerChangeStreamSplitEnumeratorTest::isAWarning)
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(creationFailure));
        }
        assertThat(initializationFailure.get())
                .as("the closed enumerator's handler does not fail")
                .isNull();
        assertThat(closeFailure.get()).isNull();
        assertThat(failedWhenCloseReturned.get())
                .as("the creation had failed when the enumerator's close returned")
                .isTrue();
    }

    @Test
    void aSecondCloseWaitsForALateClientsCloseInterruptiblyRatherThanOnTheMonitor()
            throws Exception {
        // Before #2305 a client created after the enumerator closed was closed under the
        // enumerator's monitor, so a further close() was blocked on the monitor, which no
        // interrupt ends, for as long as the client's shutdown took. Flink interrupts a closing
        // coordinator that overruns its close timeout.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch creationGate = new CountDownLatch(1);
        CountDownLatch clientCloseEntered = new CountDownLatch(1);
        CountDownLatch clientCloseGate = new CountDownLatch(1);
        client.duringClose =
                () -> {
                    clientCloseEntered.countDown();
                    clientCloseGate.await();
                };
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> {
                            creationEntered.countDown();
                            creationGate.await();
                            return client;
                        },
                        null);
        enumerator.start();
        AtomicReference<Throwable> secondCloseFailure = new AtomicReference<>();
        AtomicReference<Boolean> secondCloseInterrupted = new AtomicReference<>();
        AtomicInteger clientClosesWhenSecondCloseReturned = new AtomicInteger(-1);
        Thread initializing = new Thread(context::runAsyncCalls, "init");
        Thread firstClose = new Thread(() -> catchThrowable(enumerator::close), "close-1");
        Thread secondClose =
                new Thread(
                        () -> {
                            secondCloseFailure.set(catchThrowable(enumerator::close));
                            secondCloseInterrupted.set(Thread.currentThread().isInterrupted());
                            clientClosesWhenSecondCloseReturned.set(client.closeCalls);
                        },
                        "close-2");
        try {
            initializing.start();
            assertThat(creationEntered.await(10, TimeUnit.SECONDS)).isTrue();
            firstClose.start();
            // The first close has marked the enumerator closed once it parks, or, before #2305,
            // once it has returned; parked anywhere else, the client could be installed instead.
            assertThat(awaitParkedOrEnded(firstClose))
                    .isIn(Thread.State.WAITING, Thread.State.TERMINATED);
            creationGate.countDown();
            assertThat(clientCloseEntered.await(10, TimeUnit.SECONDS))
                    .as("the late client's close began")
                    .isTrue();

            secondClose.start();
            assertThat(awaitParkedOrEnded(secondClose))
                    .as("where the second close waits for the late client's close")
                    .isEqualTo(Thread.State.WAITING);
            secondClose.interrupt();
            secondClose.join(10_000);
            assertThat(secondClose.isAlive()).as("the interrupt ended the second close").isFalse();
        } finally {
            creationGate.countDown();
            clientCloseGate.countDown();
            initializing.join(10_000);
            firstClose.join(10_000);
            secondClose.join(10_000);
        }
        assertThat(secondCloseFailure.get()).isNull();
        assertThat(secondCloseInterrupted.get()).as("the interrupt is restored").isTrue();
        assertThat(clientClosesWhenSecondCloseReturned)
                .as("the interrupt ended the wait before the client's close finished")
                .hasValue(0);
        assertThat(firstClose.isAlive()).as("the first close ended").isFalse();
        assertThat(initializing.isAlive()).as("initialization ended").isFalse();
        assertThat(client.initializeCalls).as("the client was never installed").isZero();
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aLatestStartIsResolvedBeforeTheClientCloses() throws Exception {
        // The client closes after initialization, and its close can take seconds; a "latest" start
        // resolved after it would skip what was committed meanwhile.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        AtomicReference<Instant> closeStarted = new AtomicReference<>();
        client.duringClose =
                () -> {
                    closeStarted.set(Instant.now());
                    Thread.sleep(200);
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split ->
                                assertThat(split.getStartTimestamp())
                                        .as(
                                                "the latest start, against when the client's close began")
                                        .isBeforeOrEqualTo(closeStarted.get()));
        enumerator.close();
    }

    @Test
    void theEnumeratorsCloseWaitsForTheClientInitializationIsClosing() throws Exception {
        // The client's close releases its gRPC lease; the enumerator's close must not return
        // before it, or Flink can release the job's class loader under it.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch closeGate = new CountDownLatch(1);
        client.duringClose =
                () -> {
                    closeEntered.countDown();
                    closeGate.await();
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        AtomicReference<Throwable> initializationFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread initializing =
                new Thread(
                        () -> initializationFailure.set(catchThrowable(context::runAsyncCalls)),
                        "init");
        Thread closing =
                new Thread(() -> closeFailure.set(catchThrowable(enumerator::close)), "close");
        try {
            initializing.start();
            assertThat(closeEntered.await(10, TimeUnit.SECONDS))
                    .as(
                            "initialization reached the client's close; failure: %s",
                            initializationFailure)
                    .isTrue();

            closing.start();
            // The only place the closing thread can park while the gate is shut is the wait for
            // the client's release.
            assertThat(awaitParkedOrEnded(closing))
                    .as("the enumerator's close waits for the client's; failure: %s", closeFailure)
                    .isEqualTo(Thread.State.WAITING);
        } finally {
            closeGate.countDown();
            initializing.join(10_000);
            closing.join(10_000);
        }
        assertThat(initializing.isAlive()).as("initialization ended").isFalse();
        assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();
        assertThat(initializationFailure.get()).isNull();
        assertThat(closeFailure.get()).isNull();
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aCloseDuringInitializationClosesTheClientOnlyOnce() {
        // The enumerator's close takes the client while initialization is in flight, and the
        // release after initialization must then leave it alone: the client's contract does not
        // promise that a second close is harmless.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        client.duringInitialize =
                () -> {
                    try {
                        enumerator.close();
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                };

        enumerator.start();
        catchThrowable(context::runAsyncCalls);

        assertThat(client.initializeCalls).isOne();
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aSecondCloseWaitsForTheFirstClosesCloseOfTheInstalledClient() throws Exception {
        // Before this change the first close took the installed client over and closed it, and a
        // second close found no client and no release in progress and returned at once.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch initializeEntered = new CountDownLatch(1);
        CountDownLatch initializeGate = new CountDownLatch(1);
        client.duringInitialize =
                () -> {
                    initializeEntered.countDown();
                    try {
                        initializeGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };
        CountDownLatch clientCloseEntered = new CountDownLatch(1);
        CountDownLatch clientCloseGate = new CountDownLatch(1);
        client.duringClose =
                () -> {
                    clientCloseEntered.countDown();
                    clientCloseGate.await();
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        AtomicReference<Throwable> secondCloseFailure = new AtomicReference<>();
        AtomicInteger clientClosesWhenSecondCloseReturned = new AtomicInteger(-1);
        Thread initializing = new Thread(() -> catchThrowable(context::runAsyncCalls), "init");
        Thread firstClose = new Thread(() -> catchThrowable(enumerator::close), "close-1");
        Thread secondClose =
                new Thread(
                        () -> {
                            secondCloseFailure.set(catchThrowable(enumerator::close));
                            clientClosesWhenSecondCloseReturned.set(client.closeCalls);
                        },
                        "close-2");
        try {
            initializing.start();
            assertThat(initializeEntered.await(10, TimeUnit.SECONDS))
                    .as("the client was installed and initializing")
                    .isTrue();
            firstClose.start();
            assertThat(clientCloseEntered.await(10, TimeUnit.SECONDS))
                    .as("the first close took the installed client over and is closing it")
                    .isTrue();

            secondClose.start();
            assertThat(awaitParkedOrEnded(secondClose))
                    .as("the second close waits for the first close's close of the client")
                    .isEqualTo(Thread.State.WAITING);
        } finally {
            clientCloseGate.countDown();
            initializeGate.countDown();
            initializing.join(10_000);
            firstClose.join(10_000);
            secondClose.join(10_000);
        }
        assertThat(secondClose.isAlive()).as("the second close ended").isFalse();
        assertThat(firstClose.isAlive()).as("the first close ended").isFalse();
        assertThat(initializing.isAlive()).as("initialization ended").isFalse();
        assertThat(secondCloseFailure.get()).isNull();
        assertThat(clientClosesWhenSecondCloseReturned)
                .as("client closes when the second close returned")
                .hasValue(1);
        assertThat(client.closeCalls).isOne();
    }

    @Test
    void aSecondCloseEndsWhenTheFirstClosesCloseOfTheInstalledClientFails() throws Exception {
        // The first close throws its client's close failure; the second must not wait on.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch initializeEntered = new CountDownLatch(1);
        CountDownLatch initializeGate = new CountDownLatch(1);
        client.duringInitialize =
                () -> {
                    initializeEntered.countDown();
                    try {
                        initializeGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };
        Exception closeFailure = new IllegalStateException("close failed");
        CountDownLatch clientCloseEntered = new CountDownLatch(1);
        CountDownLatch clientCloseGate = new CountDownLatch(1);
        client.duringClose =
                () -> {
                    clientCloseEntered.countDown();
                    clientCloseGate.await();
                    throw closeFailure;
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        AtomicReference<Throwable> firstCloseFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondCloseFailure = new AtomicReference<>();
        Thread initializing = new Thread(() -> catchThrowable(context::runAsyncCalls), "init");
        Thread firstClose =
                new Thread(() -> firstCloseFailure.set(catchThrowable(enumerator::close)), "c-1");
        Thread secondClose =
                new Thread(() -> secondCloseFailure.set(catchThrowable(enumerator::close)), "c-2");
        try {
            initializing.start();
            assertThat(initializeEntered.await(10, TimeUnit.SECONDS))
                    .as("the client was installed and initializing")
                    .isTrue();
            firstClose.start();
            assertThat(clientCloseEntered.await(10, TimeUnit.SECONDS))
                    .as("the first close took the installed client over and is closing it")
                    .isTrue();
            secondClose.start();
            assertThat(awaitParkedOrEnded(secondClose))
                    .as("the second close waits for the first close's close of the client")
                    .isEqualTo(Thread.State.WAITING);
            clientCloseGate.countDown();
            secondClose.join(10_000);
            assertThat(secondClose.isAlive())
                    .as("the second close ended once the client's close failed")
                    .isFalse();
        } finally {
            clientCloseGate.countDown();
            initializeGate.countDown();
            initializing.join(10_000);
            firstClose.join(10_000);
            secondClose.join(10_000);
        }
        assertThat(firstCloseFailure.get())
                .as("the first close reports its client's close failure")
                .isInstanceOf(java.io.IOException.class)
                .hasCause(closeFailure);
        assertThat(secondCloseFailure.get()).isNull();
    }

    @ParameterizedTest
    @MethodSource("closeFailures")
    void aLateClientsCloseFailureIsLogged(Throwable closeFailure) throws Exception {
        // Before this change the failure was thrown into initialization's result, which the closed
        // enumerator drops; an Error, such as a NoClassDefFoundError once Flink has released the
        // job's class loader, is logged too.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        client.duringClose = () -> throwAs(closeFailure);
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch creationGate = new CountDownLatch(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        clientMetrics -> {
                            creationEntered.countDown();
                            creationGate.await();
                            return client;
                        },
                        null);
        enumerator.start();
        Thread initializing = new Thread(() -> catchThrowable(context::runAsyncCalls), "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        Thread closing = new Thread(() -> closeResult.set(catchThrowable(enumerator::close)), "c");
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            try {
                initializing.start();
                assertThat(creationEntered.await(10, TimeUnit.SECONDS))
                        .as("initialization began creating the client")
                        .isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for the client being created")
                        .isEqualTo(Thread.State.WAITING);
            } finally {
                creationGate.countDown();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();

            assertThat(capture.getEvents())
                    .as("the late client's close failure is logged")
                    .singleElement()
                    .satisfies(SpannerChangeStreamSplitEnumeratorTest::isAWarning)
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(closeFailure));
        }
        assertThat(closeResult.get()).isNull();
    }

    @ParameterizedTest
    @MethodSource("closeFailures")
    void aCloseFailureOfTheClientInitializationIsClosingIsLoggedOnceTheEnumeratorClosed(
            Throwable closeFailure) throws Exception {
        // Before this change the failure became initialization's failure, which the closed
        // enumerator drops.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        CountDownLatch clientCloseEntered = new CountDownLatch(1);
        CountDownLatch clientCloseGate = new CountDownLatch(1);
        client.duringClose =
                () -> {
                    clientCloseEntered.countDown();
                    clientCloseGate.await();
                    throwAs(closeFailure);
                };
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        AtomicReference<Throwable> initializationFailure = new AtomicReference<>();
        Thread initializing =
                new Thread(
                        () -> initializationFailure.set(catchThrowable(context::runAsyncCalls)),
                        "init");
        AtomicReference<Throwable> closeResult = new AtomicReference<>();
        Thread closing = new Thread(() -> closeResult.set(catchThrowable(enumerator::close)), "c");
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            try {
                initializing.start();
                assertThat(clientCloseEntered.await(10, TimeUnit.SECONDS))
                        .as("initialization began closing the client")
                        .isTrue();
                closing.start();
                assertThat(awaitParkedOrEnded(closing))
                        .as("the enumerator's close waits for the client initialization is closing")
                        .isEqualTo(Thread.State.WAITING);
            } finally {
                clientCloseGate.countDown();
                initializing.join(10_000);
                closing.join(10_000);
            }
            assertThat(initializing.isAlive()).as("initialization ended").isFalse();
            assertThat(closing.isAlive()).as("the enumerator's close ended").isFalse();

            assertThat(capture.getEvents())
                    .as("the close failure is logged")
                    .singleElement()
                    .satisfies(SpannerChangeStreamSplitEnumeratorTest::isAWarning)
                    .satisfies(event -> assertThat(event.getThrowable()).isSameAs(closeFailure));
        }
        assertThat(initializationFailure.get())
                .as("the closed enumerator's initialization does not fail")
                .isNull();
        assertThat(closeResult.get()).isNull();
    }

    @Test
    void mutablePartitionModeFailsBeforeAnySplitCanBeAssigned() {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        client.initializationFailure =
                new IllegalArgumentException("MUTABLE_KEY_RANGE is unsupported");
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");

        Throwable failure = catchThrowable(context::runAsyncCalls);

        assertThat(failure)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to initialize");
        assertThat(failure).rootCause().hasMessageContaining("MUTABLE_KEY_RANGE");
        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(client.closeCalls).as("the client closes when initialization fails").isOne();
    }

    @Test
    void childAndCompletionEventsThatArriveDuringRestoreAreReplayed() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit parent =
                child("parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, parent));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);
        Instant childStart = recent.plusSeconds(10);

        enumerator.start();
        enumerator.handleSourceEvent(0, children(parent, childStart, "child"));
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(parent.splitId(), childStart, childStart, false));
        context.runAsyncCalls();

        assertThat(enumerator.snapshotState(1).getPartitions())
                .filteredOn(p -> "child".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.SCHEDULED);
        enumerator.close();
    }

    @Test
    void childNamingAnUnknownMergeParentCannotPoisonTheCheckpoint() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        ChangeStreamPartitionSplit initial = enumerator.snapshotState(1).getPartitions().get(0);
        Instant start = initial.getStartTimestamp().plusSeconds(1);
        enumerator.handleSplitRequest(0, "localhost");

        ChildPartitionsEvent event =
                new ChildPartitionsEvent(
                        initial.splitId(),
                        start,
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "merged",
                                        Arrays.asList(initial.splitId(), "unknown-parent"))));

        assertThatThrownBy(() -> enumerator.handleSourceEvent(0, event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merged")
                .hasMessageContaining("unknown-parent");
        assertThat(enumerator.snapshotState(2).getPartitions())
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::splitId)
                .isEqualTo(initial.splitId());
        enumerator.close();
    }

    @Test
    void restoredMergedChildIsReleasedAfterItsRemainingParentFinishes() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        ChangeStreamPartitionSplit finishedParent =
                child("finished-parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ChangeStreamPartitionSplit runningParent =
                child("running-parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit merged =
                new ChangeStreamPartitionSplit(
                        "merged",
                        Arrays.asList(finishedParent.splitId(), runningParent.splitId()),
                        recent.plusSeconds(10),
                        null,
                        2_000,
                        recent.plusSeconds(10),
                        PartitionLifecycleState.CREATED,
                        recent.plusSeconds(10));
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, finishedParent, runningParent, merged));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();
        assertThat(enumerator.pendingParentDependencyCount()).isEqualTo(1);
        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        runningParent.splitId(),
                        runningParent.getCurrentPosition(),
                        runningParent.getWatermark(),
                        false));

        assertThat(context.assignedSplits(0))
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .isEqualTo("merged");
        assertThat(enumerator.pendingParentDependencyCount()).isZero();
        enumerator.close();
    }

    @Test
    void longRunningOneToOneLineageKeepsOnlyTheActivePartition() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);

        for (int transition = 0; transition < 1_000; transition++) {
            Instant childStart = running.getStartTimestamp().plusSeconds(1);
            enumerator.handleSourceEvent(0, children(running, childStart, "chain-" + transition));
            enumerator.handleSourceEvent(
                    0,
                    new PartitionFinishedEvent(running.splitId(), childStart, childStart, false));
            enumerator.handleSplitRequest(0, "localhost");
            running = context.assignedSplits(0).get(transition + 1);
        }

        SpannerChangeStreamEnumeratorState compact = enumerator.snapshotState(1);
        assertThat(compact.getPartitions())
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::splitId)
                .isEqualTo(running.splitId());
        assertThat(compact.getFinishedParentProofs()).isEmpty();
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isEqualTo(1L);
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS))
                .isZero();
        assertThat(context.counter(SpannerMetricNames.CHANGE_STREAM_PARTITIONS_DISCOVERED))
                .isEqualTo(1_000L);
        enumerator.close();
    }

    @Test
    void boundedLedgerSignalsNoMoreSplitsAfterTheFinalPartitionFinishes() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        Instant bound = Instant.now().plus(Duration.ofHours(1));
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.latest(), null, bound, null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.events()).containsExactly("assign:0");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);

        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(),
                        running.getCurrentPosition(),
                        running.getWatermark(),
                        false));
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:0");
        SpannerChangeStreamEnumeratorState finished = enumerator.snapshotState(1);
        assertThat(finished.getPartitions()).isEmpty();
        assertThat(finished.getFinishedParentProofs()).isEmpty();
        assertThat(finished.isBounded()).isTrue();
        assertThat(finished.getBoundedTimestamp()).isEqualTo(bound);
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isZero();
        assertThat(context.<Long>gauge(SpannerMetricNames.CHANGE_STREAM_FINISHED_PARENT_PROOFS))
                .isZero();
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(1);
        SpannerChangeStreamSplitEnumerator restoredEnumerator =
                configured(restoredContext, StartPosition.latest(), null, bound, finished);
        restoredEnumerator.start();
        restoredEnumerator.handleSplitRequest(0, "localhost");
        restoredContext.runAsyncCalls();

        assertThat(restoredContext.events()).containsExactly("noMoreSplits:0");
        assertThat(restoredEnumerator.snapshotState(2).isBounded()).isTrue();
        restoredEnumerator.close();
    }

    @Test
    void aBoundedRestoreWithNothingToReadEndsOnlyAfterTheClientClosed() throws Exception {
        // The job can finish, and Flink release its class loader, as soon as the end is signalled;
        // the coordinator's client must be closed by then.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        Instant bound = Instant.now().plus(Duration.ofHours(1));
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.latest(), null, bound, null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(),
                        running.getCurrentPosition(),
                        running.getWatermark(),
                        false));
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamEnumeratorState finished = enumerator.snapshotState(1);
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(1);
        ScriptedClient client = new ScriptedClient();
        List<String> eventsAtClose = new java.util.ArrayList<>();
        client.duringClose = () -> eventsAtClose.addAll(restoredContext.events());
        SpannerChangeStreamSplitEnumerator restored =
                new SpannerChangeStreamSplitEnumerator(
                        restoredContext,
                        clientMetrics -> client,
                        StartPosition.latest(),
                        null,
                        bound,
                        2_000,
                        true,
                        Duration.ofMinutes(10),
                        finished,
                        null);
        restored.start();
        restored.handleSplitRequest(0, "localhost");
        restoredContext.runAsyncCalls();

        assertThat(eventsAtClose).as("events when the client's close began").isEmpty();
        assertThat(restoredContext.events()).containsExactly("noMoreSplits:0");
        assertThat(client.closeCalls).isOne();
        restored.close();
    }

    @Test
    void freshStartResolvedPastTheBoundFailsTheSource() {
        Instant bound = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.latest(), null, bound, null);
        enumerator.start();

        assertThat(catchThrowable(context::runAsyncCalls))
                .hasMessageContaining("Failed to initialize")
                .rootCause()
                .hasMessageContaining("start position StartPosition{latest} resolved to")
                .hasMessageContaining("after the bounded timestamp " + bound);
    }

    @Test
    void freshStartAtTheBoundReadsTheBoundsInstant() throws Exception {
        Instant bound = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.at(bound), null, bound, null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getStartTimestamp()).isEqualTo(bound);
                            assertThat(split.getEndTimestamp()).isEqualTo(bound);
                        });
        enumerator.close();
    }

    @Test
    void expiredLedgerReplacedByTheFallbackStartsUnderTheConfiguredBound() throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        Instant configured = Instant.now().plus(Duration.ofHours(1));
        ChangeStreamPartitionSplit checkpointed =
                ChangeStreamPartitionSplit.initial(expired, expired.plusSeconds(60), 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        StartPosition.ago(Duration.ofHours(1)),
                        configured,
                        new SpannerChangeStreamEnumeratorState(
                                Collections.singletonList(checkpointed)));
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(split -> assertThat(split.getEndTimestamp()).isEqualTo(configured));
        enumerator.close();
    }

    @Test
    void resumeFallbackResolvedPastTheBoundFailsTheSource() {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        Instant bound = Instant.now().minus(Duration.ofHours(2));
        Instant fallback = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(expired, bound, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        StartPosition.at(fallback),
                        bound,
                        new SpannerChangeStreamEnumeratorState(Collections.singletonList(initial)));
        enumerator.start();

        assertThat(catchThrowable(context::runAsyncCalls))
                .rootCause()
                .hasMessageContaining("resume fallback")
                .hasMessageContaining("resolved to " + fallback)
                .hasMessageContaining("after the bounded timestamp " + bound);
    }

    @Test
    void restoreUnderADifferentBoundFailsTheSource() {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        Instant checkpointed = Instant.now().plus(Duration.ofHours(1));
        Instant configured = checkpointed.plus(Duration.ofHours(1));
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Collections.singletonList(
                                ChangeStreamPartitionSplit.initial(start, checkpointed, 2_000)
                                        .withLifecycleState(PartitionLifecycleState.RUNNING)));

        assertThat(restoreFailure(configured, restored))
                .hasMessageContaining("is bounded at " + checkpointed)
                .hasMessageContaining("configured with the bounded timestamp " + configured)
                .hasMessageContaining("SpannerChangeStreamSourceBuilder.boundedTimestamp(...)")
                .hasMessageContaining("scan.bounded.timestamp-millis");
    }

    @Test
    void restoreThatAddsOrDropsTheBoundFailsTheSource() {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamEnumeratorState unbounded =
                new SpannerChangeStreamEnumeratorState(
                        Collections.singletonList(
                                ChangeStreamPartitionSplit.initial(start, null, 2_000)
                                        .withLifecycleState(PartitionLifecycleState.RUNNING)));
        Instant bound = Instant.now().plus(Duration.ofHours(1));
        assertThat(restoreFailure(bound, unbounded))
                .hasMessageContaining("is unbounded, but the source is configured with");

        SpannerChangeStreamEnumeratorState finishedBounded =
                SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                        Collections.emptyList(), Collections.emptySet(), bound, Long.MIN_VALUE);
        assertThat(restoreFailure(null, finishedBounded))
                .hasMessageContaining(
                        "is bounded at " + bound + ", but the source is configured unbounded");
    }

    /**
     * Every partition of a completed ledger has finished, so only the checkpointed bound can tell a
     * restore under a later one that the interval between them was never read.
     */
    @Test
    void completedLedgerRestoredUnderALaterBoundFailsTheSource() {
        Instant checkpointed = Instant.now().minus(Duration.ofHours(1));
        Instant configured = checkpointed.plus(Duration.ofMinutes(30));
        SpannerChangeStreamEnumeratorState completed =
                SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                        Collections.emptyList(),
                        Collections.emptySet(),
                        checkpointed,
                        Long.MIN_VALUE);

        assertThat(restoreFailure(configured, completed))
                .hasMessageContaining("is bounded at " + checkpointed)
                .hasMessageContaining("configured with the bounded timestamp " + configured);
    }

    /** Version 3 recorded only that a completed ledger was bounded, so any bound restores it. */
    @Test
    void completedVersionThreeLedgerRestoresUnderAnyBound() throws Exception {
        DataOutputSerializer versionThree = new DataOutputSerializer(32);
        versionThree.writeBoolean(true);
        versionThree.writeInt(0);
        versionThree.writeInt(0);
        versionThree.writeLong(Long.MIN_VALUE);
        SpannerChangeStreamEnumeratorState completed =
                new SpannerChangeStreamEnumeratorStateSerializer()
                        .deserialize(3, versionThree.getCopyOfBuffer());
        Instant configured = Instant.now().plus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.latest(), null, configured, completed);
        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(context.events()).containsExactly("noMoreSplits:0");
        assertThat(enumerator.snapshotState(1).getBoundedTimestamp()).isEqualTo(configured);
        enumerator.close();
    }

    private static Throwable restoreFailure(
            @Nullable Instant configuredBound, SpannerChangeStreamEnumeratorState restored) {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(context, StartPosition.latest(), null, configuredBound, restored);
        enumerator.start();
        Throwable failure = catchThrowable(context::runAsyncCalls);
        assertThat(failure).hasMessageContaining("Failed to initialize");
        assertThat(context.assignedSplits(0)).isEmpty();
        return failure.getCause();
    }

    @Test
    void aChildStartsOnlyAfterACheckpointTakenAfterItsParentFinishedCompletes() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();

        // Snapshotted before the finish, so the parent's last records may follow their barriers.
        enumerator.snapshotState(1);
        enumerator.snapshotState(2);
        enumerator.handleSourceEvent(0, finished("p1", start));
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(enumerator.heldParentCount()).isEqualTo(1);

        // Completing a checkpoint taken before the finish releases nothing, whether or not a later
        // one covers the hold yet.
        enumerator.notifyCheckpointComplete(1);
        assertThat(context.assignedSplits(1)).as("before any covering snapshot").isEmpty();
        enumerator.snapshotState(3);
        enumerator.notifyCheckpointComplete(2);
        assertThat(context.assignedSplits(1)).as("covered by a later snapshot").isEmpty();
        assertThat(enumerator.heldParentCount()).isEqualTo(1);

        enumerator.notifyCheckpointComplete(3);
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        assertThat(enumerator.heldParentCount()).isZero();
        enumerator.close();
    }

    @Test
    void aCompletedCheckpointReleasesTheHoldsAnEarlierUncompletedOneCovered() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(0, finished("p1", start));
        enumerator.snapshotState(1); // aborted: never completes
        enumerator.snapshotState(2);
        enumerator.handleSplitRequest(1, "localhost");
        enumerator.notifyCheckpointComplete(2);

        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        enumerator.close();
    }

    @Test
    void nothingIsHeldAtParallelismOneOrWithAnImmediateHandOff() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> single = context(1);
        SpannerChangeStreamSplitEnumerator atParallelismOne =
                handOff(
                        single,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> two = context(2);
        SpannerChangeStreamSplitEnumerator immediate =
                handOff(
                        two,
                        false,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());

        for (SpannerChangeStreamSplitEnumerator enumerator :
                Arrays.asList(atParallelismOne, immediate)) {
            String variant = enumerator == immediate ? "IMMEDIATE" : "parallelism 1";
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                    enumerator == immediate ? two : single;
            enumerator.start();
            context.runAsyncCalls();
            enumerator.handleSourceEvent(0, finished("p1", start));
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(enumerator.heldParentCount()).as(variant).isZero();
            assertThat(context.assignedSplits(0))
                    .as(variant)
                    .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                    .containsExactly("p1-child");
            enumerator.close();
        }
    }

    @Test
    void onlyAParentThatEmittedRecordsHoldsItsChildren() throws Exception {
        // A quiet parent has nothing downstream to overtake, whatever partition it is.
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> quiet = context(2);
        SpannerChangeStreamSplitEnumerator quietParent =
                handOff(
                        quiet,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        quietParent.start();
        quiet.runAsyncCalls();
        Instant end = start.plusSeconds(1);
        quietParent.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        ChangeStreamPartitionSplit.idForToken("p1"), end, end, false));
        quietParent.handleSplitRequest(1, "localhost");
        assertThat(quietParent.heldParentCount()).isZero();
        assertThat(quiet.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        quietParent.close();

        // The initial query gets no exemption of its own: had it emitted data, it would hold.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(context, true, Duration.ofMinutes(10), null, new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant childStart = initial.getStartTimestamp().plusSeconds(1);
        enumerator.handleSourceEvent(0, children(initial, childStart, "first"));
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(initial.splitId(), childStart, childStart, true));
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(enumerator.heldParentCount()).isEqualTo(1);
        assertThat(context.assignedSplits(1)).isEmpty();

        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("first");
        enumerator.close();
    }

    @Test
    void aRestoredCheckpointReleasesTheFinishesItRecorded() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, finished("p1", start));
        assertThat(enumerator.heldParentCount()).isEqualTo(1);
        SpannerChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(2);
        SpannerChangeStreamSplitEnumerator restored =
                handOff(
                        restoredContext,
                        true,
                        Duration.ofMinutes(10),
                        checkpoint,
                        new AtomicLong());
        restored.start();
        restoredContext.runAsyncCalls();
        restored.handleSplitRequest(1, "localhost");

        // Only a completed checkpoint is restored, so the finish it records is downstream already.
        assertThat(restoredContext.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        restored.close();
    }

    @Test
    void withoutACompletedCheckpointTheWaitEndsWithOneWarningUntilOneCompletes() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        AtomicLong clock = new AtomicLong(1_000);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1", "p2", "p3", "p4"),
                        clock);
        enumerator.start();
        context.runAsyncCalls();

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            enumerator.handleSourceEvent(0, finished("p1", start));
            enumerator.handleSplitRequest(1, "localhost");
            clock.addAndGet(Duration.ofMinutes(10).toMillis() - 1);
            context.runPeriodicAsyncCalls();
            assertThat(context.assignedSplits(1)).isEmpty();

            clock.incrementAndGet();
            context.runPeriodicAsyncCalls();
            assertThat(context.assignedSplits(1))
                    .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                    .containsExactly("p1-child");

            // Until a checkpoint completes, a finish holds nothing, and the warning is not
            // repeated. A snapshot alone, as for a savepoint, does not count.
            enumerator.handleSourceEvent(0, finished("p2", start));
            enumerator.snapshotState(1);
            enumerator.handleSourceEvent(0, finished("p3", start));
            clock.addAndGet(Duration.ofHours(1).toMillis());
            context.runPeriodicAsyncCalls();
            context.runPeriodicAsyncCalls();
            assertThat(enumerator.heldParentCount()).isZero();
            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains("No checkpoint has completed"))
                    .singleElement()
                    .satisfies(
                            message ->
                                    assertThat(message)
                                            .contains("PT10M")
                                            .contains("scan.first-checkpoint-timeout")
                                            .contains("scan.change-stream.partition-handoff"));

            enumerator.notifyCheckpointComplete(1);
            enumerator.handleSourceEvent(0, finished("p4", start));
            assertThat(enumerator.heldParentCount()).isEqualTo(1);
        }
        enumerator.close();
    }

    @Test
    void aSavepointDoesNotEndTheFirstCheckpointWait() throws Exception {
        // Flink snapshots the coordinator for a savepoint but never reports its completion, so
        // a job without periodic checkpointing must still fall back.
        Instant start = Instant.now().minus(Duration.ofHours(1));
        AtomicLong clock = new AtomicLong();
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(context, true, Duration.ofMinutes(10), runningParents(start, "p1"), clock);
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(0, finished("p1", start));
        enumerator.snapshotState(1);
        clock.addAndGet(Duration.ofMinutes(10).toMillis());
        context.runPeriodicAsyncCalls();

        assertThat(enumerator.heldParentCount()).isZero();
        enumerator.close();
    }

    @Test
    void theWaitIsMeasuredFromTheOldestHoldAndNotRestartedByLaterOnes() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        AtomicLong clock = new AtomicLong();
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1", "p2"),
                        clock);
        enumerator.start();
        context.runAsyncCalls();

        // Long after start(), but the first hold has only just begun.
        clock.addAndGet(Duration.ofMinutes(20).toMillis());
        enumerator.handleSourceEvent(0, finished("p1", start));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.heldParentCount()).isEqualTo(1);

        clock.addAndGet(Duration.ofMinutes(5).toMillis());
        enumerator.handleSourceEvent(0, finished("p2", start));
        clock.addAndGet(Duration.ofMinutes(5).toMillis());
        context.runPeriodicAsyncCalls();

        assertThat(enumerator.heldParentCount()).isZero();
        enumerator.close();
    }

    @Test
    void aCompletedCheckpointOrAZeroTimeoutKeepsTheHoldPastTheTimeout() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        AtomicLong clock = new AtomicLong();
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> completed = context(2);
        SpannerChangeStreamSplitEnumerator checkpointing =
                handOff(
                        completed,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        clock);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> zero = context(2);
        SpannerChangeStreamSplitEnumerator waitsIndefinitely =
                handOff(zero, true, Duration.ZERO, runningParents(start, "p1"), clock);

        checkpointing.start();
        completed.runAsyncCalls();
        checkpointing.snapshotState(1);
        checkpointing.notifyCheckpointComplete(1);
        checkpointing.handleSourceEvent(0, finished("p1", start));
        waitsIndefinitely.start();
        zero.runAsyncCalls();
        waitsIndefinitely.handleSourceEvent(0, finished("p1", start));
        clock.addAndGet(Duration.ofDays(1).toMillis());
        completed.runPeriodicAsyncCalls();
        zero.runPeriodicAsyncCalls();

        assertThat(checkpointing.heldParentCount()).isEqualTo(1);
        assertThat(waitsIndefinitely.heldParentCount()).isEqualTo(1);
        checkpointing.snapshotState(2);
        checkpointing.notifyCheckpointComplete(2);
        assertThat(checkpointing.heldParentCount()).isZero();
        checkpointing.close();
        waitsIndefinitely.close();
    }

    @Test
    void everyFinishIsAcknowledgedAndAFinishTheLedgerAppliedAlreadyChangesNothing()
            throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(0, finished("p1", start));
        SpannerChangeStreamEnumeratorState afterFinish = enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, finished("p1", start));
        enumerator.handleSourceEvent(1, finished("never-assigned", start));

        assertThat(context.sourceEvents(0))
                .filteredOn(PartitionFinishedAckEvent.class::isInstance)
                .extracting(event -> ((PartitionFinishedAckEvent) event).getSplitId())
                .containsExactly(
                        ChangeStreamPartitionSplit.idForToken("p1"),
                        ChangeStreamPartitionSplit.idForToken("p1"));
        assertThat(context.sourceEvents(1))
                .filteredOn(PartitionFinishedAckEvent.class::isInstance)
                .extracting(event -> ((PartitionFinishedAckEvent) event).getSplitId())
                .containsExactly(ChangeStreamPartitionSplit.idForToken("never-assigned"));
        assertThat(enumerator.snapshotState(2).getPartitions())
                .isEqualTo(afterFinish.getPartitions());
        assertThat(enumerator.heldParentCount()).isEqualTo(1);
        // The repeated finish did not restart the hold that checkpoint 1 covers.
        enumerator.notifyCheckpointComplete(1);
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        enumerator.close();
    }

    @Test
    void aFinishFromAReaderThatFailedSinceIsAppliedWithoutAnAcknowledgement() throws Exception {
        Instant start = Instant.now().minus(Duration.ofHours(1));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(start, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        context.unregisterReader(0);

        enumerator.handleSourceEvent(0, finished("p1", start));

        assertThat(context.sourceEvents(0)).noneMatch(PartitionFinishedAckEvent.class::isInstance);
        assertThat(enumerator.heldParentCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aFinishTheRestoredLedgerMissedIsAppliedWhenTheRestoredReaderReportsItAgain()
            throws Exception {
        // #2205: the finish and the child report reached the coordinator after its snapshot, so
        // the restored ledger still runs the parent and has never seen its child.
        Instant start = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit initial = finishedInitial(start.minusSeconds(1));
        ChangeStreamPartitionSplit parent =
                child("parent", start, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState checkpoint =
                SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                        Collections.singletonList(parent),
                        Collections.<String>emptySet(),
                        null,
                        Long.MIN_VALUE);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator restored =
                handOff(context, true, Duration.ofMinutes(10), checkpoint, new AtomicLong());
        restored.start();
        context.runAsyncCalls();
        Instant childStart = start.plusSeconds(10);

        restored.handleSourceEvent(0, children(parent, childStart, "child"));
        restored.handleSourceEvent(
                0, new PartitionFinishedEvent(parent.splitId(), start, start, false));
        restored.handleSplitRequest(1, "localhost");

        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("child");
        assertThat(context.sourceEvents(0))
                .filteredOn(PartitionFinishedAckEvent.class::isInstance)
                .singleElement()
                .extracting(event -> ((PartitionFinishedAckEvent) event).getSplitId())
                .isEqualTo(parent.splitId());
        restored.close();
    }

    @Test
    void aRegionFailoverReadsAgainAChildThatFinishedAfterTheLastCompletedCheckpoint()
            throws Exception {
        // #2204, in the order Flink drives it: subtaskReset returns C, which was assigned after the
        // restored checkpoint, and the restarted reader then names P, which it restored.
        Lineage lineage = new Lineage();
        lineage.finishParentAndRunC();

        lineage.enumerator.addSplitsBack(Collections.singletonList(lineage.assignedC), 0);
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionsRestoredEvent(Collections.singletonList(lineage.parent.splitId())));

        assertThat(lineage.enumerator.snapshotState(1).getPartitions())
                .extracting(p -> p.getPartitionToken() + "=" + p.getLifecycleState())
                .containsExactlyInAnyOrder("p=RUNNING", "q=RUNNING", "c=CREATED", "d=CREATED");
        // P's repeated report finds C in the ledger, and P's next finish releases it again.
        lineage.enumerator.handleSourceEvent(0, lineage.reported);
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        lineage.parent.splitId(), lineage.childStart, lineage.childStart, true));
        lineage.enumerator.handleSplitRequest(0, "localhost");
        assertThat(lineage.context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("c", "c");
        assertThat(lineage.context.assignedSplits(0).get(1).getCurrentPosition())
                .isEqualTo(lineage.childStart);
        lineage.enumerator.close();
    }

    @Test
    void aFinishIsFinalOnlyOnceACompletedCheckpointSnapshottedAfterItCoversIt() throws Exception {
        Lineage lineage = new Lineage();
        lineage.enumerator.snapshotState(1);
        lineage.reportAndFinishParent();

        // Checkpoint 1's snapshot ran before the finish, and checkpoint 2 has not completed.
        lineage.enumerator.notifyCheckpointComplete(1);
        assertThat(lineage.enumerator.pendingFinishCount()).isEqualTo(1);
        lineage.enumerator.snapshotState(2);
        lineage.enumerator.snapshotState(3);
        lineage.enumerator.notifyCheckpointComplete(1);
        assertThat(lineage.enumerator.pendingFinishCount()).isEqualTo(1);
        // A pending finish is not checkpointed and not counted as a ledger entry.
        assertThat(lineage.enumerator.snapshotState(4).getPartitions())
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .doesNotContain("p");
        assertThat(
                        lineage.context.<Long>gauge(
                                SpannerMetricNames.CHANGE_STREAM_PARTITION_LEDGER_ENTRIES))
                .isEqualTo(3L);

        // Checkpoint 3 completes and covers it, as does any later one.
        lineage.enumerator.notifyCheckpointComplete(3);
        assertThat(lineage.enumerator.pendingFinishCount()).isZero();
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            lineage.enumerator.addSplitsBack(
                    Collections.singletonList(
                            lineage.parent.withProgress(lineage.start, lineage.start)),
                    0);
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(m -> assertThat(m).contains("no longer in the ledger"));
        }
        lineage.enumerator.close();
    }

    @Test
    void aReturnedRunningSplitRestartsWhereItWasAssignedNotWhereItsReaderReported()
            throws Exception {
        Lineage lineage = new Lineage();
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assigned = lineage.context.assignedSplits(0).get(0);
        Instant later = assigned.getCurrentPosition().plusSeconds(30);
        lineage.enumerator.handleSourceEvent(
                0, committedProgress(assigned.splitId(), later, later));

        lineage.enumerator.addSplitsBack(Collections.singletonList(assigned), 0);
        lineage.enumerator.handleSplitRequest(0, "localhost");

        assertThat(lineage.context.assignedSplits(0)).hasSize(2);
        assertThat(lineage.context.assignedSplits(0).get(1).getCurrentPosition())
                .isEqualTo(assigned.getCurrentPosition());
        lineage.enumerator.close();
    }

    @Test
    void aRestoredReaderReadingAPendingParentAgainMakesItsScheduledChildrenWaitAgain()
            throws Exception {
        // The #2193 residual, with a merge child: P and Q both finished, so C and D were
        // scheduled; a region failover restores P into its reader. D must wait for P again, and
        // Q, still a pending finish whose proof D's promotion released, is proven again.
        Lineage lineage = new Lineage();
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        lineage.other.splitId(), lineage.childStart, lineage.childStart, true));
        assertThat(lineage.enumerator.snapshotState(1).getPartitions())
                .extracting(p -> p.getPartitionToken() + "=" + p.getLifecycleState())
                .containsExactlyInAnyOrder("c=SCHEDULED", "d=SCHEDULED");

        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionsRestoredEvent(Collections.singletonList(lineage.parent.splitId())));

        SpannerChangeStreamEnumeratorState rolledBack = lineage.enumerator.snapshotState(2);
        assertThat(rolledBack.getPartitions())
                .extracting(p -> p.getPartitionToken() + "=" + p.getLifecycleState())
                .containsExactlyInAnyOrder("p=RUNNING", "c=CREATED", "d=CREATED");
        assertThat(rolledBack.getFinishedParentProofs()).containsExactly(lineage.other.splitId());
        assertThat(lineage.enumerator.pendingFinishCount()).isEqualTo(1);

        lineage.enumerator.handleSourceEvent(0, lineage.reported);
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        lineage.parent.splitId(), lineage.childStart, lineage.childStart, true));
        lineage.enumerator.handleSplitRequest(0, "localhost");
        lineage.enumerator.handleSplitRequest(0, "localhost");
        assertThat(lineage.context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactlyInAnyOrder("c", "d");
        lineage.enumerator.close();
    }

    @Test
    void aParentReadAgainWhileItsChildRunsElsewhereStillCheckpointsARestorableLedger()
            throws Exception {
        // Nothing is held at parallelism 1 (and with IMMEDIATE), so C runs on another region's
        // reader when P's region restores P. The ledger then holds a running child of a running
        // parent, and a checkpoint of it must still restore.
        Lineage lineage = new Lineage();
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSplitRequest(0, "localhost");

        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionsRestoredEvent(Collections.singletonList(lineage.parent.splitId())));
        SpannerChangeStreamEnumeratorState checkpoint = lineage.enumerator.snapshotState(1);

        assertThat(checkpoint.getPartitions())
                .extracting(p -> p.getPartitionToken() + "=" + p.getLifecycleState())
                .containsExactlyInAnyOrder("p=RUNNING", "q=RUNNING", "c=RUNNING", "d=CREATED");
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();
        assertThat(
                        serializer
                                .deserialize(
                                        serializer.getVersion(), serializer.serialize(checkpoint))
                                .getPartitions())
                .hasSize(4);
        lineage.enumerator.close();
    }

    @Test
    void aParentReadAgainRepeatsAMergeChildWhoseOtherParentLeftTheLedger() throws Exception {
        // Q finished first and a checkpoint made that final; P finished after it, and the merge
        // child D started, which released both proofs. P's repeated report names Q, which the
        // ledger no longer knows, and must not fail the coordinator.
        Lineage lineage = new Lineage();
        lineage.enumerator.handleSourceEvent(
                0,
                new ChildPartitionsEvent(
                        lineage.other.splitId(),
                        lineage.childStart,
                        Collections.singletonList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "d",
                                        Arrays.asList(
                                                lineage.parent.splitId(),
                                                lineage.other.splitId())))));
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        lineage.other.splitId(), lineage.childStart, lineage.childStart, true));
        lineage.enumerator.snapshotState(1);
        lineage.enumerator.notifyCheckpointComplete(1);
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSplitRequest(0, "localhost");
        lineage.enumerator.handleSplitRequest(0, "localhost");
        assertThat(lineage.context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactlyInAnyOrder("c", "d");

        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionsRestoredEvent(Collections.singletonList(lineage.parent.splitId())));
        lineage.enumerator.handleSourceEvent(0, lineage.reported);

        assertThat(lineage.enumerator.snapshotState(2).getPartitions())
                .extracting(p -> p.getPartitionToken() + "=" + p.getLifecycleState())
                .containsExactlyInAnyOrder("p=RUNNING", "c=RUNNING", "d=RUNNING");
        lineage.enumerator.close();
    }

    @Test
    void aPendingFinishHoldsTheFrontierAndCommittedProgressMovesIt() throws Exception {
        Lineage lineage = new Lineage();
        Instant ahead = lineage.childStart.plusSeconds(60);
        lineage.enumerator.handleSourceEvent(
                0, committedProgress(lineage.other.splitId(), ahead, ahead));
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSplitRequest(0, "localhost");
        lineage.enumerator.handleSourceEvent(
                0,
                committedProgress(
                        ChangeStreamPartitionSplit.idForToken("c"), lineage.childStart, ahead));

        // P's pending finish still holds the frontier at its own watermark.
        assertThat(lineage.enumerator.snapshotState(1).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(lineage.start));
        lineage.enumerator.handleSourceEvent(
                0, committedProgress(lineage.parent.splitId(), lineage.childStart, ahead));
        // D, still waiting for Q, now holds it at its start.
        assertThat(lineage.enumerator.snapshotState(2).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(lineage.childStart));
        lineage.enumerator.close();
    }

    @Test
    void whileCheckpointsCompleteOnlyCommittedPositionsMoveTheFrontier() throws Exception {
        Lineage lineage = new Lineage();
        Instant ahead = lineage.start.plusSeconds(60);
        List<PartitionProgressEvent.Progress> both = lineage.progressOf(ahead);

        lineage.enumerator.handleSourceEvent(0, PartitionProgressEvent.live(both));
        assertThat(lineage.enumerator.snapshotState(1).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(lineage.start));

        lineage.enumerator.handleSourceEvent(0, PartitionProgressEvent.committed(both));
        assertThat(lineage.enumerator.snapshotState(2).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(ahead));
        lineage.enumerator.close();
    }

    @Test
    void withoutACompletedCheckpointTheFrontierFollowsLiveProgressUntilOneCompletes()
            throws Exception {
        Lineage lineage = new Lineage();
        lineage.reportAndFinishParent();
        lineage.enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assignedC = lineage.context.assignedSplits(0).get(0);
        assertThat(lineage.enumerator.pendingFinishCount()).isEqualTo(1);

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            lineage.clock.addAndGet(Duration.ofMinutes(10).toMillis());
            lineage.context.runPeriodicAsyncCalls();
            assertThat(capture.getMessages())
                    .filteredOn(m -> m.contains("follows the readers' current positions"))
                    .singleElement()
                    .satisfies(m -> assertThat(m).contains("scan.first-checkpoint-timeout"));
        }
        assertThat(lineage.enumerator.pendingFinishCount()).isZero();
        Instant ahead = lineage.childStart.plusSeconds(60);
        List<PartitionProgressEvent.Progress> live =
                Arrays.asList(
                        new PartitionProgressEvent.Progress(lineage.other.splitId(), ahead, ahead),
                        new PartitionProgressEvent.Progress(assignedC.splitId(), ahead, ahead));
        lineage.enumerator.handleSourceEvent(0, PartitionProgressEvent.live(live));
        long live1 = lineage.enumerator.snapshotState(1).getSourceWatermark();
        assertThat(live1)
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(lineage.childStart));

        // Q finishes, final at once now, so D starts and live positions move everything ahead.
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        lineage.other.splitId(), lineage.childStart, lineage.childStart, true));
        lineage.enumerator.handleSplitRequest(0, "localhost");
        lineage.enumerator.handleSourceEvent(
                0,
                PartitionProgressEvent.live(
                        Arrays.asList(
                                new PartitionProgressEvent.Progress(
                                        assignedC.splitId(), ahead, ahead),
                                new PartitionProgressEvent.Progress(
                                        ChangeStreamPartitionSplit.idForToken("d"),
                                        ahead,
                                        ahead))));
        assertThat(lineage.enumerator.snapshotState(2).getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(ahead));

        // A region failover returns C below that frontier. It holds rather than move back, and a
        // checkpoint records no frontier ahead of the ledger, so a restore from it succeeds.
        lineage.enumerator.addSplitsBack(Collections.singletonList(assignedC), 0);
        SpannerChangeStreamEnumeratorState checkpoint = lineage.enumerator.snapshotState(3);
        assertThat(checkpoint.getSourceWatermark())
                .isEqualTo(SpannerChangeStreamWatermarks.beforeInstant(lineage.childStart));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(1);
        SpannerChangeStreamSplitEnumerator restored =
                handOff(
                        restoredContext,
                        true,
                        Duration.ofMinutes(10),
                        new SpannerChangeStreamEnumeratorStateSerializer()
                                .deserialize(
                                        new SpannerChangeStreamEnumeratorStateSerializer()
                                                .getVersion(),
                                        new SpannerChangeStreamEnumeratorStateSerializer()
                                                .serialize(checkpoint)),
                        new AtomicLong());
        restored.start();
        restoredContext.runAsyncCalls();
        restored.close();

        // The first completed checkpoint ends live mode: live positions no longer apply, and the
        // timeout does not bring it back.
        lineage.enumerator.notifyCheckpointComplete(3);
        lineage.enumerator.handleSourceEvent(
                0,
                PartitionProgressEvent.live(
                        Collections.singletonList(
                                new PartitionProgressEvent.Progress(
                                        lineage.other.splitId(),
                                        ahead.plusSeconds(60),
                                        ahead.plusSeconds(60)))));
        lineage.clock.addAndGet(Duration.ofHours(1).toMillis());
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            lineage.context.runPeriodicAsyncCalls();
            assertThat(capture.getMessages())
                    .noneMatch(m -> m.contains("follows the readers' current positions"));
        }
        lineage.enumerator.handleSplitRequest(0, "localhost");
        lineage.enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        assignedC.splitId(), lineage.childStart, lineage.childStart, true));
        assertThat(lineage.enumerator.pendingFinishCount()).isEqualTo(1);
        lineage.enumerator.close();
    }

    @Test
    void afterAFallbackARestoredReaderCannotTakeBackAFinish() throws Exception {
        // A reader that restores the discarded ledger names its old splits and then drops them,
        // so a pending finish that shares an id, such as the initial query's, stays pending.
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit unfinished =
                child("expired", expired, ChangeStreamPartitionSplit.INITIAL_PARTITION_ID)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        StartPosition.at(Instant.now().minus(Duration.ofHours(1))),
                        null,
                        new SpannerChangeStreamEnumeratorState(
                                Arrays.asList(
                                        finishedInitial(expired.minusSeconds(1)), unfinished)));
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        initial.splitId(),
                        initial.getStartTimestamp(),
                        initial.getWatermark(),
                        true));
        assertThat(enumerator.pendingFinishCount()).isEqualTo(1);

        enumerator.handleSourceEvent(
                1,
                new PartitionsRestoredEvent(
                        Arrays.asList(initial.splitId(), unfinished.splitId())));

        assertThat(enumerator.pendingFinishCount()).isEqualTo(1);
        enumerator.close();
    }

    /**
     * A restored ledger at parallelism 1, where nothing is held: P and Q run, and P's child report
     * names C and the merge child D, which also waits for Q.
     */
    private static final class Lineage {

        private final Instant start = Instant.now().minus(Duration.ofHours(1));
        private final Instant childStart = start.plusSeconds(10);
        private final ChangeStreamPartitionSplit parent =
                child("p", start, "released-p").withLifecycleState(PartitionLifecycleState.RUNNING);
        private final ChangeStreamPartitionSplit other =
                child("q", start, "released-q").withLifecycleState(PartitionLifecycleState.RUNNING);
        private final AtomicLong clock = new AtomicLong();
        private final FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        private final SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                                Arrays.asList(parent, other),
                                Collections.<String>emptySet(),
                                null,
                                Long.MIN_VALUE),
                        clock);
        private final ChildPartitionsEvent reported =
                new ChildPartitionsEvent(
                        parent.splitId(),
                        childStart,
                        Arrays.asList(
                                new ChildPartitionsEvent.ChildPartition(
                                        "c", Collections.singletonList(parent.splitId())),
                                new ChildPartitionsEvent.ChildPartition(
                                        "d", Arrays.asList(parent.splitId(), other.splitId()))));
        private ChangeStreamPartitionSplit assignedC;

        private Lineage() {
            enumerator.start();
            context.runAsyncCalls();
        }

        private void reportAndFinishParent() {
            enumerator.handleSourceEvent(0, reported);
            enumerator.handleSourceEvent(
                    0, new PartitionFinishedEvent(parent.splitId(), childStart, childStart, true));
        }

        private List<PartitionProgressEvent.Progress> progressOf(Instant position) {
            return Arrays.asList(
                    new PartitionProgressEvent.Progress(parent.splitId(), position, position),
                    new PartitionProgressEvent.Progress(other.splitId(), position, position));
        }

        private void finishParentAndRunC() {
            reportAndFinishParent();
            enumerator.handleSplitRequest(0, "localhost");
            assignedC = context.assignedSplits(0).get(0);
            assertThat(assignedC.getPartitionToken()).isEqualTo("c");
            enumerator.handleSourceEvent(
                    0,
                    new PartitionFinishedEvent(assignedC.splitId(), childStart, childStart, true));
        }
    }

    @Test
    void aFallbackDiscardsReaderStateOnlyUntilACheckpointCompletes() throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit initial = finishedInitial(expired.minusSeconds(1));
        ChangeStreamPartitionSplit unfinished =
                child("expired", expired, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState checkpoint =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, unfinished));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        StartPosition.at(Instant.now().minus(Duration.ofHours(1))),
                        null,
                        checkpoint);
        enumerator.start();
        context.runAsyncCalls();

        // A reader that fails before a checkpoint completes restores the discarded ledger's
        // splits again, even after a checkpoint that was only snapshotted; one that fails after
        // a completion restores the new checkpoint's.
        enumerator.snapshotState(1);
        context.unregisterReader(1);
        context.registerReader(1);
        enumerator.addReader(1);
        enumerator.snapshotState(2);
        enumerator.notifyCheckpointComplete(2);
        context.unregisterReader(0);
        context.registerReader(0);
        enumerator.addReader(0);

        assertThat(initializationEvents(context, 1))
                .extracting(SpannerChangeStreamInitializationEvent::shouldDiscardRestoredSplits)
                .containsExactly(true, true);
        assertThat(initializationEvents(context, 0))
                .extracting(SpannerChangeStreamInitializationEvent::shouldDiscardRestoredSplits)
                .containsExactly(true, false);
        enumerator.close();
    }

    private static List<SpannerChangeStreamInitializationEvent> initializationEvents(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, int subtaskId) {
        List<SpannerChangeStreamInitializationEvent> events = new java.util.ArrayList<>();
        for (SourceEvent event : context.sourceEvents(subtaskId)) {
            if (event instanceof SpannerChangeStreamInitializationEvent) {
                events.add((SpannerChangeStreamInitializationEvent) event);
            }
        }
        return events;
    }

    /**
     * {@code clock} counts milliseconds; the enumerator reads it as both its wall and nano clock.
     */
    private static SpannerChangeStreamSplitEnumerator handOff(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            boolean holdChildrenUntilCheckpoint,
            Duration firstCheckpointTimeout,
            @Nullable SpannerChangeStreamEnumeratorState restored,
            AtomicLong clock) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                clientMetrics -> new ScriptedClient(),
                StartPosition.latest(),
                null,
                null,
                2_000,
                holdChildrenUntilCheckpoint,
                firstCheckpointTimeout,
                restored,
                null,
                clock::get,
                () -> TimeUnit.MILLISECONDS.toNanos(clock.get()));
    }

    /** A restored ledger with each named parent running and its {@code <name>-child} created. */
    private static SpannerChangeStreamEnumeratorState runningParents(
            Instant start, String... parents) {
        List<ChangeStreamPartitionSplit> ledger = new java.util.ArrayList<>();
        for (String parent : parents) {
            ChangeStreamPartitionSplit running =
                    child(parent, start, "released-" + parent)
                            .withLifecycleState(PartitionLifecycleState.RUNNING);
            ledger.add(running);
            ledger.add(child(parent + "-child", start.plusSeconds(1), running.splitId()));
        }
        return SpannerChangeStreamEnumeratorState.snapshotOfCoordinatorLedger(
                ledger, Collections.emptySet(), null, Long.MIN_VALUE);
    }

    private static PartitionProgressEvent committedProgress(
            String splitId, Instant position, Instant watermark) {
        return PartitionProgressEvent.committed(
                Collections.singletonList(
                        new PartitionProgressEvent.Progress(splitId, position, watermark)));
    }

    private static PartitionFinishedEvent finished(String token, Instant start) {
        Instant end = start.plusSeconds(1);
        return new PartitionFinishedEvent(
                ChangeStreamPartitionSplit.idForToken(token), end, end, true);
    }

    private static SpannerChangeStreamSplitEnumerator configured(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            StartPosition start,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant bound,
            @Nullable SpannerChangeStreamEnumeratorState restored) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                clientMetrics -> new ScriptedClient(),
                start,
                resumeFallback,
                bound,
                2_000,
                true,
                Duration.ofMinutes(10),
                restored,
                null);
    }

    @Test
    void theCoordinatorsClientReceivesNoClientMetricsUnlessTheyAreExported() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        List<ClientMetrics> received = new java.util.ArrayList<>();

        runEnumerator(context, null, received);

        assertThat(received).containsExactly((ClientMetrics) null);
    }

    @Test
    void theCoordinatorsClientRecordsIntoTheSourcesSetAndNeverClosesIt() throws Exception {
        // The source registers the set once for all its enumerators (SpannerChangeStreamSource,
        // pinned through it in SpannerChangeStreamSourceBuilderTest); a coordinator only passes it
        // on.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ClientMetrics shared = ClientMetricsProbe.create();
        List<ClientMetrics> received = new java.util.ArrayList<>();

        runEnumerator(context, shared, received);

        assertThat(received).containsExactly(shared);
        assertThat(ClientMetricsProbe.isOpen(shared)).isTrue();
    }

    private static void runEnumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            @Nullable ClientMetrics clientMetrics,
            List<ClientMetrics> received)
            throws Exception {
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        metrics -> {
                            received.add(metrics);
                            return new ScriptedClient();
                        },
                        StartPosition.latest(),
                        null,
                        null,
                        2_000,
                        true,
                        Duration.ofMinutes(10),
                        null,
                        clientMetrics);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.close();
    }

    private static FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context(int parallelism) {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i);
        }
        return context;
    }

    /** A client close failure, and an Error such as the one a released class loader causes. */
    private static Stream<Throwable> closeFailures() {
        return Stream.of(
                new IllegalStateException("close failed"),
                new NoClassDefFoundError("closed loader"));
    }

    /** Throws an exception or an error as itself. */
    private static void throwAs(Throwable failure) throws Exception {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw (Exception) failure;
    }

    /** Asserts that a captured event was logged at WARN, which its rendering starts with. */
    private static void isAWarning(LogCapture.Event event) {
        assertThat(event.toString()).as("the event's level").startsWith("WARN ");
    }

    /**
     * Waits until a thread parks waiting, is blocked on a monitor, or has ended, and says which.
     */
    private static Thread.State awaitParkedOrEnded(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING
                    || state == Thread.State.BLOCKED
                    || state == Thread.State.TERMINATED) {
                return state;
            }
            assertThat(System.nanoTime())
                    .as("%s parked or ended in time", thread.getName())
                    .isLessThan(deadline);
            Thread.onSpinWait();
        }
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedClient client,
            SpannerChangeStreamEnumeratorState restored) {
        return enumerator(context, clientMetrics -> client, restored);
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            SpannerChangeStreamCoordinatorClientFactory clientFactory,
            SpannerChangeStreamEnumeratorState restored) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                clientFactory,
                StartPosition.latest(),
                null,
                null,
                2_000,
                true,
                Duration.ofMinutes(10),
                restored,
                null);
    }

    private static ChildPartitionsEvent children(
            ChangeStreamPartitionSplit parent, Instant at, String... tokens) {
        List<ChildPartitionsEvent.ChildPartition> children = new java.util.ArrayList<>();
        for (String token : tokens) {
            children.add(
                    new ChildPartitionsEvent.ChildPartition(
                            token, Collections.singletonList(parent.splitId())));
        }
        return new ChildPartitionsEvent(parent.splitId(), at, children);
    }

    private static ChangeStreamPartitionSplit child(String token, Instant start, String parentId) {
        return new ChangeStreamPartitionSplit(
                token,
                Collections.singletonList(parentId),
                start,
                null,
                2_000,
                start,
                PartitionLifecycleState.CREATED,
                start);
    }

    private static ChangeStreamPartitionSplit finishedInitial(Instant start) {
        return ChangeStreamPartitionSplit.initial(start, null, 2_000)
                .withLifecycleState(PartitionLifecycleState.FINISHED);
    }

    private static List<SpannerChangeStreamWatermarkEvent> watermarkEvents(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, int subtaskId) {
        List<SpannerChangeStreamWatermarkEvent> watermarks = new java.util.ArrayList<>();
        for (SourceEvent event : context.sourceEvents(subtaskId)) {
            if (event instanceof SpannerChangeStreamWatermarkEvent) {
                watermarks.add((SpannerChangeStreamWatermarkEvent) event);
            }
        }
        return watermarks;
    }
}
