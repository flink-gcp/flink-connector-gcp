/*
 * Copyright 2026 laughingman7743
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

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Timeout(30)
class SpannerChangeStreamSplitEnumeratorTest {

    @Test
    void freshLatestSeedsOneNullTokenWithoutReadingRetention() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(client.modeCalls).isEqualTo(1);
        assertThat(client.retentionCalls).isZero();
        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.splitId())
                                    .isEqualTo(
                                            SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
                            assertThat(split.getPartitionToken()).isNull();
                            assertThat(split.getLifecycleState())
                                    .isEqualTo(PartitionLifecycleState.RUNNING);
                        });
        assertThat(context.counter(SpannerMetricNames.SPLITS_ASSIGNED)).isEqualTo(1);
        enumerator.close();
        assertThat(client.closeCalls).isEqualTo(1);
    }

    @Test
    void restorePreservesTheLedgerAndIgnoresTheConfiguredFreshStart() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        SpannerChangeStreamPartitionSplit child =
                child("child", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, child));
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        () -> client,
                        StartPosition.at(Instant.now().plus(Duration.ofDays(1))),
                        Optional.empty(),
                        null,
                        2_000,
                        restored);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(client.retentionCalls).isEqualTo(1);
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
    void returnedSplitsKeepProgressAndBecomeScheduledAgain() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        Instant returnedProgress = running.getCurrentPosition().plusSeconds(5);
        Instant coordinatorProgress = running.getCurrentPosition().plusSeconds(10);
        Instant coordinatorWatermark = running.getWatermark().plusSeconds(20);
        Instant returnedWatermark = running.getWatermark().plusSeconds(25);
        enumerator.handleSourceEvent(
                0,
                new PartitionProgressEvent(
                        running.splitId(), coordinatorProgress, coordinatorWatermark));

        enumerator.addSplitsBack(
                Collections.singletonList(
                        running.withProgress(returnedProgress, returnedWatermark)),
                0);

        SpannerChangeStreamPartitionSplit returned =
                enumerator.snapshotState(1).getPartitions().get(0);
        assertThat(returned.getLifecycleState()).isEqualTo(PartitionLifecycleState.SCHEDULED);
        assertThat(returned.getCurrentPosition()).isEqualTo(coordinatorProgress);
        assertThat(returned.getWatermark()).isEqualTo(returnedWatermark);
        assertThat(context.counter(SpannerMetricNames.SPLITS_RETURNED)).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void discoveryCounterCountsNewChildTokensButNotDuplicateReports() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);

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
        SpannerChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        SpannerChangeStreamPartitionSplit older =
                child("older", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamPartitionSplit sameAge =
                child("same-age", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamPartitionSplit newer =
                child("newer", recent.plusMillis(500), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, older, sameAge, newer));
        AtomicLong now = new AtomicLong(recent.plusSeconds(1).toEpochMilli());
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        ScriptedClient::new,
                        StartPosition.latest(),
                        Optional.empty(),
                        null,
                        2_000,
                        restored,
                        now::get);

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
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant childStart = initial.getStartTimestamp().plusSeconds(1);

        enumerator.handleSourceEvent(0, children(initial, childStart, "left"));
        enumerator.handleSourceEvent(0, children(initial, childStart, "right"));

        assertThat(enumerator.snapshotState(1).getPartitions())
                .filteredOn(p -> p.getLifecycleState() == PartitionLifecycleState.CREATED)
                .hasSize(2);
        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.assignedSplits(1)).isEmpty();

        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(initial.splitId(), childStart, childStart));

        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(enumerator.snapshotState(2).getPartitions())
                .filteredOn(p -> p.getLifecycleState() == PartitionLifecycleState.SCHEDULED)
                .hasSize(1);
        enumerator.close();
    }

    @Test
    void mergedChildIsReleasedOnlyAfterEveryParentFinishes() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamPartitionSplit initial = context.assignedSplits(0).get(0);
        Instant splitAt = initial.getStartTimestamp().plusSeconds(1);
        enumerator.handleSourceEvent(0, children(initial, splitAt, "left", "right"));
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(initial.splitId(), splitAt, splitAt));
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        SpannerChangeStreamPartitionSplit left = context.assignedSplits(0).get(1);
        SpannerChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
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
                0, new PartitionFinishedEvent(left.splitId(), mergeAt, mergeAt));

        assertThat(enumerator.snapshotState(2).getPartitions())
                .filteredOn(p -> "merged".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(SpannerChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.CREATED);
        assertThat(enumerator.pendingParentDependencyCount()).isEqualTo(1);

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
                1, new PartitionFinishedEvent(right.splitId(), mergeAt, mergeAt));

        assertThat(enumerator.snapshotState(3).getPartitions())
                .filteredOn(p -> "merged".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(SpannerChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.SCHEDULED);
        assertThat(enumerator.pendingParentDependencyCount()).isZero();
        enumerator.close();
    }

    @Test
    void progressIsMonotonicAndSnapshotsDoNotAliasTheLiveLedger() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        SpannerChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        Instant laterPosition = running.getCurrentPosition().plusSeconds(5);
        Instant laterWatermark = running.getWatermark().plusSeconds(7);
        enumerator.handleSourceEvent(
                0, new PartitionProgressEvent(running.splitId(), laterPosition, laterWatermark));
        SpannerChangeStreamEnumeratorState snapshot = enumerator.snapshotState(1);

        enumerator.handleSourceEvent(
                0,
                new PartitionProgressEvent(
                        running.splitId(), running.getCurrentPosition(), running.getWatermark()));
        SpannerChangeStreamEnumeratorState afterStaleProgress = enumerator.snapshotState(2);
        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(), laterPosition.plusSeconds(1), laterWatermark));

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
        SpannerChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        SpannerChangeStreamPartitionSplit running =
                child("running", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, running));
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
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
        SpannerChangeStreamPartitionSplit initial = finishedInitial(expired.minusSeconds(1));
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(
                                initial,
                                child("expired", expired, initial.splitId())
                                        .withLifecycleState(PartitionLifecycleState.SCHEDULED)));
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
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
    }

    @Test
    void expiredRestoreWithFallbackReplacesTheWholeLedgerWithOneInitialPartition()
            throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        SpannerChangeStreamPartitionSplit initial = finishedInitial(expired.minusSeconds(1));
        SpannerChangeStreamPartitionSplit unfinished =
                child("expired", expired, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamPartitionSplit finished =
                child("finished", expired, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, unfinished, finished));
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        Instant fallback = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        () -> new ScriptedClient(),
                        StartPosition.latest(),
                        Optional.of(StartPosition.at(fallback)),
                        null,
                        2_000,
                        restored);

        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(enumerator.snapshotState(1).getPartitions())
                    .singleElement()
                    .satisfies(
                            partition -> {
                                assertThat(partition.splitId())
                                        .isEqualTo(
                                                SpannerChangeStreamPartitionSplit
                                                        .INITIAL_PARTITION_ID);
                                assertThat(partition.getPartitionToken()).isNull();
                                assertThat(partition.getStartTimestamp()).isEqualTo(fallback);
                                assertThat(partition.getLifecycleState())
                                        .isEqualTo(PartitionLifecycleState.SCHEDULED);
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
    void closeBeforeInitializationMakesTheLateClientCloseItself() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.start();
        enumerator.close();
        context.runAsyncCalls();

        assertThat(client.closeCalls).isEqualTo(1);
        assertThat(context.assignedSplits(0)).isEmpty();
    }

    @Test
    void mutablePartitionModeFailsBeforeAnySplitCanBeAssigned() {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        ScriptedClient client = new ScriptedClient();
        client.modeFailure = new IllegalArgumentException("MUTABLE_KEY_RANGE is unsupported");
        SpannerChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);
        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");

        Throwable failure = catchThrowable(context::runAsyncCalls);

        assertThat(failure)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to initialize");
        assertThat(failure).rootCause().hasMessageContaining("MUTABLE_KEY_RANGE");
        assertThat(context.assignedSplits(0)).isEmpty();
    }

    @Test
    void childAndCompletionEventsThatArriveDuringRestoreAreReplayed() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        SpannerChangeStreamPartitionSplit parent =
                child("parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, parent));
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), restored);
        Instant childStart = recent.plusSeconds(10);

        enumerator.start();
        enumerator.handleSourceEvent(0, children(parent, childStart, "child"));
        enumerator.handleSourceEvent(
                0, new PartitionFinishedEvent(parent.splitId(), childStart, childStart));
        context.runAsyncCalls();

        assertThat(enumerator.snapshotState(1).getPartitions())
                .filteredOn(p -> "child".equals(p.getPartitionToken()))
                .singleElement()
                .extracting(SpannerChangeStreamPartitionSplit::getLifecycleState)
                .isEqualTo(PartitionLifecycleState.SCHEDULED);
        enumerator.close();
    }

    @Test
    void childNamingAnUnknownMergeParentCannotPoisonTheCheckpoint() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                enumerator(context, new ScriptedClient(), null);
        enumerator.start();
        context.runAsyncCalls();
        SpannerChangeStreamPartitionSplit initial =
                enumerator.snapshotState(1).getPartitions().get(0);
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
                .extracting(SpannerChangeStreamPartitionSplit::splitId)
                .isEqualTo(initial.splitId());
        enumerator.close();
    }

    @Test
    void restoredMergedChildIsReleasedAfterItsRemainingParentFinishes() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        SpannerChangeStreamPartitionSplit initial = finishedInitial(recent.minusSeconds(1));
        SpannerChangeStreamPartitionSplit finishedParent =
                child("finished-parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamPartitionSplit runningParent =
                child("running-parent", recent, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamPartitionSplit merged =
                new SpannerChangeStreamPartitionSplit(
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
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
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
                        runningParent.getWatermark()));

        assertThat(context.assignedSplits(0))
                .singleElement()
                .extracting(SpannerChangeStreamPartitionSplit::getPartitionToken)
                .isEqualTo("merged");
        assertThat(enumerator.pendingParentDependencyCount()).isZero();
        enumerator.close();
    }

    @Test
    void boundedLedgerSignalsNoMoreSplitsAfterTheFinalPartitionFinishes() throws Exception {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context = context(1);
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        ScriptedClient::new,
                        StartPosition.latest(),
                        Optional.empty(),
                        Instant.now().plus(Duration.ofHours(1)),
                        2_000,
                        null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.events()).containsExactly("assign:0");
        SpannerChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);

        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(), running.getCurrentPosition(), running.getWatermark()));
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:0");
        enumerator.close();
    }

    private static FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context(
            int parallelism) {
        FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i);
        }
        return context;
    }

    private static SpannerChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context,
            ScriptedClient client,
            SpannerChangeStreamEnumeratorState restored) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                () -> client,
                StartPosition.latest(),
                Optional.empty(),
                null,
                2_000,
                restored);
    }

    private static ChildPartitionsEvent children(
            SpannerChangeStreamPartitionSplit parent, Instant at, String... tokens) {
        List<ChildPartitionsEvent.ChildPartition> children = new java.util.ArrayList<>();
        for (String token : tokens) {
            children.add(
                    new ChildPartitionsEvent.ChildPartition(
                            token, Collections.singletonList(parent.splitId())));
        }
        return new ChildPartitionsEvent(parent.splitId(), at, children);
    }

    private static SpannerChangeStreamPartitionSplit child(
            String token, Instant start, String parentId) {
        return new SpannerChangeStreamPartitionSplit(
                token,
                Collections.singletonList(parentId),
                start,
                null,
                2_000,
                start,
                PartitionLifecycleState.CREATED,
                start);
    }

    private static SpannerChangeStreamPartitionSplit finishedInitial(Instant start) {
        return SpannerChangeStreamPartitionSplit.initial(start, null, 2_000)
                .withLifecycleState(PartitionLifecycleState.FINISHED);
    }

    private static final class ScriptedClient implements SpannerChangeStreamCoordinatorClient {

        private int modeCalls;
        private int retentionCalls;
        private int closeCalls;
        private RuntimeException modeFailure;

        @Override
        public void validatePartitionMode() {
            modeCalls++;
            if (modeFailure != null) {
                throw modeFailure;
            }
        }

        @Override
        public Duration retention() {
            retentionCalls++;
            return Duration.ofDays(7);
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
