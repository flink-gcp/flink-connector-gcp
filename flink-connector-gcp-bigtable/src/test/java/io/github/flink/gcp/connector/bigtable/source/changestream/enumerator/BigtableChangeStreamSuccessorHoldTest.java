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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The successor hold (#2194): a closed partition's successors wait until a completed checkpoint
 * covers the close, so that none of them overtakes the partition's last records downstream.
 */
@Timeout(30)
class BigtableChangeStreamSuccessorHoldTest {

    private static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    private static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");
    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();

    @Test
    void aSuccessorWaitsForACheckpointTakenAfterTheClose() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);

        // A checkpoint whose coordinator snapshot ran before the close covers nothing of it.
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(
                0, transition(parent, true, successor(LEFT, "left"), successor(RIGHT, "right")));
        assertThat(assignedSince(context, parent)).isEmpty();
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(2);

        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedSince(context, parent)).isEmpty();

        // The held successors are in the checkpoint, as unassigned splits.
        assertThat(enumerator.snapshotState(2).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactly(LEFT, RIGHT);
        enumerator.notifyCheckpointComplete(2);
        assertThat(assignedSince(context, parent))
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactlyInAnyOrder(LEFT, RIGHT);
        assertThat(enumerator.heldSuccessorCount()).isZero();
        enumerator.close();
    }

    @Test
    void aLaterCompletedCheckpointReleasesWhatAnEarlierUncompletedOneCovered() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);

        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));
        enumerator.snapshotState(2);
        enumerator.snapshotState(3);
        assertThat(assignedSince(context, parent)).isEmpty();

        // Flink may notify only the latest of several completed checkpoints.
        enumerator.notifyCheckpointComplete(3);
        assertThat(assignedSince(context, parent)).hasSize(1);
        enumerator.close();
    }

    @Test
    void aMergeWaitsUntilACheckpointCoversEveryParentsClose() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, LEFT, RIGHT);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 1, 1));

        enumerator.handleSourceEvent(
                0, transition(left, true, successorForParent(WHOLE, LEFT, "left-parent")));
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(
                1, transition(right, true, successorForParent(WHOLE, RIGHT, "right-parent")));
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);

        // The first parent's close is final now, the second's is not.
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedCount(context)).isEqualTo(2);

        enumerator.snapshotState(2);
        enumerator.notifyCheckpointComplete(2);
        assertThat(assignedCount(context)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void aMergeWaitsForAnEarlierParentWhenTheLastParentEmittedNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, LEFT, RIGHT);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 1, 1));

        enumerator.handleSourceEvent(
                0, transition(left, true, successorForParent(WHOLE, LEFT, "left-parent")));
        enumerator.handleSourceEvent(
                1, transition(right, false, successorForParent(WHOLE, RIGHT, "right-parent")));

        // The last parent holds nothing back, but the first one's records may still be in flight.
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        assertThat(assignedCount(context)).isEqualTo(2);
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedCount(context)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void aReconciledRestartFromAHeldParentsTokenWaitsForThatParentsClose() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.now());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client,
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        true,
                        clock,
                        true,
                        Duration.ZERO);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(
                0, transition(left, true, successorForParent(WHOLE, LEFT, "left-parent")));

        // The service reports the merged partition while the right parent is still being read, so
        // the reconciler restarts the left half from the left parent's token.
        client.replacePartitions(WHOLE);
        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKEN_GRACE);
        context.runPeriodicAsyncCalls();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isEqualTo(1);
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        assertThat(assignedCount(context)).isEqualTo(2);

        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedCount(context)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void aTokenlessRestartOfPartOfAHeldParentsRangeWaitsForThatParentsClose() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.now());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client,
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        true,
                        clock,
                        true,
                        Duration.ZERO);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 1, 1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2, 1, 1));
        enumerator.handleSourceEvent(
                0, transition(left, true, successorForParent(WHOLE, LEFT, "left-parent")));

        // The service split again at "g": the left parent's token is wider than either gap, so
        // the reconciler restarts both without a token once the longer grace has passed.
        client.replacePartitions(
                ByteStringRange.unbounded().endOpen("g"),
                ByteStringRange.unbounded().startClosed("g"));
        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKENLESS_GRACE);
        context.runPeriodicAsyncCalls();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS))
                .isEqualTo(2);
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(2);
        assertThat(assignedCount(context)).isEqualTo(2);

        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedCount(context)).isEqualTo(4);
        enumerator.close();
    }

    @Test
    void anImmediateMergeIsNotHeldByAParentReadAgain() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, false, Clock.systemUTC(), Duration.ZERO, null, LEFT, RIGHT);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(
                0, transition(left, true, successorForParent(WHOLE, LEFT, "left-parent")));
        enumerator.addSplitsBack(Collections.singletonList(left), 0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));

        enumerator.handleSourceEvent(
                1, transition(right, true, successorForParent(WHOLE, RIGHT, "right-parent")));

        assertThat(enumerator.heldSuccessorCount()).isZero();
        assertThat(assignedCount(context)).isEqualTo(4);
        enumerator.close();
    }

    @Test
    void nothingIsHeldAtParallelismOne() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);

        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        assertThat(assignedSince(context, parent)).hasSize(1);
        assertThat(enumerator.heldSuccessorCount()).isZero();
        // The close itself still waits for a checkpoint, so that a rollback reads it again.
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void nothingIsHeldWhenTheHandOffIsImmediate() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, false, Clock.systemUTC(), Duration.ZERO, null, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);

        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        assertThat(assignedSince(context, parent)).hasSize(1);
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void nothingIsHeldAfterAPartitionThatEmittedNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);

        enumerator.handleSourceEvent(0, transition(parent, false, successor(WHOLE, "next")));

        assertThat(assignedSince(context, parent)).hasSize(1);
        enumerator.close();
    }

    @Test
    void aSuccessorWaitsWhileARollbackReadsItsParentAgain() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        // Region failover returns the parent before a checkpoint covers its close, and it is
        // handed out again while its successor stays held.
        enumerator.addSplitsBack(Collections.singletonList(parent), 0);
        assertThat(assignedCount(context)).isEqualTo(2);
        assertThat(assignedSince(context, parent)).isEmpty();
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);

        // A checkpoint completing while the parent is read again releases nothing.
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);

        // The second close creates no successor; the held one waits for that close's coverage.
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "again")));
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        enumerator.snapshotState(2);
        enumerator.notifyCheckpointComplete(2);
        assertThat(enumerator.heldSuccessorCount()).isZero();
        assertThat(assignedCount(context)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void aSuccessorWaitsWhileARestoredReaderReadsItsParentAgain() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        enumerator.handleSourceEvent(
                0, new PartitionsRestoredEvent(Collections.singletonList(parent.splitId())));
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void withoutACompletedCheckpointSuccessorsStartOnceACloseHasWaitedTheTimeout()
            throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-11-21T00:00:00Z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, true, clock, Duration.ofMinutes(10), null, WHOLE);
        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
            enumerator.handleSourceEvent(
                    0, transition(parent, true, successor(LEFT, "left"), successor(RIGHT, "r")));

            clock.advance(Duration.ofMinutes(10).minusMillis(1));
            context.runPeriodicAsyncCalls();
            assertThat(assignedSince(context, parent)).isEmpty();

            clock.advance(Duration.ofMillis(1));
            context.runPeriodicAsyncCalls();
            List<ChangeStreamPartitionSplit> successors = assignedSince(context, parent);
            assertThat(successors).hasSize(2);
            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains("successors start at once"))
                    .singleElement()
                    .satisfies(
                            message ->
                                    assertThat(message)
                                            .contains("scan.change-stream.partition-handoff")
                                            .contains("scan.first-checkpoint-timeout"));

            // Until a checkpoint completes, a close holds nothing back.
            enumerator.handleSourceEvent(
                    0, transition(successors.get(0), true, successor(LEFT, "left-next")));
            assertThat(enumerator.heldSuccessorCount()).isZero();

            // Once one completes, the hold is back for good.
            enumerator.snapshotState(1);
            enumerator.notifyCheckpointComplete(1);
            enumerator.handleSourceEvent(
                    1, transition(successors.get(1), true, successor(RIGHT, "right-next")));
            assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
            clock.advance(Duration.ofDays(1));
            context.runPeriodicAsyncCalls();
            assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        }
        enumerator.close();
    }

    @Test
    void aParentReadAgainReleasesItsSuccessorWhenItClosesWithoutACheckpoint() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-11-21T00:00:00Z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, true, clock, Duration.ofMinutes(10), null, LEFT, RIGHT);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        enumerator.handleSourceEvent(0, transition(left, true, successor(LEFT, "left-next")));
        enumerator.addSplitsBack(Collections.singletonList(left), 0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        enumerator.handleSourceEvent(1, transition(right, true));

        // The timeout makes the right close final at once, but the left parent is still read again.
        clock.advance(Duration.ofMinutes(10));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);

        // Its second close is final at once too, which frees the successor without a checkpoint.
        enumerator.handleSourceEvent(0, transition(left, true, successor(LEFT, "left-again")));
        assertThat(enumerator.heldSuccessorCount()).isZero();
        assertThat(assignedSince(context, left))
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactlyInAnyOrder(RIGHT, LEFT);
        enumerator.close();
    }

    @Test
    void aZeroTimeoutHoldsUntilACheckpointCompletes() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-11-21T00:00:00Z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, true, clock, Duration.ZERO, null, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        clock.advance(Duration.ofDays(365));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.heldSuccessorCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aRestoredCheckpointHoldsNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = holding(context, WHOLE);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        enumerator.close();

        // A restored checkpoint has completed, so it covers the close behind every successor.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(2);
        BigtableChangeStreamSplitEnumerator restored =
                enumerator(
                        restoredContext, true, Clock.systemUTC(), Duration.ZERO, checkpoint, WHOLE);
        restored.start();
        restoredContext.runAsyncCalls();
        restored.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        assertThat(restoredContext.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactly(WHOLE);
        assertThat(restored.heldSuccessorCount()).isZero();
        restored.close();
    }

    @Test
    void aBoundedRunIsNotDrainedWhileASuccessorIsHeld() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        StartPosition.latest(),
                        null,
                        null,
                        true,
                        false,
                        Clock.systemUTC(),
                        true,
                        Duration.ZERO);
        ChangeStreamPartitionSplit parent = startAndAssignOne(context, enumerator);
        enumerator.handleSourceEvent(0, transition(parent, true, successor(WHOLE, "next")));

        assertThat(context.readersToldNoMoreSplits()).isEmpty();
        enumerator.snapshotState(1);
        enumerator.notifyCheckpointComplete(1);
        ChangeStreamPartitionSplit successor = assignedSince(context, parent).get(0);
        enumerator.handleSourceEvent(0, transition(successor, true));
        assertThat(context.readersToldNoMoreSplits()).containsExactlyInAnyOrder(0, 1);
        enumerator.close();
    }

    /** Starts the enumerator and hands its one partition to subtask 0, with room for more. */
    private static ChangeStreamPartitionSplit startAndAssignOne(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            BigtableChangeStreamSplitEnumerator enumerator) {
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        // Both readers report room for more, so anything assignable is handed out at once.
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            enumerator.handleSourceEvent(
                    subtask,
                    new ReaderCapacityEvent(2, subtask == 0 ? 1 : 0, subtask == 0 ? 1 : 0));
        }
        return parent;
    }

    private static List<ChangeStreamPartitionSplit> assignedSince(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamPartitionSplit parent) {
        List<ChangeStreamPartitionSplit> assigned = new ArrayList<>();
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            for (ChangeStreamPartitionSplit split : context.assignedSplits(subtask)) {
                if (!split.splitId().equals(parent.splitId())) {
                    assigned.add(split);
                }
            }
        }
        return assigned;
    }

    private static int assignedCount(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context) {
        int count = 0;
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            count += context.assignedSplits(subtask).size();
        }
        return count;
    }

    private static FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context(int parallelism) {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i);
        }
        return context;
    }

    private static BigtableChangeStreamSplitEnumerator holding(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ByteStringRange... partitions) {
        return enumerator(context, true, Clock.systemUTC(), Duration.ZERO, null, partitions);
    }

    private static BigtableChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            boolean holdSuccessors,
            Clock clock,
            Duration firstCheckpointTimeout,
            @Nullable BigtableChangeStreamEnumeratorState restored,
            ByteStringRange... partitions) {
        return new BigtableChangeStreamSplitEnumerator(
                context,
                ScriptedChangeStreamCoordinatorClient.with(partitions),
                StartPosition.latest(),
                null,
                restored,
                false,
                false,
                clock,
                holdSuccessors,
                firstCheckpointTimeout);
    }

    private static PartitionTransitionEvent transition(
            ChangeStreamPartitionSplit parent,
            boolean emittedRecords,
            PartitionTransitionEvent.Successor... successors) {
        return new PartitionTransitionEvent(
                parent.splitId(),
                parent.getLowWatermark(),
                Arrays.asList(successors),
                emittedRecords);
    }

    private static PartitionTransitionEvent.Successor successor(
            ByteStringRange partition, String token) {
        return successorForParent(partition, partition, token);
    }

    private static PartitionTransitionEvent.Successor successorForParent(
            ByteStringRange target, ByteStringRange parent, String token) {
        return new PartitionTransitionEvent.Successor(
                target, TestChangeStreamTokens.token(parent, token));
    }

    /** A clock a test can push past the first-checkpoint wait without waiting for it. */
    private static final class AdvanceableClock extends Clock {

        private Instant instant;

        private AdvanceableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration amount) {
            instant = instant.plus(amount);
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
            return instant;
        }
    }
}
