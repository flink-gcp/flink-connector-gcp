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
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderPolledEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
 * The hand-off hold (#2194): a closed partition's successors are assigned only once a completed
 * checkpoint covers the close, as the Spanner source holds its children (ADR-0099).
 */
@Timeout(30)
class BigtableChangeStreamSuccessorHoldTest {

    static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");
    static final ByteStringRange WHOLE = ByteStringRange.unbounded();
    private static final ByteStringRange LEFT_HALF = ByteStringRange.unbounded().endOpen("f");
    private static final Instant START = Instant.parse("2026-11-21T00:00:00Z");
    private static final String FALLBACK_WARNING = "for a completed checkpoint and none has";

    @Test
    void aSuccessorStartsOnlyOnceACheckpointTakenAfterTheCloseCompletes() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);

        enumerator.handleSourceEvent(
                0, emitted(parent, successor(LEFT, "left"), successor(RIGHT, "right")));

        assertThat(assignedIds(context)).containsExactly(parent.splitId());
        assertThat(enumerator.heldParentCount()).isEqualTo(1);
        // A waiting successor is unassigned work, and the gauge says so.
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        // A checkpoint whose snapshot ran before the close does not cover it.
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).containsExactly(parent.splitId());

        // The successors exist from the transition on, so the snapshot that covers the close
        // carries them: the reader drops the closed split once it is acknowledged.
        BigtableChangeStreamEnumeratorState covering = enumerator.snapshotState(2);
        pollAfter(enumerator, context, 2);
        assertThat(covering.getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", "change-stream-2");
        assertThat(assignedIds(context)).containsExactly(parent.splitId());

        enumerator.notifyCheckpointComplete(2);

        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(parent.splitId(), "change-stream-1", "change-stream-2");
        assertThat(enumerator.heldParentCount()).isZero();
        enumerator.close();
    }

    @Test
    void aLaterCompletedCheckpointCoversOneThatNeverCompleted() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        enumerator.handleSourceEvent(0, emitted(parent, successor(WHOLE, "successor")));
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointAborted(1);
        enumerator.snapshotState(2);
        pollAfter(enumerator, context, 2);

        enumerator.notifyCheckpointComplete(2);

        assertThat(assignedIds(context)).containsExactly(parent.splitId(), "change-stream-1");
        enumerator.close();
    }

    @Test
    void nothingIsHeldAtParallelismOneWithAnImmediateHandOffOrAfterAPartitionThatEmittedNothing()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> single = context(1);
        BigtableChangeStreamSplitEnumerator one = started(single, true, Duration.ZERO);
        one.handleSourceEvent(0, emitted(only(single), successor(WHOLE, "successor")));
        assertThat(single.assignedSplits(0)).hasSize(2);
        assertThat(one.pendingCloseCount()).isEqualTo(1);
        one.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> immediate = context(2);
        BigtableChangeStreamSplitEnumerator handoff = started(immediate, false, Duration.ZERO);
        handoff.handleSourceEvent(0, emitted(only(immediate), successor(WHOLE, "successor")));
        assertThat(assignedIds(immediate)).hasSize(2);
        assertThat(handoff.pendingCloseCount()).isEqualTo(1);
        handoff.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> quiet = context(2);
        BigtableChangeStreamSplitEnumerator silent = started(quiet, true, Duration.ZERO);
        silent.handleSourceEvent(0, transition(only(quiet), successor(WHOLE, "successor")));
        assertThat(assignedIds(quiet)).hasSize(2);
        assertThat(silent.heldParentCount()).isZero();
        assertThat(silent.pendingCloseCount()).isEqualTo(1);
        silent.close();
    }

    @Test
    void aMergeChildWaitsForAParentThatEmitted() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(LEFT, RIGHT), null, true, Duration.ZERO, Clock.systemUTC());
        ChangeStreamPartitionSplit left = byId(context, "change-stream-0");
        ChangeStreamPartitionSplit right = byId(context, "change-stream-1");

        enumerator.handleSourceEvent(0, emitted(left, successorForParent(WHOLE, LEFT, "left")));
        enumerator.handleSourceEvent(1, transition(right, successorForParent(WHOLE, RIGHT, "r")));

        assertThat(assignedIds(context)).containsExactlyInAnyOrder(left.splitId(), right.splitId());
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).contains("change-stream-2");
        enumerator.close();
    }

    @Test
    void aCloseTakenBackKeepsItsSuccessorsWaitingUntilTheReReadCloseIsFinal() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        PartitionTransitionEvent close =
                emitted(parent, successor(LEFT, "left"), successor(RIGHT, "right"));
        enumerator.handleSourceEvent(0, close);

        // A region failover hands the parent back before a checkpoint covers its close.
        enumerator.addSplitsBack(Collections.singletonList(parent), 0);
        ready(enumerator);
        assertThat(assignedIds(context)).containsExactly(parent.splitId(), parent.splitId());
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).containsOnly(parent.splitId());

        // The re-read closes again: it creates no successors, and its close releases them.
        enumerator.handleSourceEvent(0, close);
        assertThat(enumerator.snapshotState(2).getNextSplitId()).isEqualTo(3);
        pollAfter(enumerator, context, 2);
        enumerator.notifyCheckpointComplete(2);

        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(
                        parent.splitId(), parent.splitId(), "change-stream-1", "change-stream-2");
        assertThat(enumerator.heldParentCount()).isZero();
        enumerator.close();
    }

    @Test
    void aRestoreAssignsEverySplitAtOnceAndAnEarlierSplitNeverWaitsForALaterOne() throws Exception {
        // A restored checkpoint has completed, so nothing it records is held. Here a partition and
        // a later split over part of its range are both unassigned, which a restore during a
        // re-read leaves behind; both close with output and are handed back before a checkpoint.
        // The enumerator checks restored positions against the change stream's retention, less a
        // one-minute safety margin, on the system clock whatever clock it is given, so they are
        // recent rather than START, which falls out of that window just under a week after it.
        Instant restoredAt = Instant.now().minus(Duration.ofMinutes(1));
        ChangeStreamPartitionSplit earlier = split("change-stream-0", WHOLE, restoredAt);
        ChangeStreamPartitionSplit later = split("change-stream-1", LEFT, restoredAt);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        restoredAt,
                        2,
                        Arrays.asList(earlier, later),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(WHOLE), restored, true, Duration.ZERO, Clock.systemUTC());
        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(earlier.splitId(), later.splitId());

        enumerator.handleSourceEvent(
                subtaskOf(context, earlier), emitted(earlier, successor(RIGHT, "right")));
        enumerator.handleSourceEvent(
                subtaskOf(context, later), emitted(later, successor(LEFT_HALF, "half")));
        assertThat(enumerator.heldParentCount()).isEqualTo(2);
        enumerator.addSplitsBack(Collections.singletonList(earlier), subtaskOf(context, earlier));
        enumerator.addSplitsBack(Collections.singletonList(later), subtaskOf(context, later));
        ready(enumerator);

        // The earlier one is read again at once; the later one waits for it.
        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(earlier.splitId(), later.splitId(), earlier.splitId());
        enumerator.handleSourceEvent(0, emitted(earlier, successor(RIGHT, "right")));
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        // The checkpoint releases the later one, to be read again, and the earlier one's successor,
        // and nothing else: the later one has not closed again since it was handed back.
        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(
                        earlier.splitId(),
                        later.splitId(),
                        earlier.splitId(),
                        later.splitId(),
                        "change-stream-2");
        enumerator.close();
    }

    @Test
    void withoutACompletedCheckpointAHoldEndsAfterTheTimeoutEvenAcrossACloseTakenBack()
            throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            BigtableChangeStreamSplitEnumerator enumerator =
                    started(context, client(WHOLE), null, true, Duration.ofMinutes(10), clock);
            ChangeStreamPartitionSplit parent = only(context);
            PartitionTransitionEvent close = emitted(parent, successor(WHOLE, "successor"));
            enumerator.handleSourceEvent(0, close);
            // A savepoint snapshots the coordinator, but Flink reports no completion for it.
            enumerator.snapshotState(1);
            pollAfter(enumerator, context, 1);

            clock.advance(Duration.ofMinutes(9));
            enumerator.addSplitsBack(Collections.singletonList(parent), 0);
            ready(enumerator);
            assertThat(enumerator.pendingCloseCount()).isZero();
            clock.advance(Duration.ofMinutes(1).minusMillis(1));
            context.runPeriodicAsyncCalls();
            assertThat(assignedIds(context)).doesNotContain("change-stream-1");

            // The hold's age survived the close being taken back.
            clock.advance(Duration.ofMillis(1));
            context.runPeriodicAsyncCalls();
            assertThat(assignedIds(context)).contains("change-stream-1");
            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains(FALLBACK_WARNING))
                    .singleElement()
                    .satisfies(message -> assertThat(message).contains("partitionHandoff"));

            // The parent being read again stays a re-read: its close creates nothing twice.
            assertThat(enumerator.snapshotState(2).getRereadSplitIds())
                    .containsExactly(parent.splitId());
            pollAfter(enumerator, context, 2);
            enumerator.handleSourceEvent(0, close);
            assertThat(enumerator.snapshotState(3).getNextSplitId()).isEqualTo(2);
            pollAfter(enumerator, context, 3);
            assertThat(enumerator.snapshotState(3).getRereadSplitIds()).isEmpty();
            pollAfter(enumerator, context, 3);

            // Until a checkpoint completes, a close holds nothing.
            ChangeStreamPartitionSplit successor = byId(context, "change-stream-1");
            enumerator.handleSourceEvent(
                    subtaskOf(context, successor), emitted(successor, successor(LEFT, "next")));
            assertThat(assignedIds(context)).contains("change-stream-2");

            // Once one has, a close holds for good: the timeout never fires again.
            enumerator.notifyCheckpointComplete(4);
            ChangeStreamPartitionSplit next = byId(context, "change-stream-2");
            enumerator.handleSourceEvent(
                    subtaskOf(context, next), emitted(next, successor(LEFT, "last")));
            clock.advance(Duration.ofDays(1));
            context.runPeriodicAsyncCalls();
            assertThat(assignedIds(context)).doesNotContain("change-stream-3");
            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains(FALLBACK_WARNING))
                    .hasSize(1);
            enumerator.close();
        }
    }

    @Test
    void aCloseOfTheReReadDoesNotRestartTheHoldsClock() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(WHOLE), null, true, Duration.ofMinutes(10), clock);
        ChangeStreamPartitionSplit parent = only(context);
        PartitionTransitionEvent close = emitted(parent, successor(WHOLE, "successor"));
        enumerator.handleSourceEvent(0, close);

        clock.advance(Duration.ofMinutes(9));
        enumerator.addSplitsBack(Collections.singletonList(parent), 0);
        ready(enumerator);
        enumerator.handleSourceEvent(0, close);
        clock.advance(Duration.ofMinutes(1));
        context.runPeriodicAsyncCalls();

        assertThat(assignedIds(context)).contains("change-stream-1");
        enumerator.close();
    }

    @Test
    void aSuccessorWaitsWhenEveryReaderIsFullToo() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client(LEFT, RIGHT),
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        false,
                        Clock.systemUTC(),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        // No free slot anywhere: what is assigned goes to the queue of the least-loaded reader.
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(0, 0, 0));
        ChangeStreamPartitionSplit left = byId(context, "change-stream-0");
        ChangeStreamPartitionSplit right = byId(context, "change-stream-1");

        enumerator.handleSourceEvent(
                subtaskOf(context, left), emitted(left, successor(LEFT, "left")));
        enumerator.handleSourceEvent(
                subtaskOf(context, right), transition(right, successor(RIGHT, "right")));

        // The quiet partition's successor is handed out beside the held one, which stays.
        assertThat(assignedIds(context)).contains("change-stream-3");
        assertThat(assignedIds(context)).doesNotContain("change-stream-2");
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).contains("change-stream-2");
        enumerator.close();
    }

    @Test
    void anImmediateHandOffWarnsOnlyAboutItsClosesAfterTheTimeout() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            BigtableChangeStreamSplitEnumerator enumerator =
                    started(context, client(WHOLE), null, false, Duration.ofMinutes(10), clock);
            enumerator.handleSourceEvent(0, emitted(only(context), successor(WHOLE, "successor")));
            clock.advance(Duration.ofMinutes(10));
            context.runPeriodicAsyncCalls();

            assertThat(enumerator.pendingCloseCount()).isZero();
            assertThat(capture.getMessages())
                    .filteredOn(message -> message.contains(FALLBACK_WARNING))
                    .singleElement()
                    .satisfies(message -> assertThat(message).doesNotContain("partitionHandoff"));
            enumerator.close();
        }
    }

    @Test
    void aZeroTimeoutHoldsUntilACheckpointCompletes() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(WHOLE), null, true, Duration.ZERO, clock);
        enumerator.handleSourceEvent(0, emitted(only(context), successor(WHOLE, "successor")));

        clock.advance(Duration.ofDays(1));
        context.runPeriodicAsyncCalls();
        assertThat(assignedIds(context)).doesNotContain("change-stream-1");

        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).contains("change-stream-1");
        enumerator.close();
    }

    @Test
    void aMergeChildDoesNotWaitForAParentThatEmittedNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(LEFT, RIGHT), null, true, Duration.ZERO, Clock.systemUTC());
        ChangeStreamPartitionSplit left = byId(context, "change-stream-0");
        ChangeStreamPartitionSplit right = byId(context, "change-stream-1");
        enumerator.handleSourceEvent(0, emitted(left, successorForParent(WHOLE, LEFT, "left")));
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);

        // The emitting parent's close is final; the quiet one's completes the merge.
        enumerator.handleSourceEvent(1, transition(right, successorForParent(WHOLE, RIGHT, "r")));

        assertThat(assignedIds(context)).contains("change-stream-2");
        enumerator.close();
    }

    @Test
    void aCloseARestoredReaderTakesBackKeepsItsHoldAndItsAge() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(WHOLE), null, true, Duration.ofMinutes(10), clock);
        ChangeStreamPartitionSplit parent = only(context);
        PartitionTransitionEvent close = emitted(parent, successor(WHOLE, "successor"));
        enumerator.handleSourceEvent(0, close);

        // A region failover restored the parent's reader from a checkpoint before the close.
        clock.advance(Duration.ofMinutes(9));
        enumerator.handleSourceEvent(
                0, new PartitionsRestoredEvent(Collections.singletonList(parent.splitId())));
        assertThat(enumerator.pendingCloseCount()).isZero();
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).doesNotContain("change-stream-1");

        // A completion ended the timeout, so only the re-read's final close releases the hold.
        enumerator.handleSourceEvent(0, close);
        enumerator.snapshotState(2);
        pollAfter(enumerator, context, 2);
        enumerator.notifyCheckpointComplete(2);
        assertThat(assignedIds(context)).contains("change-stream-1");
        assertThat(enumerator.snapshotState(3).getNextSplitId()).isEqualTo(2);
        enumerator.close();
    }

    @Test
    void aHoldARestoredReaderTakesBackKeepsItsAgeForTheTimeout() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                started(context, client(WHOLE), null, true, Duration.ofMinutes(10), clock);
        ChangeStreamPartitionSplit parent = only(context);
        enumerator.handleSourceEvent(0, emitted(parent, successor(WHOLE, "successor")));

        clock.advance(Duration.ofMinutes(9));
        enumerator.handleSourceEvent(
                0, new PartitionsRestoredEvent(Collections.singletonList(parent.splitId())));
        clock.advance(Duration.ofMinutes(1));
        context.runPeriodicAsyncCalls();

        assertThat(assignedIds(context)).contains("change-stream-1");
        enumerator.close();
    }

    @Test
    void aBoundedRunIsNotCompleteWhileASuccessorWaits() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client(WHOLE),
                        StartPosition.latest(),
                        null,
                        null,
                        true,
                        false,
                        Clock.systemUTC(),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        ready(enumerator);
        enumerator.handleSourceEvent(0, emitted(only(context), successor(WHOLE, "successor")));

        assertThat(context.readersToldNoMoreSplits()).isEmpty();
        enumerator.snapshotState(1);
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        ChangeStreamPartitionSplit successor = byId(context, "change-stream-1");
        enumerator.handleSourceEvent(subtaskOf(context, successor), transition(successor));

        assertThat(context.readersToldNoMoreSplits()).containsExactlyInAnyOrder(0, 1);
        enumerator.close();
    }

    @Test
    void aReconcilerRecoveryOfPartOfAHeldRangeWaitsToo() throws Exception {
        // One parent of a merge closed and the other has not, so the merged range is uncovered;
        // the scan recovers the closed parent's part from its token after the token grace.
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client(LEFT, RIGHT),
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        true,
                        clock,
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        ready(enumerator);
        ChangeStreamPartitionSplit left = byId(context, "change-stream-0");
        enumerator.handleSourceEvent(
                subtaskOf(context, left), emitted(left, successorForParent(WHOLE, LEFT, "left")));

        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKEN_GRACE);
        context.runPeriodicAsyncCalls();

        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactly(LEFT);
        pollAfter(enumerator, context, 1);
        assertThat(assignedIds(context)).doesNotContain("change-stream-2");
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).contains("change-stream-2");
        enumerator.close();
    }

    @Test
    void aTokenlessRestartOfPartOfAHeldRangeWaitsToo() throws Exception {
        // From #2245: the service split again at "g", so the held parent's token is wider than
        // either gap and the reconciler restarts both without a token after the longer grace.
        AdvanceableClock clock = new AdvanceableClock(START);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        ScriptedChangeStreamCoordinatorClient client = client(LEFT, RIGHT);
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
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        ready(enumerator);
        ChangeStreamPartitionSplit left = byId(context, "change-stream-0");
        enumerator.handleSourceEvent(
                subtaskOf(context, left), emitted(left, successorForParent(WHOLE, LEFT, "left")));

        client.replacePartitions(
                ByteStringRange.unbounded().endOpen("g"),
                ByteStringRange.unbounded().startClosed("g"));
        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKENLESS_GRACE);
        context.runPeriodicAsyncCalls();

        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS))
                .isEqualTo(2);
        assertThat(assignedIds(context))
                .containsExactlyInAnyOrder(left.splitId(), "change-stream-1");
        // Both gaps, each once, are waiting, rather than one gap twice.
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactlyInAnyOrder(
                        ByteStringRange.unbounded().endOpen("g"), ByteStringRange.create("g", "m"));
        pollAfter(enumerator, context, 1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(assignedIds(context)).hasSize(4);
        enumerator.close();
    }

    static FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context(int parallelism) {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i);
        }
        return context;
    }

    static ScriptedChangeStreamCoordinatorClient client(ByteStringRange... partitions) {
        return ScriptedChangeStreamCoordinatorClient.with(partitions);
    }

    static BigtableChangeStreamSplitEnumerator started(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            boolean hold,
            Duration firstCheckpointTimeout) {
        return started(
                context, client(WHOLE), null, hold, firstCheckpointTimeout, Clock.systemUTC());
    }

    static BigtableChangeStreamSplitEnumerator started(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client,
            BigtableChangeStreamEnumeratorState restored,
            boolean hold,
            Duration firstCheckpointTimeout,
            Clock clock) {
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client,
                        StartPosition.latest(),
                        null,
                        restored,
                        false,
                        false,
                        clock,
                        firstCheckpointTimeout,
                        hold);
        enumerator.start();
        context.runAsyncCalls();
        ready(enumerator);
        return enumerator;
    }

    /** Every reader reports plenty of free slots, so whatever is assignable is assigned. */
    static void ready(BigtableChangeStreamSplitEnumerator enumerator) {
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(8, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(8, 0, 0));
    }

    private static List<String> assignedIds(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context) {
        List<String> ids = new ArrayList<>();
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            for (ChangeStreamPartitionSplit split : context.assignedSplits(subtask)) {
                ids.add(split.splitId());
            }
        }
        return ids;
    }

    static ChangeStreamPartitionSplit only(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context) {
        return byId(context, "change-stream-0");
    }

    private static ChangeStreamPartitionSplit byId(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, String splitId) {
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            for (ChangeStreamPartitionSplit split : context.assignedSplits(subtask)) {
                if (split.splitId().equals(splitId)) {
                    return split;
                }
            }
        }
        throw new AssertionError(splitId + " was never assigned");
    }

    static int subtaskOf(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ChangeStreamPartitionSplit split) {
        for (int subtask = 0; subtask < context.currentParallelism(); subtask++) {
            for (ChangeStreamPartitionSplit assigned : context.assignedSplits(subtask)) {
                if (assigned.splitId().equals(split.splitId())) {
                    return subtask;
                }
            }
        }
        throw new AssertionError(split.splitId() + " was never assigned");
    }

    private static ChangeStreamPartitionSplit split(
            String splitId, ByteStringRange partition, Instant lowWatermark) {
        return new ChangeStreamPartitionSplit(
                splitId, partition, Collections.emptyList(), lowWatermark);
    }

    static PartitionTransitionEvent emitted(
            ChangeStreamPartitionSplit parent, PartitionTransitionEvent.Successor... successors) {
        return new PartitionTransitionEvent(
                parent.splitId(), parent.getLowWatermark(), Arrays.asList(successors), true);
    }

    static PartitionTransitionEvent transition(
            ChangeStreamPartitionSplit parent, PartitionTransitionEvent.Successor... successors) {
        return new PartitionTransitionEvent(
                parent.splitId(), parent.getLowWatermark(), Arrays.asList(successors), false);
    }

    static PartitionTransitionEvent.Successor successor(ByteStringRange partition, String token) {
        return successorForParent(partition, partition, token);
    }

    private static PartitionTransitionEvent.Successor successorForParent(
            ByteStringRange target, ByteStringRange parent, String token) {
        return new PartitionTransitionEvent.Successor(
                target, TestChangeStreamTokens.token(parent, token));
    }

    /** A clock a test can push past a timeout without waiting for one. */
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

    /** Every registered reader reports polling after the snapshot, as a running reader does. */
    private static void pollAfter(
            BigtableChangeStreamSplitEnumerator enumerator,
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            long checkpointId) {
        for (Integer subtaskId : new ArrayList<>(context.registeredReaders().keySet())) {
            enumerator.handleSourceEvent(subtaskId, new ReaderPolledEvent(checkpointId));
        }
    }
}
