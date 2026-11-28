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

import org.apache.flink.api.connector.source.SourceEvent;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionAckEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderPolledEvent;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.LEFT;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.RIGHT;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.WHOLE;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.client;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.context;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.emitted;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.only;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.ready;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.started;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.subtaskOf;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.successor;
import static io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSuccessorHoldTest.transition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the coordinator sends a reader that has not polled since its latest snapshot (#2404): a
 * stop-with-savepoint finishes a source task once the savepoint completes, and an event that
 * reaches the finished task fails it and the stop.
 */
@Timeout(30)
class BigtableChangeStreamSplitEnumeratorPollGateTest {

    @Test
    void aCompletedCheckpointSendsNothingToAReaderThatHasNotPolledSinceTheSnapshot()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        int otherReader = 1 - parentReader;
        enumerator.handleSourceEvent(
                parentReader, emitted(parent, successor(LEFT, "left"), successor(RIGHT, "right")));
        enumerator.snapshotState(1);
        List<String> events = context.events();
        List<SourceEvent> toParentReader = context.sourceEvents(parentReader);
        List<SourceEvent> toOtherReader = context.sourceEvents(otherReader);

        enumerator.notifyCheckpointComplete(1);
        // A capacity report reaches the assignment routines too.
        enumerator.handleSourceEvent(otherReader, new ReaderCapacityEvent(8, 0, 0));

        assertThat(enumerator.heldParentCount()).as("the close is final").isZero();
        assertThat(context.events()).isEqualTo(events);
        assertThat(context.sourceEvents(parentReader)).isEqualTo(toParentReader);
        assertThat(context.sourceEvents(otherReader)).isEqualTo(toOtherReader);

        enumerator.handleSourceEvent(otherReader, new ReaderPolledEvent(1));
        assertThat(context.assignedSplits(otherReader))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactlyInAnyOrder("change-stream-1", "change-stream-2");
        assertThat(context.assignedSplits(parentReader))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .as("the parent's reader has not polled")
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void aTransitionAcknowledgementWaitsForAPollAfterTheGatingSnapshot() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        enumerator.snapshotState(2);

        // A transition the reader sent before snapshot 2 can arrive after it, behind its poll for
        // 1.
        enumerator.handleSourceEvent(parentReader, emitted(parent, successor(WHOLE, "successor")));
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        assertThat(transitionAcknowledgements(context, parentReader)).isEmpty();

        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(2));
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(2));
        assertThat(transitionAcknowledgements(context, parentReader))
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void aBoundedRunsEndSignalWaitsForEachReadersPollAndGoesOutOnce() throws Exception {
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
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        enumerator.snapshotState(1);

        enumerator.handleSourceEvent(parentReader, transition(parent));
        assertThat(endSignals(context)).as("nobody has polled").isEmpty();

        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        assertThat(endSignals(context)).containsExactly("noMoreSplits:" + parentReader);

        enumerator.notifyCheckpointAborted(1);
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        assertThat(endSignals(context))
                .containsExactlyInAnyOrder("noMoreSplits:0", "noMoreSplits:1");
        enumerator.close();
    }

    @Test
    void anAbortedSnapshotLeavesTheLatestEarlierOneGatingUntilEveryOneIsAborted() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(parentReader, emitted(parent, successor(WHOLE, "successor")));
        // Snapshot 2 stands for a stop-with-savepoint's savepoint, snapshot 3 for a manual
        // savepoint taken while it is pending, which fails.
        enumerator.snapshotState(2);
        enumerator.snapshotState(3);
        enumerator.notifyCheckpointAborted(3);
        assertThat(transitionAcknowledgements(context, parentReader)).isEmpty();

        enumerator.notifyCheckpointAborted(2);
        assertThat(transitionAcknowledgements(context, parentReader))
                .as("snapshot 1 still gates")
                .isEmpty();
        enumerator.notifyCheckpointAborted(1);

        assertThat(transitionAcknowledgements(context, parentReader))
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void aNewAttemptIsReachedAtOnceAndInheritsNoAcknowledgementItsPredecessorWaitedFor()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, false, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        // Snapshot 1 never completes as far as the coordinator knows, as after an ordinary
        // savepoint, whose completion Flink does not announce.
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(
                parentReader, emitted(parent, successor(LEFT, "left"), successor(RIGHT, "right")));

        // The attempt fails; Flink returns its splits and registers the next attempt.
        context.unregisterReader(parentReader);
        enumerator.addSplitsBack(java.util.Collections.emptyList(), parentReader);
        context.registerReader(parentReader);
        enumerator.addReader(parentReader);
        enumerator.handleSourceEvent(parentReader, new ReaderCapacityEvent(8, 0, 0));

        assertThat(context.assignedSplits(parentReader))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .contains("change-stream-1", "change-stream-2");
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        assertThat(transitionAcknowledgements(context, parentReader)).isEmpty();
        enumerator.close();
    }

    @Test
    void theHandOutBeyondFreeSlotsGoesToTheLeastLoadedReaderThatPolled() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, false, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        int otherReader = 1 - parentReader;
        // Both readers are full and equally loaded, so a tie would pick subtask 0.
        enumerator.handleSourceEvent(parentReader, new ReaderCapacityEvent(0, 1, 1));
        enumerator.handleSourceEvent(otherReader, new ReaderCapacityEvent(0, 1, 0));
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(
                parentReader, emitted(parent, successor(LEFT, "left"), successor(RIGHT, "right")));
        assertThat(assignedIds(context)).containsExactly(parent.splitId());

        enumerator.handleSourceEvent(otherReader, new ReaderPolledEvent(1));

        assertThat(context.assignedSplits(otherReader))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactlyInAnyOrder("change-stream-1", "change-stream-2");
        assertThat(context.assignedSplits(parentReader)).containsExactly(parent);
        enumerator.close();
    }

    @Test
    void anAbortSendsAtOnceToAReaderThatReportedTheSnapshotNowGating() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        enumerator.snapshotState(2);
        enumerator.handleSourceEvent(parentReader, emitted(parent, successor(WHOLE, "successor")));
        assertThat(transitionAcknowledgements(context, parentReader)).isEmpty();

        enumerator.notifyCheckpointAborted(1);
        assertThat(transitionAcknowledgements(context, parentReader))
                .as("an older abort leaves snapshot 2 gating")
                .isEmpty();
        enumerator.notifyCheckpointAborted(2);

        assertThat(transitionAcknowledgements(context, parentReader))
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void abortingOnlyTheNewerSnapshotSendsAtOnceToAReaderThatReportedTheOlder() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(parentReader, new ReaderPolledEvent(1));
        enumerator.snapshotState(2);
        enumerator.handleSourceEvent(parentReader, emitted(parent, successor(WHOLE, "successor")));

        // The reader is idle and sends nothing more; the abort alone must deliver.
        enumerator.notifyCheckpointAborted(2);

        assertThat(transitionAcknowledgements(context, parentReader))
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void snapshotsEveryReaderHasPassedAreForgottenOnReportsAndRegistrations() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        // Ordinary savepoints, whose completion Flink does not announce.
        enumerator.snapshotState(1);
        enumerator.snapshotState(2);
        enumerator.snapshotState(3);
        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(3));
        assertThat(enumerator.heldSnapshotCount()).as("reader 1 reported only 1").isEqualTo(3);

        // Reader 1's attempt is replaced; the new one is reached at once, so 1 and 2 gate nobody.
        context.unregisterReader(1);
        context.registerReader(1);
        enumerator.addReader(1);

        assertThat(enumerator.heldSnapshotCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aBatchRecoverySnapshotGatesNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator = started(context, true, Duration.ZERO);
        ChangeStreamPartitionSplit parent = only(context);
        int parentReader = subtaskOf(context, parent);
        // Flink's BATCH_CHECKPOINT_ID, which a HybridSource forwards from batch job recovery.
        enumerator.snapshotState(-1);

        enumerator.handleSourceEvent(parentReader, emitted(parent, successor(WHOLE, "successor")));

        assertThat(transitionAcknowledgements(context, parentReader))
                .containsExactly(parent.splitId());
        enumerator.close();
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

    private static List<String> transitionAcknowledgements(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, int subtaskId) {
        List<String> splitIds = new ArrayList<>();
        for (SourceEvent event : context.sourceEvents(subtaskId)) {
            if (event instanceof PartitionTransitionAckEvent) {
                splitIds.add(((PartitionTransitionAckEvent) event).getSplitId());
            }
        }
        return splitIds;
    }

    /** Every end signal sent, repeats included, unlike {@code readersToldNoMoreSplits()}. */
    private static List<String> endSignals(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context) {
        List<String> signals = new ArrayList<>();
        for (String event : context.events()) {
            if (event.startsWith("noMoreSplits:")) {
                signals.add(event);
            }
        }
        return signals;
    }
}
