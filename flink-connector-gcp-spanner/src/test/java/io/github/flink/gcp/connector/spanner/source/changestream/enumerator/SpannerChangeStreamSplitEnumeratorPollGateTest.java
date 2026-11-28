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

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedAckEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.ReaderPolledEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarkEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarks;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.committedProgress;
import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.configured;
import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.context;
import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.finished;
import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.handOff;
import static io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumeratorTest.runningParents;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the coordinator sends a reader that has not polled since its latest snapshot (#2404): a
 * stop-with-savepoint finishes a source task once the savepoint completes, and an event that
 * reaches the finished task fails it and the stop.
 */
@Timeout(30)
class SpannerChangeStreamSplitEnumeratorPollGateTest {

    private static final Instant START = Instant.now().minus(Duration.ofHours(1));
    private static final long CHILD_WATERMARK =
            SpannerChangeStreamWatermarks.beforeInstant(START.plusSeconds(1));

    @Test
    void aCompletedCheckpointSendsNothingToAReaderThatHasNotPolledSinceTheSnapshot()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.handleSourceEvent(0, finished("p1", START));
        enumerator.handleSplitRequest(1, "localhost");
        enumerator.snapshotState(1);
        List<String> events = context.events();
        List<SourceEvent> toReader0 = context.sourceEvents(0);
        List<SourceEvent> toReader1 = context.sourceEvents(1);

        enumerator.notifyCheckpointComplete(1);

        assertThat(enumerator.pendingFinishCount()).as("the finish is final").isZero();
        assertThat(enumerator.heldParentCount()).as("the child is released").isZero();
        assertThat(context.events()).isEqualTo(events);
        assertThat(context.sourceEvents(0)).isEqualTo(toReader0);
        assertThat(context.sourceEvents(1)).isEqualTo(toReader1);

        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        assertThat(watermarksSince(context, 1, toReader1)).containsExactly(CHILD_WATERMARK);
        assertThat(context.sourceEvents(0)).as("reader 0 has not polled").isEqualTo(toReader0);

        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        assertThat(watermarksSince(context, 0, toReader0)).containsExactly(CHILD_WATERMARK);
        enumerator.close();
    }

    @Test
    void aPollReportedForAnOlderSnapshotOpensNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        enumerator.snapshotState(2);

        // A finish the reader sent before snapshot 2 can arrive after it, behind its poll for 1.
        enumerator.handleSourceEvent(0, finished("p1", START));
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        assertThat(finishAcknowledgements(context, 0)).isEmpty();

        enumerator.handleSourceEvent(0, new ReaderPolledEvent(2));
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(2));
        assertThat(finishAcknowledgements(context, 0))
                .containsExactly(ChangeStreamPartitionSplit.idForToken("p1"));
        enumerator.close();
    }

    @Test
    void aCommittedProgressReportSendsTheRaisedWatermarkOnlyToReadersThatPolled() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1", "p2");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        List<SourceEvent> toReader0 = context.sourceEvents(0);
        List<SourceEvent> toReader1 = context.sourceEvents(1);

        // Reader 0 reports from the completion of checkpoint 1, and then stops: the frontier, held
        // by the parents, moves to their children's start.
        enumerator.notifyCheckpointComplete(1);
        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        ChangeStreamPartitionSplit.idForToken("p1"),
                        START.plusSeconds(1),
                        START.plusSeconds(1)));
        enumerator.handleSourceEvent(
                0,
                committedProgress(
                        ChangeStreamPartitionSplit.idForToken("p2"),
                        START.plusSeconds(1),
                        START.plusSeconds(1)));

        assertThat(context.sourceEvents(0)).isEqualTo(toReader0);
        assertThat(watermarksSince(context, 1, toReader1)).containsExactly(CHILD_WATERMARK);
        enumerator.close();
    }

    @Test
    void aSplitRequestAfterTheSnapshotWaitsForTheReadersPoll() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        false,
                        Duration.ofMinutes(10),
                        runningParents(START, "p1"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, finished("p1", START));
        enumerator.snapshotState(1);

        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.assignedSplits(1)).isEmpty();

        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        enumerator.close();
    }

    @Test
    void aBoundedLedgersEndSignalWaitsForTheReadersPoll() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        null,
                        Instant.now().plus(Duration.ofHours(1)),
                        null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit running = context.assignedSplits(0).get(0);
        enumerator.snapshotState(1);

        enumerator.handleSourceEvent(
                0,
                new PartitionFinishedEvent(
                        running.splitId(),
                        running.getCurrentPosition(),
                        running.getWatermark(),
                        false));
        assertThat(context.events()).containsExactly("assign:0");

        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1");
        enumerator.close();
    }

    @Test
    void anAbortedSnapshotLeavesTheLatestEarlierOneGating() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.handleSplitRequest(1, "localhost");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, finished("p1", START));
        // Snapshot 2 stands for a stop-with-savepoint's savepoint, snapshot 3 for a manual
        // savepoint taken while it is pending, which fails.
        enumerator.snapshotState(2);
        enumerator.snapshotState(3);
        enumerator.notifyCheckpointAborted(3);
        enumerator.notifyCheckpointComplete(2);
        assertThat(context.assignedSplits(1)).as("snapshot 2 still gates").isEmpty();
        assertThat(finishAcknowledgements(context, 0)).isEmpty();

        enumerator.handleSourceEvent(1, new ReaderPolledEvent(2));
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        enumerator.close();
    }

    @Test
    void abortingEverySnapshotSendsWhatWaited() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, finished("p1", START));
        enumerator.snapshotState(2);
        List<SourceEvent> toReader0 = context.sourceEvents(0);

        enumerator.notifyCheckpointAborted(2);
        assertThat(finishAcknowledgements(context, 0)).as("snapshot 1 still gates").isEmpty();
        enumerator.notifyCheckpointAborted(1);

        assertThat(finishAcknowledgements(context, 0))
                .containsExactly(ChangeStreamPartitionSplit.idForToken("p1"));
        assertThat(watermarksSince(context, 0, toReader0)).isEmpty();
        enumerator.close();
    }

    @Test
    void abortingTheOlderSnapshotFirstStillSendsWhatWaitedOnceTheNewerIsAborted() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, finished("p1", START));
        enumerator.snapshotState(2);

        enumerator.notifyCheckpointAborted(1);
        assertThat(finishAcknowledgements(context, 0)).as("snapshot 2 still gates").isEmpty();
        enumerator.notifyCheckpointAborted(2);

        assertThat(finishAcknowledgements(context, 0))
                .containsExactly(ChangeStreamPartitionSplit.idForToken("p1"));
        enumerator.close();
    }

    @Test
    void anAbortSendsAtOnceToAReaderThatReportedTheSnapshotNowGating() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        enumerator.snapshotState(2);
        enumerator.handleSourceEvent(0, finished("p1", START));
        assertThat(finishAcknowledgements(context, 0)).isEmpty();

        // Reader 0 is idle and sends nothing more; the abort alone must deliver.
        enumerator.notifyCheckpointAborted(2);

        assertThat(finishAcknowledgements(context, 0))
                .containsExactly(ChangeStreamPartitionSplit.idForToken("p1"));
        enumerator.close();
    }

    @Test
    void snapshotsEveryReaderHasPassedAreForgottenOnReportsAndRegistrations() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
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
        SpannerChangeStreamSplitEnumerator enumerator = holding(context, "p1");
        // Flink's BATCH_CHECKPOINT_ID, which a HybridSource forwards from batch job recovery.
        enumerator.snapshotState(-1);

        enumerator.handleSourceEvent(0, finished("p1", START));

        assertThat(finishAcknowledgements(context, 0))
                .containsExactly(ChangeStreamPartitionSplit.idForToken("p1"));
        enumerator.close();
    }

    @Test
    void aNewAttemptIsReachedAtOnceAndInheritsNothingItsPredecessorWaitedFor() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        false,
                        Duration.ofMinutes(10),
                        runningParents(START, "p1", "p2"),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        // Snapshot 1 never completes as far as the coordinator knows, as after an ordinary
        // savepoint, whose completion Flink does not announce.
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(1, finished("p2", START));
        enumerator.handleSourceEvent(0, new ReaderPolledEvent(1));
        enumerator.handleSplitRequest(0, "localhost");
        // Nothing is left to assign, so reader 1's request waits.
        enumerator.handleSplitRequest(1, "localhost");

        // The attempt fails; Flink returns its splits and registers the next attempt.
        context.unregisterReader(1);
        enumerator.addSplitsBack(java.util.Collections.emptyList(), 1);
        context.registerReader(1);
        enumerator.addReader(1);
        enumerator.handleSourceEvent(0, finished("p1", START));
        assertThat(context.assignedSplits(1)).as("the old attempt's request is moot").isEmpty();

        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .containsExactly("p1-child");
        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        assertThat(finishAcknowledgements(context, 1)).as("the old attempt's").isEmpty();
        enumerator.close();
    }

    @Test
    void aSplitRequestAfterTheBoundedLedgerDrainedWaitsForThePollToo() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        SpannerChangeStreamSplitEnumerator enumerator =
                configured(
                        context,
                        StartPosition.latest(),
                        null,
                        Instant.now().plus(Duration.ofHours(1)),
                        null);
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
        enumerator.snapshotState(1);

        enumerator.handleSplitRequest(1, "localhost");
        assertThat(context.events()).containsExactly("assign:0");

        enumerator.handleSourceEvent(1, new ReaderPolledEvent(1));
        assertThat(context.events()).containsExactly("assign:0", "noMoreSplits:1");
        enumerator.close();
    }

    /** A started coordinator holding children, with each named parent running. */
    private static SpannerChangeStreamSplitEnumerator holding(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, String... parents)
            throws Exception {
        SpannerChangeStreamSplitEnumerator enumerator =
                handOff(
                        context,
                        true,
                        Duration.ofMinutes(10),
                        runningParents(START, parents),
                        new AtomicLong());
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }

    private static List<String> finishAcknowledgements(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context, int subtaskId) {
        List<String> splitIds = new ArrayList<>();
        for (SourceEvent event : context.sourceEvents(subtaskId)) {
            if (event instanceof PartitionFinishedAckEvent) {
                splitIds.add(((PartitionFinishedAckEvent) event).getSplitId());
            }
        }
        return splitIds;
    }

    /** The source watermarks sent to the reader since {@code before} was copied. */
    private static List<Long> watermarksSince(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            int subtaskId,
            List<SourceEvent> before) {
        List<SourceEvent> all = context.sourceEvents(subtaskId);
        assertThat(all.subList(0, before.size())).isEqualTo(before);
        List<Long> watermarks = new ArrayList<>();
        for (SourceEvent event : all.subList(before.size(), all.size())) {
            if (event instanceof SpannerChangeStreamWatermarkEvent) {
                watermarks.add(((SpannerChangeStreamWatermarkEvent) event).getSourceWatermark());
            }
        }
        return watermarks;
    }
}
