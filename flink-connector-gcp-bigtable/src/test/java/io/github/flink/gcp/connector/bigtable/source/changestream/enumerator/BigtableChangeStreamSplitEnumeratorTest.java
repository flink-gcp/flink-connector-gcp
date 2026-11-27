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
import org.apache.flink.util.FlinkRuntimeException;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorStateSerializer;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionAckEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionsRestoredEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class BigtableChangeStreamSplitEnumeratorTest {

    private static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    private static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");
    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();
    private static final ByteStringRange FIRST = ByteStringRange.unbounded().endOpen("f");
    private static final ByteStringRange SECOND = ByteStringRange.create("f", "s");
    private static final ByteStringRange THIRD = ByteStringRange.unbounded().startClosed("s");

    @Test
    void anEnumeratorBuiltOnTheSameGroupKeepsReportingThroughTheFirstRegistrations()
            throws Exception {
        // A coordinator reset builds the next enumerator on the group the first one registered
        // on, where Flink drops a second registration of a name (#1908).
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator first =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        first.start();
        first.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        context.runAsyncCalls();
        first.close();

        BigtableChangeStreamSplitEnumerator second =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        second.start();
        second.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        context.runAsyncCalls();

        assertThat(context.counter(BigtableMetricNames.SPLITS_ASSIGNED)).isEqualTo(2);
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(2);
        second.close();
    }

    @Test
    void fillsOnlyFreeSlotsUntilEveryReaderHasReported() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE),
                        null);

        enumerator.start();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        context.runAsyncCalls();

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0", "change-stream-1");
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-2");

        // Subtask 1 has still not reported, so only a slot subtask 0 frees takes the third split.
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 1, 2));

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0", "change-stream-1", "change-stream-2");
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(3);
        assertThat(context.counter(BigtableMetricNames.SPLITS_ASSIGNED)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void aReportOlderThanItsSplitsAddsNoSlots() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE),
                        null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));

        // The same report again, sent before the two splits reached the reader, still has them in
        // flight: they fill the two slots it advertises.
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));

        assertThat(context.assignedSplits(0)).hasSize(2);
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).hasSize(1);
        enumerator.close();
    }

    @Test
    void handsSplitsBeyondItsFreeSlotsToTheOnlyReader() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE, FIRST),
                        null);

        enumerator.start();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        context.runAsyncCalls();

        // The reader queues the two its open reads cannot take and rotates them in at service
        // heartbeats; waiting for a free slot would leave them unread while its partitions last.
        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly(
                        "change-stream-0", "change-stream-1", "change-stream-2", "change-stream-3");
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
        assertThat(checkpoint.getAssignedSplits())
                .containsExactlyElementsOf(context.assignedSplits(0));
        assertThat(context.<Long>gauge("unassignedSplits")).isZero();
        assertThat(context.counter(BigtableMetricNames.SPLITS_ASSIGNED)).isEqualTo(4);
        enumerator.close();
    }

    @Test
    void handsTheExcessToTheReaderHoldingTheFewest() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(
                                LEFT, RIGHT, WHOLE, FIRST, SECOND, THIRD),
                        null);
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2, 0, 0));

        // Free slots first: one to subtask 0 and two to subtask 1. The three left then go one at a
        // time to whichever holds fewer, the lower subtask on a tie.
        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0", "change-stream-3", "change-stream-4");
        assertThat(context.assignedSplits(1))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", "change-stream-2", "change-stream-5");
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).isEmpty();
        enumerator.close();
    }

    @Test
    void weighsTheSplitsEachReaderReportsHolding() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT), null);
        enumerator.start();
        context.runAsyncCalls();

        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 5, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(0, 1, 0));

        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(context.assignedSplits(1)).hasSize(2);
        enumerator.close();
    }

    @Test
    void countsTheSplitsAReaderHasNotYetAcknowledged() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(
                                LEFT, RIGHT, WHOLE, FIRST, SECOND, THIRD),
                        null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));
        List<ChangeStreamPartitionSplit> lost = context.assignedSplits(1);
        assertThat(context.assignedSplits(0)).hasSize(3);
        assertThat(lost).hasSize(3);

        // Subtask 0 reports having received only its first split; the other two are still in
        // flight to it, so it still holds three when subtask 1 fails and its splits come back.
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 1, 1));
        context.unregisterReader(1);
        enumerator.addSplitsBack(lost, 1);
        context.registerReader(1);
        enumerator.addReader(1);
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));

        assertThat(context.assignedSplits(1).subList(3, 6)).containsExactlyElementsOf(lost);
        assertThat(context.assignedSplits(0)).hasSize(3);
        enumerator.close();
    }

    @Test
    void holdsTheExcessBackWhileAFailedReaderIsAway() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE, FIRST),
                        null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2, 0, 0));
        List<ChangeStreamPartitionSplit> lost = context.assignedSplits(1);
        assertThat(lost).hasSize(2);

        // Flink unregisters a failed reader before it returns the reader's splits.
        context.unregisterReader(1);
        enumerator.addSplitsBack(lost, 1);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 2, 2));

        assertThat(context.assignedSplits(0)).hasSize(2);
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .containsExactlyElementsOf(lost);

        // Registered again, but until it reports, its report from before the failure counts for
        // nothing, so subtask 0's next report still hands nothing out.
        context.registerReader(1);
        enumerator.addReader(1);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 2, 2));

        assertThat(context.assignedSplits(0)).hasSize(2);

        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2, 0, 0));

        assertThat(context.assignedSplits(1).subList(2, 4)).containsExactlyElementsOf(lost);
        assertThat(context.assignedSplits(0)).hasSize(2);
        assertThat(enumerator.snapshotState(2).getUnassignedSplits()).isEmpty();
        enumerator.close();
    }

    @Test
    void aFailedAttemptsReportDeferredByInitializationIsNotReplayedForTheNextOne()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE, FIRST),
                        null);
        enumerator.start();

        // Subtask 0's first attempt reports and fails, and its next attempt registers and reports,
        // all before initialization finishes; subtask 1 never reports.
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        context.unregisterReader(0);
        enumerator.addSplitsBack(Collections.emptyList(), 0);
        context.registerReader(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        context.runAsyncCalls();

        // Replayed, the failed attempt's report would fill two slots the next attempt does not
        // have, ahead of the excess subtask 1 has yet to report for.
        assertThat(context.assignedSplits(0)).hasSize(2);
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).hasSize(2);
        enumerator.close();
    }

    @Test
    void handsAFullReaderTheSuccessorsOfItsSplit() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 1, 1));

        enumerator.handleSourceEvent(
                0, transition(parent, successor(LEFT, "left"), successor(RIGHT, "right")));

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactly(WHOLE, LEFT, RIGHT);
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
        assertThat(checkpoint.getAssignedSplits())
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactly(LEFT, RIGHT);
        enumerator.close();
    }

    @Test
    void redistributesReturnedSplitsDeterministically() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofMinutes(1));
        ChangeStreamPartitionSplit first = restoredSplit("change-stream-7", LEFT, recent);
        ChangeStreamPartitionSplit second = restoredSplit("change-stream-8", RIGHT, recent);
        ChangeStreamPartitionSplit third = restoredSplit("change-stream-9", WHOLE, recent);
        ChangeStreamPartitionSplit fourth = restoredSplit("change-stream-10", LEFT, recent);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        recent,
                        11,
                        Collections.emptyList(),
                        Arrays.asList(first, second, third, fourth),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), restored);
        enumerator.start();
        context.runAsyncCalls();

        enumerator.addSplitsBack(Arrays.asList(first, second, third, fourth), 0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2, 0, 0));

        assertThat(context.assignedSplits(0)).containsExactly(first, fourth);
        assertThat(context.assignedSplits(1)).containsExactly(second, third);
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.SPLITS_RETURNED)).isEqualTo(4);
        enumerator.close();
    }

    @Test
    void reportsUnassignedLagFromTheOldestPartitionPosition() throws Exception {
        Instant lowWatermark = Instant.now().minus(Duration.ofMinutes(1));
        Instant metricTime = lowWatermark.plusSeconds(5);
        ChangeStreamPartitionSplit older = restoredSplit("change-stream-7", LEFT, lowWatermark);
        ChangeStreamPartitionSplit newer =
                restoredSplit("change-stream-8", RIGHT, lowWatermark.plusSeconds(2));
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        lowWatermark,
                        9,
                        Arrays.asList(older, newer),
                        Collections.emptyList(),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        StartPosition.latest(),
                        null,
                        restored,
                        false,
                        false,
                        Clock.fixed(metricTime, ZoneOffset.UTC),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();

        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis"))
                .isEqualTo(5_000L);
        enumerator.close();
    }

    @Test
    void generatesInitialPartitionsOnceAndServesAWaitingReader() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT);
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.assignedSplits(0)).isEmpty();

        context.runAsyncCalls();

        assertThat(client.validationCalls()).isEqualTo(1);
        assertThat(client.generationCalls()).isEqualTo(1);
        // latest() is the one fresh-start form that needs no retention metadata.
        assertThat(client.retentionCalls()).isZero();
        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0");
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).hasSize(1);
        enumerator.close();
    }

    @Test
    void aSplitCloseCreatesEachChildWithoutWaitingForAnotherParent() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0, 1, 1));

        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        parent.splitId(),
                        parent.getLowWatermark(),
                        Arrays.asList(successor(LEFT, "left"), successor(RIGHT, "right")),
                        false));

        // Where the children went is the assignment policy's business; that both exist at once,
        // each with its own token, is this test's.
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(2);
        List<ChangeStreamPartitionSplit> children =
                new ArrayList<>(checkpoint.getUnassignedSplits());
        children.addAll(checkpoint.getAssignedSplits());
        assertThat(children)
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactlyInAnyOrder(LEFT, RIGHT);
        assertThat(children)
                .flatExtracting(ChangeStreamPartitionSplit::getContinuationTokens)
                .extracting(ChangeStreamContinuationToken::getToken)
                .containsExactlyInAnyOrder("left", "right");
        assertThat(checkpoint.getPendingMerges()).isEmpty();
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(3);
        assertThat(context.counter("changeStreamPartitionSplits")).isEqualTo(2);
        assertThat(context.counter("changeStreamPartitionMerges")).isZero();
        enumerator.close();
    }

    @Test
    void aMergeWaitsUntilEveryParentTokenArrives() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);
        enumerator.handleSplitRequest(0, "localhost");

        enumerator.handleSourceEvent(
                0, transition(left, successorForParent(WHOLE, LEFT, "left-parent")));

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(enumerator.pendingMergeMaterializations()).isZero();
        assertThat(enumerator.snapshotState(2).getPendingMerges())
                .singleElement()
                .satisfies(
                        merge ->
                                assertThat(merge.getContinuationTokens())
                                        .extracting(ChangeStreamContinuationToken::getToken)
                                        .containsExactly("left-parent"));

        enumerator.handleSourceEvent(
                1, transition(right, successorForParent(WHOLE, RIGHT, "right-parent")));

        assertThat(context.assignedSplits(0)).hasSize(2);
        ChangeStreamPartitionSplit merged = context.assignedSplits(0).get(1);
        assertThat(merged.getPartition()).isEqualTo(WHOLE);
        assertThat(merged.getContinuationTokens())
                .extracting(ChangeStreamContinuationToken::getToken)
                .containsExactly("left-parent", "right-parent");
        assertThat(enumerator.snapshotState(3).getPendingMerges()).isEmpty();
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(3);
        assertThat(context.counter("changeStreamPartitionSplits")).isZero();
        assertThat(context.counter("changeStreamPartitionMerges")).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void overlappingParentTokensDoNotCompleteAMerge() throws Exception {
        ByteStringRange target =
                ByteStringRange.create(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("z"));
        ByteStringRange overlappingLeft =
                ByteStringRange.create(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("n"));
        ByteStringRange overlappingRight =
                ByteStringRange.create(ByteString.copyFromUtf8("m"), ByteString.copyFromUtf8("z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(1).get(0);

        enumerator.handleSourceEvent(
                0, transition(left, successorForParent(target, overlappingLeft, "left-parent")));
        enumerator.handleSourceEvent(
                1, transition(right, successorForParent(target, overlappingRight, "right-parent")));

        assertThat(enumerator.snapshotState(2).getPendingMerges())
                .singleElement()
                .satisfies(
                        merge ->
                                assertThat(merge.getContinuationTokens())
                                        .extracting(ChangeStreamContinuationToken::getToken)
                                        .containsExactly("left-parent", "right-parent"));
        assertThat(enumerator.snapshotState(2).getUnassignedSplits()).isEmpty();
        enumerator.close();
    }

    @Test
    void restoreNeverRegeneratesInitialPartitions() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-7", LEFT, Collections.emptyList(), recent);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        recent,
                        8,
                        Collections.singletonList(split),
                        Collections.emptyList(),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client, restored);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(client.generationCalls()).isZero();
        assertThat(client.retentionCalls()).isEqualTo(1);
        assertThat(context.assignedSplits(0)).containsExactly(split);
        enumerator.close();
    }

    @Test
    void replaysATransitionThatArrivesWhileRestoreIsInitializing() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit restoredSplit =
                new ChangeStreamPartitionSplit(
                        "change-stream-7", WHOLE, Collections.emptyList(), recent);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        recent,
                        8,
                        Collections.emptyList(),
                        Collections.singletonList(restoredSplit),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), restored);

        enumerator.start();
        enumerator.handleSourceEvent(0, transition(restoredSplit, successor(WHOLE, "next")));
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-8");
        enumerator.close();
    }

    @Test
    void reconcilesASplitReturnedWhileRestoreIsInitializing() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit restoredSplit =
                new ChangeStreamPartitionSplit(
                        "change-stream-7", WHOLE, Collections.emptyList(), recent);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        recent,
                        8,
                        Collections.emptyList(),
                        Collections.singletonList(restoredSplit),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), restored);

        enumerator.start();
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);
        context.runAsyncCalls();

        assertThat(enumerator.snapshotState(0).getAssignedSplits()).isEmpty();
        assertThat(enumerator.snapshotState(0).getUnassignedSplits())
                .containsExactly(restoredSplit);
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.assignedSplits(0)).containsExactly(restoredSplit);
        assertThat(enumerator.snapshotState(1).getAssignedSplits()).containsExactly(restoredSplit);
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).isEmpty();
        enumerator.close();
    }

    @Test
    void restoresAPartialMergeAndContinuesSplitIdsFromTheLedger() throws Exception {
        Instant recent = Instant.now().minus(Duration.ofHours(1));
        ChangeStreamPartitionSplit right =
                new ChangeStreamPartitionSplit(
                        "change-stream-7", RIGHT, Collections.emptyList(), recent);
        PendingMerge partialMerge =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(
                                TestChangeStreamTokens.token(LEFT, "left-parent")),
                        recent);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        recent,
                        8,
                        Collections.emptyList(),
                        Collections.singletonList(right),
                        Collections.singletonList(partialMerge));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), restored);

        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(
                0, transition(right, successorForParent(WHOLE, RIGHT, "right-parent")));
        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-8");
        ChangeStreamPartitionSplit merged = context.assignedSplits(0).get(0);
        assertThat(merged.getContinuationTokens())
                .extracting(ChangeStreamContinuationToken::getToken)
                .containsExactly("left-parent", "right-parent");
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getNextSplitId()).isEqualTo(9);
        assertThat(checkpoint.getPendingMerges()).isEmpty();
        assertThat(checkpoint.getAssignedSplits()).containsExactly(merged);
        enumerator.close();
    }

    @Test
    void returnedSplitClearsFailedReaderCapacityAndServesAnotherReader() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assigned = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1, 1, 1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1, 0, 0));

        enumerator.addSplitsBack(Collections.singletonList(assigned), 0);

        assertThat(context.assignedSplits(0)).containsExactly(assigned);
        assertThat(context.assignedSplits(1)).containsExactly(assigned);
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(2);
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
        assertThat(checkpoint.getAssignedSplits()).containsExactly(assigned);
        enumerator.close();
    }

    @Test
    void expiredRestoreFailsByDefault() throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-7", WHOLE, Collections.emptyList(), expired);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        expired,
                        8,
                        Collections.singletonList(split),
                        Collections.emptyList(),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                Duration.ofDays(7), Collections.singletonList(WHOLE)),
                        restored);

        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to initialize Bigtable Change Streams")
                .hasRootCauseInstanceOf(FlinkRuntimeException.class)
                .hasStackTraceContaining("older than the computed earliest position");
    }

    @Test
    void expiredRestoreUsesTheOptInFallbackWithoutItsOldToken() throws Exception {
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "change-stream-7",
                        WHOLE,
                        Collections.singletonList(
                                TestChangeStreamTokens.token(WHOLE, "expired-token")),
                        expired);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        expired,
                        8,
                        Collections.singletonList(split),
                        Collections.emptyList(),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                new ScriptedChangeStreamCoordinatorClient(
                        Duration.ofDays(7), Collections.singletonList(WHOLE));
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client,
                        StartPosition.latest(),
                        StartPosition.latest(),
                        restored,
                        false,
                        false,
                        Clock.systemUTC(),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        ChangeStreamPartitionSplit restarted = context.assignedSplits(0).get(0);
        assertThat(restarted.splitId()).isEqualTo(split.splitId());
        assertThat(restarted.getContinuationTokens()).isEmpty();
        assertThat(restarted.getLowWatermark()).isAfter(expired);
        enumerator.close();
    }

    /**
     * Expiry is a property of the low watermark a tokenless restart would read from, not of the
     * grace timer, so the {@code firstObserved} here is deliberately recent and retained. The
     * reconciliation-disabled {@code enumerator(...)} helper pins that the check happens at
     * initialize regardless of whether a reconciler would ever consume the partition.
     */
    @Test
    void expiredRestoredMissingPartitionFailsByDefault() throws Exception {
        Instant now = Instant.now();
        BigtableChangeStreamEnumeratorState restored =
                restoredWithMissing(
                        now.minusSeconds(60),
                        7,
                        new MissingPartition(
                                WHOLE, now.minusSeconds(120), now.minus(Duration.ofDays(8))));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                Duration.ofDays(7), Collections.singletonList(WHOLE)),
                        restored);

        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to initialize Bigtable Change Streams")
                .hasRootCauseInstanceOf(FlinkRuntimeException.class)
                .hasStackTraceContaining("older than the computed earliest position");
    }

    /**
     * The opt-in fallback replaces only the expired low watermark; the grace timer survives the
     * rebuild, as checkpointed timers always have. With the timer already elapsed the first scan
     * restarts immediately — a rebased timer would make that scan a no-op, which is what the final
     * assertions rule out.
     */
    @Test
    void expiredRestoredMissingPartitionUsesTheOptInFallbackAndKeepsItsGraceTimer()
            throws Exception {
        Instant base = Instant.now();
        MissingPartition missing =
                new MissingPartition(
                        WHOLE,
                        base.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        base.minus(Duration.ofDays(8)));
        BigtableChangeStreamEnumeratorState restored =
                restoredWithMissing(base.minusSeconds(60), 7, missing);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                Duration.ofDays(7), Collections.singletonList(WHOLE)),
                        StartPosition.latest(),
                        StartPosition.latest(),
                        restored,
                        false,
                        true,
                        Clock.fixed(base, ZoneOffset.UTC),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();

        // The resolver reads the system clock, so the fallback resolves at or after base; an
        // exact-value assertion is impossible, but "no longer the expired position" is not.
        assertThat(enumerator.snapshotState(1).getMissingPartitions())
                .singleElement()
                .satisfies(
                        rebuilt -> {
                            assertThat(rebuilt.getFirstObserved())
                                    .isEqualTo(missing.getFirstObserved());
                            assertThat(rebuilt.getLowWatermark()).isAfterOrEqualTo(base);
                        });

        enumerator.handleSplitRequest(0, "localhost");
        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getContinuationTokens()).isEmpty();
                            assertThat(split.getLowWatermark()).isAfterOrEqualTo(base);
                        });
        assertThat(enumerator.snapshotState(2).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS))
                .isEqualTo(1);
        enumerator.close();
    }

    @Test
    void closesTheCoordinatorClientItOwns() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        enumerator.close();

        assertThat(client.closeCalls()).isEqualTo(1);
    }

    @Test
    void logsACoordinatorClientThatRefusesToCloseBeforeThrowing() {
        // Flink's source coordinator closes an enumerator through IOUtils.closeQuietly, which drops
        // what close() throws, so at job shutdown the log line is the failure's only trace.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        IllegalStateException failure = new IllegalStateException("channel still in use");
        client.failOnClose(failure);
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            assertThatThrownBy(enumerator::close).isInstanceOf(IOException.class).hasCause(failure);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("WARN ");
                                assertThat(event.getMessage())
                                        .contains("Change Streams coordinator");
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void logsNothingWhenTheCoordinatorClientCloses() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);

        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            enumerator.close();

            assertThat(capture.getEvents()).isEmpty();
        }
    }

    @Test
    void logsAnErrorFromTheCoordinatorClientsCloseAndThrowsItAsItself() {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        NoClassDefFoundError failure = new NoClassDefFoundError("released class loader");
        client.failOnClose(failure);
        BigtableChangeStreamSplitEnumerator enumerator = enumerator(context, client, null);

        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            assertThatThrownBy(enumerator::close).isSameAs(failure);

            assertThat(capture.getEvents())
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.toString()).startsWith("WARN ");
                                assertThat(event.getMessage())
                                        .contains("Change Streams coordinator");
                                assertThat(event.getThrowable()).isSameAs(failure);
                            });
        }
    }

    @Test
    void closesTheCoordinatorClientWhenAGaugeThrowsAnErrorOnItsLastRead() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        Instant lowWatermark = Instant.now().minus(Duration.ofMinutes(1));
        AdvanceableClock clock = new AdvanceableClock(lowWatermark.plusSeconds(5));
        BigtableChangeStreamSplitEnumerator enumerator =
                withAnUnassignedPartition(context, client, lowWatermark, clock);
        StackOverflowError failure = new StackOverflowError("gauge");
        clock.failWith(failure);

        assertThatThrownBy(enumerator::close).isSameAs(failure);

        assertThat(client.closeCalls()).isEqualTo(1);
    }

    @Test
    void aGaugeStopsReadingAnEnumeratorWhoseCoordinatorClientRefusesToClose() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        client.failOnClose(new IllegalStateException("channel still in use"));
        Instant lowWatermark = Instant.now().minus(Duration.ofMinutes(1));
        AdvanceableClock clock = new AdvanceableClock(lowWatermark.plusSeconds(5));
        BigtableChangeStreamSplitEnumerator enumerator =
                withAnUnassignedPartition(context, client, lowWatermark, clock);
        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis"))
                .isEqualTo(5_000L);

        assertThatThrownBy(enumerator::close).isInstanceOf(IOException.class);
        clock.advance(Duration.ofSeconds(1));

        assertThat(context.<Long>gauge("unassignedChangeStreamPartitionLagMillis"))
                .isEqualTo(5_000L);
    }

    /** A started enumerator holding one unassigned partition, so its lag gauge reads the clock. */
    private static BigtableChangeStreamSplitEnumerator withAnUnassignedPartition(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client,
            Instant lowWatermark,
            Clock clock) {
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        lowWatermark,
                        9,
                        Collections.singletonList(
                                restoredSplit("change-stream-7", WHOLE, lowWatermark)),
                        Collections.emptyList(),
                        Collections.emptyList());
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
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }

    @Test
    void periodicReconciliationRestoresCheckpointedTokenlessTimer() throws Exception {
        // Anchored to the real clock: the restored ledger must pass the system-clock
        // restore-expiry check, which a parse-anchored date fails once it ages past retention.
        Instant now = Instant.now();
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        now.minusSeconds(60),
                        7,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new MissingPartition(
                                        WHOLE,
                                        now.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                                        now.minusSeconds(90))));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        StartPosition.latest(),
                        null,
                        restored,
                        false,
                        true,
                        Clock.fixed(now, ZoneOffset.UTC),
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.splitId()).isEqualTo("change-stream-7");
                            assertThat(split.getContinuationTokens()).isEmpty();
                            assertThat(split.getLowWatermark()).isEqualTo(now.minusSeconds(90));
                        });
        assertThat(enumerator.snapshotState(1).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isEqualTo(1);
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS))
                .isEqualTo(1);
        enumerator.close();
    }

    @Test
    void rejectsCheckpointBeforeAsynchronousInitializationCompletes() throws Exception {
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context(1), ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();

        assertThatThrownBy(() -> enumerator.snapshotState(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("initialization is still outstanding");
        enumerator.close();
    }

    @Test
    void aReturnedSplitWhoseCloseNoCheckpointCoversIsReadAgainFromWhereItWasAssigned()
            throws Exception {
        // #2206: a region failover returns a split assigned after the restored checkpoint, and
        // its downstream rolled back with it, so a close after that checkpoint is taken back.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, progress(parent, "later", parent.getLowWatermark()));
        enumerator.handleSourceEvent(
                0, transition(parent, successor(LEFT, "left"), successor(RIGHT, "right")));

        enumerator.addSplitsBack(Collections.singletonList(parent), 0);

        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getAssignedSplits()).isEmpty();
        assertThat(checkpoint.getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", "change-stream-2", parent.splitId());
        assertThat(checkpoint.getUnassignedSplits().get(2)).isEqualTo(parent);
        assertThat(checkpoint.getRereadSplitIds()).containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void aReaderRegisteredAfterBoundedCompletionIsToldNothingIsComing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(2);
        context.registerReader(0);
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
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit only = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        only.splitId(), only.getLowWatermark(), Collections.emptyList(), false));
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);

        // SourceCoordinator registers a reader in its context before addReader reaches us.
        context.registerReader(1);
        enumerator.addReader(1);
        // A new attempt of a subtask already told, with nothing to return, is told again.
        enumerator.addReader(0);

        assertThat(context.readersToldNoMoreSplits()).containsExactly(0, 1);
        assertThat(context.events())
                .filteredOn(event -> event.startsWith("noMoreSplits:"))
                .containsExactly("noMoreSplits:0", "noMoreSplits:1", "noMoreSplits:0");
        assertThat(context.assignedSplits(1)).isEmpty();
        enumerator.close();
    }

    @Test
    void boundedSourceSignalsCompletionOnlyAfterEveryPartitionFinishes() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(3);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT),
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
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        enumerator.handleSplitRequest(2, "localhost");
        ChangeStreamPartitionSplit first = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit second = context.assignedSplits(1).get(0);

        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        first.splitId(), first.getLowWatermark(), Collections.emptyList(), false));

        assertThat(context.readersToldNoMoreSplits()).isEmpty();
        enumerator.handleSourceEvent(
                1,
                new PartitionTransitionEvent(
                        second.splitId(),
                        second.getLowWatermark(),
                        Collections.emptyList(),
                        false));
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0, 1, 2);
        enumerator.close();
    }

    /**
     * The test above runs with reconciliation off, which is the combination that hid #951. A
     * bounded partition reaches the end time by closing with no successors, so it leaves the ledger
     * and nothing replaces it — while the service keeps reporting its range for as long as the
     * table exists. A scan that lands after one partition has finished and before the last one has
     * therefore called a finished partition missing, and a non-empty missing ledger blocks the
     * completion signal that is the only thing that stops the scans.
     */
    @Test
    void aReconciliationScanBetweenTwoBoundedCompletionsDoesNotBlockTheCompletionSignal()
            throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                boundedReconciling(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT),
                        null,
                        Clock.fixed(Instant.now(), ZoneOffset.UTC));
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit first = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit second = context.assignedSplits(1).get(0);

        enumerator.handleSourceEvent(0, transition(first));
        context.runPeriodicAsyncCalls();
        enumerator.handleSourceEvent(1, transition(second));

        assertThat(context.readersToldNoMoreSplits()).containsExactly(0, 1);
        assertThat(enumerator.snapshotState(1).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isZero();
        enumerator.close();
    }

    /**
     * The sharper half of the same defect. Here the run never drains, so its completion flag never
     * turns the scans off, and only the record of what the run already finished can keep a
     * partition that reached the end time from being restarted twenty minutes later.
     */
    @Test
    void aBoundedRunDoesNotRestartAFinishedPartitionWhileAnotherIsStillReading() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.now());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(2);
        BigtableChangeStreamSplitEnumerator enumerator =
                boundedReconciling(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT),
                        null,
                        clock);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSplitRequest(1, "localhost");
        ChangeStreamPartitionSplit first = context.assignedSplits(0).get(0);

        enumerator.handleSourceEvent(0, transition(first));
        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKENLESS_GRACE.plusMinutes(1));
        context.runPeriodicAsyncCalls();

        assertThat(context.readersToldNoMoreSplits()).isEmpty();
        assertThat(enumerator.snapshotState(1).getUnassignedSplits()).isEmpty();
        assertThat(enumerator.snapshotState(2).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS)).isZero();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isZero();
        enumerator.close();
    }

    /**
     * The completed ranges have to survive a restore, or a bounded run that checkpointed midway
     * through draining would come back believing the service keyspace it already read is missing.
     */
    @Test
    void aRestoredBoundedRunKeepsTheRangesItAlreadyFinished() throws Exception {
        Instant now = Instant.now();
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        now.minusSeconds(60),
                        7,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Arrays.asList(LEFT, RIGHT));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                boundedReconciling(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT),
                        restored,
                        Clock.fixed(now, ZoneOffset.UTC));
        enumerator.start();
        context.runAsyncCalls();

        context.runPeriodicAsyncCalls();

        assertThat(enumerator.snapshotState(1).getCompletedPartitions())
                .containsExactly(LEFT, RIGHT);
        assertThat(enumerator.snapshotState(2).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isZero();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        enumerator.close();
    }

    /**
     * The counter says a range was uncovered; only the warning says by what, and it has to name
     * what the scan saw rather than what the restart left behind — the restart puts the range
     * straight back into the unassigned collection, so a rendering taken after the loop would
     * report the partition as covered by the very split the warning is about.
     */
    @Test
    void theTokenlessWarningNamesTheLedgerTheScanMeasuredAgainst() throws Exception {
        Instant now = Instant.now();
        MissingPartition missing =
                new MissingPartition(
                        RIGHT,
                        now.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        now.minusSeconds(90));
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        now.minusSeconds(60),
                        7,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(missing),
                        Collections.singletonList(LEFT));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);

        try (LogCapture capture = LogCapture.of(BigtableChangeStreamSplitEnumerator.class)) {
            BigtableChangeStreamSplitEnumerator enumerator =
                    boundedReconciling(
                            context,
                            ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT),
                            restored,
                            Clock.fixed(now, ZoneOffset.UTC));
            enumerator.start();
            context.runAsyncCalls();

            context.runPeriodicAsyncCalls();

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("unassigned none")
                    .contains("assigned none")
                    .contains("pending merges none")
                    .contains("completed " + RowRanges.format(LEFT));
            enumerator.close();
        }
    }

    /**
     * The opposite direction, which is what keeps the fix from being "stop reconciling". A
     * continuous run has no end time to close a stream at, so a partition that disappears without
     * successors is a loss rather than a completion and must still be restarted.
     */
    @Test
    void aContinuousRunStillRestartsAPartitionThatVanishedWithoutSuccessors() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.now());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
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
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit only = context.assignedSplits(0).get(0);

        enumerator.handleSourceEvent(0, transition(only));
        context.runPeriodicAsyncCalls();
        clock.advance(ChangeStreamPartitionReconciler.TOKENLESS_GRACE.plusMinutes(1));
        context.runPeriodicAsyncCalls();

        assertThat(enumerator.snapshotState(1).getCompletedPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_TOKENLESS_RESTARTS))
                .isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aBoundedSourceWithNoPartitionsTellsAReaderThatAsksThatNothingIsComing() throws Exception {
        // The completion signal above is reached through a partition transition. This is the path
        // that has no transition to ride on: a bounded run whose partition set is empty is drained
        // the moment it starts, and the only thing that ever happens is a reader asking for work.
        // Without the signal on that path the reader waits for a split that cannot arrive.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(),
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
        assertThat(context.readersToldNoMoreSplits()).isEmpty();

        enumerator.handleSplitRequest(0, "localhost");

        assertThat(context.readersToldNoMoreSplits()).containsExactly(0);
        assertThat(context.assignedSplits(0)).isEmpty();
        enumerator.close();
    }

    @Test
    void everyProgressReportReplacesTheAssignedPositionEvenAnOlderOne() throws Exception {
        // A committed report follows the live ones a reader sent before its first checkpoint
        // completed, and it is older: the ledger takes it, because it is what a failover restores.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assigned = context.assignedSplits(0).get(0);
        Instant live = assigned.getLowWatermark().plusSeconds(60);
        enumerator.handleSourceEvent(0, progress(assigned, "live", live));

        enumerator.handleSourceEvent(0, progress(assigned, "committed", live.minusSeconds(30)));
        enumerator.handleSourceEvent(
                0,
                progress(
                        restoredSplit("change-stream-99", WHOLE, live),
                        "unknown",
                        live.plusSeconds(30)));

        assertThat(enumerator.snapshotState(1).getAssignedSplits())
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.splitId()).isEqualTo(assigned.splitId());
                            assertThat(split.getLowWatermark()).isEqualTo(live.minusSeconds(30));
                            assertThat(split.getContinuationTokens())
                                    .containsExactly(
                                            TestChangeStreamTokens.token(WHOLE, "committed"));
                        });
        enumerator.close();
    }

    @Test
    void aSourceEventOfAnUnknownKindIsRejected() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();

        assertThatThrownBy(() -> enumerator.handleSourceEvent(0, new SourceEvent() {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Bigtable Change Streams source event");
        enumerator.close();
    }

    @Test
    void aRepeatedTransitionDoesNotReapplyItsSuccessors() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        PartitionTransitionEvent transition =
                transition(parent, successor(LEFT, "left"), successor(RIGHT, "right"));

        enumerator.handleSourceEvent(0, transition);
        enumerator.handleSourceEvent(0, transition);

        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", "change-stream-2");
        enumerator.close();
    }

    /**
     * A tokenless restart may not ask for a position the change stream no longer retains, so the
     * recovery starts at the retention edge instead.
     *
     * <p>The enumerator clock is fixed two hours ahead of the resolver's startup instant to model a
     * position that was retained when the job was restored and then aged past retention while the
     * job ran, as a backpressured ledger's tracked watermark can against a short retention. Aging
     * during downtime is caught by the restore-expiry check on the next restore instead, so only
     * this mid-run path reaches the clamp; the clamp, not an expiry failure, is the answer here
     * because mid-run there is no restore decision point left to fail.
     */
    @Test
    void aMissingPartitionAgingPastRetentionMidRunRestartsAtTheRetentionEdge() throws Exception {
        Instant restoreTime = Instant.now();
        Instant scanTime = restoreTime.plus(Duration.ofHours(2));
        Duration retention = Duration.ofHours(1);
        BigtableChangeStreamEnumeratorState restored =
                restoredWithMissing(
                        restoreTime.minusSeconds(60),
                        7,
                        new MissingPartition(
                                WHOLE,
                                scanTime.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                                restoreTime.minusSeconds(60)));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                reconciling(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                retention, Collections.singletonList(WHOLE)),
                        restored,
                        scanTime);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split ->
                                assertThat(split.getLowWatermark())
                                        .isEqualTo(scanTime.minus(retention).plusSeconds(60)));
        enumerator.close();
    }

    @Test
    void aFailedReconciliationScanKeepsTheMissingPartitionForTheNextOne() throws Exception {
        // Anchored to the real clock: the restored ledger must pass the system-clock
        // restore-expiry check, which a parse-anchored date fails once it ages past retention.
        Instant now = Instant.now();
        MissingPartition missing =
                new MissingPartition(
                        WHOLE,
                        now.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        now.minusSeconds(90));
        BigtableChangeStreamEnumeratorState restored =
                restoredWithMissing(now.minusSeconds(60), 7, missing);
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        BigtableChangeStreamSplitEnumerator enumerator =
                reconciling(context, client, restored, now);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        client.failNextGenerationWith(new IllegalStateException("scripted scan failure"));

        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(enumerator.snapshotState(1).getMissingPartitions()).containsExactly(missing);
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isZero();

        // The timer the failed scan preserved is what the next one recovers from, which is the
        // reason to keep it rather than an incidental consequence of returning early.
        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(enumerator.snapshotState(2).getMissingPartitions()).isEmpty();
        assertThat(context.counter(BigtableMetricNames.CHANGE_STREAM_PARTITIONS_RECONCILED))
                .isEqualTo(1);
        enumerator.close();
    }

    @Test
    void thePublicConstructorTurnsReconciliationOn() throws Exception {
        // The only case here that goes through the constructor production takes. Every other one
        // uses the package-private seam and passes the flag explicitly, so without this the
        // hardcoded true could be flipped to false -- every job silently stops reconciling -- and
        // no test in the module would notice.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        ScriptedChangeStreamCoordinatorClient client =
                ScriptedChangeStreamCoordinatorClient.with(WHOLE);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        client,
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        int beforeScan = client.retentionCalls();

        context.runPeriodicAsyncCalls();
        context.runAsyncCalls();

        // A scan reads retention before it can decide anything, so the count moving is the scan
        // having run. With reconciliation off nothing is registered for runPeriodicAsyncCalls to
        // run at all, and the count stands still. No clock assertion, so no wall-clock coupling.
        assertThat(client.retentionCalls()).isGreaterThan(beforeScan);
        enumerator.close();
    }

    @Test
    void acknowledgesEveryTransitionAppliedOrNot() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        PartitionTransitionEvent transition = transition(parent, successor(LEFT, "left"));

        enumerator.handleSourceEvent(0, transition);
        enumerator.handleSourceEvent(0, transition);
        enumerator.handleSourceEvent(
                0, transition(restoredSplit("change-stream-99", WHOLE, Instant.now())));

        assertThat(context.sourceEvents(0))
                .extracting(event -> ((PartitionTransitionAckEvent) event).getSplitId())
                .containsExactly(parent.splitId(), parent.splitId(), "change-stream-99");
        enumerator.close();
    }

    @Test
    void aCloseIsFinalOnlyOnceACheckpointTakenAfterItCompletes() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        ChangeStreamPartitionSplit left = context.assignedSplits(0).get(0);
        ChangeStreamPartitionSplit right = context.assignedSplits(0).get(1);
        enumerator.handleSourceEvent(0, transition(left, successor(LEFT, "left-child")));
        enumerator.snapshotState(1);
        enumerator.handleSourceEvent(0, transition(right, successor(RIGHT, "right-child")));

        enumerator.notifyCheckpointComplete(1);

        // Checkpoint 1's snapshot ran after the left close, so it covers it; the right close
        // came after that snapshot and is still pending.
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.addSplitsBack(Arrays.asList(left, right), 0);
        assertThat(enumerator.snapshotState(2).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly(right.splitId());

        enumerator.notifyCheckpointComplete(2);
        assertThat(enumerator.pendingCloseCount()).isZero();
        enumerator.close();
    }

    @Test
    void aSplitReadAgainAfterItsCloseCreatesItsSuccessorsOnce() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        PartitionTransitionEvent close =
                transition(parent, successor(LEFT, "left"), successor(RIGHT, "right"));
        enumerator.handleSourceEvent(0, close);
        enumerator.addSplitsBack(Collections.singletonList(parent), 0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(3, 0, 0));

        enumerator.handleSourceEvent(0, close);

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly(
                        parent.splitId(), "change-stream-1", "change-stream-2", parent.splitId());
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
        assertThat(checkpoint.getAssignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", "change-stream-2");
        assertThat(checkpoint.getNextSplitId()).isEqualTo(3);
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.notifyCheckpointComplete(1);
        assertThat(enumerator.pendingCloseCount()).isZero();
        assertThat(enumerator.snapshotState(2).getRereadSplitIds()).isEmpty();
        enumerator.close();
    }

    @Test
    void aRestoredReaderTakesBackAPendingCloseAndItsNextCloseCreatesNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        PartitionTransitionEvent close = transition(parent, successor(LEFT, "left"));
        enumerator.handleSourceEvent(0, close);

        // A region failover restored the parent from a checkpoint taken before it closed.
        enumerator.handleSourceEvent(
                0,
                new PartitionsRestoredEvent(
                        Arrays.asList(parent.splitId(), "change-stream-unknown")));

        assertThat(enumerator.pendingCloseCount()).isZero();
        BigtableChangeStreamEnumeratorState reopened = enumerator.snapshotState(1);
        assertThat(reopened.getAssignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly(parent.splitId());
        assertThat(reopened.getRereadSplitIds()).containsExactly(parent.splitId());
        assertThat(reopened.getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");

        enumerator.handleSourceEvent(0, close);

        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        BigtableChangeStreamEnumeratorState reclosed = enumerator.snapshotState(2);
        assertThat(reclosed.getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");
        assertThat(reclosed.getNextSplitId()).isEqualTo(2);
        enumerator.close();
    }

    @Test
    void aRestoreKeepsTheRereadMarkSoTheNextCloseCreatesNothing() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        PartitionTransitionEvent close = transition(parent, successor(LEFT, "left"));
        enumerator.handleSourceEvent(0, close);
        enumerator.addSplitsBack(Collections.singletonList(parent), 0);
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(1);
        BigtableChangeStreamSplitEnumerator restored =
                enumerator(
                        restoredContext,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        new BigtableChangeStreamEnumeratorStateSerializer()
                                .deserialize(
                                        new BigtableChangeStreamEnumeratorStateSerializer()
                                                .getVersion(),
                                        new BigtableChangeStreamEnumeratorStateSerializer()
                                                .serialize(checkpoint)));
        restored.start();
        restoredContext.runAsyncCalls();
        restored.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        assertThat(restoredContext.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1", parent.splitId());

        restored.handleSourceEvent(0, close);

        BigtableChangeStreamEnumeratorState reclosed = restored.snapshotState(2);
        assertThat(reclosed.getAssignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");
        assertThat(reclosed.getUnassignedSplits()).isEmpty();
        assertThat(reclosed.getNextSplitId()).isEqualTo(2);
        restored.close();
    }

    @Test
    void aTransitionTheRestoredSnapshotMissedIsAppliedWhenTheReaderReportsItAgain()
            throws Exception {
        // #2206: the close was reported after the coordinator's snapshot and before the reader's
        // barrier, so the restored ledger holds the split as assigned.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        enumerator.close();

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> restoredContext = context(1);
        BigtableChangeStreamSplitEnumerator restored =
                enumerator(
                        restoredContext,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        checkpoint);
        restored.start();
        // The re-sent transition arrives before initialization completes and waits for it.
        restored.handleSourceEvent(0, transition(parent, successor(WHOLE, "successor")));
        restoredContext.runAsyncCalls();
        restored.handleSourceEvent(0, new ReaderCapacityEvent(1, 0, 0));

        assertThat(restoredContext.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");
        assertThat(restoredContext.sourceEvents(0))
                .extracting(event -> ((PartitionTransitionAckEvent) event).getSplitId())
                .containsExactly(parent.splitId());
        restored.close();
    }

    @Test
    void withoutACompletedCheckpointClosesBecomeFinalOnceOneHasWaitedTheTimeout() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-11-21T00:00:00Z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE),
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        false,
                        clock,
                        Duration.ofMinutes(10),
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(3, 0, 0));
        List<ChangeStreamPartitionSplit> splits = context.assignedSplits(0);
        ChangeStreamPartitionSplit first = splits.get(0);
        ChangeStreamPartitionSplit second = splits.get(1);

        // The wait counts from a close, not from the start: nothing closed, so nothing is final.
        clock.advance(Duration.ofMinutes(11));
        context.runPeriodicAsyncCalls();
        enumerator.handleSourceEvent(0, transition(first));
        enumerator.handleSourceEvent(0, transition(second));
        assertThat(enumerator.pendingCloseCount()).isEqualTo(2);

        // Both are read again; the second closes again before the timeout, the first does not.
        enumerator.addSplitsBack(Arrays.asList(first, second), 0);
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2, 0, 0));
        enumerator.handleSourceEvent(0, transition(second));
        clock.advance(Duration.ofMinutes(10).minusMillis(1));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);

        clock.advance(Duration.ofMillis(1));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.pendingCloseCount()).isZero();
        // The first is still being read again, so it stays a re-read; the second is final.
        assertThat(enumerator.snapshotState(1).getRereadSplitIds())
                .containsExactly(first.splitId());
        enumerator.handleSourceEvent(0, transition(first));
        assertThat(enumerator.pendingCloseCount()).isZero();
        assertThat(enumerator.snapshotState(2).getRereadSplitIds()).isEmpty();
        enumerator.addSplitsBack(Arrays.asList(first, second), 0);
        assertThat(enumerator.snapshotState(3).getUnassignedSplits()).isEmpty();

        // A completed checkpoint makes closes wait for coverage again, for good.
        enumerator.notifyCheckpointComplete(3);
        enumerator.handleSourceEvent(0, transition(splits.get(2)));
        clock.advance(Duration.ofDays(1));
        context.runPeriodicAsyncCalls();
        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aNotificationForALaterCheckpointSettlesAnEarlierCoveredClose() throws Exception {
        // Flink may notify only the latest of several completed checkpoints.
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSourceEvent(0, transition(context.assignedSplits(0).get(0)));
        enumerator.snapshotState(3);
        enumerator.snapshotState(4);

        enumerator.notifyCheckpointComplete(4);

        assertThat(enumerator.pendingCloseCount()).isZero();
        enumerator.close();
    }

    @Test
    void aDeferredTransitionFromAReaderThatFailedSinceIsAppliedWithoutAnAck() throws Exception {
        // An unguarded send to an attempt without a gateway throws and fails the job in Flink.
        ChangeStreamPartitionSplit parent =
                restoredSplit("change-stream-0", WHOLE, Instant.now().minus(Duration.ofHours(1)));
        BigtableChangeStreamEnumeratorState checkpoint =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        parent.getLowWatermark(),
                        1,
                        Collections.emptyList(),
                        Collections.singletonList(parent),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), checkpoint);
        enumerator.start();
        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        parent.splitId(),
                        parent.getLowWatermark(),
                        Collections.singletonList(successor(WHOLE, "successor")),
                        false));
        context.unregisterReader(0);

        context.runAsyncCalls();

        assertThat(context.sourceEvents(0)).isEmpty();
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");
        enumerator.close();
    }

    @Test
    void aRestoreKeepsAnExpiredAssignedSplitForTheReaderThatRestoresIt() throws Exception {
        // Readers check the splits they restore; the ledger's copy of an assigned split carries
        // only the previous completed checkpoint's position, so it is not checked here.
        Instant expired = Instant.now().minus(Duration.ofDays(8));
        ChangeStreamPartitionSplit assigned =
                new ChangeStreamPartitionSplit(
                        "change-stream-0",
                        WHOLE,
                        Collections.singletonList(TestChangeStreamTokens.token(WHOLE, "old")),
                        expired);
        BigtableChangeStreamEnumeratorState checkpoint =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        expired,
                        1,
                        Collections.emptyList(),
                        Collections.singletonList(assigned),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                Duration.ofDays(7), Collections.singletonList(WHOLE)),
                        checkpoint);

        enumerator.start();
        context.runAsyncCalls();

        assertThat(enumerator.snapshotState(1).getAssignedSplits()).containsExactly(assigned);
        enumerator.close();
    }

    @Test
    void aDeferredTransitionFromAnEarlierAttemptIsNotAcknowledgedToTheNextOne() throws Exception {
        // The next attempt may close the same split again; a stale acknowledgement would drop
        // that close from its checkpoint before the coordinator had applied it.
        ChangeStreamPartitionSplit parent =
                restoredSplit("change-stream-0", WHOLE, Instant.now().minus(Duration.ofHours(1)));
        BigtableChangeStreamEnumeratorState checkpoint =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        parent.getLowWatermark(),
                        1,
                        Collections.emptyList(),
                        Collections.singletonList(parent),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), checkpoint);
        enumerator.start();
        enumerator.addReader(0);
        PartitionTransitionEvent close = transition(parent, successor(WHOLE, "successor"));
        enumerator.handleSourceEvent(0, close);
        enumerator.addReader(0);

        context.runAsyncCalls();

        assertThat(context.sourceEvents(0)).isEmpty();
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-1");
        // The current attempt's own transition is acknowledged.
        enumerator.handleSourceEvent(0, close);
        assertThat(context.sourceEvents(0))
                .extracting(event -> ((PartitionTransitionAckEvent) event).getSplitId())
                .containsExactly(parent.splitId());
        enumerator.close();
    }

    @Test
    void aZeroTimeoutKeepsClosesPendingWithoutACheckpoint() throws Exception {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-11-21T00:00:00Z"));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                new BigtableChangeStreamSplitEnumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(WHOLE),
                        StartPosition.latest(),
                        null,
                        null,
                        false,
                        false,
                        clock,
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        enumerator.handleSourceEvent(0, transition(context.assignedSplits(0).get(0)));

        clock.advance(Duration.ofDays(365));
        context.runPeriodicAsyncCalls();

        assertThat(enumerator.pendingCloseCount()).isEqualTo(1);
        enumerator.close();
    }

    @Test
    void aBoundedRunThatDrainedReadsAReturnedPendingCloseAgain() throws Exception {
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
                        Duration.ZERO,
                        true);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit only = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(0, transition(only));
        assertThat(context.events()).containsSubsequence("noMoreSplits:0", "noMoreSplits:1");

        // Subtask 0 fails before a checkpoint covers the close; its next attempt registers.
        enumerator.addSplitsBack(Collections.singletonList(only), 0);
        enumerator.addReader(0);
        enumerator.handleSplitRequest(0, "localhost");
        assertThat(context.assignedSplits(0)).containsExactly(only, only);
        enumerator.handleSourceEvent(0, transition(only));

        // The new attempt is told only after it has read the split again, and subtask 1, which was
        // told and may have finished, is not told again.
        assertThat(context.events())
                .containsExactly(
                        "assign:0",
                        "noMoreSplits:0",
                        "noMoreSplits:1",
                        "assign:0",
                        "noMoreSplits:0");
        assertThat(enumerator.snapshotState(1).getCompletedPartitions()).containsExactly(WHOLE);
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

    private static BigtableChangeStreamSplitEnumerator enumerator(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client,
            BigtableChangeStreamEnumeratorState restored) {
        return new BigtableChangeStreamSplitEnumerator(
                context,
                client,
                StartPosition.latest(),
                null,
                restored,
                false,
                false,
                Clock.systemUTC(),
                Duration.ZERO,
                true);
    }

    private static BigtableChangeStreamSplitEnumerator reconciling(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client,
            BigtableChangeStreamEnumeratorState restored,
            Instant now) {
        return new BigtableChangeStreamSplitEnumerator(
                context,
                client,
                StartPosition.latest(),
                null,
                restored,
                false,
                true,
                Clock.fixed(now, ZoneOffset.UTC),
                Duration.ZERO,
                true);
    }

    private static BigtableChangeStreamSplitEnumerator boundedReconciling(
            FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            ScriptedChangeStreamCoordinatorClient client,
            BigtableChangeStreamEnumeratorState restored,
            Clock clock) {
        return new BigtableChangeStreamSplitEnumerator(
                context,
                client,
                StartPosition.latest(),
                null,
                restored,
                true,
                true,
                clock,
                Duration.ZERO,
                true);
    }

    private static BigtableChangeStreamEnumeratorState restoredWithMissing(
            Instant startTime, long nextSplitId, MissingPartition missing) {
        return new BigtableChangeStreamEnumeratorState(
                true,
                startTime,
                nextSplitId,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(missing));
    }

    private static ChangeStreamPartitionSplit restoredSplit(
            String id, ByteStringRange partition, Instant lowWatermark) {
        return new ChangeStreamPartitionSplit(id, partition, Collections.emptyList(), lowWatermark);
    }

    private static PartitionProgressEvent progress(
            ChangeStreamPartitionSplit split, String token, Instant lowWatermark) {
        return new PartitionProgressEvent(
                Collections.singletonList(
                        PartitionProgressEvent.Progress.of(
                                new ChangeStreamPartitionSplit(
                                        split.splitId(),
                                        split.getPartition(),
                                        Collections.singletonList(
                                                TestChangeStreamTokens.token(
                                                        split.getPartition(), token)),
                                        lowWatermark))));
    }

    private static PartitionTransitionEvent transition(
            ChangeStreamPartitionSplit parent, PartitionTransitionEvent.Successor... successors) {
        return new PartitionTransitionEvent(
                parent.splitId(), parent.getLowWatermark(), Arrays.asList(successors), false);
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

    /** A clock a test can push past a grace period without waiting for one. */
    private static final class AdvanceableClock extends Clock {

        private Instant instant;

        /** What reading the clock throws, once a test sets it. */
        @Nullable private Error failure;

        private AdvanceableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration amount) {
            instant = instant.plus(amount);
        }

        private void failWith(Error failure) {
            this.failure = failure;
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
            if (failure != null) {
                throw failure;
            }
            return instant;
        }
    }
}
