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
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.ReaderCapacityEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class BigtableChangeStreamSplitEnumeratorTest {

    private static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    private static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");
    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();

    @Test
    void assignsOnlyTheAbsoluteCapacityAdvertisedByAReader() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(
                        context,
                        ScriptedChangeStreamCoordinatorClient.with(LEFT, RIGHT, WHOLE),
                        null);

        enumerator.start();
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(2));
        context.runAsyncCalls();

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0", "change-stream-1");
        assertThat(enumerator.snapshotState(1).getUnassignedSplits())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-2");

        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1));

        assertThat(context.assignedSplits(0))
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-0", "change-stream-1", "change-stream-2");
        assertThat(context.counter("changeStreamPartitionsDiscovered")).isEqualTo(3);
        assertThat(context.counter(BigtableMetricNames.SPLITS_ASSIGNED)).isEqualTo(3);
        enumerator.close();
    }

    @Test
    void redistributesRestoredSplitsDeterministicallyWithinReaderCapacity() throws Exception {
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
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(2));
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1));

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
                        Optional.empty(),
                        restored,
                        false,
                        false,
                        Clock.fixed(metricTime, ZoneOffset.UTC));
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
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(0));

        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        parent.splitId(),
                        parent.getLowWatermark(),
                        Arrays.asList(successor(LEFT, "left"), successor(RIGHT, "right"))));

        assertThat(context.assignedSplits(0)).containsExactly(parent);
        List<ChangeStreamPartitionSplit> children =
                enumerator.snapshotState(2).getUnassignedSplits();
        assertThat(children)
                .extracting(ChangeStreamPartitionSplit::getPartition)
                .containsExactlyInAnyOrder(LEFT, RIGHT);
        assertThat(children)
                .flatExtracting(ChangeStreamPartitionSplit::getContinuationTokens)
                .extracting(ChangeStreamContinuationToken::getToken)
                .containsExactlyInAnyOrder("left", "right");
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);
        assertThat(enumerator.snapshotState(2).getPendingMerges()).isEmpty();
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
        enumerator.handleSourceEvent(0, new ReaderCapacityEvent(1));
        enumerator.handleSourceEvent(1, new ReaderCapacityEvent(1));

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
                        Optional.of(StartPosition.latest()),
                        restored);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        ChangeStreamPartitionSplit restarted = context.assignedSplits(0).get(0);
        assertThat(restarted.splitId()).isEqualTo(split.splitId());
        assertThat(restarted.getContinuationTokens()).isEmpty();
        assertThat(restarted.getLowWatermark()).isAfter(expired);
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
    void periodicReconciliationRestoresCheckpointedTokenlessTimer() throws Exception {
        Instant now = Instant.parse("2026-08-11T12:00:00Z");
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
                        Optional.empty(),
                        restored,
                        false,
                        true,
                        Clock.fixed(now, ZoneOffset.UTC));
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
    void lateReturnedParentIsNotResurrectedAfterItsTransition() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit parent = context.assignedSplits(0).get(0);
        enumerator.handleSourceEvent(
                0,
                new PartitionTransitionEvent(
                        parent.splitId(), parent.getLowWatermark(), Collections.emptyList()));

        enumerator.addSplitsBack(Collections.singletonList(parent), 0);

        BigtableChangeStreamEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getAssignedSplits()).isEmpty();
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
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
                        Optional.empty(),
                        null,
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
                        first.splitId(), first.getLowWatermark(), Collections.emptyList()));

        assertThat(context.readersToldNoMoreSplits()).isEmpty();
        enumerator.handleSourceEvent(
                1,
                new PartitionTransitionEvent(
                        second.splitId(), second.getLowWatermark(), Collections.emptyList()));
        assertThat(context.readersToldNoMoreSplits()).containsExactly(0, 1, 2);
        enumerator.close();
    }

    @Test
    void aProgressEventOlderThanTheAssignedPositionDoesNotRewindIt() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                enumerator(context, ScriptedChangeStreamCoordinatorClient.with(WHOLE), null);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");
        ChangeStreamPartitionSplit assigned = context.assignedSplits(0).get(0);
        Instant advanced = assigned.getLowWatermark().plusSeconds(60);
        enumerator.handleSourceEvent(
                0,
                new PartitionProgressEvent(
                        assigned.splitId(),
                        TestChangeStreamTokens.token(WHOLE, "recent"),
                        advanced));

        enumerator.handleSourceEvent(
                0,
                new PartitionProgressEvent(
                        assigned.splitId(),
                        TestChangeStreamTokens.token(WHOLE, "stale"),
                        advanced.minusSeconds(30)));

        assertThat(enumerator.snapshotState(1).getAssignedSplits())
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getLowWatermark()).isEqualTo(advanced);
                            assertThat(split.getContinuationTokens())
                                    .containsExactly(TestChangeStreamTokens.token(WHOLE, "recent"));
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
     * <p>The position is older than retention because it was <em>restored</em>: unlike a restored
     * split or pending merge, a restored missing partition is not put through {@code
     * StartPositionResolver.resolveRestored}, so ADR-0094's expiry rejection does not see it. That
     * asymmetry is what makes this state reachable, and it is recorded on #726 rather than settled
     * here — this test pins the clamp, not the omission that lets an expired position reach it.
     */
    @Test
    void aTokenlessRestartOlderThanRetentionStartsAtTheRetentionEdge() throws Exception {
        Instant now = Instant.parse("2026-08-11T12:00:00Z");
        Duration retention = Duration.ofHours(1);
        BigtableChangeStreamEnumeratorState restored =
                restoredWithMissing(
                        now.minusSeconds(60),
                        7,
                        new MissingPartition(
                                WHOLE,
                                now.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                                now.minus(Duration.ofDays(3))));
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context = context(1);
        BigtableChangeStreamSplitEnumerator enumerator =
                reconciling(
                        context,
                        new ScriptedChangeStreamCoordinatorClient(
                                retention, Collections.singletonList(WHOLE)),
                        restored,
                        now);
        enumerator.start();
        context.runAsyncCalls();
        enumerator.handleSplitRequest(0, "localhost");

        context.runPeriodicAsyncCalls();

        assertThat(context.assignedSplits(0))
                .singleElement()
                .satisfies(
                        split ->
                                assertThat(split.getLowWatermark())
                                        .isEqualTo(now.minus(retention).plusSeconds(60)));
        enumerator.close();
    }

    @Test
    void aFailedReconciliationScanKeepsTheMissingPartitionForTheNextOne() throws Exception {
        Instant now = Instant.parse("2026-08-11T12:00:00Z");
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
                context, client, StartPosition.latest(), Optional.empty(), restored);
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
                Optional.empty(),
                restored,
                false,
                true,
                Clock.fixed(now, ZoneOffset.UTC));
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

    private static PartitionTransitionEvent transition(
            ChangeStreamPartitionSplit parent, PartitionTransitionEvent.Successor... successors) {
        return new PartitionTransitionEvent(
                parent.splitId(), parent.getLowWatermark(), Arrays.asList(successors));
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
}
