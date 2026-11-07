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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerChunkEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerUploadedEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for how {@link PubSubSplitEnumerator} redistributes the restore-deduplication ledger. */
class PubSubSplitEnumeratorLedgerTest {

    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of("test-project", "subscription-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of("test-project", "subscription-b");
    private static final PubSubEnumeratorState RESTORED =
            new PubSubEnumeratorState(Arrays.asList(SUB_A, SUB_B), true);

    private static final SubscriptionSplit CHUNK_A =
            SubscriptionSplit.ledgerChunk(SUB_A, "7-0-0", new byte[] {1});
    private static final SubscriptionSplit CHUNK_B =
            SubscriptionSplit.ledgerChunk(SUB_B, "7-1-0", new byte[] {2});

    @Test
    void aRestoreAssignsNothingUntilEveryReaderHasReportedItsUpload() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, RESTORED);
        register(context, enumerator, 2);

        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(context.assignedSplits(1)).isEmpty();

        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
    }

    @Test
    void eachReaderIsSentTheChunksOfTheSubscriptionsItConsumesBeforeItsSplits() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, RESTORED);
        register(context, enumerator, 2);

        // Both chunks were restored onto subtask 1, whatever the plan says now.
        enumerator.handleSourceEvent(1, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(1, new LedgerChunkEvent(CHUNK_B));
        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        for (int subtask = 0; subtask < 2; subtask++) {
            List<Object> deliveries = context.deliveries(subtask);
            SubscriptionDestination consumed =
                    context.assignedSplits(subtask).get(0).getSubscription();
            SubscriptionSplit expectedChunk = consumed.equals(SUB_A) ? CHUNK_A : CHUNK_B;
            assertThat(deliveries).hasSize(2);
            assertThat(((LedgerChunkEvent) deliveries.get(0)).chunk()).isEqualTo(expectedChunk);
            assertThat(deliveries.get(1)).isEqualTo(context.assignedSplits(subtask));
        }
    }

    @Test
    void underNoOrderingEveryReaderOfASubscriptionReceivesItsWholeLedger() {
        // Three readers, one subscription: Pub/Sub may redeliver to any of the three streams.
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.NONE,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 3);

        enumerator.handleSourceEvent(2, new LedgerChunkEvent(CHUNK_A));
        for (int subtask = 0; subtask < 3; subtask++) {
            enumerator.handleSourceEvent(subtask, new LedgerUploadedEvent());
        }

        for (int subtask = 0; subtask < 3; subtask++) {
            assertThat(chunksSentTo(context, subtask)).containsExactly(CHUNK_A);
        }
    }

    @Test
    void underPerKeyOrderingOnlyTheOwnerReceivesTheLedger() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.PER_KEY,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 3);

        // A scale-out restored the chunk onto subtask 2, which the plan now leaves idle.
        enumerator.handleSourceEvent(2, new LedgerChunkEvent(CHUNK_A));
        for (int subtask = 0; subtask < 3; subtask++) {
            enumerator.handleSourceEvent(subtask, new LedgerUploadedEvent());
        }

        assertThat(chunksSentTo(context, 0)).containsExactly(CHUNK_A);
        assertThat(chunksSentTo(context, 1)).isEmpty();
        assertThat(chunksSentTo(context, 2)).isEmpty();
        assertThat(context.readersToldNoMoreSplits()).containsExactly(1, 2);
    }

    @Test
    void aReuploadedChunkIsForwardedOnce() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.NONE,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 1);

        // The reader failed after uploading and uploaded the same restored chunk again.
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        assertThat(chunksSentTo(context, 0)).containsExactly(CHUNK_A);
    }

    @Test
    void theTimeoutAssignsWithoutTheMissingReadersLedger() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, RESTORED);
        register(context, enumerator, 2);
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        assertThat(context.periodicCallCount()).isEqualTo(1);
        context.firePeriodicCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        // What did arrive is still forwarded: one slow reader costs only its own chunks.
        int consumerOfA = context.assignedSplits(0).get(0).getSubscription().equals(SUB_A) ? 0 : 1;
        assertThat(chunksSentTo(context, consumerOfA)).containsExactly(CHUNK_A);
        // The late report changes nothing, and a second timer run does not assign twice.
        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());
        context.firePeriodicCalls();
        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
    }

    @Test
    void aFreshStartDoesNotWaitForTheLedger() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, null);

        register(context, enumerator, 2);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(context.periodicCallCount()).isZero();
    }

    @Test
    void withDeduplicationOffARestoreDoesNotWaitAndIgnoresChunks() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, false, RESTORED);

        register(context, enumerator, 2);
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(chunksSentTo(context, 0)).isEmpty();
        assertThat(context.periodicCallCount()).isZero();
    }

    @Test
    void aRegionFailoverWaitsForTheResetSubtasksAndForwardsTheirLedger() {
        // The default scheduler's region failover keeps this enumerator and resets subtasks
        // through addSplitsBack; their restored chunks must reach them as on a whole restore.
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, null);
        register(context, enumerator, 2);
        context.forgetAssignments();

        for (int subtask = 0; subtask < 2; subtask++) {
            enumerator.addSplitsBack(List.of(), subtask);
        }
        register(context, enumerator, 2);
        assertThat(context.assignedSplits(0)).isEmpty();
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_B));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());
        assertThat(context.assignedSplits(1)).isEmpty();
        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());

        for (int subtask = 0; subtask < 2; subtask++) {
            SubscriptionDestination consumed =
                    context.assignedSplits(subtask).get(0).getSubscription();
            assertThat(chunksSentTo(context, subtask))
                    .containsExactly(consumed.equals(SUB_A) ? CHUNK_A : CHUNK_B);
        }
    }

    @Test
    void aSubtaskResetAloneLeavesTheRunningReadersAlone() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, null);
        register(context, enumerator, 2);
        List<SubscriptionSplit> before = List.copyOf(context.assignedSplits(1));
        context.forgetAssignments();

        enumerator.addSplitsBack(List.of(), 1);
        enumerator.addReader(1);
        enumerator.handleSourceEvent(1, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(1, new LedgerChunkEvent(CHUNK_B));
        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());

        assertThat(context.assignedSplits(1)).isEqualTo(before);
        SubscriptionSplit own = before.get(0).getSubscription().equals(SUB_A) ? CHUNK_A : CHUNK_B;
        assertThat(chunksSentTo(context, 1)).containsExactly(own);
        // Subtask 0 kept running: it is sent neither a ledger nor a second assignment.
        assertThat(context.deliveries(0)).hasSize(1);
        assertThat(chunksSentTo(context, 0)).isEmpty();
    }

    @Test
    void aResetBeforeTheNextCheckpointCompletesIsSentTheChunksItWasForwarded() {
        // Restored at a higher parallelism: B's chunk sits in subtask 0's state while the plan
        // gives B to subtask 1, which therefore restores no chunk of its own if it alone resets.
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.PER_KEY, true, RESTORED);
        register(context, enumerator, 2);
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_B));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());
        enumerator.handleSourceEvent(1, new LedgerUploadedEvent());
        int ownerOfB = context.assignedSplits(0).get(0).getSubscription().equals(SUB_B) ? 0 : 1;
        assertThat(chunksSentTo(context, ownerOfB)).containsExactly(CHUNK_B);

        enumerator.addSplitsBack(List.of(), ownerOfB);
        enumerator.addReader(ownerOfB);
        enumerator.handleSourceEvent(ownerOfB, new LedgerUploadedEvent());

        assertThat(chunksSentTo(context, ownerOfB)).containsExactly(CHUNK_B, CHUNK_B);

        // Once a checkpoint completes, a reset restores that checkpoint instead.
        enumerator.notifyCheckpointComplete(8L);
        enumerator.addSplitsBack(List.of(), ownerOfB);
        enumerator.addReader(ownerOfB);
        enumerator.handleSourceEvent(ownerOfB, new LedgerUploadedEvent());

        assertThat(chunksSentTo(context, ownerOfB)).containsExactly(CHUNK_B, CHUNK_B);
    }

    @Test
    void aCheckpointCompletingWhileAResetWaitsDoesNotTakeItsLedger() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.NONE,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 1);
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        // The reset restores the checkpoint that was latest when it began; one completing while
        // the reset subtask is still parked does not change what that subtask restored.
        enumerator.addSplitsBack(List.of(), 0);
        enumerator.addReader(0);
        enumerator.notifyCheckpointComplete(3L);
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        assertThat(chunksSentTo(context, 0)).containsExactly(CHUNK_A, CHUNK_A);
    }

    @Test
    void aChunkFromASubtaskTheWaitDoesNotAwaitIsDropped() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.NONE,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 1);
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());
        context.forgetAssignments();

        // Arrives after the wait ended, without a reset that reopens it.
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));

        assertThat(chunksSentTo(context, 0)).isEmpty();
    }

    @Test
    void aSecondReportFromOneSubtaskDoesNotEndTheWait() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = start(context, OrderingMode.NONE, true, RESTORED);
        register(context, enumerator, 2);

        // Subtask 0 failed over during the wait and reported again; subtask 1 has not reported.
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(context.assignedSplits(1)).isEmpty();
    }

    @Test
    void theLedgerWaitAndTheStartupCheckMayFinishInEitherOrder() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                startWithoutRunningTheCheck(
                        context, new PubSubEnumeratorState(List.of(SUB_A), true), SUB_A);
        register(context, enumerator, 1);
        enumerator.handleSourceEvent(0, new LedgerChunkEvent(CHUNK_A));
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());

        // The ledger is in, but the subscriptions are not verified yet.
        assertThat(context.assignedSplits(0)).isEmpty();

        context.runAsyncCalls();

        assertThat(context.deliveries(0)).hasSize(2);
        assertThat(((LedgerChunkEvent) context.deliveries(0).get(0)).chunk()).isEqualTo(CHUNK_A);
        assertThat(context.deliveries(0).get(1)).isEqualTo(context.assignedSplits(0));
    }

    @Test
    void theTimeoutStartsWithTheFirstRegistrationNotWithTheEnumerator() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        start(context, OrderingMode.NONE, true, RESTORED);

        // Slot allocation and the restore of chained operators' state happen before any reader
        // registers; none of that may count against the wait.
        assertThat(context.periodicCallCount()).isZero();
    }

    @Test
    void aTimeoutArmedForAnEarlierWaitDoesNotEndALaterOne() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        OrderingMode.NONE,
                        true,
                        new PubSubEnumeratorState(List.of(SUB_A), true),
                        SUB_A);
        register(context, enumerator, 1);
        enumerator.handleSourceEvent(0, new LedgerUploadedEvent());
        context.forgetAssignments();

        enumerator.addSplitsBack(List.of(), 0);
        enumerator.addReader(0);
        // The first wait's timer fires late, during the second wait.
        context.firePeriodicCall(0);

        assertThat(context.assignedSplits(0)).isEmpty();
        context.firePeriodicCall(1);
        assertThat(context.assignedSplits(0)).hasSize(1);
    }

    private static List<SubscriptionSplit> chunksSentTo(
            FakeSplitEnumeratorContext context, int subtask) {
        return context.deliveries(subtask).stream()
                .filter(LedgerChunkEvent.class::isInstance)
                .map(event -> ((LedgerChunkEvent) event).chunk())
                .collect(Collectors.toList());
    }

    private static PubSubSplitEnumerator startWithoutRunningTheCheck(
            FakeSplitEnumeratorContext context,
            PubSubEnumeratorState restored,
            SubscriptionDestination... subscriptions) {
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context,
                        config(OrderingMode.NONE, true, subscriptions),
                        admin(OrderingMode.NONE, subscriptions),
                        restored);
        enumerator.start();
        return enumerator;
    }

    private static PubSubSplitEnumerator start(
            FakeSplitEnumeratorContext context,
            OrderingMode orderingMode,
            boolean deduplicate,
            @Nullable PubSubEnumeratorState restored) {
        return start(context, orderingMode, deduplicate, restored, SUB_A, SUB_B);
    }

    private static PubSubSplitEnumerator start(
            FakeSplitEnumeratorContext context,
            OrderingMode orderingMode,
            boolean deduplicate,
            @Nullable PubSubEnumeratorState restored,
            SubscriptionDestination... subscriptions) {
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context,
                        config(orderingMode, deduplicate, subscriptions),
                        admin(orderingMode, subscriptions),
                        restored);
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }

    private static PubSubSourceConfig<?> config(
            OrderingMode orderingMode,
            boolean deduplicate,
            SubscriptionDestination... subscriptions) {
        return ((PubSubStreamingPullSource<String>)
                        PubSubSource.<String>builder()
                                .deserializer(
                                        PubSubDeserializationSchema.payload(
                                                new SimpleStringSchema()))
                                .subscriptions(subscriptions)
                                .orderingMode(orderingMode)
                                .restoreDeduplication(deduplicate)
                                .build())
                .getConfig();
    }

    private static FakeSubscriptionAdmin admin(
            OrderingMode orderingMode, SubscriptionDestination... subscriptions) {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        SubscriptionInfo info =
                SubscriptionInfo.builder()
                        .messageOrderingEnabled(orderingMode == OrderingMode.PER_KEY)
                        .build();
        for (SubscriptionDestination subscription : subscriptions) {
            admin.withSubscription(subscription, info);
        }
        return admin;
    }

    private static void register(
            FakeSplitEnumeratorContext context, PubSubSplitEnumerator enumerator, int parallelism) {
        for (int subtask = 0; subtask < parallelism; subtask++) {
            context.registerReader(subtask);
            enumerator.addReader(subtask);
        }
    }
}
