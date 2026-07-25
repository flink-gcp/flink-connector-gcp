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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PubSubSplitEnumerator}. */
class PubSubSplitEnumeratorTest {

    private static final String PROJECT = "test-project";
    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of(PROJECT, "subscription-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of(PROJECT, "subscription-b");

    @Test
    void everyRegisteredReaderReceivesItsSplits() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = enumerator(context, OrderingMode.NONE, SUB_A, SUB_B);
        enumerator.start();

        context.registerReader(0);
        enumerator.addReader(0);
        context.registerReader(1);
        enumerator.addReader(1);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(subscriptionsOf(context.assignedSplits(0)))
                .doesNotContainAnyElementsOf(subscriptionsOf(context.assignedSplits(1)));
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void oneSubscriptionIsSpreadOverEveryReaderWhenOrderingIsNotRequired() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator = enumerator(context, OrderingMode.NONE, SUB_A);
        enumerator.start();

        registerAll(context, enumerator, 3);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(context.assignedSplits(2)).hasSize(1);
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void orderedModeLeavesSurplusReadersWithNothingToDoAndFinishesThem() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator = enumerator(context, OrderingMode.PER_KEY, SUB_A);
        enumerator.start();

        registerAll(context, enumerator, 3);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.assignedSplits(2)).isEmpty();
        // Finishing idle subtasks keeps them from holding the watermark back forever.
        assertThat(context.readersToldNoMoreSplits()).containsExactly(1, 2);
    }

    @Test
    void aRestartedReaderIsHandedExactlyTheSplitsItReturned() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = enumerator(context, OrderingMode.NONE, SUB_A, SUB_B);
        enumerator.start();
        registerAll(context, enumerator, 2);
        List<SubscriptionSplit> beforeFailure = List.copyOf(context.assignedSplits(1));

        context.forgetAssignments();
        enumerator.addSplitsBack(beforeFailure, 1);
        enumerator.addReader(1);

        assertThat(context.assignedSplits(1)).containsExactlyElementsOf(beforeFailure);
    }

    @Test
    void restoringWithADifferentParallelismRecomputesTheAssignment() {
        PubSubEnumeratorState restored = new PubSubEnumeratorState(Arrays.asList(SUB_A, SUB_B));
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(4);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context, Arrays.asList(SUB_A, SUB_B), OrderingMode.NONE, restored);
        enumerator.start();

        registerAll(context, enumerator, 4);

        // Four readers, two subscriptions: the plan grows to four splits so nobody idles.
        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(3)).hasSize(1);
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void restoringWithChangedSubscriptionsUsesTheConfiguredOnes() {
        PubSubEnumeratorState restored =
                new PubSubEnumeratorState(Collections.singletonList(SUB_A));
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context, Arrays.asList(SUB_A, SUB_B), OrderingMode.NONE, restored);
        enumerator.start();
        context.registerReader(0);
        enumerator.addReader(0);

        assertThat(subscriptionsOf(context.assignedSplits(0))).containsExactly(SUB_A, SUB_B);
    }

    @Test
    void snapshotRecordsTheConfiguredSubscriptions() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator = enumerator(context, OrderingMode.NONE, SUB_A, SUB_B);
        enumerator.start();

        assertThat(enumerator.snapshotState(1L).getSubscriptions()).containsExactly(SUB_A, SUB_B);
    }

    private static PubSubSplitEnumerator enumerator(
            FakeSplitEnumeratorContext context,
            OrderingMode orderingMode,
            SubscriptionDestination... subscriptions) {
        return new PubSubSplitEnumerator(context, Arrays.asList(subscriptions), orderingMode, null);
    }

    private static void registerAll(
            FakeSplitEnumeratorContext context, PubSubSplitEnumerator enumerator, int parallelism) {
        for (int subtask = 0; subtask < parallelism; subtask++) {
            context.registerReader(subtask);
            enumerator.addReader(subtask);
        }
    }

    private static List<SubscriptionDestination> subscriptionsOf(List<SubscriptionSplit> splits) {
        return splits.stream().map(SubscriptionSplit::getSubscription).collect(Collectors.toList());
    }
}
