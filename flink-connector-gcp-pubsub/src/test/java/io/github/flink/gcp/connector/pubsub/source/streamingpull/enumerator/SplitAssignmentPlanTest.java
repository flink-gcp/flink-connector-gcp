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

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SplitAssignmentPlan}. */
class SplitAssignmentPlanTest {

    private static final String PROJECT = "test-project";

    @ParameterizedTest(name = "{0} subscriptions at parallelism {1}, ordering {2}")
    @CsvSource({
        "1, 1, NONE",
        "1, 4, NONE",
        "3, 1, NONE",
        "3, 2, NONE",
        "3, 3, NONE",
        "4, 3, NONE",
        "2, 7, NONE",
        "1, 1, PER_KEY",
        "1, 4, PER_KEY",
        "3, 2, PER_KEY",
        "3, 3, PER_KEY",
        "5, 2, PER_KEY",
    })
    void everySubscriptionIsConsumedByExactlyTheExpectedNumberOfSubtasks(
            int subscriptionCount, int parallelism, OrderingMode orderingMode) {
        List<SubscriptionDestination> subscriptions = subscriptions(subscriptionCount);

        SplitAssignmentPlan plan =
                SplitAssignmentPlan.create(subscriptions, orderingMode, parallelism);

        // Every subscription is covered: one that no subtask consumed would silently build a
        // backlog.
        Set<SubscriptionDestination> covered =
                plan.splits().stream()
                        .map(SubscriptionSplit::getSubscription)
                        .collect(Collectors.toSet());
        assertThat(covered).containsExactlyInAnyOrderElementsOf(subscriptions);

        // Splits are partitioned across subtasks: each split is owned exactly once.
        List<SubscriptionSplit> owned = new ArrayList<>();
        IntStream.range(0, parallelism).forEach(subtask -> owned.addAll(plan.splitsFor(subtask)));
        assertThat(owned).containsExactlyInAnyOrderElementsOf(plan.splits());
        assertThat(new HashSet<>(owned)).hasSize(plan.splits().size());
    }

    @ParameterizedTest(name = "{0} subscriptions at parallelism {1}")
    @CsvSource({"1, 1", "1, 4", "3, 2", "3, 3", "4, 3", "2, 7"})
    void unorderedAssignmentKeepsEverySubtaskBusy(int subscriptionCount, int parallelism) {
        SplitAssignmentPlan plan =
                SplitAssignmentPlan.create(
                        subscriptions(subscriptionCount), OrderingMode.NONE, parallelism);

        assertThat(plan.idleSubtaskCount()).isZero();
        IntStream.range(0, parallelism)
                .forEach(subtask -> assertThat(plan.splitsFor(subtask)).isNotEmpty());
    }

    @ParameterizedTest(name = "{0} subscriptions at parallelism {1}")
    @CsvSource({"1, 1", "1, 4", "3, 2", "3, 3", "5, 2"})
    void orderedAssignmentGivesEachSubscriptionExactlyOneSplit(
            int subscriptionCount, int parallelism) {
        SplitAssignmentPlan plan =
                SplitAssignmentPlan.create(
                        subscriptions(subscriptionCount), OrderingMode.PER_KEY, parallelism);

        // One split per subscription is what keeps an ordering key inside a single subtask.
        assertThat(plan.splits()).hasSize(subscriptionCount);
        assertThat(plan.splits().stream().map(SubscriptionSplit::getSubscription))
                .doesNotHaveDuplicates();
    }

    @Test
    void orderedAssignmentLeavesSubtasksBeyondTheSubscriptionCountIdle() {
        SplitAssignmentPlan plan =
                SplitAssignmentPlan.create(subscriptions(2), OrderingMode.PER_KEY, 5);

        assertThat(plan.idleSubtaskCount()).isEqualTo(3);
        assertThat(plan.splitsFor(0)).hasSize(1);
        assertThat(plan.splitsFor(1)).hasSize(1);
        assertThat(plan.splitsFor(4)).isEmpty();
    }

    @Test
    void planIsDeterministicSoRestoresAndReassignmentsAgree() {
        List<SubscriptionDestination> subscriptions = subscriptions(3);

        SplitAssignmentPlan first = SplitAssignmentPlan.create(subscriptions, OrderingMode.NONE, 4);
        SplitAssignmentPlan second =
                SplitAssignmentPlan.create(subscriptions, OrderingMode.NONE, 4);

        assertThat(second.splits()).isEqualTo(first.splits());
        IntStream.range(0, 4)
                .forEach(
                        subtask ->
                                assertThat(second.splitsFor(subtask))
                                        .isEqualTo(first.splitsFor(subtask)));
    }

    @Test
    void surplusUnorderedSplitsAreSpreadOverTheSubscriptionsInTurn() {
        List<SubscriptionDestination> subscriptions = subscriptions(2);

        SplitAssignmentPlan plan = SplitAssignmentPlan.create(subscriptions, OrderingMode.NONE, 5);

        assertThat(plan.splits()).hasSize(5);
        assertThat(plan.splits().stream().map(SubscriptionSplit::getSubscription))
                .containsExactly(
                        subscriptions.get(0),
                        subscriptions.get(1),
                        subscriptions.get(0),
                        subscriptions.get(1),
                        subscriptions.get(0));
    }

    @Test
    void splitIdsAreUniqueWhenSeveralSplitsShareASubscription() {
        SplitAssignmentPlan plan =
                SplitAssignmentPlan.create(subscriptions(1), OrderingMode.NONE, 3);

        assertThat(plan.splits().stream().map(SubscriptionSplit::splitId)).doesNotHaveDuplicates();
    }

    @Test
    void rejectsEmptySubscriptionsAndNonPositiveParallelism() {
        assertThatThrownBy(
                        () -> SplitAssignmentPlan.create(new ArrayList<>(), OrderingMode.NONE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("subscriptions must not be empty");
        assertThatThrownBy(() -> SplitAssignmentPlan.create(subscriptions(1), OrderingMode.NONE, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelism must be positive");
    }

    private static List<SubscriptionDestination> subscriptions(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> SubscriptionDestination.of(PROJECT, "subscription-" + i))
                .collect(Collectors.toList());
    }
}
