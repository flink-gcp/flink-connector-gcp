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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The split universe of a job and its subtask ownership, computed deterministically from the
 * subscription list, the ordering mode and the source parallelism.
 *
 * <p>Splits carry no progress state, so nothing has to be preserved across a recomputation: the
 * plan is rebuilt on restore and whenever splits are returned, and the same inputs always produce
 * the same splits with the same owners.
 *
 * <pre>
 * splitCount = (mode == PER_KEY) ? |subscriptions| : max(|subscriptions|, parallelism)
 * split i    -&gt; subscription[i % |subscriptions|], uid = i
 * owner(i)   -&gt; i % parallelism
 * </pre>
 *
 * <p>Two properties follow. {@code splitCount >= |subscriptions|} means every subscription is
 * consumed by some subtask — a subscription nobody reads would simply accumulate a backlog. Under
 * {@link OrderingMode#NONE}, {@code splitCount >= parallelism} additionally means no subtask sits
 * idle.
 *
 * <p>Under {@link OrderingMode#PER_KEY} the split count is pinned to the subscription count, so
 * each subscription is owned by exactly one subtask and Pub/Sub can never spread one ordering key
 * across subtasks. The cost is that subtasks beyond the subscription count receive nothing; the
 * enumerator reports this.
 */
@Internal
public final class SplitAssignmentPlan {

    private final List<SubscriptionSplit> splits;
    private final int parallelism;

    private SplitAssignmentPlan(List<SubscriptionSplit> splits, int parallelism) {
        this.splits = splits;
        this.parallelism = parallelism;
    }

    /**
     * Computes the plan.
     *
     * @param subscriptions the subscriptions to consume, in a stable order
     * @param orderingMode the ordering mode
     * @param parallelism the source parallelism
     * @return the plan
     */
    public static SplitAssignmentPlan create(
            List<SubscriptionDestination> subscriptions,
            OrderingMode orderingMode,
            int parallelism) {
        Preconditions.checkNotNull(subscriptions, "subscriptions");
        Preconditions.checkNotNull(orderingMode, "orderingMode");
        Preconditions.checkArgument(!subscriptions.isEmpty(), "subscriptions must not be empty");
        Preconditions.checkArgument(parallelism > 0, "parallelism must be positive");

        int splitCount =
                orderingMode == OrderingMode.PER_KEY
                        ? subscriptions.size()
                        : Math.max(subscriptions.size(), parallelism);
        List<SubscriptionSplit> splits = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; i++) {
            splits.add(
                    new SubscriptionSplit(
                            subscriptions.get(i % subscriptions.size()), Integer.toString(i)));
        }
        return new SplitAssignmentPlan(Collections.unmodifiableList(splits), parallelism);
    }

    /**
     * Returns the splits owned by the given subtask, which is empty when parallelism exceeds the
     * split count.
     *
     * @param subtaskId the subtask index
     * @return the splits owned by that subtask
     */
    public List<SubscriptionSplit> splitsFor(int subtaskId) {
        List<SubscriptionSplit> owned = new ArrayList<>();
        for (int i = subtaskId; i < splits.size(); i += parallelism) {
            owned.add(splits.get(i));
        }
        return owned;
    }

    /** Returns every split in the plan. */
    public List<SubscriptionSplit> splits() {
        return splits;
    }

    /** Returns the subtask indices that own no split. */
    public Set<Integer> subtasksWithoutSplits() {
        Set<Integer> idle = new LinkedHashSet<>();
        for (int subtask = splits.size(); subtask < parallelism; subtask++) {
            idle.add(subtask);
        }
        return idle;
    }
}
