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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Assigns subscription splits to reader subtasks.
 *
 * <p>Assignment is a pure function of the subscription list, the ordering mode and the current
 * parallelism (see {@link SplitAssignmentPlan}), which makes the enumerator almost stateless:
 * returned splits need no bookkeeping because re-registering the subtask recomputes exactly the
 * same assignment, and a restore recomputes the plan from the current parallelism rather than
 * replaying a stale one.
 *
 * <p>The upstream connector instead mints one split per registered subtask, all pointing at a
 * single hard-coded subscription — which supports neither multiple subscriptions nor ordering,
 * since every subtask then opens its own streaming pull against the same subscription.
 */
@Internal
public class PubSubSplitEnumerator
        implements SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitEnumerator.class);

    private final SplitEnumeratorContext<SubscriptionSplit> context;
    private final List<SubscriptionDestination> subscriptions;
    private final OrderingMode orderingMode;
    @Nullable private final PubSubEnumeratorState restoredState;

    private SplitAssignmentPlan plan;

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param subscriptions the subscriptions to consume
     * @param orderingMode the ordering mode
     * @param restoredState the checkpointed state when restoring, or {@code null} on a fresh start
     */
    public PubSubSplitEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context,
            List<SubscriptionDestination> subscriptions,
            OrderingMode orderingMode,
            @Nullable PubSubEnumeratorState restoredState) {
        this.context = context;
        this.subscriptions = subscriptions;
        this.orderingMode = orderingMode;
        this.restoredState = restoredState;
    }

    @Override
    public void start() {
        if (restoredState != null && !restoredState.getSubscriptions().equals(subscriptions)) {
            LOG.warn(
                    "The configured subscriptions {} differ from the checkpointed subscriptions {};"
                            + " the configured list takes effect. Messages already delivered on a"
                            + " removed subscription but not yet acknowledged stay on it.",
                    subscriptions,
                    restoredState.getSubscriptions());
        }
        plan =
                SplitAssignmentPlan.create(
                        subscriptions, orderingMode, context.currentParallelism());
        if (plan.idleSubtaskCount() > 0) {
            LOG.warn(
                    "Ordering mode {} assigns one split per subscription, so {} of the {} source"
                            + " subtasks receive no work ({} subscriptions). Lower the source"
                            + " parallelism to {} or consume more subscriptions.",
                    orderingMode,
                    plan.idleSubtaskCount(),
                    context.currentParallelism(),
                    subscriptions.size(),
                    subscriptions.size());
        }
    }

    @Override
    public void addReader(int subtaskId) {
        List<SubscriptionSplit> splits = plan.splitsFor(subtaskId);
        if (splits.isEmpty()) {
            // Nothing will ever arrive for this subtask, so let it finish instead of holding the
            // watermark back for the lifetime of the job.
            LOG.info(
                    "Source subtask {} has no subscription to consume; signalling no more splits.",
                    subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        LOG.info("Assigning splits {} to source subtask {}.", splits, subtaskId);
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    @Override
    public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
        // Splits carry no progress state and the assignment is deterministic, so the returned
        // splits need no bookkeeping: the restarted subtask re-registers through addReader and is
        // handed exactly the same splits again.
        LOG.info(
                "Source subtask {} returned splits {}; they will be reassigned when it"
                        + " re-registers.",
                subtaskId,
                splits);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Assignment is push-based and complete from addReader onwards; readers never need to ask.
    }

    @Override
    public PubSubEnumeratorState snapshotState(long checkpointId) {
        return new PubSubEnumeratorState(subscriptions);
    }

    @Override
    public void close() {}
}
