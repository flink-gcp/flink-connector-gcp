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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    static final String ASSIGNED_SPLITS = "assignedSplits";
    static final String UNASSIGNED_READERS = "unassignedReaders";

    private final SplitEnumeratorContext<SubscriptionSplit> context;
    private final List<SubscriptionDestination> subscriptions;
    private final OrderingMode orderingMode;
    @Nullable private final PubSubEnumeratorState restoredState;

    private SplitAssignmentPlan plan;

    /**
     * Subtasks currently holding splits, and subtasks currently registered with none.
     *
     * <p>Sets rather than counters, because {@code addReader} is not called once per subtask: after
     * a failover the coordinator drops the subtask from its registered readers and calls it again,
     * while {@code addSplitsBack} only returns the portion of the assignment no completed
     * checkpoint covers — often nothing. Counting deltas would therefore drift upward on every
     * failover and drive the derived unassigned-splits gauge negative. Re-registration is
     * idempotent here.
     */
    private final Set<Integer> subtasksWithSplits = new HashSet<>();

    private final Set<Integer> subtasksWithoutSplits = new HashSet<>();

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
        registerMetrics();
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

    /**
     * Registers the enumerator gauges. Both are derived from which subtasks are registered rather
     * than accumulated, so they settle once every reader has registered and stay settled across
     * failovers; a value that stays below the plan's split count means a subtask never came back.
     */
    private void registerMetrics() {
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        metricGroup.gauge(ASSIGNED_SPLITS, (Gauge<Integer>) this::assignedSplitCount);
        metricGroup.gauge(UNASSIGNED_READERS, (Gauge<Integer>) subtasksWithoutSplits::size);
        metricGroup.setUnassignedSplitsGauge(
                () -> (long) (plan.splitCount() - assignedSplitCount()));
    }

    /** Counts the splits currently held by registered subtasks. */
    private int assignedSplitCount() {
        int assigned = 0;
        for (int subtaskId : subtasksWithSplits) {
            assigned += plan.splitsFor(subtaskId).size();
        }
        return assigned;
    }

    @Override
    public void addReader(int subtaskId) {
        List<SubscriptionSplit> splits = plan.splitsFor(subtaskId);
        if (splits.isEmpty()) {
            subtasksWithoutSplits.add(subtaskId);
            // Nothing will ever arrive for this subtask, so let it finish instead of holding the
            // watermark back for the lifetime of the job.
            LOG.info(
                    "Source subtask {} has no subscription to consume; signalling no more splits.",
                    subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        LOG.info("Assigning splits {} to source subtask {}.", splits, subtaskId);
        subtasksWithSplits.add(subtaskId);
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    }

    @Override
    public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
        // The subtask is being reset; it re-registers through addReader and is handed the same
        // splits again. Forget it either way — the returned list is only the uncheckpointed part
        // of its assignment, so its size says nothing about what the subtask still holds.
        subtasksWithSplits.remove(subtaskId);
        subtasksWithoutSplits.remove(subtaskId);
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
