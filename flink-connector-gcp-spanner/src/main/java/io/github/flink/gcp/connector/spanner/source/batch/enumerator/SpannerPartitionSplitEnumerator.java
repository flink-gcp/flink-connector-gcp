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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Partition;
import io.github.flink.gcp.connector.base.source.EnumeratorCounters;
import io.github.flink.gcp.connector.base.source.PullAssignmentSplitEnumerator;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceConfig;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchEnumeratorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Opens one batch read, asks Spanner to plan it into partitions, and hands the partitions out one
 * at a time.
 *
 * <p>The assignment protocol — pull-based, and keeping no record of which subtask holds which split
 * — is {@link PullAssignmentSplitEnumerator}'s, and the reasoning behind it lives there. What this
 * class adds is the plan, and the plan is the service's: {@code PartitionOptions} is documented as
 * a hint, so how many subtasks end up reading is decided by how finely Spanner chose to divide the
 * read, not by the job's parallelism.
 *
 * <p><b>A restore never plans again.</b> Planning a second time would open a second batch
 * transaction, at a second timestamp, and hand out its partitions under the ordinal split ids the
 * readers are already holding — so the job would read two snapshots and report one. The
 * checkpointed flag is what prevents it, and it is not the same statement as "the queue is
 * non-empty": a plan that has been fully handed out must not be recomputed either.
 */
@Internal
public class SpannerPartitionSplitEnumerator
        extends PullAssignmentSplitEnumerator<
                PartitionSplit, SpannerBatchEnumeratorState, PartitionPlan> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SpannerPartitionSplitEnumerator.class);

    private final SpannerSourceConfig<?> config;
    @Nullable private final SpannerBatchEnumeratorState restoredState;

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     */
    public SpannerPartitionSplitEnumerator(
            SplitEnumeratorContext<PartitionSplit> context,
            SpannerSourceConfig<?> config,
            @Nullable SpannerBatchEnumeratorState restoredState) {
        super(
                context,
                planner(config),
                "partition split",
                planningFailureMessage(config),
                "Failed to close the Spanner partition planner.");
        this.config = config;
        this.restoredState = restoredState;
    }

    /**
     * Takes the planner out of the configuration, checking the configuration on the way.
     *
     * <p>Static because it is evaluated as a {@code super(...)} argument, and first among them, so
     * a null configuration is named here rather than thrown from the message below.
     */
    private static PartitionPlanner planner(SpannerSourceConfig<?> config) {
        Preconditions.checkNotNull(config, "config must not be null");
        return config.getPlanner();
    }

    private static String planningFailureMessage(SpannerSourceConfig<?> config) {
        return "Failed to plan the Spanner "
                + config.getReadOperation()
                + " of "
                + config.getDatabase()
                + "; the source cannot start.";
    }

    @Override
    protected boolean restore() {
        if (restoredState == null || !restoredState.isPlanned()) {
            return false;
        }
        addPlannedSplits(restoredState.getPendingSplits());
        LOG.info(
                "Restored the Spanner batch read of {} with {} unassigned partition split(s); the"
                        + " read is not planned again, so every split keeps reading the snapshot it"
                        + " was planned at.",
                config.getDatabase(),
                pendingSplitCount());
        return true;
    }

    @Override
    protected void onPlanningStarted() {
        LOG.info(
                "Planning the Spanner {} of {} at timestamp bound {} (parallelism={},"
                        + " dataBoost={}).",
                config.getReadOperation(),
                config.getDatabase(),
                config.getTimestampBound(),
                context.currentParallelism(),
                config.isDataBoostEnabled());
    }

    @Override
    protected PartitionPlan plan() throws Exception {
        // Not softened into a single unpartitioned read when it fails. A query Spanner will not
        // partition comes back as INVALID_ARGUMENT naming what it could not distribute, and a
        // fallback would turn that into a job quietly reading everything on one subtask.
        return config.getPlanner()
                .plan(
                        config.getReadOperation(),
                        config.getTimestampBound(),
                        config.getPartitionOptions(),
                        config.isDataBoostEnabled(),
                        config.getRpcPriority());
    }

    @Override
    protected void onPlanned(PartitionPlan plan) {
        List<Partition> partitions = plan.getPartitions();
        List<PartitionSplit> splits = new ArrayList<>(partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            splits.add(
                    new PartitionSplit(
                            String.valueOf(i), plan.getBatchTransactionId(), partitions.get(i)));
        }
        addPlannedSplits(splits);
        logPlan(plan);
    }

    /**
     * Reports the plan, which is the only place a read Spanner declined to divide shows itself
     * before the job runs.
     *
     * <p>A plan of one partition is called out by name rather than folded into the
     * fewer-than-parallelism warning: it is the shape a small table, a narrow key range or an
     * emulator produces, and a reader of the log has to be able to tell "the service saw no reason
     * to divide this" from "the job is running with fewer subtasks than it asked for".
     */
    private void logPlan(PartitionPlan plan) {
        int planned = plan.getPartitions().size();
        if (planned <= 1) {
            LOG.info(
                    "Spanner planned the {} of {} as {} partition(s) at read timestamp {}; the"
                            + " whole read runs on one subtask, whatever the parallelism ({}).",
                    config.getReadOperation(),
                    config.getDatabase(),
                    planned,
                    plan.getReadTimestamp(),
                    context.currentParallelism());
        } else if (planned < context.currentParallelism()) {
            LOG.warn(
                    "Spanner planned {} partition(s) for the {} of {} at parallelism {}; the"
                            + " subtasks left without one finish immediately. The count is the"
                            + " service's decision — maxPartitions and partitionSizeBytes are"
                            + " hints it may ignore.",
                    planned,
                    config.getReadOperation(),
                    config.getDatabase(),
                    context.currentParallelism());
        } else {
            LOG.info(
                    "Spanner planned {} partition(s) for the {} of {} at read timestamp {} and"
                            + " parallelism {}.",
                    planned,
                    config.getReadOperation(),
                    config.getDatabase(),
                    plan.getReadTimestamp(),
                    context.currentParallelism());
        }
    }

    @Override
    protected EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup) {
        return new EnumeratorCounters(
                metricGroup.counter(
                        SpannerMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        SpannerMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        SpannerMetricNames.READS_PLANNED, new ThreadSafeSimpleCounter()));
    }

    @Override
    public SpannerBatchEnumeratorState snapshotState(long checkpointId) {
        return new SpannerBatchEnumeratorState(isPlanned(), pendingSplits());
    }
}
