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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.EnumeratorCounters;
import io.github.flink.gcp.connector.base.source.PullAssignmentSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Samples the table once, cuts the configured ranges at the sampled boundaries, and hands the
 * pieces out one at a time.
 *
 * <p>The assignment protocol — pull-based, and keeping no record of which subtask holds which split
 * — is {@link PullAssignmentSplitEnumerator}'s, and the reasoning behind it lives there. What this
 * class adds is the plan: parallelism comes from how finely the table happens to be sampled, and a
 * table with one tablet is read by one subtask however many are running.
 *
 * <p><b>A restore never samples again.</b> Tablets split and merge while a job runs, so a second
 * sampling would produce different boundaries and therefore different splits — under the same
 * ordinal ids the readers are already holding, which would make {@link #addSplitsBack} and the
 * readers' restored splits disagree about which range each id names. The checkpointed flag is what
 * prevents it, and it is not the same statement as "the queue is non-empty": a plan that has been
 * fully handed out must not be recomputed either.
 */
@Internal
public class BigtableScanSplitEnumerator
        extends PullAssignmentSplitEnumerator<
                RowRangeSplit, BigtableScanEnumeratorState, List<RowKeySample>> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableScanSplitEnumerator.class);

    private final BigtableSourceConfig<?> config;
    private final RowKeySampler sampler;
    @Nullable private final BigtableScanEnumeratorState restoredState;

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param sampler the sampler this enumerator owns and closes; the source mints one per
     *     enumerator, so it is never one an earlier enumerator already closed
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     */
    public BigtableScanSplitEnumerator(
            SplitEnumeratorContext<RowRangeSplit> context,
            BigtableSourceConfig<?> config,
            RowKeySampler sampler,
            @Nullable BigtableScanEnumeratorState restoredState) {
        super(
                context,
                checkedSampler(config, sampler),
                "scan split",
                samplingFailureMessage(config),
                "Failed to close the Bigtable row key sampler.");
        this.config = config;
        this.sampler = sampler;
        this.restoredState = restoredState;
    }

    /**
     * Checks both arguments the {@code super(...)} call needs.
     *
     * <p>Static because it is evaluated as a {@code super(...)} argument, and first among them, so
     * a null configuration is named here rather than thrown from the message below.
     */
    private static RowKeySampler checkedSampler(
            BigtableSourceConfig<?> config, RowKeySampler sampler) {
        Preconditions.checkNotNull(config, "config must not be null");
        return Preconditions.checkNotNull(sampler, "sampler must not be null");
    }

    private static String samplingFailureMessage(BigtableSourceConfig<?> config) {
        return "Failed to sample the row keys of "
                + config.getTable()
                + "; the scan cannot be planned.";
    }

    @Override
    protected boolean restore() {
        if (restoredState == null || !restoredState.isPlanned()) {
            return false;
        }
        addPlannedSplits(restoredState.getPendingSplits());
        LOG.info(
                "Restored the Bigtable scan plan for {} with {} unassigned split(s); the table"
                        + " is not sampled again, so the split ids the readers hold keep their"
                        + " meaning.",
                config.getTable(),
                pendingSplitCount());
        return true;
    }

    @Override
    protected void onPlanningStarted() {
        LOG.info(
                "Sampling row keys of {} to plan the scan (ranges={}, parallelism={}).",
                config.getTable(),
                describeRanges(),
                context.currentParallelism());
    }

    @Override
    protected List<RowKeySample> plan() throws Exception {
        // Not softened into a single unsplit plan when it fails. The client already retried the
        // transient codes under a total timeout of its own, so a failure that reaches the
        // enumerator is a permission, quota or configuration problem — and a fallback would turn
        // it into a job that reads the whole table on one subtask for reasons nothing reports.
        return sampler.sample(config.getTable());
    }

    @Override
    protected void onPlanned(List<RowKeySample> samples) {
        List<PlannedSplit> plan = RowRangeSplitPlanner.plan(config.getRanges(), samples);
        List<RowRangeSplit> splits = new ArrayList<>();
        for (PlannedSplit split : plan) {
            splits.add(split.getSplit());
        }
        addPlannedSplits(splits);
        logPlan(samples, plan);
    }

    /**
     * Reports the plan, which is the only place a skewed table shows itself before the job runs.
     *
     * <p>An empty sample list is called out by name: it is what the emulator answers for every
     * table, and a reader of the log has to be able to tell "the service offered no boundaries"
     * from "planning went wrong".
     */
    private void logPlan(List<RowKeySample> samples, List<PlannedSplit> plan) {
        if (samples.isEmpty()) {
            LOG.info(
                    "Bigtable offered no row key samples for {}; the scan runs as {} split(s), one"
                            + " per configured range, at parallelism {}.",
                    config.getTable(),
                    plan.size(),
                    context.currentParallelism());
        } else if (plan.size() < context.currentParallelism()) {
            LOG.warn(
                    "Planned {} split(s) for {} at parallelism {}; the subtasks left without one"
                            + " finish immediately. The boundaries are the service's: a table"
                            + " stored in few tablets is read by few subtasks.",
                    plan.size(),
                    config.getTable(),
                    context.currentParallelism());
        } else {
            LOG.info(
                    "Planned {} split(s) for {} from {} row key sample(s) at parallelism {}.",
                    plan.size(),
                    config.getTable(),
                    samples.size(),
                    context.currentParallelism());
        }
        LOG.info("Bigtable scan plan for {}: {}.", config.getTable(), describeSizes(plan));
    }

    /**
     * Renders the estimated sizes of a plan.
     *
     * <p>The estimates are logged here and then dropped. They describe the range as it was planned,
     * and a checkpoint truncates that range — so an estimate travelling in a split would describe
     * nothing after the first checkpoint, while being a field the split's serializer would have to
     * carry for ever.
     */
    private String describeSizes(List<PlannedSplit> plan) {
        long total = 0;
        long smallest = Long.MAX_VALUE;
        long largest = Long.MIN_VALUE;
        int unestimated = 0;
        for (PlannedSplit split : plan) {
            OptionalLong bytes = split.getEstimatedBytes();
            if (!bytes.isPresent()) {
                unestimated++;
                continue;
            }
            total += bytes.getAsLong();
            smallest = Math.min(smallest, bytes.getAsLong());
            largest = Math.max(largest, bytes.getAsLong());
        }
        if (unestimated == plan.size()) {
            return "no size estimate is available for any split";
        }
        return "about "
                + total
                + " byte(s) across "
                + (plan.size() - unestimated)
                + " estimated split(s), smallest "
                + smallest
                + ", largest "
                + largest
                + ", "
                + unestimated
                + " past the last sampled key and therefore unestimated";
    }

    private String describeRanges() {
        List<String> rendered = new ArrayList<>();
        config.getRanges().forEach(range -> rendered.add(RowRanges.format(range)));
        return String.join(", ", rendered);
    }

    @Override
    protected EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup) {
        return new EnumeratorCounters(
                metricGroup.counter(
                        BigtableMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        BigtableMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        BigtableMetricNames.ROW_KEY_SAMPLES_TAKEN, new ThreadSafeSimpleCounter()));
    }

    @Override
    public BigtableScanEnumeratorState snapshotState(long checkpointId) {
        return new BigtableScanEnumeratorState(isPlanned(), pendingSplits());
    }
}
