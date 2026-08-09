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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Samples the table once, cuts the configured ranges at the sampled boundaries, and hands the
 * pieces out one at a time.
 *
 * <p>Assignment is pull-based: a reader asks for a split when it starts with none and asks again
 * whenever it finishes one, so a range that takes longer than its siblings does not hold up a
 * subtask that could be reading another. Parallelism therefore comes from how finely the table
 * happens to be sampled, and a table with one tablet is read by one subtask however many are
 * running.
 *
 * <p>The enumerator keeps <em>no</em> record of which subtask holds which split. Its whole state is
 * a queue of unassigned splits and the flag saying the plan exists, and every question a ledger
 * would answer is answered instead by what this class is handed — a request, or a returned split.
 * Flink's {@code SourceCoordinator} does the per-reader half already: it suppresses a further
 * request from a subtask it has told there are no more splits, and clears that only on a reset,
 * which is also what returns a failed reader's splits, so a returned split is always reachable by
 * the subtask that comes back for it.
 *
 * <p><b>A restore never samples again.</b> Tablets split and merge while a job runs, so a second
 * sampling would produce different boundaries and therefore different splits — under the same
 * ordinal ids the readers are already holding, which would make {@link #addSplitsBack} and the
 * readers' restored splits disagree about which range each id names. The checkpointed flag is what
 * prevents it, and it is not the same statement as "the queue is non-empty": a plan that has been
 * fully handed out must not be recomputed either.
 *
 * <p>The metrics follow the same rule as the state: counting assignments and returns needs no
 * reconciliation, while a gauge of currently-assigned splits would need the ledger this class
 * exists without. The unassigned side is Flink's own gauge, reading the queue directly — a
 * best-effort read, since the reporter thread samples a queue the coordinator thread mutates.
 */
@Internal
public class BigtableScanSplitEnumerator
        implements SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableScanSplitEnumerator.class);

    private final SplitEnumeratorContext<RowRangeSplit> context;
    private final BigtableSourceConfig<?> config;
    private final RowKeySampler sampler;
    @Nullable private final BigtableScanEnumeratorState restoredState;

    /** Splits no reader currently holds, in assignment order. */
    private final Deque<RowRangeSplit> pending = new ArrayDeque<>();

    /** Subtasks that asked for a split before the plan existed, in the order they asked. */
    private final Set<Integer> awaitingPlan = new LinkedHashSet<>();

    private boolean planned;

    /** Written by {@link #close()} on the scheduler thread, read by the completion handler. */
    private volatile boolean closed;

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter rowKeySamplesTaken = new ThreadSafeSimpleCounter();

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     */
    public BigtableScanSplitEnumerator(
            SplitEnumeratorContext<RowRangeSplit> context,
            BigtableSourceConfig<?> config,
            @Nullable BigtableScanEnumeratorState restoredState) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.sampler = config.getSampler();
        this.restoredState = restoredState;
    }

    @Override
    public void start() {
        registerMetrics();
        if (restoredState != null && restoredState.isPlanned()) {
            planned = true;
            pending.addAll(restoredState.getPendingSplits());
            LOG.info(
                    "Restored the Bigtable scan plan for {} with {} unassigned split(s); the table"
                            + " is not sampled again, so the split ids the readers hold keep their"
                            + " meaning.",
                    config.getTable(),
                    pending.size());
            return;
        }
        LOG.info(
                "Sampling row keys of {} to plan the scan (ranges={}, parallelism={}).",
                config.getTable(),
                describeRanges(),
                context.currentParallelism());
        context.callAsync(() -> sampler.sample(config.getTable()), this::onSampled);
    }

    /**
     * Runs on the coordinator thread once sampling finishes, so it needs no synchronization.
     * Throwing is how a split enumerator fails the job from an asynchronous call.
     */
    private void onSampled(@Nullable List<RowKeySample> samples, @Nullable Throwable error) {
        if (closed) {
            // The job is being torn down; failing it now would turn a clean cancellation into a
            // failure whose cause is our own shutdown.
            return;
        }
        if (error != null) {
            // Not softened into a single unsplit plan. The client already retried the transient
            // codes under a total timeout of its own, so a failure that reaches here is a
            // permission, quota or configuration problem — and a fallback would turn it into a job
            // that reads the whole table on one subtask for reasons nothing reports.
            throw new FlinkRuntimeException(
                    "Failed to sample the row keys of "
                            + config.getTable()
                            + "; the scan cannot be planned.",
                    error);
        }
        List<PlannedSplit> plan = RowRangeSplitPlanner.plan(config.getRanges(), samples);
        for (PlannedSplit split : plan) {
            pending.add(split.getSplit());
        }
        planned = true;
        rowKeySamplesTaken.inc();
        logPlan(samples, plan);

        List<Integer> waiting = new ArrayList<>(awaitingPlan);
        awaitingPlan.clear();
        for (int subtaskId : waiting) {
            serve(subtaskId);
        }
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

    /**
     * Registers the enumerator's metrics.
     *
     * <p>The null check is defensive: {@code SplitEnumeratorContext#metricGroup()} carries no
     * nullability annotation, and a context that answered with nothing would otherwise fail the job
     * at startup over its metrics. Flink's own contexts always provide one.
     */
    private void registerMetrics() {
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        splitsAssigned = metricGroup.counter(BigtableMetricNames.SPLITS_ASSIGNED, splitsAssigned);
        splitsReturned = metricGroup.counter(BigtableMetricNames.SPLITS_RETURNED, splitsReturned);
        rowKeySamplesTaken =
                metricGroup.counter(BigtableMetricNames.ROW_KEY_SAMPLES_TAKEN, rowKeySamplesTaken);
        metricGroup.setUnassignedSplitsGauge(() -> (long) pending.size());
    }

    @Override
    public void addReader(int subtaskId) {
        // Assignment is pull-based: a reader with no splits asks for one when it starts, and asks
        // again whenever it finishes one. Nothing to do when it merely registers.
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!planned) {
            LOG.info(
                    "Source subtask {} asked for a split before the scan was planned; it waits for"
                            + " the plan.",
                    subtaskId);
            awaitingPlan.add(subtaskId);
            return;
        }
        serve(subtaskId);
    }

    /** Hands a subtask the next unassigned split, or finishes it when there is none left. */
    private void serve(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // It failed while it was parked. Its splits, if it held any, come back through
            // addSplitsBack, and it asks again when it restarts.
            LOG.info("Source subtask {} is no longer registered; skipping its request.", subtaskId);
            return;
        }
        RowRangeSplit split = pending.poll();
        if (split == null) {
            // Nothing left right now. Nothing records that this subtask was told so: if a failed
            // reader returns a split later, this subtask is served again when it next asks.
            LOG.info("No Bigtable scan split left for source subtask {}.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        LOG.info("Assigning {} to source subtask {}.", split, subtaskId);
        splitsAssigned.inc();
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))));
    }

    @Override
    public void addSplitsBack(List<RowRangeSplit> splits, int subtaskId) {
        pending.addAll(splits);
        splitsReturned.inc(splits.size());
        LOG.info(
                "Source subtask {} returned {} scan split(s); they are reassigned on the next"
                        + " request.",
                subtaskId,
                splits.size());
    }

    @Override
    public BigtableScanEnumeratorState snapshotState(long checkpointId) {
        return new BigtableScanEnumeratorState(planned, new ArrayList<>(pending));
    }

    @Override
    public void close() throws IOException {
        // Runs on the scheduler thread, possibly while sampling is still in flight; the flag is
        // volatile so the completion handler sees it and stays quiet.
        closed = true;
        try {
            Closers.closeAll(sampler);
        } catch (Exception e) {
            throw new IOException("Failed to close the Bigtable row key sampler.", e);
        }
    }
}
