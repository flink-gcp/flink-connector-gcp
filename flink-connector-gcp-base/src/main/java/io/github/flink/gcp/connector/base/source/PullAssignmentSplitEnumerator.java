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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A bounded source's split enumerator that plans once, asynchronously, and then hands the plan out
 * one split per request.
 *
 * <p>Assignment is pull-based: a reader asks for a split when it starts with none and asks again
 * whenever it finishes one, so a split that takes longer than its siblings does not hold up a
 * subtask that could be reading another. A request that arrives before the plan exists is parked
 * and served, in arrival order, as soon as it does.
 *
 * <p>The enumerator keeps <em>no</em> record of which subtask holds which split. Its whole state is
 * a queue of unassigned splits and the flag saying the plan exists, and every question a ledger
 * would answer is answered instead by what this class is handed — a request, or a returned split.
 * That is deliberate: the reference implementation the first of these enumerators was drawn from
 * records in its own change log a "critical data loss bug in reader split handling", fixed "by
 * signaling no-more-splits per reader and removing completed readers from queue" (read 2026-08-09).
 * What that establishes is that assignment and completion is where a hand-written enumerator goes
 * wrong quietly — and the per-reader half of it is something Flink's coordinator already does.
 *
 * <p>It does: {@code SourceCoordinator} suppresses a further split request from a subtask it has
 * already told there are no more splits, and clears that only when the subtask is reset. Since a
 * reset is also what returns a failed reader's splits ({@link #addSplitsBack}), a returned split is
 * always reachable by the subtask that comes back for it — but a <em>different</em> subtask that
 * already finished will not pick it up, because its requests no longer reach here.
 *
 * <p>The metrics follow the same rule. Counting assignments and returns needs no reconciliation,
 * while a gauge of currently-assigned splits would need the ledger this class exists without; the
 * unassigned side is Flink's own gauge, reading the queue directly — a best-effort read, since the
 * reporter thread samples a queue the coordinator thread mutates.
 *
 * <p><b>What a connector supplies</b> is the planning step and the words for it: {@link #restore()}
 * (a checkpointed plan is never recomputed), {@link #onPlanningStarted()}, {@link #plan()}, {@link
 * #onPlanned(Object)}, {@link #registerCounters(SplitEnumeratorMetricGroup)} and {@link
 * #snapshotState(long)}. Everything else here is {@code final}: the queue, the parking, the
 * assignment protocol, the guard that keeps a completion arriving after teardown quiet, and the
 * close of the one seam the enumerator owns.
 *
 * <p>Every method runs on the coordinator thread — including {@link #onPlanned(Object)}, which the
 * completion handler calls — so nothing here synchronizes. The two exceptions are named where they
 * are: {@link #plan()} runs off it, and {@link #close()} runs on the scheduler thread.
 *
 * <p>Logging goes to a logger named after the concrete subclass, so a log configuration scoped to
 * one connector keeps matching every line this protocol emits.
 *
 * @param <SplitT> the split type
 * @param <StateT> the checkpointed enumerator state
 * @param <PlanT> what the asynchronous planning call answers with
 */
@Internal
public abstract class PullAssignmentSplitEnumerator<SplitT extends SourceSplit, StateT, PlanT>
        implements SplitEnumerator<SplitT, StateT> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /** The enumerator context; a subclass reads the parallelism it plans against. */
    protected final SplitEnumeratorContext<SplitT> context;

    private final AutoCloseable planner;
    private final String splitNoun;
    private final String planFailureMessage;
    private final String closeFailureMessage;

    /** Splits no reader currently holds, in assignment order. */
    private final Deque<SplitT> pending = new ArrayDeque<>();

    /** Subtasks that asked for a split before the plan existed, in the order they asked. */
    private final Set<Integer> awaitingPlan = new LinkedHashSet<>();

    private boolean planned;

    /** Written by {@link #close()} on the scheduler thread, read by the completion handler. */
    private volatile boolean closed;

    private EnumeratorCounters counters = EnumeratorCounters.unregistered();

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param planner the seam the planning call goes through; the enumerator owns it and closes it
     * @param splitNoun what this connector calls one split, as it reads mid-sentence ("read stream
     *     split"), used in the assignment log lines
     * @param planFailureMessage what the job fails with when the planning call fails; the source
     *     cannot start
     * @param closeFailureMessage what the {@link java.io.IOException} says when the seam refuses to
     *     close. Both messages are whole sentences rather than nouns this class writes around, so a
     *     call site that swapped them would say so
     */
    protected PullAssignmentSplitEnumerator(
            SplitEnumeratorContext<SplitT> context,
            AutoCloseable planner,
            String splitNoun,
            String planFailureMessage,
            String closeFailureMessage) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.planner = Preconditions.checkNotNull(planner, "planner must not be null");
        this.splitNoun = Preconditions.checkNotNull(splitNoun, "splitNoun must not be null");
        this.planFailureMessage =
                Preconditions.checkNotNull(
                        planFailureMessage, "planFailureMessage must not be null");
        this.closeFailureMessage =
                Preconditions.checkNotNull(
                        closeFailureMessage, "closeFailureMessage must not be null");
    }

    @Override
    public final void start() {
        registerMetrics();
        if (restore()) {
            planned = true;
            return;
        }
        onPlanningStarted();
        context.callAsync(this::plan, this::onPlanCompleted);
    }

    /**
     * Restores a checkpointed plan, if there is one.
     *
     * <p>An implementation adds the restored splits with {@link #addPlannedSplits(Collection)},
     * takes whatever else its state carries, and says what it restored. Answering {@code true}
     * means the plan exists and <em>must not</em> be recomputed: a second planning call would
     * describe the source at a second instant, under the split ids the readers are already holding.
     *
     * @return whether a plan was restored, in which case no planning call is made
     */
    protected abstract boolean restore();

    /** Reports what is about to be planned, on the coordinator thread, before the call is made. */
    protected abstract void onPlanningStarted();

    /**
     * Plans the source's splits.
     *
     * <p>Runs <em>off</em> the coordinator thread, once, and only when nothing was restored. Its
     * result reaches {@link #onPlanned(Object)}; a failure fails the job with the message this
     * enumerator was built with.
     *
     * @return whatever the connector plans from
     * @throws Exception if the planning call fails
     */
    protected abstract PlanT plan() throws Exception;

    /**
     * Takes the plan, on the coordinator thread.
     *
     * <p>An implementation adds the planned splits with {@link #addPlannedSplits(Collection)},
     * keeps whatever its checkpointed state needs, and reports the plan — which is the only place a
     * skewed or unexpectedly small plan shows itself before the job runs.
     *
     * @param plan what {@link #plan()} answered with
     */
    protected abstract void onPlanned(PlanT plan);

    /**
     * Registers this connector's three enumerator counters on the group Flink supplied.
     *
     * <p>Called once, from {@link #start()}, and only when the context offered a metric group.
     * Registering here rather than in this class keeps every metric name in the connector that owns
     * it; see {@link EnumeratorCounters}.
     *
     * @param metricGroup the enumerator's metric group
     * @return the registered counters
     */
    protected abstract EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup);

    /**
     * Adds splits to the unassigned queue, in assignment order.
     *
     * @param splits the splits a restore or a plan produced
     */
    protected final void addPlannedSplits(Collection<? extends SplitT> splits) {
        pending.addAll(splits);
    }

    /**
     * Returns whether the plan exists, which is what a checkpoint records so a restore does not
     * plan again.
     *
     * <p>Not the same statement as "the queue is non-empty": a plan that has been fully handed out
     * must not be recomputed either.
     *
     * @return whether the source has been planned
     */
    protected final boolean isPlanned() {
        return planned;
    }

    /**
     * Returns the unassigned splits, as a copy a checkpoint can keep.
     *
     * @return the splits no reader currently holds, in assignment order
     */
    protected final List<SplitT> pendingSplits() {
        return new ArrayList<>(pending);
    }

    /**
     * Returns how many splits no reader currently holds.
     *
     * @return the unassigned split count
     */
    protected final int pendingSplitCount() {
        return pending.size();
    }

    /**
     * Runs on the coordinator thread once the planning call finishes, so it needs no
     * synchronization. Throwing is how a split enumerator fails the job from an asynchronous call.
     */
    private void onPlanCompleted(@Nullable PlanT plan, @Nullable Throwable error) {
        if (closed) {
            // The job is being torn down; failing it now would turn a clean cancellation into a
            // failure whose cause is our own shutdown.
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(planFailureMessage, error);
        }
        onPlanned(plan);
        planned = true;
        counters.plansCompleted().inc();
        List<Integer> waiting = new ArrayList<>(awaitingPlan);
        awaitingPlan.clear();
        for (int subtaskId : waiting) {
            serve(subtaskId);
        }
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
        counters =
                Preconditions.checkNotNull(
                        registerCounters(metricGroup), "registerCounters must not answer null");
        metricGroup.setUnassignedSplitsGauge(() -> (long) pending.size());
    }

    @Override
    public final void addReader(int subtaskId) {
        // Assignment is pull-based: a reader with no splits asks for one when it starts, and asks
        // again whenever it finishes one. Nothing to do when it merely registers.
    }

    @Override
    public final void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!planned) {
            log.info(
                    "Source subtask {} asked for a {} before the source was planned; it waits for"
                            + " the plan.",
                    subtaskId,
                    splitNoun);
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
            log.info("Source subtask {} is no longer registered; skipping its request.", subtaskId);
            return;
        }
        SplitT split = pending.poll();
        if (split == null) {
            // Nothing left right now. Nothing records that this subtask was told so: if a failed
            // reader returns a split later, this subtask is served again when it next asks.
            log.info("No {} left for source subtask {}.", splitNoun, subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        log.info("Assigning {} to source subtask {}.", split, subtaskId);
        counters.splitsAssigned().inc();
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))));
    }

    @Override
    public final void addSplitsBack(List<SplitT> splits, int subtaskId) {
        pending.addAll(splits);
        counters.splitsReturned().inc(splits.size());
        log.info(
                "Source subtask {} returned {} {}(s); they are reassigned on the next request.",
                subtaskId,
                splits.size(),
                splitNoun);
    }

    @Override
    public final void close() throws IOException {
        // Runs on the scheduler thread, possibly while the planning call is still in flight; the
        // flag is volatile so the completion handler sees it and stays quiet.
        closed = true;
        try {
            Closers.closeAll(planner);
        } catch (Exception e) {
            throw new IOException(closeFailureMessage, e);
        }
    }
}
