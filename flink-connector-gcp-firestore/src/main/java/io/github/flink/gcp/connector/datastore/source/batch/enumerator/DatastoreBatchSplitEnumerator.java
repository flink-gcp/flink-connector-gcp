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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.Query;
import io.github.flink.gcp.connector.base.source.EnumeratorCounters;
import io.github.flink.gcp.connector.base.source.PullAssignmentSplitEnumerator;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchEnumeratorState;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Plans the read once — its snapshot time and its queries — and hands the queries out one at a
 * time.
 *
 * <p>The assignment protocol is {@link PullAssignmentSplitEnumerator}'s. What this class adds is
 * the plan, and the rule that <b>a restore never plans again</b>: a second plan would pick a second
 * snapshot time, so a restored job would read part of the data at one time and the rest at another,
 * and new key-range boundaries would give the ids the readers hold a different meaning. The
 * checkpointed flag is what prevents it.
 */
@Internal
public class DatastoreBatchSplitEnumerator
        extends PullAssignmentSplitEnumerator<
                QuerySplit, DatastoreBatchEnumeratorState, QueryPlan> {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreBatchSplitEnumerator.class);

    private final DatastoreSourceConfig<?> config;
    private final QueryPlanner planner;
    @Nullable private final DatastoreBatchEnumeratorState restoredState;

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param planner the planner this enumerator owns and closes; the source mints one per
     *     enumerator, so it is never one an earlier enumerator already closed
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     */
    public DatastoreBatchSplitEnumerator(
            SplitEnumeratorContext<QuerySplit> context,
            DatastoreSourceConfig<?> config,
            QueryPlanner planner,
            @Nullable DatastoreBatchEnumeratorState restoredState) {
        super(
                context,
                checkedPlanner(config, planner),
                "query split",
                "Failed to plan the Datastore read of "
                        + describe(config)
                        + "; the source cannot start.",
                "Failed to close the Datastore query planner.");
        this.config = config;
        this.planner = planner;
        this.restoredState = restoredState;
    }

    /**
     * Checks both arguments the {@code super(...)} call needs, first among them so that a null
     * configuration is named here rather than thrown from the message.
     */
    private static QueryPlanner checkedPlanner(
            DatastoreSourceConfig<?> config, QueryPlanner planner) {
        Preconditions.checkNotNull(config, "config must not be null");
        return Preconditions.checkNotNull(planner, "planner must not be null");
    }

    private static String describe(DatastoreSourceConfig<?> config) {
        String namespace =
                config.getNamespace().isEmpty()
                        ? "the default namespace"
                        : "namespace '" + config.getNamespace() + "'";
        Query query = config.getQuery();
        String kind = query != null ? SplittableQueries.kindOf(query) : null;
        String shape =
                query == null
                        ? "a GQL query"
                        : kind != null ? "a query of kind '" + kind + "'" : "a query";
        return shape + " in " + namespace + " of " + config.getDatabase();
    }

    @Override
    protected boolean restore() {
        if (restoredState == null || !restoredState.isPlanned()) {
            return false;
        }
        addPlannedSplits(restoredState.getPendingSplits());
        LOG.info(
                "Restored the Datastore read plan for {} with {} unassigned split(s); the read is"
                        + " not planned again, so it stays on the snapshot it started on.",
                describe(config),
                pendingSplitCount());
        return true;
    }

    @Override
    protected void onPlanningStarted() {
        LOG.info(
                "Planning the Datastore read of {} (splitCount={}, readTime={}, parallelism={}).",
                describe(config),
                config.getSplitCount() != null ? config.getSplitCount() : "estimated",
                config.getReadTime() != null ? config.getReadTime() : "the service's now",
                context.currentParallelism());
    }

    @Override
    protected QueryPlan plan() throws Exception {
        return planner.plan(config, context.currentParallelism());
    }

    @Override
    protected void onPlanned(QueryPlan plan) {
        List<QuerySplit> splits = new ArrayList<>();
        for (int i = 0; i < plan.getRequests().size(); i++) {
            splits.add(
                    new QuerySplit(
                            String.valueOf(i), plan.getRequests().get(i), plan.getReadTime()));
        }
        addPlannedSplits(splits);
        if (plan.getUnsplittableReason() != null) {
            LOG.info(
                    "Planned one split for {} at read time {}: the query is read as one split"
                            + " because {}.",
                    describe(config),
                    plan.getReadTime(),
                    plan.getUnsplittableReason());
        } else if (splits.size() < context.currentParallelism()) {
            LOG.warn(
                    "Planned {} split(s) for {} at read time {} and parallelism {}; the subtasks"
                            + " left without one finish immediately. A configured splitCount"
                            + " below the parallelism does this, and so does the splitter when the"
                            + " kind is small or when it samples no keys, as the emulator always"
                            + " does.",
                    splits.size(),
                    describe(config),
                    plan.getReadTime(),
                    context.currentParallelism());
        } else {
            LOG.info(
                    "Planned {} split(s) for {} at read time {} and parallelism {}.",
                    splits.size(),
                    describe(config),
                    plan.getReadTime(),
                    context.currentParallelism());
        }
    }

    @Override
    protected EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup) {
        return new EnumeratorCounters(
                metricGroup.counter(
                        DatastoreMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        DatastoreMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        DatastoreMetricNames.READS_PLANNED, new ThreadSafeSimpleCounter()));
    }

    @Override
    public DatastoreBatchEnumeratorState snapshotState(long checkpointId) {
        return new DatastoreBatchEnumeratorState(isPlanned(), pendingSplits());
    }
}
