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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;

import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Runs the staged files of one run — a whole batch job, or one checkpoint of a streaming job — into
 * their destination tables: submits every load, copy and terminal query through a {@link
 * LoadJobRunner}, then deletes the temporary tables and the staged objects.
 *
 * <p><b>The whole job graph is planned before any of it happens.</b> The {@code CommitPlanner} this
 * class builds in its constructor turns the committables into a {@link CommitPlan} — every job, its
 * deterministic id, its destination and its dispositions — and validates the plan's size, all
 * without touching BigQuery ({@code docs/adr/0018}). Why the plan has the shape it has, and what
 * makes it reproducible across retries, lives on that class rather than here.
 *
 * <p><b>Every load consults the live table first</b>, through the {@code FileLoadsSchemaReconciler}
 * this class builds in its constructor: one reconciliation per destination per commit, shared by
 * direct loads and the temp-table path alike, so whether a run fits one partition cannot decide
 * whether its records load. The policy and its rationale live on that class, not here.
 *
 * <p><b>Concurrency.</b> Independent jobs are submitted first and awaited second in deterministic
 * waves of at most 50,000 submissions, within BigQuery's per-project, per-region pending-job limit.
 * Interactive terminal queries use waves of at most 1,000, within their narrower queued-query
 * limit. Copy levels are barriers: a level is complete before a job that reads its tables is
 * submitted. BigQuery runs each wave concurrently server-side, so no thread pool is needed.
 *
 * <p><b>Retries.</b> Because the plan is a function of the committables alone, a retried run
 * submits the same job ids and the {@link LoadJobRunner} re-attaches instead of double-loading. On
 * success staged files are deleted best-effort; on failure everything is deliberately left in place
 * for the retry (temporary tables rely on the temp dataset's expiration, staging objects on the
 * bucket's lifecycle rule).
 */
@Internal
public final class LoadJobOrchestrator {

    // BigQuery admits at most 1,000 queued interactive queries per project and region.
    private static final int MAX_QUERY_SUBMISSIONS_PER_WAVE = 1_000;

    private static final Logger LOG = LoggerFactory.getLogger(LoadJobOrchestrator.class);

    private final CommitPlanner planner;
    private final FileLoadsSchemaReconciler reconciler;
    private final LoadJobRunner runner;
    private final StagingStorage storage;
    @Nullable private final Long checkpointId;
    private final Counter loadJobsSubmitted;
    private final Limits limits;

    /**
     * Creates an orchestrator.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param runner the job runner
     * @param tableAdmin the table admin (pre-load table creation and schema reconciliation)
     * @param storage the staging storage (post-load cleanup)
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     * @param checkpointId the checkpoint whose files this run loads, or {@code null} for a batch
     *     run; a non-null id scopes streaming job ids and temporary-table names
     * @param loadJobsSubmitted the committer's load-job counter. Passed as the counter rather than
     *     as a metric group because this type is constructed once per commit, while the metric it
     *     feeds is registered once per committer
     */
    public LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted) {
        this(
                config,
                options,
                runner,
                tableAdmin,
                storage,
                flinkJobId,
                checkpointId,
                loadJobsSubmitted,
                Limits.BIGQUERY);
    }

    @VisibleForTesting
    LoadJobOrchestrator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            LoadJobRunner runner,
            TableAdmin tableAdmin,
            StagingStorage storage,
            String flinkJobId,
            @Nullable Long checkpointId,
            Counter loadJobsSubmitted,
            Limits limits) {
        this.planner = new CommitPlanner(config, options, flinkJobId, checkpointId, limits);
        // Built here rather than accepted as a parameter: the memo inside it must not outlive this
        // commit, and this constructor is the only thing that can promise that (ADR-0021).
        this.reconciler = new FileLoadsSchemaReconciler(config, options, tableAdmin);
        this.runner = runner;
        this.storage = storage;
        this.loadJobsSubmitted = loadJobsSubmitted;
        this.checkpointId = checkpointId;
        this.limits = limits;
    }

    /**
     * Loads all staged files of the run into their destination tables.
     *
     * @param committables the staged files
     * @throws IOException if any load, copy, or terminal query fails; staged files are left in
     *     place
     */
    public void run(List<FileLoadsCommittable> committables) throws IOException {
        if (committables.isEmpty()) {
            LOG.info("No staged files; nothing to load");
            return;
        }
        CommitPlan plan = planner.plan(committables);

        runInWaves(plan.loads, this::submitLoad, load -> load.jobId);
        for (int level = 0; level < plan.intermediateLevelCount; level++) {
            List<PlannedCopy> jobs = new ArrayList<>();
            for (DestinationCopy copy : plan.copies) {
                if (level < copy.intermediateLevels.size()) {
                    jobs.addAll(copy.intermediateLevels.get(level));
                }
            }
            runInWaves(jobs, this::submitCopy, copy -> copy.jobId);
        }
        List<PlannedCopy> finalCopies = new ArrayList<>(plan.copies.size());
        for (DestinationCopy copy : plan.copies) {
            finalCopies.add(copy.finalCopy);
        }
        runInWaves(finalCopies, this::submitCopy, copy -> copy.jobId);

        List<PlannedQuery> terminalQueries = new ArrayList<>();
        for (DestinationCopy copy : plan.copies) {
            if (copy.terminalQuery != null) {
                terminalQueries.add(copy.terminalQuery);
            }
        }
        runInWaves(
                terminalQueries,
                this::submitQuery,
                query -> query.jobId,
                Math.min(limits.maxSubmissionsPerWave, MAX_QUERY_SUBMISSIONS_PER_WAVE));

        for (DestinationCopy copy : plan.copies) {
            for (TableDestination tempTable : copy.cleanupTables) {
                runner.deleteTable(tempTable);
            }
        }

        List<String> uris = new ArrayList<>(committables.size());
        long rows = 0;
        for (FileLoadsCommittable committable : committables) {
            uris.add(committable.getUri());
            rows += committable.getRowCount();
        }
        storage.deleteObjects(uris);
        LOG.info(
                "Loaded {} rows from {} staged files into {} tables{}",
                rows,
                committables.size(),
                plan.destinationCount,
                checkpointId != null ? " for checkpoint " + checkpointId : "");
    }

    private <T> void runInWaves(List<T> jobs, JobSubmitter<T> submitter, Function<T, String> jobId)
            throws IOException {
        runInWaves(jobs, submitter, jobId, limits.maxSubmissionsPerWave);
    }

    private <T> void runInWaves(
            List<T> jobs,
            JobSubmitter<T> submitter,
            Function<T, String> jobId,
            int maxSubmissionsPerWave)
            throws IOException {
        for (int start = 0; start < jobs.size(); start += maxSubmissionsPerWave) {
            int end = Math.min(start + maxSubmissionsPerWave, jobs.size());
            for (int i = start; i < end; i++) {
                submitter.submit(jobs.get(i));
            }
            for (int i = start; i < end; i++) {
                runner.awaitJob(jobId.apply(jobs.get(i)));
            }
        }
    }

    private void submitLoad(PlannedLoad load) throws IOException {
        // Reconcile, then count, then submit: a reconcile failure must not have counted a load job.
        Schema schema = reconciler.finalTableSchema(load.finalDestination);
        loadJobsSubmitted.inc();
        runner.submitLoad(
                load.jobId,
                new LoadJobSpec(
                        load.jobDestination,
                        load.uris,
                        schema,
                        load.createDisposition,
                        load.writeDisposition,
                        load.schemaUpdateOptions,
                        load.format));
    }

    private void submitCopy(PlannedCopy copy) throws IOException {
        runner.submitCopy(copy.jobId, copy.spec);
    }

    private void submitQuery(PlannedQuery query) throws IOException {
        runner.submitQuery(query.jobId, query.spec);
    }

    @FunctionalInterface
    private interface JobSubmitter<T> {

        void submit(T job) throws IOException;
    }
}
