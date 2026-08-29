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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.BigQueryLoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.DestinationCommitExecutor;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobOrchestrator;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The real FILE_LOADS commit: turns one committable batch — a whole batch run, or one checkpoint of
 * a streaming run — into BigQuery load jobs through a {@link LoadJobOrchestrator}.
 *
 * <p>The pre-commit topology routes every writer subtask's committables to one committer subtask (a
 * global exchange; in streaming behind the checkpoint-stamping stage), so a single instance sees
 * all files and can group them per destination table. Running the loads <em>in the committer</em> —
 * not in a post-commit topology — is what makes the final batch of a streaming job safe: the
 * framework commits it during the final-checkpoint wait of task shutdown, before the job can
 * terminate, whereas records emitted to a post-commit topology at that point are not guaranteed to
 * be processed. Everything not yet committed rides in the framework's committer state and is
 * re-committed on restore; job ids are derived from the committables themselves (which carry their
 * originating Flink job id), so the runner re-attaches instead of double-loading even when the
 * restore runs under a new Flink job id.
 *
 * <p>Loads are synchronous: a commit returns only when every load, copy, and terminal query job of
 * its batch has completed. Independent destinations run through a bounded, committer-lifetime
 * worker pool; each worker owns its BigQuery client. Each commit-wide job wave is submitted across
 * all destinations before it is awaited, preserving server-side job parallelism while bounding
 * concurrent client calls. In streaming execution a slow destination therefore delays the next
 * checkpoint — the backpressure mechanism — and an ordinary destination failure stops new dispatch,
 * drains every job already submitted in the wave, and fails the Flink job with every retry input
 * left in place. A JVM-fatal failure aborts its current worker action immediately.
 */
@Internal
public final class FileLoadsCommitter implements Committer<FileLoadsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsCommitter.class);

    private static final long QUOTA_WARN_THROTTLE_MS = 10 * 60 * 1_000L;

    private final BigQuerySinkConfig<?> config;

    private final FileLoadsOptions options;
    private final StagingStorage storage;
    private final DestinationCommitExecutor.WorkerFactory workerFactory;
    private final DestinationCommitExecutor destinationExecutor;

    /**
     * Metrics for the FILE_LOADS commit side. The load-job counter turns "the checkpoint took a
     * while" into "the checkpoint issued N load jobs"; the aggregate destination and duration
     * gauges expose whether that time is queued or active without per-destination cardinality. The
     * overflow path's copy jobs are deliberately not counted because the counter says load jobs.
     *
     * <p>The framework registers the standard committer metrics ({@code totalCommittables} and
     * friends) itself; nothing here has to.
     */
    private final FileLoadsCommitterMetrics metrics;

    /** Committer-lifetime collaborators; the clients they hold are expensive to build. */
    private DestinationCommitExecutor.Worker testWorker;

    private long lastStreamingCommitMillis;
    private long lastQuotaWarnMillis;

    /**
     * Creates the committer.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param storage the staging storage (post-load cleanup)
     * @param metricGroup the committer's metric group
     */
    public FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkCommitterMetricGroup metricGroup) {
        this(config, options, storage, metricGroup, productionWorkerFactory(config, options));
    }

    @VisibleForTesting
    FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkCommitterMetricGroup metricGroup,
            Supplier<LoadJobRunner> runnerFactory,
            Supplier<TableAdmin> tableAdminFactory) {
        this(
                config,
                options,
                storage,
                metricGroup,
                () ->
                        new DestinationCommitExecutor.Worker(
                                runnerFactory.get(), tableAdminFactory.get()));
    }

    @VisibleForTesting
    FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkCommitterMetricGroup metricGroup,
            DestinationCommitExecutor.WorkerFactory workerFactory) {
        this.config = config;
        this.options = options;
        this.storage = storage;
        this.metrics = new FileLoadsCommitterMetrics(metricGroup);
        this.workerFactory = workerFactory;
        this.destinationExecutor =
                new DestinationCommitExecutor(
                        options.getMaxConcurrentDestinations(), workerFactory, metrics);
    }

    @Override
    public void commit(Collection<CommitRequest<FileLoadsCommittable>> requests)
            throws IOException {
        if (requests.isEmpty()) {
            return;
        }
        // The framework commits one checkpoint at a time, so every committable of a call shares
        // one (possibly null) checkpoint id and one originating Flink job id; a mix would break
        // the per-checkpoint job-id attribution, so it fails loudly instead of being split up.
        List<FileLoadsCommittable> committables = new ArrayList<>(requests.size());
        for (CommitRequest<FileLoadsCommittable> request : requests) {
            committables.add(request.getCommittable());
        }
        FileLoadsCommittable first = committables.get(0);
        for (FileLoadsCommittable committable : committables) {
            Preconditions.checkState(
                    Objects.equals(committable.getCheckpointId(), first.getCheckpointId())
                            && committable.getFlinkJobId().equals(first.getFlinkJobId()),
                    "A commit batch mixes checkpoints or Flink job ids: %s vs %s",
                    first,
                    committable);
        }
        Long checkpointId = first.getCheckpointId();
        if (checkpointId != null) {
            warnIfCommitsAreTooFrequent();
        }
        // A thrown IOException fails the ongoing commit; requests stay in committer state and are
        // re-committed after the restart (deterministic job ids re-attach on the retry).
        new LoadJobOrchestrator(
                        config,
                        options,
                        storage,
                        first.getFlinkJobId(),
                        checkpointId,
                        metrics.loadJobsSubmitted(),
                        destinationExecutor)
                .run(committables);
        // Requests left unsignaled are treated as committed.
    }

    /**
     * The admin this committer creates and reconciles destination tables through.
     *
     * <p>Package-private rather than private so a test can assert that the public constructor's
     * default factory wraps it for the creation retry (#383): every other test here injects its own
     * factory, so a default that stopped wrapping would leave them all green.
     */
    @VisibleForTesting
    TableAdmin tableAdmin() {
        return testWorker().tableAdmin();
    }

    private DestinationCommitExecutor.Worker testWorker() {
        if (testWorker == null) {
            try {
                testWorker = workerFactory.create();
            } catch (IOException failure) {
                throw new IllegalStateException("Could not create FILE_LOADS test worker", failure);
            }
        }
        return testWorker;
    }

    @VisibleForTesting
    static DestinationCommitExecutor.WorkerFactory productionWorkerFactory(
            BigQuerySinkConfig<?> config, FileLoadsOptions options) {
        BigQueryLoadJobRunner.SharedJobs sharedJobs = new BigQueryLoadJobRunner.SharedJobs();
        return () -> {
            BigQuery client =
                    config.getServiceAccountKeyFile() == null
                            ? BigQueryOptions.getDefaultInstance().getService()
                            : BigQueryCredentials.bigQueryOptions(config.getServiceAccountKeyFile())
                                    .getService();
            LoadJobRunner runner =
                    new BigQueryLoadJobRunner(
                            client,
                            config.getLocation(),
                            options.toLoadJobPollSchedule(),
                            sharedJobs);
            // Wrapped for the reason the storage writers' admins are (#383). Each worker owns one
            // client and admin, so SDK clients and table-admin caches never cross worker threads.
            // Only the concurrent submitted-job registry is shared: the global wait phase may
            // assign a job to a different worker from the global submission phase.
            TableAdmin tableAdmin =
                    new RetryingTableAdmin(
                            new BigQueryTableAdmin(client), options.toSchemaReconcileSchedule());
            return new DestinationCommitExecutor.Worker(runner, tableAdmin);
        };
    }

    /**
     * Runtime backstop for the graph-construction interval guard (which cannot see cluster-side
     * checkpoint configuration): warns when streaming commits arrive faster than the configured
     * minimum checkpoint interval — the same threshold the graph-side guard enforces, so an
     * explicit {@code minCheckpointInterval} opt-in silences this warning too.
     */
    private void warnIfCommitsAreTooFrequent() {
        long warnGapMs = options.getMinCheckpointInterval().toMillis();
        long now = System.currentTimeMillis();
        if (lastStreamingCommitMillis > 0
                && now - lastStreamingCommitMillis < warnGapMs
                && now - lastQuotaWarnMillis > QUOTA_WARN_THROTTLE_MS) {
            LOG.warn(
                    "Checkpoints are completing less than {} ms apart; each checkpoint issues a"
                            + " direct load or an overflow copy per destination table, and"
                            + " BigQuery allows 1,500 modifications per standard table per day"
                            + " (2 min = 720/day, 1 min ="
                            + " 1,440/day). Increase the checkpoint interval for sustained"
                            + " streaming.",
                    warnGapMs);
            lastQuotaWarnMillis = now;
        }
        lastStreamingCommitMillis = now;
    }

    /**
     * Shuts down the destination worker pool, then releases the staging client. Worker-pool
     * shutdown waits up to 30 seconds for an orderly stop and, if needed, another 30 seconds after
     * interrupting the workers. The committer holds its own staging client, not the writer's: the
     * global exchange that {@code BigQueryFileLoadsSink.addPreCommitTopology} ends with puts the
     * committer in a vertex of its own, so it deserializes its own copy of the sink and builds its
     * own client. The writer closing its copy releases nothing here.
     *
     * <p>The lazily built {@link LoadJobRunner} and {@link TableAdmin} are not released here
     * because neither declares a {@code close()}, and neither has to: {@code
     * com.google.cloud.bigquery.BigQuery}, the client behind both, is not {@code AutoCloseable}.
     *
     * @throws Exception if the worker pool or staging client cannot close
     */
    @Override
    public void close() throws Exception {
        Closers.closeAll(destinationExecutor::close, storage::close);
    }
}
