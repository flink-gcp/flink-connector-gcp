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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.BigQueryLoadJobRunner;
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
 * <p>Loads are synchronous: a commit returns only when every load and copy job of its batch has
 * completed. Independent jobs are submitted then awaited in bounded waves, so BigQuery still runs
 * each wave concurrently server-side; a copy level is awaited before its dependent level begins. In
 * streaming execution a slow job therefore delays the next checkpoint — the backpressure mechanism
 * — and a failed job fails the Flink job with every retry input left in place.
 */
@Internal
public final class FileLoadsCommitter implements Committer<FileLoadsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsCommitter.class);

    private static final long QUOTA_WARN_THROTTLE_MS = 10 * 60 * 1_000L;

    /**
     * Load jobs this committer has submitted to BigQuery. The one custom metric of the FILE_LOADS
     * path's commit side: it is what turns "the checkpoint took a while" into "the checkpoint
     * issued N load jobs". The overflow path's copy jobs are deliberately not counted because the
     * name says load jobs.
     *
     * <p>The framework registers the standard committer metrics ({@code totalCommittables} and
     * friends) itself; nothing here has to.
     */
    private final BigQuerySinkConfig<?> config;

    private final FileLoadsOptions options;
    private final StagingStorage storage;
    private final Supplier<LoadJobRunner> runnerFactory;
    private final Supplier<TableAdmin> tableAdminFactory;
    private final Counter loadJobsSubmitted;

    /** Committer-lifetime collaborators; the clients they hold are expensive to build. */
    private LoadJobRunner runner;

    private TableAdmin tableAdmin;

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
        this(
                config,
                options,
                storage,
                metricGroup,
                () ->
                        new BigQueryLoadJobRunner(
                                config.getLocation(),
                                options.toLoadJobPollSchedule(),
                                config.getServiceAccountKeyFile()),
                // Wrapped for the reason the storage writers' admins are (#383): a creation the
                // per-table metadata quota rate-limits is repeated rather than failing the commit.
                // This path races less — loads commit on one subtask, so nothing here competes with
                // itself — but a second job or a restart still can, and leaving it unwrapped would
                // make "a creation is retried" depend on which write method you picked.
                //
                // The reconcile schedule rather than a knob of its own: it is already this write
                // method's budget for contention on exactly that quota, since the etag race
                // updateSchema loses is the same per-table budget a creation spends.
                () ->
                        new RetryingTableAdmin(
                                new BigQueryTableAdmin(config.getServiceAccountKeyFile(), null),
                                options.toSchemaReconcileSchedule()));
    }

    @VisibleForTesting
    FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            SinkCommitterMetricGroup metricGroup,
            Supplier<LoadJobRunner> runnerFactory,
            Supplier<TableAdmin> tableAdminFactory) {
        this.config = config;
        this.options = options;
        this.storage = storage;
        this.loadJobsSubmitted = metricGroup.counter(BigQueryMetricNames.LOAD_JOBS_SUBMITTED);
        this.runnerFactory = runnerFactory;
        this.tableAdminFactory = tableAdminFactory;
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
                        runner(),
                        tableAdmin(),
                        storage,
                        first.getFlinkJobId(),
                        checkpointId,
                        loadJobsSubmitted)
                .run(committables);
        // Requests left unsignaled are treated as committed.
    }

    /** Returns the load-job runner wired from this committer's configuration. */
    @VisibleForTesting
    LoadJobRunner runner() {
        if (runner == null) {
            runner = runnerFactory.get();
        }
        return runner;
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
        if (tableAdmin == null) {
            tableAdmin = tableAdminFactory.get();
        }
        return tableAdmin;
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

    @Override
    public void close() {}
}
