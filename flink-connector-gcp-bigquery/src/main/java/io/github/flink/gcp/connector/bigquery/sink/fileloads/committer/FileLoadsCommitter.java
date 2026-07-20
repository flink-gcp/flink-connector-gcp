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

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.BigQueryLoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobOrchestrator;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * The real FILE_LOADS commit: turns one committable batch — a whole batch run, or one checkpoint of
 * a streaming run — into BigQuery load jobs through a {@link LoadJobOrchestrator}.
 *
 * <p>The pre-commit topology routes every writer subtask's committables to one committer subtask (a
 * global exchange; in streaming behind the {@link FileLoadsCheckpointStamper}), so a single
 * instance sees all files and can group them per destination table. Running the loads <em>in the
 * committer</em> — not in a post-commit topology — is what makes the final batch of a streaming job
 * safe: the framework commits it during the final-checkpoint wait of task shutdown, before the job
 * can terminate, whereas records emitted to a post-commit topology at that point are not guaranteed
 * to be processed. Everything not yet committed rides in the framework's committer state and is
 * re-committed on restore, where deterministic job ids let the runner re-attach instead of
 * double-loading.
 *
 * <p>Loads are synchronous: a commit returns only when every load job of its batch has completed
 * (jobs are submitted first and awaited second, so BigQuery still runs them concurrently
 * server-side). In streaming execution a slow load therefore delays the next checkpoint — the
 * backpressure mechanism — and a failed load fails the job with the staged files left in place for
 * the post-restart retry.
 */
@Internal
public final class FileLoadsCommitter implements Committer<FileLoadsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoadsCommitter.class);

    /** Sustained streaming-commit cadence below this gap triggers the runtime quota warning. */
    private static final long QUOTA_WARN_GAP_MS = Duration.ofMinutes(2).toMillis();

    private static final long QUOTA_WARN_THROTTLE_MS = Duration.ofMinutes(10).toMillis();

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final StagingStorage storage;
    private final String flinkJobId;
    private final Supplier<LoadJobRunner> runnerFactory;
    private final Supplier<TableAdmin> tableAdminFactory;

    private long lastStreamingCommitMillis;
    private long lastQuotaWarnMillis;

    /**
     * Creates the committer.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     * @param storage the staging storage (post-load cleanup)
     * @param flinkJobId the Flink job id (hex), scoping temporary table names and job ids
     */
    public FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId) {
        this(
                config,
                options,
                storage,
                flinkJobId,
                () -> new BigQueryLoadJobRunner(config.getLocation()),
                BigQueryTableAdmin::new);
    }

    @VisibleForTesting
    FileLoadsCommitter(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            String flinkJobId,
            Supplier<LoadJobRunner> runnerFactory,
            Supplier<TableAdmin> tableAdminFactory) {
        this.config = config;
        this.options = options;
        this.storage = storage;
        this.flinkJobId = flinkJobId;
        this.runnerFactory = runnerFactory;
        this.tableAdminFactory = tableAdminFactory;
    }

    @Override
    public void commit(Collection<CommitRequest<FileLoadsCommittable>> requests)
            throws IOException {
        // The framework commits one checkpoint at a time, so all committables normally share one
        // checkpoint id; grouping keeps a defensive boundary (and covers the batch null id).
        Map<Long, List<FileLoadsCommittable>> byCheckpoint = new TreeMap<>();
        List<FileLoadsCommittable> batch = new ArrayList<>();
        for (CommitRequest<FileLoadsCommittable> request : requests) {
            FileLoadsCommittable committable = request.getCommittable();
            if (committable.getCheckpointId() == null) {
                batch.add(committable);
            } else {
                byCheckpoint
                        .computeIfAbsent(committable.getCheckpointId(), unused -> new ArrayList<>())
                        .add(committable);
            }
        }
        // A thrown IOException fails the ongoing commit; requests stay in committer state and are
        // re-committed after the restart (deterministic job ids re-attach on the retry).
        if (!batch.isEmpty()) {
            orchestrator(null).run(batch);
        }
        if (!byCheckpoint.isEmpty()) {
            maybeWarnQuota();
        }
        for (Map.Entry<Long, List<FileLoadsCommittable>> entry : byCheckpoint.entrySet()) {
            orchestrator(entry.getKey()).run(entry.getValue());
        }
        // Requests left unsignaled are treated as committed.
    }

    /**
     * Runtime backstop for the graph-construction interval guard (which cannot see cluster-side
     * checkpoint configuration): warns when streaming commits arrive faster than the sustained-safe
     * cadence for BigQuery's 1,500 load jobs per table per day.
     */
    private void maybeWarnQuota() {
        long now = System.currentTimeMillis();
        if (lastStreamingCommitMillis > 0
                && now - lastStreamingCommitMillis < QUOTA_WARN_GAP_MS
                && now - lastQuotaWarnMillis > QUOTA_WARN_THROTTLE_MS) {
            LOG.warn(
                    "Checkpoints are completing less than {} minutes apart; each checkpoint"
                            + " issues at least one load job per destination table and BigQuery"
                            + " allows 1,500 load jobs per table per day (2 min = 720/day,"
                            + " 1 min = 1,440/day). Increase the checkpoint interval for"
                            + " sustained streaming.",
                    QUOTA_WARN_GAP_MS / 60_000);
            lastQuotaWarnMillis = now;
        }
        lastStreamingCommitMillis = now;
    }

    private LoadJobOrchestrator orchestrator(Long checkpointId) {
        return new LoadJobOrchestrator(
                config,
                options,
                runnerFactory.get(),
                tableAdminFactory.get(),
                storage,
                flinkJobId,
                checkpointId);
    }

    @Override
    public void close() {}
}
