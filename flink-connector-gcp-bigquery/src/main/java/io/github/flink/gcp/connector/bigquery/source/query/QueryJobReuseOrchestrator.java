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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Turns a {@link QueryJobIdentity}'s deterministic id into the query's result, by reusing a
 * previous attempt's job where one is still usable and submitting where none is.
 *
 * <p>The shape is {@code BigQueryLoadJobRunner#submitOrAttach}'s, not a call to it: the load
 * runner's machinery exists for the sink's exactly-once commit and carries an active-jobs map and
 * copy jobs this path has no use for (ADR-0087 records the decision not to hoist it). The one
 * judgment they must share is shared by construction — a job that finished with an error can never
 * be reused, and its id can never be resubmitted, so each failed id is probed past to the next
 * {@code _rN}.
 *
 * <p>The walk itself is {@link #run()}; what one id yields is {@link #probeOnce(int)}, and the two
 * ways an id can be unusable — a failed job, a finished job whose result table is gone — are each
 * logged and answered with no result, which moves the walk to the next id.
 *
 * <p><b>Every call to the service goes through the runner that made this one</b>, including the one
 * only this path makes — the check that an adopted job's result table still exists. The runner
 * opens the REST client on first use and keeps it private, so what is here is which id to try next
 * and whether what was found under it may be adopted, and nothing that could open a second client.
 * Separated for that reason: without the deterministic id nothing is ever reused, and a reader of
 * the ordinary path should not have to walk past a chain that never runs for it.
 *
 * <p>Built for one {@code run} call and then dropped, so — unlike the runner — nothing here travels
 * in the job graph or has to be serializable.
 */
@Internal
final class QueryJobReuseOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(QueryJobReuseOrchestrator.class);

    private final BigQueryQueryRunner runner;
    private final QueryJobIdentity identity;
    private final QuerySpec spec;

    QueryJobReuseOrchestrator(
            BigQueryQueryRunner runner, QueryJobIdentity identity, QuerySpec spec) {
        this.runner = runner;
        this.identity = identity;
        this.spec = spec;
    }

    /**
     * Walks the {@code _rN} chain until an id yields the query's result.
     *
     * <p>The walk has no cap. Every id it passes holds a job some attempt created, so the first
     * unused id ends it, and how often a failing read is retried is the Flink restart strategy's to
     * decide. A query refused as this attempt submits it fails the attempt rather than moving on,
     * since the next id would be refused the same way (#2181).
     *
     * @return the query's result, and whether a previous attempt's job was reused
     * @throws IOException if this attempt's own submission is refused, or a call to the service
     *     fails
     */
    QueryResult run() throws IOException {
        for (int probe = 0; ; probe++) {
            QueryResult result = probeOnce(probe);
            if (result != null) {
                return result;
            }
        }
    }

    /**
     * Probes one id of the chain: adopts what a previous attempt left there, or submits under it.
     *
     * <p>The previous window's id is consulted only when the current one has no job at all, which
     * is the one case a rollover between the first attempt and the re-plan produces; it is only
     * ever attached to, never submitted, and only when the job's creation time is still inside the
     * window — the id alone already bounds it to twice the window, and the check is what makes the
     * documented window exact rather than "up to twice".
     */
    @Nullable
    private QueryResult probeOnce(int probe) throws IOException {
        String queryJobId = retryJobId(identity.getCurrentJobId(), probe);
        Job existing = runner.lookUp(jobId(queryJobId), spec.getProject());
        if (existing != null) {
            return adopt(queryJobId, existing);
        }
        if (probe == 0) {
            Job straddled = previousWindowJob();
            if (straddled != null) {
                LOG.info(
                        "Re-attached to the BigQuery query job {} from the previous reuse window"
                                + " (state {}).",
                        straddled.getJobId().getJob(),
                        state(straddled.getStatus()));
                return new QueryResult(awaitAndLand(straddled), true);
            }
        }
        return submitOrAttach(queryJobId);
    }

    /**
     * Adopts the job a previous attempt left under this id, or reports why it cannot be.
     *
     * <p>Adopting a <em>finished</em> job spends one {@code getTable} on its result table, because
     * the job's metadata names that table whether or not it still exists (#485). A table gone
     * <em>early</em> — deleted by hand from a named dataset, or an anonymous cached-results table
     * BigQuery dropped inside its nominal day — would otherwise surface only at session creation,
     * and the restarted job would re-plan into the same adoption until the bucket rolled. A
     * vanished table is treated exactly like a failed link: probed past, so the query is submitted
     * fresh under the next retry id.
     */
    @Nullable
    private QueryResult adopt(String queryJobId, Job existing) throws IOException {
        if (BigQueryQueryRunner.isFailed(existing.getStatus())) {
            probePast(queryJobId, existing.getStatus());
            return null;
        }
        TableId vanished = runner.vanishedResultTable(existing, spec.getProject());
        if (vanished != null) {
            probePastVanished(queryJobId, vanished);
            return null;
        }
        LOG.info(
                "Re-attached to the BigQuery query job {} from a previous attempt (state {}).",
                queryJobId,
                state(existing.getStatus()));
        return new QueryResult(awaitAndLand(existing), true);
    }

    /**
     * Submits under this id, adopting instead the job a racing attempt got in first.
     *
     * <p>Reached only where the look-up found nothing, so the two judgments {@link #adopt(String,
     * Job)} makes are made again here — on the conflict winner's job, which is as much a previous
     * attempt's as one the look-up would have found.
     */
    @Nullable
    private QueryResult submitOrAttach(String queryJobId) throws IOException {
        BigQueryQueryRunner.Submitted created =
                runner.submit(
                        jobId(queryJobId),
                        BigQueryQueryRunner.configuration(spec, queryJobId),
                        spec);
        if (BigQueryQueryRunner.isFailed(created.job.getStatus())) {
            if (!created.conflicted) {
                // This attempt's own submission, refused as it was submitted. The next id would
                // be refused the same way, one job after another for as long as the attempt
                // lasted; failing the attempt submits one, and the retry probes past it.
                throw new IOException(
                        "The BigQuery query job "
                                + queryJobId
                                + " was refused as it was submitted: "
                                + created.job.getStatus().getError());
            }
            // create lost its race to a zombie that had itself already failed, or this
            // submission's response was lost and the SDK's retry was answered 409; either way the
            // id is as unusable as one the look-up would have found failed, and the walk moves on.
            probePast(queryJobId, created.job.getStatus());
            return null;
        }
        if (created.conflicted) {
            TableId vanished = runner.vanishedResultTable(created.job, spec.getProject());
            if (vanished != null) {
                probePastVanished(queryJobId, vanished);
                return null;
            }
            LOG.info(
                    "Re-attached to the BigQuery query job {} another attempt submitted first"
                            + " (state {}).",
                    queryJobId,
                    state(created.job.getStatus()));
        } else {
            LOG.info("Running the BigQuery source's query as job {}: {}", queryJobId, spec);
        }
        return new QueryResult(awaitAndLand(created.job), created.conflicted);
    }

    /**
     * Returns the previous window's job where attaching to it is still allowed, or {@code null}.
     *
     * <p>Walks the same {@code _rN} chain the submitter would have left behind, because a previous
     * attempt that probed past a failed job left its live one under a retry id. A failed link is
     * walked past — as is a finished one whose result table has vanished, the same judgment the
     * current window applies before adopting (#485) — the first absent id ends the chain; and the
     * job the chain ends at is reused only if it reports a creation time inside the window. No
     * creation time reads as "do not reuse" — running the query again costs money, not correctness.
     */
    @Nullable
    private Job previousWindowJob() throws IOException {
        for (int probe = 0; ; probe++) {
            String queryJobId = retryJobId(identity.getPreviousJobId(), probe);
            Job job = runner.lookUp(jobId(queryJobId), spec.getProject());
            if (job == null) {
                return null;
            }
            if (BigQueryQueryRunner.isFailed(job.getStatus())) {
                continue;
            }
            Long created = creationTime(job);
            if (created == null || !identity.isWithinWindow(created, System.currentTimeMillis())) {
                return null;
            }
            TableId vanished = runner.vanishedResultTable(job, spec.getProject());
            if (vanished != null) {
                // Not probePastVanished: this look-back never runs anything, and the chain's next
                // link — left by an attempt that made this same walk — may be adopted.
                LOG.warn(
                        "The BigQuery query job {} from the previous reuse window completed, but"
                                + " its result table {}.{} is gone; walking past it.",
                        queryJobId,
                        vanished.getDataset(),
                        vanished.getTable());
                continue;
            }
            return job;
        }
    }

    /** The job id one {@code _rN} of the chain names, scoped to the query's project. */
    private JobId jobId(String queryJobId) {
        return BigQueryQueryRunner.jobId(spec, queryJobId);
    }

    /** Polls a job to completion and reads the table its result landed in. */
    private TableDestination awaitAndLand(Job job) throws IOException {
        return runner.landed(runner.await(job, spec), spec);
    }

    private static String retryJobId(String base, int probe) {
        // An underscore, not the load runner's hyphen: this string is also the result table's
        // name, and a hyphen is not legal there.
        return probe == 0 ? base : base + "_r" + probe;
    }

    @Nullable
    private static Long creationTime(Job job) {
        JobStatistics statistics = job.getStatistics();
        return statistics == null ? null : statistics.getCreationTime();
    }

    private static String state(@Nullable JobStatus status) {
        return status == null || status.getState() == null ? "unknown" : status.getState().name();
    }

    /** Logs the failed job being walked past. */
    private static void probePast(String queryJobId, JobStatus status) {
        String error = status.getError().toString();
        LOG.warn(
                "The BigQuery query job {} from a previous attempt failed ({}); probing the next"
                        + " retry id.",
                queryJobId,
                error);
    }

    /** Logs the vanished result table being walked past. */
    private static void probePastVanished(String queryJobId, TableId vanished) {
        String error =
                "its result table "
                        + vanished.getDataset()
                        + "."
                        + vanished.getTable()
                        + " is gone";
        LOG.warn(
                "The BigQuery query job {} from a previous attempt completed, but {}; probing the"
                        + " next retry id.",
                queryJobId,
                error);
    }
}
