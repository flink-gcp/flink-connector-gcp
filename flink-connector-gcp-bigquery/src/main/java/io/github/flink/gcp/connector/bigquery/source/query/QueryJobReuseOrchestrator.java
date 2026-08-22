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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

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
 * reported as a {@link ProbeOutcome} carrying the reason the give-up message quotes.
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

    /**
     * How many {@code _rN} ids past a failed previous attempt are tried.
     *
     * <p>The load runner's bound, for the load runner's reason: each probe means a previous attempt
     * failed at this same query, so a chain this long is a systemic failure no fresh id will fix.
     */
    @VisibleForTesting static final int MAX_RETRY_PROBES = 5;

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
     * Walks the {@code _rN} chain until an id yields the query's result, or reports that none can.
     *
     * @return the query's result, and whether a previous attempt's job was reused
     * @throws IOException if every id in the chain is unusable, or a call to the service fails
     */
    QueryResult run() throws IOException {
        String lastError = null;
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
            ProbeOutcome outcome = probeOnce(probe);
            if (outcome.result != null) {
                return outcome.result;
            }
            lastError = outcome.error;
        }
        throw new IOException(
                "The BigQuery query job "
                        + identity.getCurrentJobId()
                        + " and all its retry ids are unusable from previous attempts; last"
                        + " error: "
                        + lastError);
    }

    /**
     * What one id of the {@code _rN} chain yielded: the query's result, or the reason that id
     * cannot be used and the walk moves on to the next.
     *
     * <p>Exactly one of the two is set, and the factories check it rather than document it: the
     * walk quotes the last reason in the message it gives up with, so "an id that yields no result
     * yields a reason" has to hold — as a property of this type rather than of every exit from a
     * probe remembering to record one. A probe has four such exits and gains one with each new way
     * an id can turn out unusable, which is the shape that makes an invariant held by convention
     * worth moving into a type.
     */
    private static final class ProbeOutcome {

        @Nullable final QueryResult result;
        @Nullable final String error;

        private ProbeOutcome(@Nullable QueryResult result, @Nullable String error) {
            this.result = result;
            this.error = error;
        }

        /** The query's result, from a job this probe ran, adopted, or attached to. */
        static ProbeOutcome of(QueryResult result) {
            return new ProbeOutcome(Preconditions.checkNotNull(result, "result"), null);
        }

        /** The reason this id is unusable, already logged by whichever check produced it. */
        static ProbeOutcome unusable(String error) {
            // Checked rather than trusted, because the invariant above is what the give-up
            // message rests on: a null here would surface as "last error: null" on a
            // JobManager, a page past the probe that failed to record a reason.
            return new ProbeOutcome(null, Preconditions.checkNotNull(error, "error"));
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
    private ProbeOutcome probeOnce(int probe) throws IOException {
        String queryJobId = retryJobId(identity.getCurrentJobId(), probe);
        Job existing = runner.lookUp(jobId(queryJobId), spec.getProject());
        if (existing != null) {
            return adopt(queryJobId, existing, probe);
        }
        if (probe == 0) {
            Job straddled = previousWindowJob();
            if (straddled != null) {
                LOG.info(
                        "Re-attached to the BigQuery query job {} from the previous reuse window"
                                + " (state {}).",
                        straddled.getJobId().getJob(),
                        state(straddled.getStatus()));
                return ProbeOutcome.of(new QueryResult(awaitAndLand(straddled), true));
            }
        }
        return submitOrAttach(queryJobId, probe);
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
    private ProbeOutcome adopt(String queryJobId, Job existing, int probe) throws IOException {
        if (BigQueryQueryRunner.isFailed(existing.getStatus())) {
            return ProbeOutcome.unusable(probePast(queryJobId, existing.getStatus(), probe));
        }
        TableId vanished = runner.vanishedResultTable(existing, spec.getProject());
        if (vanished != null) {
            return ProbeOutcome.unusable(probePastVanished(queryJobId, vanished, probe));
        }
        LOG.info(
                "Re-attached to the BigQuery query job {} from a previous attempt (state {}).",
                queryJobId,
                state(existing.getStatus()));
        return ProbeOutcome.of(new QueryResult(awaitAndLand(existing), true));
    }

    /**
     * Submits under this id, adopting instead the job a racing attempt got in first.
     *
     * <p>Reached only where the look-up found nothing, so the two judgments {@link #adopt(String,
     * Job, int)} makes are made again here — on the conflict winner's job, which is as much a
     * previous attempt's as one the look-up would have found.
     */
    private ProbeOutcome submitOrAttach(String queryJobId, int probe) throws IOException {
        BigQueryQueryRunner.Submitted created =
                runner.submit(
                        jobId(queryJobId),
                        BigQueryQueryRunner.configuration(spec, queryJobId),
                        spec);
        if (BigQueryQueryRunner.isFailed(created.job.getStatus())) {
            // create lost its race to a zombie that had itself already failed, whose id is as
            // unusable as one the look-up would have found failed.
            return ProbeOutcome.unusable(probePast(queryJobId, created.job.getStatus(), probe));
        }
        if (created.conflicted) {
            TableId vanished = runner.vanishedResultTable(created.job, spec.getProject());
            if (vanished != null) {
                return ProbeOutcome.unusable(probePastVanished(queryJobId, vanished, probe));
            }
            LOG.info(
                    "Re-attached to the BigQuery query job {} another attempt submitted first"
                            + " (state {}).",
                    queryJobId,
                    state(created.job.getStatus()));
        } else {
            LOG.info("Running the BigQuery source's query as job {}: {}", queryJobId, spec);
        }
        return ProbeOutcome.of(new QueryResult(awaitAndLand(created.job), created.conflicted));
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
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
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
        return null;
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

    /** Logs the failed job being walked past and answers its error for the give-up message. */
    private static String probePast(String queryJobId, JobStatus status, int probe) {
        String error = status.getError().toString();
        if (probe < MAX_RETRY_PROBES) {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt failed ({}); probing the"
                            + " next retry id.",
                    queryJobId,
                    error);
        } else {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt failed ({}), and no retry"
                            + " ids are left to probe.",
                    queryJobId,
                    error);
        }
        return error;
    }

    /** Logs the vanished result table being walked past and answers the give-up line's error. */
    private static String probePastVanished(String queryJobId, TableId vanished, int probe) {
        String error =
                "its result table "
                        + vanished.getDataset()
                        + "."
                        + vanished.getTable()
                        + " is gone";
        if (probe < MAX_RETRY_PROBES) {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt completed, but {}; probing"
                            + " the next retry id.",
                    queryJobId,
                    error);
        } else {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt completed, but {}, and no"
                            + " retry ids are left to probe.",
                    queryJobId,
                    error);
        }
        return error;
    }
}
