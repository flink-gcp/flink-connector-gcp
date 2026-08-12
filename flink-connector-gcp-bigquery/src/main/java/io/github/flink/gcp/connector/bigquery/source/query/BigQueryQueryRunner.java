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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.UUID;

/**
 * Runs the source's query through the REST {@link BigQuery} client.
 *
 * <p><b>Where the result lands is the caller's choice, and the two places differ in who cleans
 * up.</b> With no result dataset the job is submitted with no destination table at all, and
 * BigQuery writes the result into its own anonymous dataset, expires it after about a day and
 * charges no storage for it — nothing here creates a resource, so nothing here has to remove one.
 * Naming a dataset instead puts the result in a table this class creates there, and the expiration
 * it sets on that table is the only cleanup: a job's teardown also runs on a JobManager failover,
 * where the restored job goes on reading the read session that table backs, so deleting on teardown
 * would break the recovery it appears to tidy up after.
 *
 * <p><b>The job id is random by default, and nothing is ever reused then</b>: a JobManager failover
 * before the first checkpoint re-plans and runs the query again, which against the anonymous
 * dataset is answered from cache — free, landing on the same table (measured) — and against a named
 * dataset writes a second result table that expires on its own. A {@link QuerySpec} carrying a
 * {@link QueryJobIdentity} opts into the deterministic id instead, under which a re-plan finds the
 * previous attempt's job and reuses it; what the identity is derived from, and why reuse has a
 * bounded window at all, is that class's record. BigQuery keeps a finished job's id for six months
 * and refuses to reuse one, which shapes both paths here: a failed id is probed past to {@code _rN}
 * ids exactly as {@code BigQueryLoadJobRunner} probes, and an id from an expired window is never
 * submitted again because the window rides in the id.
 *
 * <p>The client is opened on first use rather than in the constructor: this object is built where
 * the job graph is, and a client built there would demand credentials on the submitting machine.
 */
@Internal
public final class BigQueryQueryRunner implements QueryRunner {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryQueryRunner.class);

    /**
     * How long a result table this class created is kept.
     *
     * <p>Not a knob, and the read session's own lifetime is why it does not need to be: a session
     * expires six hours after it is created and a bounded read has to finish inside that, so a day
     * cannot cut a read short however the job is restarted meanwhile. It matches what BigQuery
     * applies to the anonymous tables of the other path, so the two behave alike.
     */
    @VisibleForTesting static final Duration RESULT_TABLE_EXPIRATION = Duration.ofHours(24);

    /**
     * How many {@code _rN} ids past a failed previous attempt the deterministic path probes.
     *
     * <p>The load runner's bound, for the load runner's reason: each probe means a previous attempt
     * failed at this same query, so a chain this long is a systemic failure no fresh id will fix.
     */
    @VisibleForTesting static final int MAX_RETRY_PROBES = 5;

    /**
     * How the completion of the query job is polled for.
     *
     * <p>Deliberately not exposed, as the sink's schema-wait schedule is not: a query runs once per
     * job, on the JobManager, and no workload makes a particular poll interval the right answer.
     *
     * <p>The polling carries <b>no attempt bound</b>, matching {@code BigQueryLoadJobRunner}, and
     * here the service is what makes that terminate: BigQuery ends a query job at its own execution
     * limit, so the job reaches {@code DONE} — successfully or not — without anything on this side
     * counting. A bound short enough to be useful would be short enough to abandon a legitimately
     * long query, which is the opposite of the problem ADR-0084 solved for {@code ReadRows}, where
     * the client would have retried for a day without the service ever ending anything. The
     * schedule's own {@code maxAttempts} is therefore unread; the type requires one.
     */
    private static final RetrySchedule POLL_SCHEDULE =
            new RetrySchedule(500, 10_000, Integer.MAX_VALUE, RetrySchedule.DEFAULT_JITTER_RATIO);

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    private transient BigQuery client;

    /**
     * Creates the runner.
     *
     * @param emulatorEndpoint the emulator's REST endpoint, or {@code null} for BigQuery itself
     */
    public BigQueryQueryRunner(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(null, emulatorEndpoint);
    }

    /**
     * Creates the runner.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's REST endpoint, or {@code null} for BigQuery itself
     */
    public BigQueryQueryRunner(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @VisibleForTesting
    BigQueryQueryRunner(BigQuery client) {
        this.serviceAccountKeyFile = null;
        this.emulatorEndpoint = null;
        this.client = client;
    }

    @VisibleForTesting
    @Nullable
    EmulatorEndpoint emulatorEndpoint() {
        return emulatorEndpoint;
    }

    @Override
    public QueryResult run(QuerySpec spec) throws IOException {
        QueryJobIdentity identity = spec.getJobIdentity();
        if (identity == null) {
            // One suffix for both names, so the job and the table it wrote are found from each
            // other in a log line or in the BigQuery console.
            String suffix = QueryJobIdentity.PREFIX + UUID.randomUUID().toString().replace("-", "");
            LOG.info("Running the BigQuery source's query as job {}: {}", suffix, spec);
            Job job = await(submit(jobId(spec, suffix), configuration(spec, suffix), spec), spec);
            return new QueryResult(landed(job, spec), false);
        }
        return runReusable(identity, spec);
    }

    /**
     * Submits under the identity's deterministic id, reusing a previous attempt's job where one is
     * still usable.
     *
     * <p>The shape is {@code BigQueryLoadJobRunner#submitOrAttach}'s, not a call to it: the load
     * runner's machinery exists for the sink's exactly-once commit and carries an active-jobs map
     * and copy jobs this path has no use for (ADR-0087 records the decision not to hoist it). The
     * one judgment they must share is shared by construction — a job that finished with an error
     * can never be reused, and its id can never be resubmitted, so each failed id is probed past to
     * the next {@code _rN}.
     *
     * <p>The walk itself is this method; what one id yields is {@link #probeOnce}, and the two ways
     * an id can be unusable — a failed job, a finished job whose result table is gone — are each
     * reported as a {@link ProbeOutcome} carrying the reason the give-up message below quotes.
     */
    private QueryResult runReusable(QueryJobIdentity identity, QuerySpec spec) throws IOException {
        String lastError = null;
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
            ProbeOutcome outcome = probeOnce(identity, spec, probe);
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
    private ProbeOutcome probeOnce(QueryJobIdentity identity, QuerySpec spec, int probe)
            throws IOException {
        String suffix = retrySuffix(identity.getCurrentJobId(), probe);
        Job existing = lookUp(jobId(spec, suffix), spec.getProject());
        if (existing != null) {
            return adopt(suffix, existing, probe, spec);
        }
        if (probe == 0) {
            Job straddled = previousWindowJob(identity, spec);
            if (straddled != null) {
                LOG.info(
                        "Re-attached to the BigQuery query job {} from the previous reuse window"
                                + " (state {}).",
                        straddled.getJobId().getJob(),
                        state(straddled.getStatus()));
                return ProbeOutcome.of(new QueryResult(landed(await(straddled, spec), spec), true));
            }
        }
        return submitOrAttach(suffix, spec, probe);
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
    private ProbeOutcome adopt(String suffix, Job existing, int probe, QuerySpec spec)
            throws IOException {
        if (isFailed(existing.getStatus())) {
            return ProbeOutcome.unusable(probePast(suffix, existing.getStatus(), probe));
        }
        TableId vanished = vanishedResultTable(existing, spec.getProject());
        if (vanished != null) {
            return ProbeOutcome.unusable(probePastVanished(suffix, vanished, probe));
        }
        LOG.info(
                "Re-attached to the BigQuery query job {} from a previous attempt (state {}).",
                suffix,
                state(existing.getStatus()));
        return ProbeOutcome.of(new QueryResult(landed(await(existing, spec), spec), true));
    }

    /**
     * Submits under this id, adopting instead the job a racing attempt got in first.
     *
     * <p>Reached only where the look-up found nothing, so the two judgments {@link #adopt} makes
     * are made again here — on the conflict winner's job, which is as much a previous attempt's as
     * one the look-up would have found.
     */
    private ProbeOutcome submitOrAttach(String suffix, QuerySpec spec, int probe)
            throws IOException {
        Created created = create(jobId(spec, suffix), configuration(spec, suffix), spec);
        if (isFailed(created.job.getStatus())) {
            // create lost its race to a zombie that had itself already failed, whose id is as
            // unusable as one the look-up would have found failed.
            return ProbeOutcome.unusable(probePast(suffix, created.job.getStatus(), probe));
        }
        if (created.conflicted) {
            TableId vanished = vanishedResultTable(created.job, spec.getProject());
            if (vanished != null) {
                return ProbeOutcome.unusable(probePastVanished(suffix, vanished, probe));
            }
            LOG.info(
                    "Re-attached to the BigQuery query job {} another attempt submitted first"
                            + " (state {}).",
                    suffix,
                    state(created.job.getStatus()));
        } else {
            LOG.info("Running the BigQuery source's query as job {}: {}", suffix, spec);
        }
        return ProbeOutcome.of(
                new QueryResult(landed(await(created.job, spec), spec), created.conflicted));
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
    private Job previousWindowJob(QueryJobIdentity identity, QuerySpec spec) throws IOException {
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
            String suffix = retrySuffix(identity.getPreviousJobId(), probe);
            Job job = lookUp(jobId(spec, suffix), spec.getProject());
            if (job == null) {
                return null;
            }
            if (isFailed(job.getStatus())) {
                continue;
            }
            Long created = creationTime(job);
            if (created == null || !identity.isWithinWindow(created, System.currentTimeMillis())) {
                return null;
            }
            TableId vanished = vanishedResultTable(job, spec.getProject());
            if (vanished != null) {
                // Not probePastVanished: this walk never runs anything, and the chain's next
                // link — left by an attempt that made this same walk — may be adopted.
                LOG.warn(
                        "The BigQuery query job {} from the previous reuse window completed, but"
                                + " its result table {}.{} is gone; walking past it.",
                        suffix,
                        vanished.getDataset(),
                        vanished.getTable());
                continue;
            }
            return job;
        }
        return null;
    }

    private static String retrySuffix(String base, int probe) {
        // An underscore, not the load runner's hyphen: this suffix is also the result table's
        // name, and a hyphen is not legal there.
        return probe == 0 ? base : base + "_r" + probe;
    }

    @Nullable
    private static Long creationTime(Job job) {
        JobStatistics statistics = job.getStatistics();
        return statistics == null ? null : statistics.getCreationTime();
    }

    /**
     * Whether the job has finished and reports an error — the one state that can never be reused,
     * since BigQuery keeps the failed id for six months and refuses it.
     *
     * <p>{@code null} is not failed, and that is load-bearing: the job the SDK's own already-exists
     * absorber returns carries no status at all (measured 2026-08-08 against google-cloud-bigquery
     * 2.68.0, ADR-0018), and such a job must be attached to and polled rather than probed past.
     */
    private static boolean isFailed(@Nullable JobStatus status) {
        return status != null
                && status.getState() == JobStatus.State.DONE
                && status.getError() != null;
    }

    private static String state(@Nullable JobStatus status) {
        return status == null || status.getState() == null ? "unknown" : status.getState().name();
    }

    /** Logs the failed job being walked past and answers its error for the give-up message. */
    private static String probePast(String suffix, JobStatus status, int probe) {
        String error = status.getError().toString();
        if (probe < MAX_RETRY_PROBES) {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt failed ({}); probing the"
                            + " next retry id.",
                    suffix,
                    error);
        } else {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt failed ({}), and no retry"
                            + " ids are left to probe.",
                    suffix,
                    error);
        }
        return error;
    }

    /**
     * Returns the finished job's result table where it no longer exists, or {@code null}.
     *
     * <p>{@code null} — adopt the job — covers three cases deliberately. A job that is not {@code
     * DONE} without error has no result table to check: a running one's table is created when it
     * completes, a statusless one (the SDK's already-exists absorber) is attached to and polled,
     * and a failed one was probed past before this is asked. A completed job naming no destination
     * at all is a contract violation, and adopting it is what routes it to {@link #landed}'s report
     * rather than guessing here. Only a job whose named table answers {@code getTable} with nothing
     * is refused — one more use of the call {@link #expire}'s backstop already makes, not a new
     * client surface.
     */
    @Nullable
    private TableId vanishedResultTable(Job job, String project) throws IOException {
        if (!isDone(job) || isFailed(job.getStatus())) {
            return null;
        }
        TableId landed = ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
        if (landed == null) {
            return null;
        }
        try {
            return client(project).getTable(landed) == null ? landed : null;
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to look up the result table "
                            + landed.getDataset()
                            + "."
                            + landed.getTable()
                            + " of the BigQuery query job "
                            + job.getJobId().getJob()
                            + " before reusing it.",
                    e);
        }
    }

    /** Logs the vanished result table being walked past and answers the give-up line's error. */
    private static String probePastVanished(String suffix, TableId vanished, int probe) {
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
                    suffix,
                    error);
        } else {
            LOG.warn(
                    "The BigQuery query job {} from a previous attempt completed, but {}, and no"
                            + " retry ids are left to probe.",
                    suffix,
                    error);
        }
        return error;
    }

    private static JobId jobId(QuerySpec spec, String suffix) {
        JobId.Builder jobId = JobId.newBuilder().setJob(suffix).setProject(spec.getProject());
        if (spec.getLocation() != null) {
            jobId.setLocation(spec.getLocation());
        }
        return jobId.build();
    }

    private static QueryJobConfiguration configuration(QuerySpec spec, String suffix) {
        QueryJobConfiguration.Builder configuration =
                QueryJobConfiguration.newBuilder(spec.getSql());
        if (spec.getResultDataset() != null) {
            configuration
                    .setDestinationTable(
                            TableId.of(spec.getProject(), spec.getResultDataset(), suffix))
                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);
        }
        return configuration.build();
    }

    /**
     * Reads the table the completed job's result landed in, and backstops its expiration.
     *
     * <p>The expiration is set on a reused job's table too, not only on a fresh submission's: the
     * attempt that submitted the job may have died before setting one, and re-applying it merely
     * moves the expiration to a day from now.
     */
    private TableDestination landed(Job done, QuerySpec spec) throws IOException {
        TableId landed = ((QueryJobConfiguration) done.getConfiguration()).getDestinationTable();
        if (landed == null) {
            // BigQuery fills the destination in on the completed job whether or not one was asked
            // for, so this is a contract violation rather than a case to fall back from — and
            // guessing a table here would read someone else's data.
            throw new IOException(
                    "The BigQuery query job "
                            + done.getJobId().getJob()
                            + " completed but reported no result table, so there is nothing to"
                            + " read.");
        }
        if (spec.getResultDataset() != null) {
            expire(landed, spec.getProject());
        }
        TableDestination result =
                TableDestination.of(landed.getProject(), landed.getDataset(), landed.getTable());
        LOG.info(
                "The BigQuery source's query job {} wrote its result to {}; the read session is"
                        + " created against that table.",
                done.getJobId().getJob(),
                result);
        return result;
    }

    @Override
    public boolean isView(TableDestination table) throws IOException {
        TableId id = BigQueryTableAdmin.toTableId(table);
        Table live;
        try {
            live = client(table.getProject()).getTable(id);
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to look up " + table + " to see whether it is a view.", e);
        }
        if (live == null) {
            // Reported here rather than left to the read session, which is the call that would
            // otherwise meet it: this lookup has already been made, and unlike the session it can
            // tell "absent" from "not a table".
            throw new IOException("The BigQuery table or view " + table + " does not exist.");
        }
        TableDefinition definition = live.getDefinition();
        return isViewType(definition == null ? null : definition.getType());
    }

    /**
     * Returns whether a table type is one the Storage Read API cannot read directly.
     *
     * <p>Split from the lookup so the decision is testable without a {@link Table} or a client at
     * all: the vendor-package helper mints a {@code Table} ({@code docs/adr/0067}), but a static
     * predicate needs neither the mint nor a stub. What is left in {@link #isView} is the round
     * trip itself, which the gated real-GCP case covers.
     *
     * <p>Only the two view types answer {@code true}. An external table and a snapshot are read
     * differently — an external table the API also refuses, a snapshot it accepts — and neither is
     * a view, so materializing them would be answering a question nobody asked.
     *
     * @param type the type the service reported, or {@code null} if it reported none
     * @return whether it is a logical or materialized view
     */
    @VisibleForTesting
    static boolean isViewType(@Nullable TableDefinition.Type type) {
        // equals rather than ==: Type is a StringEnumValue and not a Java enum, so a value from a
        // newer service arrives as a fresh instance rather than as one of these constants.
        return TableDefinition.Type.VIEW.equals(type)
                || TableDefinition.Type.MATERIALIZED_VIEW.equals(type);
    }

    /**
     * Submits the job, answering the runner's {@link IOException} rather than the client's type.
     */
    private Job submit(JobId jobId, QueryJobConfiguration configuration, QuerySpec spec)
            throws IOException {
        try {
            return client(spec.getProject()).create(JobInfo.of(jobId, configuration));
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to submit the BigQuery query job " + jobId.getJob() + ".", e);
        }
    }

    /** What {@link #create} produced: the job, and whether another attempt created it first. */
    private static final class Created {
        final Job job;
        final boolean conflicted;

        Created(Job job, boolean conflicted) {
            this.job = job;
            this.conflicted = conflicted;
        }
    }

    /**
     * Submits under a deterministic id, absorbing the conflict a racing attempt produces.
     *
     * <p>An HTTP 409 means another attempt — a coordinator this one is failing over from, most
     * likely — submitted the id between this attempt's look-up and its create. Its job is looked up
     * and handed back for the caller's judgment, marked as a reuse; a conflict whose job then
     * cannot be found is reported as the submission failure it is, with the conflict kept as a
     * suppressed exception because it names the one thing the look-up failure does not.
     */
    private Created create(JobId jobId, QueryJobConfiguration configuration, QuerySpec spec)
            throws IOException {
        try {
            return new Created(
                    client(spec.getProject()).create(JobInfo.of(jobId, configuration)), false);
        } catch (BigQueryException e) {
            if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
                Job existing;
                try {
                    existing = client(spec.getProject()).getJob(jobId);
                } catch (BigQueryException lookupFailure) {
                    lookupFailure.addSuppressed(e);
                    throw new IOException(
                            "Failed to submit the BigQuery query job " + jobId.getJob() + ".",
                            lookupFailure);
                }
                if (existing != null) {
                    return new Created(existing, true);
                }
            }
            throw new IOException(
                    "Failed to submit the BigQuery query job " + jobId.getJob() + ".", e);
        }
    }

    /** Polls until the job is done, then reports a job that failed as a failure. */
    private Job await(Job submitted, QuerySpec spec) throws IOException {
        Job job = submitted;
        int attempt = 1;
        while (!isDone(job)) {
            Retries.sleep(
                    POLL_SCHEDULE.backoffMs(attempt++),
                    "Interrupted while waiting for the BigQuery query job "
                            + job.getJobId().getJob());
            // Deliberately not Job#reload(): the same request, minus its
            // throw-if-the-job-carries-an-error behaviour, which is what routed an ordinary failure
            // past the message below on the sink side (ADR-0018). Do not simplify it back.
            Job reloaded = lookUp(job.getJobId(), spec.getProject());
            if (reloaded == null) {
                throw new IOException(
                        "The BigQuery query job "
                                + job.getJobId().getJob()
                                + " disappeared while it was being polled.");
            }
            job = reloaded;
        }
        JobStatus status = job.getStatus();
        if (status.getError() != null) {
            throw new IOException(
                    "The BigQuery query job "
                            + job.getJobId().getJob()
                            + " failed: "
                            + status.getError()
                            + ". The query was: "
                            + spec.getSql());
        }
        return job;
    }

    @Nullable
    private Job lookUp(JobId jobId, String project) throws IOException {
        try {
            return client(project).getJob(jobId);
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to look up the BigQuery query job "
                            + jobId.getJob()
                            + " while polling for its completion.",
                    e);
        }
    }

    /**
     * Sets the expiration on a result table this runner created.
     *
     * <p>A failure here is logged rather than thrown: the table exists and is readable, so failing
     * the job would turn a missing backstop into a missing read. What it costs is a table that
     * outlives the job in the user's own dataset, which is why it is a warning and not a debug
     * line.
     */
    private void expire(TableId table, String project) {
        try {
            Table live = client(project).getTable(table);
            if (live == null) {
                LOG.warn(
                        "The BigQuery query's result table {} was gone before an expiration could"
                                + " be set on it.",
                        table);
                return;
            }
            client(project)
                    .update(
                            live.toBuilder()
                                    .setExpirationTime(
                                            System.currentTimeMillis()
                                                    + RESULT_TABLE_EXPIRATION.toMillis())
                                    .build());
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "Failed to set an expiration on the BigQuery query's result table {}; it will"
                            + " stay in the dataset until something removes it.",
                    table,
                    e);
        }
    }

    private static boolean isDone(Job job) {
        JobStatus status = job.getStatus();
        return status != null && status.getState() == JobStatus.State.DONE;
    }

    /**
     * The REST client, opened on first use.
     *
     * <p>The emulator options are the sink's, not a second copy: the argument for requiring a
     * project id there — {@code BigQueryOptions} refuses to build without one it can determine, and
     * an emulator offers no environment to determine it from — holds identically here, and a second
     * spelling of it is a second thing to keep true.
     *
     * <p>Guarded, though {@link #run} is called once per job and from one thread: a global failover
     * before the first checkpoint builds a second enumerator over this same object, and its
     * planning call runs on a different coordinator worker thread. Without the guard that thread
     * may not see the first one's write and would build a second client — nothing this class can
     * release, since the REST client has no {@code close}. The lock covers construction only.
     *
     * @param project the project the job is submitted to, which is also what satisfies the
     *     emulator's builder
     */
    private synchronized BigQuery client(String project) throws IOException {
        if (client == null) {
            if (emulatorEndpoint != null) {
                client = BigQueryTableAdmin.emulatorOptions(emulatorEndpoint, project).getService();
            } else if (serviceAccountKeyFile == null) {
                client = BigQueryOptions.getDefaultInstance().getService();
            } else {
                client = BigQueryCredentials.bigQueryOptions(serviceAccountKeyFile).getService();
            }
        }
        return client;
    }
}
