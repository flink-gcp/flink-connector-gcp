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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
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
 * <p><b>The job id is random, and re-attaching to a previous attempt is deliberately not done</b> —
 * which is the opposite of {@code BigQueryLoadJobRunner}'s choice, for a reason that does not apply
 * there. A deterministic id would have to be derived from the query, and BigQuery keeps a job's
 * metadata — and so its id — for six months after it was created; the second run of the same
 * pipeline would then find the first run's completed job and read its stale result rather than
 * running the query again. Making the id stable across a failover but not across pipeline runs
 * needs a nonce in the checkpointed enumerator state, which buys only the case where the JobManager
 * fails between submitting the query and the first checkpoint. In that case a re-plan runs the
 * query a second time: against the anonymous dataset BigQuery answers it from its cache, free and
 * landing on the same table (measured), and against a named dataset it writes a second result table
 * that expires on its own.
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

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    private transient BigQuery client;

    /**
     * Creates the runner.
     *
     * @param emulatorEndpoint the emulator's REST endpoint, or {@code null} for BigQuery itself
     */
    public BigQueryQueryRunner(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @VisibleForTesting
    BigQueryQueryRunner(BigQuery client) {
        this.emulatorEndpoint = null;
        this.client = client;
    }

    @Override
    public TableDestination run(QuerySpec spec) throws IOException {
        // One suffix for both names, so the job and the table it wrote are found from each other in
        // a log line or in the BigQuery console.
        String suffix = "flink_bigquery_source_" + UUID.randomUUID().toString().replace("-", "");
        TableId destination =
                spec.getResultDataset() == null
                        ? null
                        : TableId.of(spec.getProject(), spec.getResultDataset(), suffix);
        QueryJobConfiguration.Builder configuration =
                QueryJobConfiguration.newBuilder(spec.getSql());
        if (destination != null) {
            configuration
                    .setDestinationTable(destination)
                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);
        }
        JobId.Builder jobId = JobId.newBuilder().setJob(suffix).setProject(spec.getProject());
        if (spec.getLocation() != null) {
            jobId.setLocation(spec.getLocation());
        }

        LOG.info("Running the BigQuery source's query as job {}: {}", jobId.build().getJob(), spec);
        Job job = await(submit(jobId.build(), configuration.build(), spec), spec);

        TableId landed = ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
        if (landed == null) {
            // BigQuery fills the destination in on the completed job whether or not one was asked
            // for, so this is a contract violation rather than a case to fall back from — and
            // guessing a table here would read someone else's data.
            throw new IOException(
                    "The BigQuery query job "
                            + job.getJobId().getJob()
                            + " completed but reported no result table, so there is nothing to"
                            + " read.");
        }
        if (destination != null) {
            expire(landed, spec.getProject());
        }
        TableDestination result =
                TableDestination.of(landed.getProject(), landed.getDataset(), landed.getTable());
        LOG.info(
                "The BigQuery source's query job {} wrote its result to {}; the read session is"
                        + " created against that table.",
                job.getJobId().getJob(),
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
     * <p>Split from the lookup so the decision is testable without a {@link Table}, which has no
     * constructor reachable outside the vendor's package — minting one would need a second helper
     * there, which {@code docs/adr/0067} asks to be decided deliberately rather than reached for.
     * What is left in {@link #isView} is the round trip itself, which the gated real-GCP case
     * covers.
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
        } catch (RuntimeException e) {
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
    private synchronized BigQuery client(String project) {
        if (client == null) {
            client =
                    emulatorEndpoint == null
                            ? BigQueryOptions.getDefaultInstance().getService()
                            : BigQueryTableAdmin.emulatorOptions(emulatorEndpoint, project)
                                    .getService();
        }
        return client;
    }
}
