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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.ParquetOptions;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link LoadJobRunner} over the BigQuery REST client with configured credentials or ADC.
 *
 * <p><b>Exactly-once via deterministic job ids.</b> BigQuery job ids are single-use per project, so
 * a deterministic id doubles as an idempotency key: before submitting, the runner looks the id up
 * and re-attaches to a job an earlier attempt of the same Flink run already created — a completed
 * job is not re-run, a still-running job is awaited — instead of loading the same files twice. Only
 * when the existing job <em>failed</em> does the runner probe {@code -r1}, {@code -r2}, ... (a
 * bounded number of times, since ids of failed jobs cannot be reused) for a fresh deterministic id.
 *
 * <p><b>Every job id the runner builds names a location.</b> BigQuery scopes a job to (project,
 * location, id), and a look-up naming no location resolves against the US multi-region only — so a
 * location-less re-attach probe can never find a previous attempt's job on any other dataset, and
 * the colliding resubmission fails instead of attaching (measured 2026-08-10, #491; ADR-0018). The
 * configured location wins when set; otherwise the runner derives each job's location from its
 * destination dataset's metadata — the destination dataset decides, because BigQuery runs a load
 * job in the location of the dataset it writes — one {@code datasets.get} per dataset for the
 * runner's lifetime.
 *
 * <p>Completion is polled with capped exponential backoff and effectively no attempt bound: batch
 * load jobs may legitimately run long, and overall timeouts are the Flink job's to enforce.
 */
@Internal
public final class BigQueryLoadJobRunner implements LoadJobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobRunner.class);

    /** How many {@code -rN} ids to probe past a failed deterministic id before giving up. */
    private static final int MAX_RETRY_PROBES = 5;

    private final RetrySchedule pollSchedule;
    @Nullable private final String location;
    @Nullable private final String serviceAccountKeyFile;
    private final Map<String, Job> activeJobs = new HashMap<>();
    private final Map<DatasetId, String> datasetLocations = new HashMap<>();
    private BigQuery client;

    /**
     * Creates a runner.
     *
     * @param location the BigQuery location jobs run in, or {@code null} to derive each job's
     *     location from its destination dataset (see the class javadoc)
     * @param pollSchedule how completion polling backs off
     */
    public BigQueryLoadJobRunner(@Nullable String location, RetrySchedule pollSchedule) {
        this(location, pollSchedule, null);
    }

    /** Creates a runner with optional runtime-loaded production credentials. */
    public BigQueryLoadJobRunner(
            @Nullable String location,
            RetrySchedule pollSchedule,
            @Nullable String serviceAccountKeyFile) {
        this.location = location;
        this.pollSchedule = pollSchedule;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    /** Returns the configured key-file path, or {@code null} for ADC. */
    @VisibleForTesting
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @VisibleForTesting
    BigQueryLoadJobRunner(BigQuery client, @Nullable String location, RetrySchedule pollSchedule) {
        this.client = client;
        this.location = location;
        this.pollSchedule = pollSchedule;
        this.serviceAccountKeyFile = null;
    }

    @Override
    public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
        LoadJobConfiguration.Builder load =
                LoadJobConfiguration.newBuilder(
                                BigQueryTableAdmin.toTableId(spec.getDestination()),
                                spec.getSourceUris())
                        .setSchema(spec.getSchema())
                        .setCreateDisposition(spec.getCreateDisposition())
                        .setWriteDisposition(spec.getWriteDisposition());
        configureFormat(load, spec.getFormat());
        if (!spec.getSchemaUpdateOptions().isEmpty()) {
            load.setSchemaUpdateOptions(spec.getSchemaUpdateOptions());
        }
        submitOrAttach(jobId, load.build(), spec.getDestination(), spec.toString());
    }

    /**
     * Applies the source format the staged files were written in.
     *
     * <p>{@code enableListInference} is <b>not</b> optional on the Parquet side and must not be
     * "simplified" away: without it a {@code REPEATED} column loads as an <em>empty array with no
     * error at all</em> — measured against a provided destination schema naming the column {@code
     * STRING REPEATED}, which returned every row with zero elements and reported success. That is
     * the failure shape of #206, and {@code
     * BigQueryLoadJobRunnerTest.parquetLoadsEnableListInference} is what fails if it is dropped.
     */
    private static void configureFormat(LoadJobConfiguration.Builder load, StagingFormat format) {
        switch (format) {
            case AVRO:
                load.setFormatOptions(FormatOptions.avro()).setUseAvroLogicalTypes(true);
                return;
            case PARQUET:
                load.setFormatOptions(
                        ParquetOptions.newBuilder().setEnableListInference(true).build());
                return;
            default:
                throw new IllegalStateException("Unhandled staging format: " + format);
        }
    }

    @Override
    public void submitCopy(String jobId, CopyJobSpec spec) throws IOException {
        CopyJobConfiguration copy =
                CopyJobConfiguration.newBuilder(
                                BigQueryTableAdmin.toTableId(spec.getDestination()),
                                spec.getSourceTables().stream()
                                        .map(BigQueryTableAdmin::toTableId)
                                        .collect(Collectors.toList()))
                        .setCreateDisposition(spec.getCreateDisposition())
                        .setWriteDisposition(spec.getWriteDisposition())
                        .build();
        submitOrAttach(jobId, copy, spec.getDestination(), spec.toString());
    }

    @Override
    public void awaitJob(String jobId) throws IOException {
        Job job = activeJobs.remove(jobId);
        Preconditions.checkState(job != null, "Job %s was never submitted", jobId);
        int attempt = 1;
        while (!isDone(job)) {
            Retries.sleep(
                    pollSchedule.backoffMs(attempt++),
                    "Interrupted while waiting for BigQuery job " + jobId);
            // Deliberately not Job#reload(): the same request, minus its
            // throw-if-the-job-carries-an-error behaviour, which routed a load that failed while
            // being polled past the message composed below (ADR-0018). Do not simplify it back.
            Job reloaded = getJob(job.getJobId(), "polling for completion");
            if (reloaded == null) {
                throw new IOException(
                        "BigQuery job " + job.getJobId().getJob() + " disappeared while polling.");
            }
            job = reloaded;
        }
        JobStatus status = job.getStatus();
        if (status.getError() != null) {
            throw new IOException(
                    "BigQuery job "
                            + job.getJobId().getJob()
                            + " failed: "
                            + status.getError()
                            + (status.getExecutionErrors() != null
                                    ? " (execution errors: " + status.getExecutionErrors() + ")"
                                    : ""));
        }
        LOG.info("BigQuery job {} completed", job.getJobId().getJob());
    }

    @Override
    public void deleteTable(TableDestination table) {
        try {
            client().delete(BigQueryTableAdmin.toTableId(table));
        } catch (IOException | RuntimeException e) {
            LOG.warn("Failed to delete temporary table {}", table, e);
        }
    }

    private void submitOrAttach(
            String baseJobId,
            JobConfiguration configuration,
            TableDestination destination,
            String what)
            throws IOException {
        String jobLocation = jobLocation(destination);
        String lastError = null;
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
            String jobName = probe == 0 ? baseJobId : baseJobId + "-r" + probe;
            JobId jobId = JobId.newBuilder().setJob(jobName).setLocation(jobLocation).build();
            Job existing = getJob(jobId, "looking for a previous attempt's job");
            if (existing == null) {
                Job submitted = create(jobId, configuration);
                JobStatus submittedStatus = submitted.getStatus();
                if (isFailed(submittedStatus)) {
                    // create lost its race to a zombie that had itself already failed, whose id
                    // is as unusable as one the probe above found failed.
                    lastError = probePastFailedJob(jobName, baseJobId, submittedStatus, probe);
                    continue;
                }
                activeJobs.put(baseJobId, submitted);
                LOG.info("Submitted BigQuery job {}: {}", jobName, what);
                return;
            }
            JobStatus status = existing.getStatus();
            if (isFailed(status)) {
                lastError = probePastFailedJob(jobName, baseJobId, status, probe);
                continue;
            }
            activeJobs.put(baseJobId, existing);
            LOG.info(
                    "Re-attached to BigQuery job {} from a previous attempt (state {})",
                    jobName,
                    status == null ? "unknown" : status.getState());
            return;
        }
        throw new IOException(
                "BigQuery job "
                        + baseJobId
                        + " and all its retry ids failed in previous attempts; last error: "
                        + lastError);
    }

    /**
     * A failed job's id cannot be reused, and attaching to one would fail the commit with a failure
     * that predates this attempt; only a fresh id can still load the data. Logs the same warning
     * however the failed job was met — found by the probe, or handed back by {@code create} after a
     * conflict — and answers the error for {@code lastError}, so the give-up message reports
     * whichever failed job was met last. Callers have established {@code isFailed(status)}, which
     * is what makes the error dereference safe.
     */
    private static String probePastFailedJob(
            String jobName, String baseJobId, JobStatus status, int probe) {
        String error = status.getError().toString();
        if (probe < MAX_RETRY_PROBES) {
            LOG.warn(
                    "BigQuery job {} from a previous attempt failed ({}); probing {}-r{}",
                    jobName,
                    error,
                    baseJobId,
                    probe + 1);
        } else {
            LOG.warn(
                    "BigQuery job {} from a previous attempt failed ({}), and no retry ids are"
                            + " left to probe",
                    jobName,
                    error);
        }
        return error;
    }

    /**
     * Whether the job has finished and reports an error — the one state the runner must move past
     * to a fresh {@code -rN} id, since a failed id can never be reused (see the class javadoc).
     *
     * <p>{@code null} is not failed, and that is load-bearing: the job the SDK's own already-exists
     * absorber returns carries no status at all (it re-fetches with {@code
     * JobOption.fields(STATISTICS)}, whose required fields exclude the status; measured 2026-08-08
     * against google-cloud-bigquery 2.68.0, ADR-0018), and such a job must be attached to and
     * polled rather than probed past.
     */
    private static boolean isFailed(@Nullable JobStatus status) {
        return status != null
                && status.getState() == JobStatus.State.DONE
                && status.getError() != null;
    }

    private Job create(JobId jobId, JobConfiguration configuration) throws IOException {
        try {
            return client().create(JobInfo.of(jobId, configuration));
        } catch (BigQueryException e) {
            if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
                // Lost a race against a zombie of a previous attempt; hand its job back for the
                // probe loop to judge — a still-usable zombie is attached to, a failed one is
                // probed past.
                Job existing;
                try {
                    existing = client().getJob(jobId);
                } catch (BigQueryException lookupFailure) {
                    // The conflict is why this lookup happens at all, and it names the one thing
                    // the lookup failure does not: that the id is already taken.
                    lookupFailure.addSuppressed(e);
                    throw new IOException(
                            "Failed to submit BigQuery job " + jobId.getJob(), lookupFailure);
                }
                if (existing != null) {
                    return existing;
                }
            }
            throw new IOException("Failed to submit BigQuery job " + jobId.getJob(), e);
        }
    }

    /**
     * Looks a job up, as {@link BigQuery#getJob} does, but answering a failed lookup with the
     * {@link IOException} the {@link LoadJobRunner} contract promises rather than with the client's
     * unchecked {@link BigQueryException}.
     *
     * <p>The SDK has already retried by the time this throws — its own retry settings govern the
     * call — so a failure here is one the commit cannot recover from either way, and the type is
     * all that changes. What it buys is the job id in the message: a {@code jobs.get} failure names
     * the resource in neither the exception's message nor its code.
     *
     * @param jobId the job to look up
     * @param what what the caller was doing, for the failure message
     * @return the job, or {@code null} if the service reports none under that id
     * @throws IOException if the lookup fails
     */
    @Nullable
    private Job getJob(JobId jobId, String what) throws IOException {
        try {
            return client().getJob(jobId);
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to look up BigQuery job " + jobId.getJob() + " while " + what, e);
        }
    }

    private static boolean isDone(Job job) {
        JobStatus status = job.getStatus();
        return status != null && status.getState() == JobStatus.State.DONE;
    }

    /**
     * The location every id of this job is scoped to: the configured location when set, otherwise
     * the destination dataset's own — the class javadoc carries why a job id must name one at all.
     *
     * <p>Derived once per dataset for the runner's lifetime. A dataset's location is immutable, so
     * the memo can never go stale, and a recovery looking for a previous attempt's job derives the
     * same value that attempt's job was inferred into.
     *
     * @param destination the job's destination table, whose dataset decides the location
     * @return the location, never {@code null}
     * @throws IOException if the dataset cannot be looked up or does not exist
     */
    private String jobLocation(TableDestination destination) throws IOException {
        if (location != null) {
            return location;
        }
        DatasetId datasetId = DatasetId.of(destination.getProject(), destination.getDataset());
        String derived = datasetLocations.get(datasetId);
        if (derived != null) {
            return derived;
        }
        Dataset dataset;
        try {
            dataset = client().getDataset(datasetId);
        } catch (BigQueryException e) {
            throw new IOException(
                    "Failed to look up dataset "
                            + datasetId.getProject()
                            + "."
                            + datasetId.getDataset()
                            + " while resolving the location BigQuery jobs writing it run in",
                    e);
        }
        if (dataset == null) {
            // A 404 does not establish non-existence — GCP's disclosure convention can answer it
            // for a dataset the caller may not see — so this cannot claim which of the two it met.
            throw new IOException(
                    "Dataset "
                            + datasetId.getProject()
                            + "."
                            + datasetId.getDataset()
                            + " was not found while resolving the location BigQuery jobs writing"
                            + " it run in: it does not exist, or this principal cannot read its"
                            + " metadata (bigquery.datasets.get). Create it, grant the"
                            + " permission, or set location(...) on the sink.");
        }
        datasetLocations.put(datasetId, dataset.getLocation());
        return dataset.getLocation();
    }

    private BigQuery client() throws IOException {
        if (client == null) {
            client =
                    serviceAccountKeyFile == null
                            ? BigQueryOptions.getDefaultInstance().getService()
                            : BigQueryCredentials.bigQueryOptions(serviceAccountKeyFile)
                                    .getService();
        }
        return client;
    }
}
