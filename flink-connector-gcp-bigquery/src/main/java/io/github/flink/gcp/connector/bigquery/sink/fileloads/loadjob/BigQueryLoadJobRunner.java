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
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * {@link LoadJobRunner} over the BigQuery REST client with configured credentials or ADC.
 *
 * <p><b>Exactly-once via deterministic job ids.</b> BigQuery job ids are single-use per project, so
 * a deterministic id doubles as an idempotency key: before submitting, the runner looks the id up
 * and re-attaches to a job an earlier attempt of the same Flink run already created — a completed
 * job is not re-run, a still-running job is awaited — instead of loading the same files twice. Only
 * when the existing job <em>failed</em> does the runner probe {@code -r1}, {@code -r2}, ... for a
 * fresh deterministic id, since ids of failed jobs cannot be reused. The walk has no cap: every id
 * it passes is a failed job some attempt created, so the first unused id ends it, and how often a
 * failing commit is retried is the Flink restart strategy's to decide (#2181). A job that the
 * runner's own submission gets back already failed is not probed past: it fails the attempt, and
 * the next attempt's probe steps past its id (ADR-0018, #2181).
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
 * <p><b>Every job id names the spec's job project when it has one</b> (#2178): BigQuery scopes a
 * job id to its project, so the creation and every look-up of a job must name the same project, or
 * a re-attach probe looks where the previous attempt never created anything. A spec without one
 * leaves the id to the client's own project, as every job did before the option existed.
 *
 * <p>Completion is polled with capped exponential backoff and effectively no attempt bound: batch
 * load jobs may legitimately run long, and overall timeouts are the Flink job's to enforce. Workers
 * keep separate REST clients and location caches, but share a thread-safe submitted-job registry so
 * the global wait phase can run on a different worker from the submission phase.
 */
@Internal
public final class BigQueryLoadJobRunner implements LoadJobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobRunner.class);

    private final RetrySchedule pollSchedule;
    @Nullable private final String location;
    @Nullable private final String serviceAccountKeyFile;

    /** Thread-safe submitted-job state shared by every worker of one committer. */
    public static final class SharedJobs {
        private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

        /** Creates an empty submitted-job registry. */
        public SharedJobs() {}
    }

    private final SharedJobs sharedJobs;
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
        this(location, pollSchedule, serviceAccountKeyFile, new SharedJobs());
    }

    private BigQueryLoadJobRunner(
            @Nullable String location,
            RetrySchedule pollSchedule,
            @Nullable String serviceAccountKeyFile,
            SharedJobs sharedJobs) {
        this.location = location;
        this.pollSchedule = pollSchedule;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.sharedJobs = sharedJobs;
    }

    /** Returns the injected REST client, or {@code null} before a lazily configured one is used. */
    @VisibleForTesting
    @Nullable
    public BigQuery getClient() {
        return client;
    }

    /**
     * Creates a runner over an already configured REST client.
     *
     * @param client the BigQuery REST client
     * @param location the BigQuery location jobs run in, or {@code null} to derive it per dataset
     * @param pollSchedule how completion polling backs off
     */
    public BigQueryLoadJobRunner(
            BigQuery client, @Nullable String location, RetrySchedule pollSchedule) {
        this(client, location, pollSchedule, new SharedJobs());
    }

    /** Creates a runner over a configured client and a committer-wide submitted-job registry. */
    public BigQueryLoadJobRunner(
            BigQuery client,
            @Nullable String location,
            RetrySchedule pollSchedule,
            SharedJobs sharedJobs) {
        this.client = client;
        this.location = location;
        this.pollSchedule = pollSchedule;
        this.serviceAccountKeyFile = null;
        this.sharedJobs = sharedJobs;
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
        configureLayout(load, spec.getLayout());
        if (spec.getKmsKeyName() != null) {
            load.setDestinationEncryptionConfiguration(
                    BigQueryTableAdmin.encryptionConfiguration(spec.getKmsKeyName()));
        }
        submitOrAttach(
                jobId,
                load.build(),
                spec.getDestination(),
                spec.getJobProject(),
                spec.toString(),
                null);
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

    /**
     * Gives the temporary table a load fills its destination's partitioning and clustering. A copy
     * job refuses a non-partitioned source for a column- or range-partitioned destination, and a
     * source whose clustering differs from an existing destination's (measured, ADR-0183).
     *
     * <p>A time-partitioned temporary table also gets {@link
     * TableLayout#TEMPORARY_PARTITION_EXPIRATION_MS}, so it cannot inherit its dataset's default
     * partition expiration. The table exists already, created by {@code
     * TableAdmin.prepareTemporaryTable} with the same layout; the load states that layout so its
     * configuration matches the table it truncates, and under {@code CREATE_NEVER} a table that
     * vanished since fails the attempt.
     */
    private static void configureLayout(
            LoadJobConfiguration.Builder load, @Nullable TableLayout layout) {
        if (layout == null) {
            return;
        }
        if (layout.getTimePartitioning() != null) {
            load.setTimePartitioning(layout.getTemporaryTimePartitioning());
        }
        if (layout.getRangePartitioning() != null) {
            load.setRangePartitioning(layout.getRangePartitioning());
        }
        if (layout.getClustering() != null) {
            load.setClustering(layout.getClustering());
        }
    }

    @Override
    public void submitCopy(String jobId, CopyJobSpec spec) throws IOException {
        CopyJobConfiguration.Builder copy =
                CopyJobConfiguration.newBuilder(
                                BigQueryTableAdmin.toTableId(spec.getDestination()),
                                spec.getSourceTables().stream()
                                        .map(BigQueryTableAdmin::toTableId)
                                        .collect(Collectors.toList()))
                        .setCreateDisposition(spec.getCreateDisposition())
                        .setWriteDisposition(spec.getWriteDisposition());
        if (spec.getKmsKeyName() != null) {
            copy.setDestinationEncryptionConfiguration(
                    BigQueryTableAdmin.encryptionConfiguration(spec.getKmsKeyName()));
        }
        submitOrAttach(
                jobId,
                copy.build(),
                spec.getDestination(),
                spec.getJobProject(),
                spec.toString(),
                spec.getEquivalentJobId());
    }

    @Override
    public void submitQuery(String jobId, QueryJobSpec spec) throws IOException {
        QueryJobConfiguration.Builder query =
                QueryJobConfiguration.newBuilder(spec.getSql())
                        .setUseLegacySql(false)
                        .setPriority(QueryJobConfiguration.Priority.INTERACTIVE)
                        .setDestinationTable(BigQueryTableAdmin.toTableId(spec.getDestination()))
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE_DATA);
        if (!spec.getSchemaUpdateOptions().isEmpty()) {
            query.setSchemaUpdateOptions(spec.getSchemaUpdateOptions());
        }
        submitOrAttach(
                jobId,
                query.build(),
                spec.getDestination(),
                spec.getJobProject(),
                spec.toString(),
                null);
    }

    @Override
    public void awaitJob(String jobId) throws IOException {
        Job job = sharedJobs.jobs.get(jobId);
        Preconditions.checkState(job != null, "Job %s was never submitted", jobId);
        boolean preserveForInterruptedDrain = false;
        try {
            Job polled = pollUntilDone(job, jobId, "polling for completion");
            if (polled == null) {
                throw new IOException(
                        "BigQuery job " + job.getJobId().getJob() + " disappeared while polling.");
            }
            job = polled;
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
        } catch (IOException failure) {
            // The orchestrator drains a worker-only interrupt serially. Keep the submitted handle
            // so that drain can resume polling instead of treating the job as never submitted.
            preserveForInterruptedDrain = Thread.currentThread().isInterrupted();
            throw failure;
        } finally {
            if (!preserveForInterruptedDrain) {
                sharedJobs.jobs.remove(jobId);
            }
        }
    }

    @Override
    public boolean copySucceeded(String jobId, CopyJobSpec spec) throws IOException {
        List<String> baseJobIds = new ArrayList<>(2);
        baseJobIds.add(jobId);
        if (spec.getEquivalentJobId() != null) {
            baseJobIds.add(spec.getEquivalentJobId());
        }
        return anySucceeded(baseJobIds, spec.getJobProject(), jobLocation(spec.getDestination()));
    }

    @Override
    public boolean querySucceeded(String jobId, QueryJobSpec spec) throws IOException {
        return anySucceeded(
                List.of(jobId), spec.getJobProject(), jobLocation(spec.getDestination()));
    }

    /**
     * Whether a job under one of {@code baseJobIds}, or one of their retry ids, succeeded, once
     * none of them is pending or running. A job that has not finished is waited for: it may read
     * the temporary tables the caller is about to create anew, so answering before it finishes
     * would let it copy a table the caller recreated empty. One that then fails is passed over, and
     * the next look-up may find a later retry id.
     */
    private boolean anySucceeded(
            List<String> baseJobIds, @Nullable String jobProject, String jobLocation)
            throws IOException {
        while (true) {
            List<Job> unfinished = new ArrayList<>(baseJobIds.size());
            for (String baseJobId : baseJobIds) {
                Job job = findNotFailed(baseJobId, jobProject, jobLocation);
                if (succeeded(job)) {
                    return true;
                }
                if (job != null) {
                    unfinished.add(job);
                }
            }
            if (unfinished.isEmpty()) {
                return false;
            }
            for (Job job : unfinished) {
                waitUntilFinished(job);
            }
        }
    }

    /** Polls {@code job} until it is done, or gone. */
    private void waitUntilFinished(Job job) throws IOException {
        LOG.info(
                "Waiting for BigQuery job {}, left running by a previous attempt, to finish",
                job.getJobId().getJob());
        pollUntilDone(job, job.getJobId().getJob(), "waiting for a previous attempt's job");
    }

    /**
     * Polls {@code job} until it is done and returns it, or returns {@code null} once it is gone.
     *
     * @param job the job as last read
     * @param name the job's name in the message of an interruption
     * @param what what the look-up is for, in the message of a failed one
     */
    @Nullable
    private Job pollUntilDone(Job job, String name, String what) throws IOException {
        int attempt = 1;
        Job current = job;
        while (!isDone(current)) {
            Retries.sleep(
                    pollSchedule.backoffMs(attempt++),
                    "Interrupted while waiting for BigQuery job " + name);
            // Deliberately not Job#reload(): the same request, minus its
            // throw-if-the-job-carries-an-error behaviour, which routed a load that failed while
            // being polled past the message awaitJob composes (ADR-0018). Do not simplify it back.
            current = getJob(current.getJobId(), what);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /**
     * Whether {@code job}, which {@link #findNotFailed(String, String, String)} returned and so has
     * not failed, is done; {@code false} for {@code null}, which means no job.
     */
    private static boolean succeeded(@Nullable Job job) {
        return job != null && isDone(job);
    }

    /**
     * Returns the first job under {@code baseJobId} or one of its retry ids that has not failed, or
     * {@code null}. Retry ids are taken in order, past failed ones, so the first absent id ends the
     * search.
     */
    @Nullable
    private Job findNotFailed(String baseJobId, @Nullable String jobProject, String jobLocation)
            throws IOException {
        for (int probe = 0; ; probe++) {
            Job existing =
                    getJob(
                            jobId(retryJobName(baseJobId, probe), jobProject, jobLocation),
                            "looking for a previous attempt's job");
            if (existing == null) {
                return null;
            }
            if (!isFailed(existing.getStatus())) {
                return existing;
            }
        }
    }

    /**
     * Re-attaches to a job under {@code equivalentBaseJobId} or one of its retry ids that has not
     * failed, which ran the same copy under the id an earlier attempt gave it. Retry ids are taken
     * in order, past failed ones, so the first absent id ends the search.
     *
     * @return whether such a job was found and stands for this one
     */
    private boolean attachToEquivalent(
            String baseJobId,
            String equivalentBaseJobId,
            @Nullable String jobProject,
            String jobLocation)
            throws IOException {
        Job existing = findNotFailed(equivalentBaseJobId, jobProject, jobLocation);
        if (existing == null) {
            return false;
        }
        sharedJobs.jobs.put(baseJobId, existing);
        JobStatus status = existing.getStatus();
        LOG.info(
                "Re-attached to BigQuery job {}, which stands for {}, from a previous attempt"
                        + " (state {})",
                existing.getJobId().getJob(),
                baseJobId,
                status == null ? "unknown" : status.getState());
        return true;
    }

    /** The id the {@code probe}-th attempt at a deterministic job submits under. */
    private static String retryJobName(String baseJobId, int probe) {
        return probe == 0 ? baseJobId : baseJobId + "-r" + probe;
    }

    /**
     * The id one attempt at a job is created and looked up under: in {@code jobProject} when given,
     * otherwise in the client's own project, and always in {@code jobLocation}.
     */
    private static JobId jobId(String jobName, @Nullable String jobProject, String jobLocation) {
        JobId.Builder id = JobId.newBuilder().setJob(jobName).setLocation(jobLocation);
        if (jobProject != null) {
            id.setProject(jobProject);
        }
        return id.build();
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
            @Nullable String jobProject,
            String what,
            @Nullable String equivalentBaseJobId)
            throws IOException {
        String jobLocation = jobLocation(destination);
        if (equivalentBaseJobId != null
                && attachToEquivalent(baseJobId, equivalentBaseJobId, jobProject, jobLocation)) {
            return;
        }
        for (int probe = 0; ; probe++) {
            String jobName = retryJobName(baseJobId, probe);
            JobId jobId = jobId(jobName, jobProject, jobLocation);
            Job existing = getJob(jobId, "looking for a previous attempt's job");
            if (existing == null) {
                Job submitted = create(jobId, configuration);
                JobStatus submittedStatus = submitted.getStatus();
                if (isFailed(submittedStatus)) {
                    // create lost its race to a zombie that had itself already failed, whose id
                    // is as unusable as one the probe above found failed. A job this submission
                    // got back from its own insert does not get here, since create throws for it,
                    // unless the insert's response was lost and the SDK's retry was answered 409:
                    // create's conflict lookup then hands the job back here, and its id is probed
                    // past within the attempt like any other failed id.
                    probePastFailedJob(jobName, baseJobId, submittedStatus, probe);
                    continue;
                }
                sharedJobs.jobs.put(baseJobId, submitted);
                LOG.info("Submitted BigQuery job {}: {}", jobName, what);
                return;
            }
            JobStatus status = existing.getStatus();
            if (isFailed(status)) {
                probePastFailedJob(jobName, baseJobId, status, probe);
                continue;
            }
            sharedJobs.jobs.put(baseJobId, existing);
            LOG.info(
                    "Re-attached to BigQuery job {} from a previous attempt (state {})",
                    jobName,
                    status == null ? "unknown" : status.getState());
            return;
        }
    }

    /**
     * A failed job's id cannot be reused, and attaching to one would fail the commit with a failure
     * that predates this attempt; only a fresh id can still load the data. Logs the same warning
     * however the failed job was met — found by the probe, or handed back by {@code create} after a
     * conflict. Callers have established {@code isFailed(status)}, which is what makes the error
     * dereference safe.
     */
    private static void probePastFailedJob(
            String jobName, String baseJobId, JobStatus status, int probe) {
        LOG.warn(
                "BigQuery job {} from a previous attempt failed ({}); probing {}-r{}",
                jobName,
                status.getError(),
                baseJobId,
                probe + 1);
    }

    /**
     * Whether the job has finished and reports an error — the one state the runner must move past
     * to a fresh {@code -rN} id, since a failed id can never be reused (see the class javadoc), and
     * the state in which {@code create} fails the attempt when its own submission returns it.
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
        Job created;
        try {
            created = client().create(JobInfo.of(jobId, configuration));
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
        JobStatus status = created.getStatus();
        if (isFailed(status)) {
            // This submission's own job, refused as it was submitted: a load under CREATE_NEVER
            // into a table dropped since the commit reconciled it answers this way, measured on
            // 2026-11-17 (#2181). The same request under the next retry id would be refused the
            // same way, so probing on would submit one refused job after another for as long as
            // the attempt lasted. Failing the attempt submits one; the retried commit can remove
            // the cause, as its reconciliation does by creating the table, and probes past it.
            throw new IOException(
                    "BigQuery job "
                            + jobId.getJob()
                            + " was refused as it was submitted: "
                            + status.getError());
        }
        return created;
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
            // The metadata view: the location is all this reads, and the default view also returns
            // the access controls, which fine-grained dataset access control guards with
            // bigquery.datasets.getIamPolicy on top of bigquery.datasets.get.
            dataset =
                    client().getDataset(
                                    datasetId,
                                    BigQuery.DatasetOption.datasetView(
                                            BigQuery.DatasetView.METADATA));
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
