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
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import io.github.flink.gcp.connector.bigquery.sink.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
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
 * {@link LoadJobRunner} over the BigQuery REST client with application-default credentials.
 *
 * <p><b>Exactly-once via deterministic job ids.</b> BigQuery job ids are single-use per project, so
 * a deterministic id doubles as an idempotency key: before submitting, the runner looks the id up
 * and re-attaches to a job an earlier attempt of the same Flink run already created — a completed
 * job is not re-run, a still-running job is awaited — instead of loading the same files twice. Only
 * when the existing job <em>failed</em> does the runner probe {@code -r1}, {@code -r2}, ... (a
 * bounded number of times, since ids of failed jobs cannot be reused) for a fresh deterministic id.
 *
 * <p>Completion is polled with capped exponential backoff and effectively no attempt bound: batch
 * load jobs may legitimately run long, and overall timeouts are the Flink job's to enforce.
 */
@Internal
public final class BigQueryLoadJobRunner implements LoadJobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobRunner.class);

    /** How many {@code -rN} ids to probe past a failed deterministic id before giving up. */
    private static final int MAX_RETRY_PROBES = 5;

    private static final RetrySchedule POLL_SCHEDULE =
            new RetrySchedule(1_000, 30_000, Integer.MAX_VALUE, 0.25);

    @Nullable private final String location;
    private final Map<String, Job> activeJobs = new HashMap<>();
    private BigQuery client;

    /**
     * Creates a runner.
     *
     * @param location the BigQuery location jobs run in, or {@code null} for the API default
     */
    public BigQueryLoadJobRunner(@Nullable String location) {
        this.location = location;
    }

    @VisibleForTesting
    BigQueryLoadJobRunner(BigQuery client, @Nullable String location) {
        this.client = client;
        this.location = location;
    }

    @Override
    public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
        LoadJobConfiguration.Builder load =
                LoadJobConfiguration.newBuilder(
                                BigQueryTableAdmin.toTableId(spec.getDestination()),
                                spec.getSourceUris())
                        .setFormatOptions(FormatOptions.avro())
                        .setUseAvroLogicalTypes(true)
                        .setSchema(spec.getSchema())
                        .setCreateDisposition(spec.getCreateDisposition())
                        .setWriteDisposition(spec.getWriteDisposition());
        if (!spec.getSchemaUpdateOptions().isEmpty()) {
            load.setSchemaUpdateOptions(spec.getSchemaUpdateOptions());
        }
        if (spec.getTimePartitioning() != null) {
            load.setTimePartitioning(spec.getTimePartitioning());
        }
        if (spec.getClustering() != null) {
            load.setClustering(spec.getClustering());
        }
        submitOrAttach(jobId, load.build(), spec.toString());
    }

    @Override
    public void submitCopy(String jobId, CopyJobSpec spec) throws IOException {
        CopyJobConfiguration copy =
                CopyJobConfiguration.newBuilder(
                                BigQueryTableAdmin.toTableId(spec.getDestination()),
                                spec.getSourceTables().stream()
                                        .map(BigQueryTableAdmin::toTableId)
                                        .collect(Collectors.toList()))
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                        .setWriteDisposition(spec.getWriteDisposition())
                        .build();
        submitOrAttach(jobId, copy, spec.toString());
    }

    @Override
    public void awaitJob(String jobId) throws IOException {
        Job job = activeJobs.remove(jobId);
        Preconditions.checkState(job != null, "Job %s was never submitted", jobId);
        int attempt = 1;
        while (!isDone(job)) {
            sleep(POLL_SCHEDULE.backoffMs(attempt++), jobId);
            Job reloaded = job.reload();
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
        } catch (RuntimeException e) {
            LOG.warn("Failed to delete temporary table {}", table, e);
        }
    }

    private void submitOrAttach(String baseJobId, JobConfiguration configuration, String what)
            throws IOException {
        String lastError = null;
        for (int probe = 0; probe <= MAX_RETRY_PROBES; probe++) {
            String jobName = probe == 0 ? baseJobId : baseJobId + "-r" + probe;
            JobId jobId = toJobId(jobName);
            Job existing = client().getJob(jobId);
            if (existing == null) {
                activeJobs.put(baseJobId, create(jobId, configuration));
                LOG.info("Submitted BigQuery job {}: {}", jobName, what);
                return;
            }
            JobStatus status = existing.getStatus();
            if (status != null
                    && status.getState() == JobStatus.State.DONE
                    && status.getError() != null) {
                // A failed job's id cannot be reused; probe the next deterministic id.
                lastError = status.getError().toString();
                LOG.warn(
                        "BigQuery job {} from a previous attempt failed ({}); probing {}-r{}",
                        jobName,
                        lastError,
                        baseJobId,
                        probe + 1);
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

    private Job create(JobId jobId, JobConfiguration configuration) throws IOException {
        try {
            return client().create(JobInfo.of(jobId, configuration));
        } catch (BigQueryException e) {
            if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
                // Lost a race against a zombie of a previous attempt; attach to its job.
                Job existing = client().getJob(jobId);
                if (existing != null) {
                    return existing;
                }
            }
            throw new IOException("Failed to submit BigQuery job " + jobId.getJob(), e);
        }
    }

    private static boolean isDone(Job job) {
        JobStatus status = job.getStatus();
        return status != null && status.getState() == JobStatus.State.DONE;
    }

    private static void sleep(long millis, String jobId) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for BigQuery job " + jobId, e);
        }
    }

    private JobId toJobId(String jobName) {
        JobId.Builder jobId = JobId.newBuilder().setJob(jobName);
        if (location != null) {
            jobId.setLocation(location);
        }
        return jobId.build();
    }

    private BigQuery client() {
        if (client == null) {
            client = BigQueryOptions.getDefaultInstance().getService();
        }
        return client;
    }
}
