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

package com.google.cloud.bigquery;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Mints {@link Job} and {@link JobStatus} values for tests, from inside the package that owns them.
 *
 * <p><b>Why this class is in {@code com.google.cloud.bigquery}.</b> A test driving a class that
 * reads what {@link BigQuery#getJob} and {@link BigQuery#create} return has to produce those
 * values, and neither type can be built from outside this package: {@code Job(BigQuery,
 * JobInfo.BuilderImpl)}, both {@link Job.Builder} constructors, {@code Job.Builder#setStatus},
 * {@code Job.fromPb} and both {@link JobStatus} constructors are package-private, and {@code Job} —
 * though not {@code final} — has no constructor a subclass elsewhere could call. Declaring the
 * package is the only reach that does not require either a mocking framework (this project has
 * none, deliberately) or an abstraction over {@code Job} in production code. The record is {@code
 * docs/adr/0067}.
 *
 * <p>The coupling is to package-private members of a pinned dependency, so a {@code
 * google-cloud-bigquery} release that moves any of them breaks this file at <em>compile</em> time,
 * in a test — not silently at runtime. Verified against 2.68.0, the version {@code libraries-bom}
 * resolves today.
 */
public final class TestJobs {

    /**
     * The configuration every minted job carries.
     *
     * <p>Which configuration it is never matters: nothing a caller drives reads it back, and the
     * one method that inspects a job's configuration ({@code Job#checkNotDryRun}) only looks for a
     * {@link QueryJobConfiguration}. A copy job is the cheapest thing to build that is not one.
     */
    private static final JobConfiguration ANY_CONFIGURATION =
            CopyJobConfiguration.of(
                    TableId.of("test-project", "test_dataset", "destination"),
                    TableId.of("test-project", "test_dataset", "source"));

    private TestJobs() {}

    /**
     * Returns a status in the given state, carrying no error.
     *
     * @param state the job state
     * @return the status
     */
    public static JobStatus status(JobStatus.State state) {
        // Through the three-argument constructor, not JobStatus(State): one package-private member
        // fewer to reach is one fewer for an SDK release to move.
        return status(state, null, null);
    }

    /**
     * Returns a status in the given state, carrying the given errors.
     *
     * <p>The state and the error are independent on purpose: the service pairs an error result with
     * {@code DONE}, but a caller inspecting only one of the two is exactly what a test wants to be
     * able to catch.
     *
     * @param state the job state
     * @param error the error result, or {@code null} for none
     * @param executionErrors the execution errors, or {@code null} if the service reported none
     * @return the status
     */
    public static JobStatus status(
            JobStatus.State state,
            @Nullable BigQueryError error,
            @Nullable List<BigQueryError> executionErrors) {
        return new JobStatus(state, error, executionErrors);
    }

    /**
     * Returns a job bound to the given client, as {@link BigQuery#getJob} and {@link
     * BigQuery#create} return one.
     *
     * @param bigquery the client the job is bound to
     * @param jobId the job id the job reports
     * @param status the status the job reports, or {@code null} to model a response that carried no
     *     status
     * @return the job
     */
    public static Job job(BigQuery bigquery, JobId jobId, @Nullable JobStatus status) {
        return new Job.Builder(bigquery, ANY_CONFIGURATION)
                .setJobId(jobId)
                .setStatus(status)
                .build();
    }
}
