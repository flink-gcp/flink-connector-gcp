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

package com.google.cloud.bigquery;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mints {@link Job}, {@link JobStatus}, {@link Table} and {@link Dataset} values for tests, from
 * inside the package that owns them.
 *
 * <p><b>Why this class is in {@code com.google.cloud.bigquery}.</b> A test driving a class that
 * reads what {@link BigQuery#getJob}, {@link BigQuery#create}, {@link BigQuery#getTable} and {@link
 * BigQuery#getDataset} return has to produce those values, and none of the types can be built from
 * outside this package: {@code Job(BigQuery, JobInfo.BuilderImpl)}, both {@link Job.Builder}
 * constructors, {@code Job.Builder#setStatus}, {@code Job.fromPb} and both {@link JobStatus}
 * constructors are package-private, and {@code Job} — though not {@code final} — has no constructor
 * a subclass elsewhere could call; {@link Table} and {@link Dataset} are the same shape ({@link
 * #table(BigQuery, TableId)} and {@link #dataset(BigQuery, DatasetId, String)} name their reaches).
 * Declaring the package is the only reach that does not require either a mocking framework (this
 * project has none, deliberately) or an abstraction over the vendor's types in production code. The
 * record is {@code docs/adr/0067}.
 *
 * <p>The coupling is to package-private members of a pinned dependency, so a {@code
 * google-cloud-bigquery} release that moves any of them breaks this file at <em>compile</em> time,
 * in a test — not silently at runtime. Verified against 2.68.0, the version {@code libraries-bom}
 * resolves today.
 */
public final class TestJobs {

    /**
     * The configuration a job minted by {@link #job(BigQuery, JobId, JobStatus)} carries.
     *
     * <p>Which configuration it is does not matter to that overload's callers: the load runner
     * never reads one back, and the one method that inspects a job's configuration ({@code
     * Job#checkNotDryRun}) only looks for a {@link QueryJobConfiguration}. A copy job is the
     * cheapest thing to build that is not one. A caller that <em>does</em> read it back — the query
     * runner takes its result table off the completed job — passes its own through the
     * four-argument overload.
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
        return job(bigquery, jobId, status, ANY_CONFIGURATION);
    }

    /**
     * Returns a job carrying the given configuration, for a caller that reads one back.
     *
     * @param bigquery the client the job is bound to
     * @param jobId the job id the job reports
     * @param status the status the job reports, or {@code null} to model a response that carried no
     *     status
     * @param configuration the configuration the job reports — for a query job that has run, this
     *     is where BigQuery reports the table the result landed in — or {@code null} for the
     *     placeholder a caller that never reads one back gets
     * @return the job
     */
    public static Job job(
            BigQuery bigquery,
            JobId jobId,
            @Nullable JobStatus status,
            @Nullable JobConfiguration configuration) {
        return job(bigquery, jobId, status, configuration, null);
    }

    /**
     * Returns a job that also reports a creation time, for a caller that reads the job's age.
     *
     * <p>Two more package-private reaches than the overloads above, both verified against 2.68.0:
     * {@code Job.Builder#setStatistics}, and {@code JobStatistics.CopyStatistics#fromPb} — the one
     * road to a {@link JobStatistics} value, whose builders' constructors are all {@code private}.
     * The subtype does not matter to a caller reading {@link JobStatistics#getCreationTime}, so it
     * matches {@link #ANY_CONFIGURATION}'s copy job.
     *
     * @param bigquery the client the job is bound to
     * @param jobId the job id the job reports
     * @param status the status the job reports, or {@code null} to model a response that carried no
     *     status
     * @param configuration the configuration the job reports, or {@code null} for the placeholder
     * @param creationTimeMillis the creation time the job's statistics report, or {@code null} to
     *     model a response that carried no statistics
     * @return the job
     */
    public static Job job(
            BigQuery bigquery,
            JobId jobId,
            @Nullable JobStatus status,
            @Nullable JobConfiguration configuration,
            @Nullable Long creationTimeMillis) {
        Job.Builder job =
                new Job.Builder(bigquery, configuration == null ? ANY_CONFIGURATION : configuration)
                        .setJobId(jobId)
                        .setStatus(status);
        if (creationTimeMillis != null) {
            job.setStatistics(
                    JobStatistics.CopyStatistics.fromPb(
                            new com.google.api.services.bigquery.model.JobStatistics()
                                    .setCreationTime(creationTimeMillis)));
        }
        return job.build();
    }

    /**
     * Returns a table bound to the given client, as {@link BigQuery#getTable} returns one.
     *
     * <p>One more package-private reach, verified against 2.68.0: the {@code
     * Table.Builder(BigQuery, TableId, TableDefinition)} constructor. It is the narrowest road to a
     * {@link Table} — the class's own constructor takes the package-private {@code
     * TableInfo.BuilderImpl}, {@code Table.fromPb} would reach a second member <em>and</em> drag in
     * the wire model, and the public {@code TableInfo.of(...)} factories produce a {@link
     * TableInfo}, never a {@code Table}. The definition is an empty standard table because a caller
     * asking only "is it there" never reads one.
     *
     * @param bigquery the client the table is bound to; its {@code getOptions()} must answer, which
     *     the stub's do
     * @param tableId the table id the table reports
     * @return the table
     */
    public static Table table(BigQuery bigquery, TableId tableId) {
        return new Table.Builder(bigquery, tableId, StandardTableDefinition.of(Schema.of()))
                .build();
    }

    /** Returns a table carrying the metadata needed by conditional table-update tests. */
    public static Table table(
            BigQuery bigquery, TableId tableId, String etag, Map<String, String> labels) {
        return new Table.Builder(bigquery, tableId, StandardTableDefinition.of(Schema.of()))
                .setEtag(etag)
                .setLabels(labels)
                .build();
    }

    /**
     * Returns a dataset bound to the given client, reporting the given location, as {@link
     * BigQuery#getDataset(DatasetId, BigQuery.DatasetOption...)} returns one.
     *
     * <p>One package-private reach, verified against 2.68.0: the {@link Dataset.Builder} used here
     * — {@code Dataset} has no public constructor or factory, and {@code DatasetInfo.newBuilder}
     * builds the info type, not the {@code Dataset} the client returns. {@code setLocation} and
     * {@code build()} on that builder are public.
     *
     * @param bigquery the client the dataset is bound to
     * @param datasetId the dataset id
     * @param location the location the dataset reports
     * @return the dataset
     */
    public static Dataset dataset(BigQuery bigquery, DatasetId datasetId, String location) {
        return new Dataset.Builder(bigquery, datasetId).setLocation(location).build();
    }
}
