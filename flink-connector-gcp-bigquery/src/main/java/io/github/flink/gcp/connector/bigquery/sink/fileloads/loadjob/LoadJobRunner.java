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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * Executes BigQuery load, copy and terminal query jobs for the FILE_LOADS orchestration.
 *
 * <p>Submission and completion are split so the orchestrator can submit an independent wave first,
 * let BigQuery run it concurrently server-side, and then wait through its bounded destination
 * workers.
 *
 * <p>Implementations own the exactly-once mechanics behind a caller-chosen <em>deterministic</em>
 * job id: re-submitting an id that already ran must re-attach to (or skip after) the existing
 * BigQuery job instead of loading the data again.
 *
 * <p>Abstracts the BigQuery REST client so orchestration logic is unit-testable.
 */
@Internal
public interface LoadJobRunner {

    /**
     * Submits a load job, or re-attaches to the BigQuery job a previous run of the same {@code
     * jobId} left behind.
     *
     * @param jobId the deterministic job id
     * @param spec the load job
     * @throws IOException if the job cannot be submitted
     */
    void submitLoad(String jobId, LoadJobSpec spec) throws IOException;

    /**
     * Submits a copy job, or re-attaches to the BigQuery job a previous run of the same {@code
     * jobId} left behind.
     *
     * <p>When the spec names an equivalent job id ({@link CopyJobSpec#getEquivalentJobId()}), a job
     * under that id, or one of its retry ids, that has not failed stands for this copy: the runner
     * re-attaches to it instead of copying the rows again.
     *
     * @param jobId the deterministic job id
     * @param spec the copy job
     * @throws IOException if the job cannot be submitted
     */
    void submitCopy(String jobId, CopyJobSpec spec) throws IOException;

    /**
     * Returns whether this copy already succeeded, under its id, one of its retry ids, its
     * equivalent id ({@link CopyJobSpec#getEquivalentJobId()}) or one of that id's retry ids. A
     * commit asks about the final copy of every destination it routes through temporary tables,
     * except under {@code WRITE_TRUNCATE_DATA}, whose last job is a terminal query it asks {@link
     * #querySucceeded} about instead, before its loads and before it prepares any table: a
     * destination whose final copy succeeded needs none of them, and preparing them anew after the
     * commit deleted them would load staged files the commit deleted too. A copy still pending or
     * running is waited for before the answer, and a failed one is not a success: an unfinished
     * copy may read a temporary table the commit is about to create anew after it expired, and
     * would then succeed with that table's rows missing. A commit also asks about each copy that
     * fills a temporary table it prepares, once it has prepared that table and before it prepares
     * any table the copy reads, for the same wait, and does not fill those tables again when the
     * copy succeeded.
     *
     * <p>The default answers {@code false}, for a runner that cannot see previous attempts' jobs.
     * That is not a harmless shortcut: a commit retried after its final copy succeeded then
     * prepares and loads its temporary tables anew, which fails on every attempt once the staged
     * files are gone. A runner used in production overrides it.
     *
     * @param jobId the deterministic job id
     * @param spec the copy job
     * @return whether such a job succeeded
     * @throws IOException if the jobs cannot be looked up
     */
    default boolean copySucceeded(String jobId, CopyJobSpec spec) throws IOException {
        return false;
    }

    /**
     * Returns whether this terminal query already succeeded, under its id or one of its retry ids.
     * A {@code WRITE_TRUNCATE_DATA} commit asks for every destination it routes through temporary
     * tables before it prepares any table, as other dispositions ask {@link #copySucceeded}: a
     * destination whose terminal query succeeded needs none of its temporary tables, and preparing
     * them anew after the commit deleted them would load staged files the commit deleted too. A
     * query still pending or running is waited for before the answer, for the reason {@link
     * #copySucceeded(String, CopyJobSpec)} gives, and a failed one is not a success.
     *
     * <p>The default answers {@code false}, for a runner that cannot see previous attempts' jobs,
     * with the consequence {@link #copySucceeded(String, CopyJobSpec)} describes. A runner used in
     * production overrides it.
     *
     * @param jobId the deterministic job id
     * @param spec the query job
     * @return whether such a job succeeded
     * @throws IOException if the jobs cannot be looked up
     */
    default boolean querySucceeded(String jobId, QueryJobSpec spec) throws IOException {
        return false;
    }

    /**
     * Submits a terminal query job, or re-attaches to the BigQuery job a previous run of the same
     * {@code jobId} left behind.
     *
     * @param jobId the deterministic job id
     * @param spec the query job
     * @throws IOException if the job cannot be submitted
     */
    void submitQuery(String jobId, QueryJobSpec spec) throws IOException;

    /**
     * Waits for a previously submitted job to complete.
     *
     * <p>Every runner built for one concurrent committer must share its submitted-job handles, so a
     * worker may wait for a job another worker submitted in the preceding global wave phase.
     *
     * @param jobId the deterministic job id passed at submission
     * @throws IOException if the job failed
     */
    void awaitJob(String jobId) throws IOException;

    /**
     * Deletes a temporary table, best-effort: failures are logged and swallowed.
     *
     * @param table the table to delete
     */
    void deleteTable(TableDestination table);
}
