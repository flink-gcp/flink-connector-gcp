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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * Executes BigQuery load and copy jobs for the FILE_LOADS orchestration.
 *
 * <p>Submission and completion are split so the orchestrator can submit every job first — BigQuery
 * runs them concurrently server-side — and only then wait, without managing threads itself.
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
     * @param jobId the deterministic job id
     * @param spec the copy job
     * @throws IOException if the job cannot be submitted
     */
    void submitCopy(String jobId, CopyJobSpec spec) throws IOException;

    /**
     * Waits for a previously submitted job to complete.
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
