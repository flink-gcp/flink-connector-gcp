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

package io.github.flink.gcp.connector.bigquery;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers itself, in one place so that this file is the
 * connector's inventory: what it reports can be read here without opening a writer.
 *
 * <p>Each connector has one of these, and comparing them is how the repository's metric naming
 * convention is held across connectors — a name that means the same thing in two connectors should
 * be spelled the same way, and a diff of these files is what shows it. The convention itself (a
 * counter names the event, a gauge names the state, and neither takes Flink's {@code num} prefix)
 * is recorded in the base module's detailed agent guidance.
 *
 * <p>What is <em>not</em> here: Flink's standard sink names, which come from {@code
 * SinkWriterMetricGroup} accessors rather than from a name, and the subgroup leaves {@code
 * base.metrics} registers on this connector's behalf ({@code errorClass.CODE.errors}, {@code
 * destination.TABLE.recordsSend}). The user-facing meaning of each name is on the connector's
 * documentation page, not duplicated here.
 */
@Internal
public final class BigQueryMetricNames {

    // Registered by the STORAGE_API_AT_LEAST_ONCE writer (DefaultStreamWriterMetrics).
    public static final String IN_FLIGHT_BATCHES = "inFlightBatches";
    public static final String TABLES_CREATED = "tablesCreated";
    public static final String SCHEMA_RECONCILIATIONS = "schemaReconciliations";

    // Registered by the STORAGE_API_EXACTLY_ONCE writer (BufferedStreamWriterMetrics).
    public static final String IN_FLIGHT_APPENDS = "inFlightAppends";

    // Registered by both Storage Write API writers.
    public static final String APPEND_RETRIES = "appendRetries";

    // Registered by the FILE_LOADS writer (FileLoadsWriterMetrics) and its committer.
    public static final String FILES_STAGED = "filesStaged";
    public static final String DESTINATION_ACTIVATIONS = "destinationActivations";
    public static final String CAPACITY_EVICTIONS = "capacityEvictions";
    public static final String IDLE_EVICTIONS = "idleEvictions";
    public static final String PENDING_FILES = "pendingFiles";
    public static final String LOAD_JOBS_SUBMITTED = "loadJobsSubmitted";
    public static final String QUEUED_COMMIT_DESTINATIONS = "queuedCommitDestinations";
    public static final String ACTIVE_COMMIT_DESTINATIONS = "activeCommitDestinations";
    public static final String CURRENT_COMMIT_DURATION_MILLIS = "currentCommitDurationMillis";
    public static final String LAST_COMMIT_DURATION_MILLIS = "lastCommitDurationMillis";

    // Registered by the default-stream and FILE_LOADS writers, which hold per-destination state.
    public static final String OPEN_DESTINATIONS = "openDestinations";

    // Registered by all three writers and by the source's reader. Writers count a record whose
    // serializer returned null; the source counts an input row whose deserializer emitted nothing.
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    // Registered by the source's reader (BigQuerySourceReaderMetrics). Rows and bytes are what the
    // Storage Read API bills, so these are the two numbers a cost question is answered with.
    // A zero-to-many deserializer makes rowsRead independent of Flink's own numRecordsIn.
    public static final String ROWS_READ = "rowsRead";
    public static final String BYTES_READ = "bytesRead";

    // Registered by the source's reader. Counts the client library's own ReadRows retries, which
    // nothing else reports: a stream that keeps failing and resuming is making progress, so it
    // never reaches retryMaxAttempts and never fails the job — it just reads slowly. A plain noun
    // phrase for the same reason appendRetries is one; there is no actor to name.
    public static final String READ_RETRIES = "readRetries";

    // Registered by the source's split enumerator. Counters rather than an assigned-splits gauge:
    // a gauge would need a ledger of which subtask holds what, and not keeping one is the whole
    // design of that enumerator. readSessionsCreated is 1 for a job that started and 0 for one that
    // restored an existing session; any other value means the restore guard failed and the job is
    // reading a second snapshot of the table.
    public static final String SPLITS_ASSIGNED = "splitsAssigned";
    public static final String SPLITS_RETURNED = "splitsReturned";
    public static final String READ_SESSIONS_CREATED = "readSessionsCreated";

    // Registered by the source's split enumerator, by every source and not only one reading a
    // query, so a zero reads as "this source named a table" rather than "nothing registered it".
    // Above 1 means what readSessionsCreated above 1 means — the guard that plans once did not
    // hold — and here that also means the query has been billed more than once.
    public static final String QUERY_JOBS_SUBMITTED = "queryJobsSubmitted";

    // A re-plan that found a previous attempt's query job under the deterministic reuse id and
    // adopted it instead of submitting a new one. Counted apart from queryJobsSubmitted so that
    // counter keeps meaning "the query was billed": a reuse is precisely a billing that did not
    // happen.
    public static final String QUERY_JOBS_REATTACHED = "queryJobsReattached";

    private BigQueryMetricNames() {}
}
